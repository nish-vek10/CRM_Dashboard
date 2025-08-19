import pandas as pd
from pathlib import Path

# === 1. Input Paths ===
data_dir = Path(r"C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard")

file_tpaccount = data_dir / "Lv_tpaccount.xlsx"
file_transaction = data_dir / "Lv_monetarytransaction.xlsx"
file_account = data_dir / "Account.xlsx"
output_file = data_dir / "merged_clean_outputNEW.xlsx"

# === 2. Read Excel Files ===
df_tp = pd.read_excel(file_tpaccount)
df_tx = pd.read_excel(file_transaction)
df_ac = pd.read_excel(file_account)

# === 3. Normalize keys to strings (avoid 12345.0 vs "12345") ===
def norm_key(series):
    # Convert to string, strip .0 when the value is numeric-looking
    def _clean(v):
        if pd.isna(v):
            return None
        s = str(v).strip()
        # remove trailing .0 if it looks like a floatified int
        if s.endswith(".0") and s.replace(".0", "").isdigit():
            return s[:-2]
        return s
    return series.apply(_clean)

# Align column names for AccountID
if 'lv_accountid' in df_tp.columns:
    df_tp = df_tp.rename(columns={'lv_accountid': 'AccountID'})
if 'lv_accountid' in df_tx.columns:
    df_tx = df_tx.rename(columns={'lv_accountid': 'AccountID'})

# Ensure AccountDF has AccountID
if 'AccountID' not in df_ac.columns:
    for col in df_ac.columns:
        if col.strip().lower() == 'accountid':
            print(f"Found similar column: '{col}'. Renaming to 'AccountID'.")
            df_ac = df_ac.rename(columns={col: 'AccountID'})
            break
    else:
        raise KeyError("No column matching 'AccountID' found in Account.xlsx")

# Normalize AccountID everywhere
for df in (df_tp, df_tx, df_ac):
    if 'AccountID' in df.columns:
        df['AccountID'] = norm_key(df['AccountID'])

# === 4. Quick duplicate diagnostics BEFORE merging ===
def dup_report(df, name):
    if 'AccountID' not in df.columns:
        print(f"[WARN] {name}: no AccountID column")
        return
    counts = df.groupby('AccountID', dropna=True).size()
    mult = counts[counts > 1]
    print(f"[DIAG] {name} rows: {len(df):,} | unique AccountID: {counts.size:,} | dup AccountID: {len(mult):,}")
    if not mult.empty:
        print(f"[DIAG] {name} top dup keys:\n{mult.sort_values(ascending=False).head(10)}")

dup_report(df_tp, "Lv_tpaccount")
dup_report(df_tx, "Lv_monetarytransaction")
dup_report(df_ac, "Account")

# === 5. Reduce transactions to one row per AccountID (LATEST) to prevent many-to-many explosion ===
# Pick timestamp column for sorting
tx_time_col = None
for cand in ["CreatedOn_y", "CreatedOn", "Time", "ModifiedOn_y", "ModifiedOn"]:
    if cand in df_tx.columns:
        tx_time_col = cand
        break

if tx_time_col:
    # Coerce to datetime for reliable sort
    df_tx = df_tx.copy()
    df_tx[tx_time_col] = pd.to_datetime(df_tx[tx_time_col], errors='coerce')
    df_tx = df_tx.sort_values(by=tx_time_col, ascending=False)
else:
    # Fallback: keep file order
    print("[WARN] No time column found in transactions; keeping file order for dedup.")

# Keep first (latest) per AccountID
if 'AccountID' in df_tx.columns:
    df_tx_dedup = df_tx.drop_duplicates(subset=['AccountID'], keep='first').copy()
else:
    df_tx_dedup = df_tx.copy()
print(f"[DIAG] Transactions reduced: {len(df_tx):,} -> {len(df_tx_dedup):,} rows (one per AccountID).")

# === 6. Merge with LEFT joins using Lv_tpaccount as anchor ===
pre_rows = len(df_tp)
merged_12 = pd.merge(df_tp, df_tx_dedup, on='AccountID', how='left', suffixes=('_tp', '_trans'))
after_12 = len(merged_12)
print(f"[MERGE] tp ⟕ tx_dedup: {pre_rows:,} -> {after_12:,} rows (LEFT)")

final_df = pd.merge(merged_12, df_ac, on='AccountID', how='left', suffixes=('', '_account'))
after_123 = len(final_df)
print(f"[MERGE] (tp+tx) ⟕ account: {after_12:,} -> {after_123:,} rows (LEFT)")

# === 7. Columns to keep (as in your list) ===
keep_columns = [
    'lv_maintpaccountidName',
    'Lv_name',
    'lv_countryidName',
    'AccountID',
    'Name',
    'EMailAddress1',
    'CreatedOn_x',
    'Lv_DateOfBirth',
    'lv_DateofFTD',
    'Lv_FirstName',
    'Lv_LastName',
    'Lv_Phone1CountryCode',
    'Lv_Phone1Phone',
    'Lv_SubAffiliate',
    'Lv_Tag1',
    'lv_TimeZone',
    'lv_FTDDateru_Date',
    'lv_LastDepositDate_Date',
    'lv_accountidName',
    'lv_tpaccountidName',
    'lv_transactioncaseidName',
    'Lv_TempName',
    'CreatedOn_y',
    'Lv_Amount',
    'Lv_ApprovedOn',
    'Lv_CardAcquirerReference',
    'Lv_Comment',
    'Lv_FirstTimeDeposit',
    'lv_NetDepositUSDValue',
    'Lv_TransactionApproved',
    'lv_AdditionalInfo'
]

# Because we changed suffixes to (_tp, _trans, _account), we must map variants
existing_columns = []
missing_columns = []

def find_col(base):
    # Try exact
    if base in final_df.columns:
        return base
    # Try previous suffix styles
    variants = [
        f"{base}_tp", f"{base}_trans", f"{base}_account",
        f"{base}_x", f"{base}_y"  # in case original columns persisted
    ]
    for v in variants:
        if v in final_df.columns:
            return v
    return None

for col in keep_columns:
    found = find_col(col)
    if found:
        existing_columns.append(found)
        if found != col:
            print(f"[OK] Using '{found}' for '{col}'")
    else:
        missing_columns.append(col)

print(f"\n[OK] Found {len(existing_columns)} of {len(keep_columns)} requested columns")
if missing_columns:
    print(f"[WARN] Missing columns: {missing_columns}")

# === 8. Slice and rename back to base names ===
final_filtered_df = final_df[existing_columns].copy()

rename_dict = {}
for found in existing_columns:
    # map back to base name by stripping known suffixes
    for suf in ['_tp', '_trans', '_account', '_x', '_y']:
        if found.endswith(suf):
            base = found[: -len(suf)]
            rename_dict[found] = base
            break

if rename_dict:
    final_filtered_df = final_filtered_df.rename(columns=rename_dict)

# === 9. Save final ===
final_filtered_df.to_excel(output_file, index=False)
print(f"\n[DONE] Final merged Excel file saved to: {output_file}")
print(f"[DONE] Final shape: {final_filtered_df.shape}")

# === 10. Post-merge explosion check (sanity) ===
if 'AccountID' in final_filtered_df.columns:
    dup = final_filtered_df['AccountID'].duplicated().sum()
    print(f"[CHECK] Duplicated AccountID in final output: {dup:,}")
else:
    print("[CHECK] No AccountID in final output?!")
