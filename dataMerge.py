import pandas as pd
from pathlib import Path

# === 1. Input Paths ===
data_dir = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard")

file_tpaccount = data_dir / "Lv_tpaccount.xlsx"
file_transaction = data_dir / "Lv_monetarytransaction.xlsx"
file_account = data_dir / "Account.xlsx"
output_file = data_dir / "merged_clean_output.xlsx"

# === 2. Read Excel Files ===
df1 = pd.read_excel(file_tpaccount)
df2 = pd.read_excel(file_transaction)
df3 = pd.read_excel(file_account)

# === 3. Normalise AccountID column names ===
df1 = df1.rename(columns={'lv_accountid': 'AccountID'})
df2 = df2.rename(columns={'lv_accountid': 'AccountID'})

if 'AccountID' not in df3.columns:
    for col in df3.columns:
        if col.strip().lower() == 'accountid':
            print(f"Found similar column: '{col}'. Renaming to 'AccountID'.")
            df3 = df3.rename(columns={col: 'AccountID'})
            break
    else:
        raise KeyError("No column matching 'AccountID' found in Account.xlsx")

# === 4. Merge DataFrames ===
merged_12 = pd.merge(df1, df2, on='AccountID', how='outer')
final_df = pd.merge(merged_12, df3, on='AccountID', how='outer', suffixes=('_trans', '_account'))

# === 5. Define columns to keep ===
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

# === 6. Handle suffix columns ===
existing_columns = []
missing_columns = []

for col in keep_columns:
    if col in final_df.columns:
        existing_columns.append(col)
    else:
        # Try suffixes
        variants = [f"{col}_x", f"{col}_y", f"{col}_trans", f"{col}_account"]
        for variant in variants:
            if variant in final_df.columns:
                existing_columns.append(variant)
                print(f"[OK] Using '{variant}' for '{col}'")
                break
        else:
            missing_columns.append(col)

print(f"\n[OK] Found {len(existing_columns)} of {len(keep_columns)} requested columns")
if missing_columns:
    print(f"[ERROR] Missing columns: {missing_columns}")

# === 7. Filter to existing columns only ===
final_filtered_df = final_df[existing_columns]

# === 8. Rename columns back to base names ===
rename_dict = {}
for col in existing_columns:
    for suffix in ['_x', '_y', '_trans', '_account']:
        if col.endswith(suffix):
            base = col.rsplit('_', 1)[0]
            if base not in rename_dict.values():
                rename_dict[col] = base

final_filtered_df = final_filtered_df.rename(columns=rename_dict)

# === 9. Save final result to Excel ===
final_filtered_df.to_excel(output_file, index=False)
print(f"\n[DONE] Final merged Excel file saved to: {output_file}")
print(f"[DONE] Final shape: {final_filtered_df.shape}")
