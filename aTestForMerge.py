import os
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional
import numpy as np
import pandas as pd
import requests

# =========================
# CONFIG
# =========================
BASE_DIR = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard")
SAVED_DIR = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\newDATA")

FILE_TP       = BASE_DIR / "Lv_tpaccount.xlsx"
FILE_TX       = BASE_DIR / "Lv_monetarytransaction.xlsx"
FILE_ACCOUNT  = BASE_DIR / "Account.xlsx"

OUT_REPORT_XLSX   = SAVED_DIR / "merge_report.xlsx"
OUT_FULL_XLSX     = SAVED_DIR / "merged_full_results.xlsx"
OUT_DASHBOARD_JSON= SAVED_DIR / "dashboard_ready.json"

# Filters (business logic)
KEEP_TRANSACTIONCASE = "Deposit Approval"
EXCLUDE_TEMP_CONTAINS = "Purchases"

# Optional SiRiX API enrichment (set to True to enable)
ENABLE_SIRIX_API = False
SIRIX_API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
SIRIX_TOKEN   = "t1_a7xeQOJPnfBzuCncH60yjLFu"
SIRIX_TIMEOUT = 12  # seconds
SIRIX_SLEEP   = 0.2 # seconds between calls

# Date columns to convert if present
DATE_COLS = [
    'CreatedOn', 'CreatedOn_y', 'CreatedOn_x',
    'Lv_DateOfBirth', 'lv_DateofFTD',
    'lv_FTDDateru_Date', 'lv_LastDepositDate_Date',
    'Lv_ApprovedOn'
]

# =========================
# UTILS
# =========================
def clean_tp_id(s: Any) -> Optional[str]:
    """
    Normalize TP user id to a clean string (strip, drop .0 if present).
    """
    if pd.isna(s):
        return None
    s = str(s).strip()
    # drop trailing .0 from excel floats
    if s.endswith(".0"):
        s = s[:-2]
    return s

def to_datetime_inplace(df: pd.DataFrame, cols):
    """
    Convert date-like columns to datetime safely.
    """
    for col in cols:
        if col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                valid = df[col] > 1_000_000_000  # treat as ms timestamps
                df[col] = df[col].astype('object')
                df.loc[valid, col] = pd.to_datetime(df.loc[valid, col], unit='ms', errors='coerce')
                df.loc[~valid, col] = pd.NaT
            else:
                df[col] = pd.to_datetime(df[col], errors='coerce')

def pct(n, d) -> str:
    if d == 0:
        return "0.0%"
    return f"{(n/d)*100:,.1f}%"

def extract_plan_fields(row) -> pd.Series:
    try:
        info = json.loads(row.get('lv_AdditionalInfo', '{}') or '{}')
        plan = info.get('name')
        plan_sb = (info.get('challenges') or {}).get('funding')
    except Exception:
        plan = None
        plan_sb = None
    return pd.Series([plan, plan_sb])

def fetch_sirix_bal(user_id: str) -> Dict[str, Optional[float]]:
    """
    Call SiRiX once for Balance/Equity/OpenPnL.
    """
    headers = {
        "Authorization": f"Bearer {SIRIX_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "UserID": str(user_id),
        "GetOpenPositions": False,
        "GetPendingPositions": False,
        "GetClosePositions": False,
        "GetMonetaryTransactions": False,
    }
    try:
        r = requests.post(SIRIX_API_URL, headers=headers, json=payload, timeout=SIRIX_TIMEOUT)
        if r.status_code != 200:
            print(f"[API] {user_id} → HTTP {r.status_code}")
            return {"Balance": None, "Equity": None, "OpenPnL": None}
        data = r.json()
        bal = (data.get("UserData") or {}).get("AccountBalance") or {}
        return {
            "Balance": bal.get("Balance"),
            "Equity": bal.get("Equity"),
            "OpenPnL": bal.get("OpenPnL"),
        }
    except Exception as e:
        print(f"[API] {user_id} → Error: {e}")
        return {"Balance": None, "Equity": None, "OpenPnL": None}

# =========================
# LOAD
# =========================
print("[1/7] Loading Excel files...")
tp = pd.read_excel(FILE_TP)
tx = pd.read_excel(FILE_TX)
acct = pd.read_excel(FILE_ACCOUNT)

print(f"    Lv_tpaccount rows: {len(tp):,}")
print(f"    Lv_monetarytransaction rows: {len(tx):,}")
print(f"    Account rows: {len(acct):,}")

# =========================
# NORMALIZE KEYS
# =========================
print("[2/7] Normalizing keys...")

# CRM Account GUID
if 'AccountID' not in acct.columns:
    # try to find the AccountID column (case-insensitive)
    cand = [c for c in acct.columns if c.strip().lower() == 'accountid']
    if not cand:
        raise KeyError("Account.xlsx must contain AccountID column.")
    acct = acct.rename(columns={cand[0]: 'AccountID'})

# TP: bring AccountID forward
if 'lv_accountid' in tp.columns:
    tp['AccountID'] = tp['lv_accountid']
if 'lv_accountid' in tx.columns:
    tx['AccountID'] = tx['lv_accountid']

# TP user id fields
tp['TP_UserID'] = tp['Lv_name'].apply(clean_tp_id) if 'Lv_name' in tp.columns else None
tx['TP_UserID'] = tx['lv_tpaccountidName'].apply(clean_tp_id) if 'lv_tpaccountidName' in tx.columns else None

# Ensure AccountID string cleanup
for df_ in (tp, tx, acct):
    if 'AccountID' in df_.columns:
        df_['AccountID'] = df_['AccountID'].astype(str).str.strip()

# =========================
# CHECKS: uniqueness & coverage
# =========================
print("[3/7] Running uniqueness & coverage checks...")

report = {}

# Uniqueness
report['Account_AccountID_unique'] = acct['AccountID'].is_unique if 'AccountID' in acct.columns else False
report['TP_TP_UserID_unique'] = tp['TP_UserID'].dropna().is_unique if 'TP_UserID' in tp.columns else False

# Coverage counts
cov_tp_to_acct = tp['AccountID'].isin(acct['AccountID']).sum() if 'AccountID' in tp.columns else 0
cov_tx_to_acct = tx['AccountID'].isin(acct['AccountID']).sum() if 'AccountID' in tx.columns else 0
cov_tx_to_tp   = tx['TP_UserID'].isin(tp['TP_UserID']).sum() if 'TP_UserID' in tx.columns else 0

report['TP→Account coverage'] = f"{cov_tp_to_acct:,}/{len(tp):,} ({pct(cov_tp_to_acct, len(tp))})"
report['Tx→Account coverage'] = f"{cov_tx_to_acct:,}/{len(tx):,} ({pct(cov_tx_to_acct, len(tx))})"
report['Tx→TP coverage']      = f"{cov_tx_to_tp:,}/{len(tx):,} ({pct(cov_tx_to_tp, len(tx))})"

print("    Uniqueness:")
print(f"      Account.AccountID unique? {report['Account_AccountID_unique']}")
print(f"      Lv_tpaccount.TP_UserID unique? {report['TP_TP_UserID_unique']}")
print("    Coverage:")
print(f"      TP→Account: {report['TP→Account coverage']}")
print(f"      Tx→Account: {report['Tx→Account coverage']}")
print(f"      Tx→TP     : {report['Tx→TP coverage']}")

# =========================
# SAFE 2-STAGE JOIN
# =========================
print("[4/7] Performing safe 2-stage join... (transaction grain)")

# Stage A: join TP→Account (person-level enrichment for TP rows)
tp_acct = tp.merge(
    acct, on='AccountID', how='left', suffixes=('_tp', '_acct')
)

# Stage B: bring TP->Account linkage onto transactions via TP_UserID
tx_enriched = tx.merge(
    tp[['TP_UserID', 'AccountID']], on='TP_UserID', how='left'
)

# Coalesce AccountID from tx (x) and tp (y) into a single column
if 'AccountID_x' in tx_enriched.columns and 'AccountID_y' in tx_enriched.columns:
    tx_enriched['AccountID'] = tx_enriched['AccountID_x'].where(
        tx_enriched['AccountID_x'].notna(), tx_enriched['AccountID_y']
    )
    tx_enriched = tx_enriched.drop(columns=['AccountID_x', 'AccountID_y'])
elif 'AccountID_x' in tx_enriched.columns:
    tx_enriched = tx_enriched.rename(columns={'AccountID_x': 'AccountID'})
elif 'AccountID_y' in tx_enriched.columns:
    tx_enriched = tx_enriched.rename(columns={'AccountID_y': 'AccountID'})
else:
    # if neither exists, create empty and let coverage logs show gaps
    tx_enriched['AccountID'] = np.nan

# Optional debug
missing_acctid = tx_enriched['AccountID'].isna().sum()
print(f"    AccountID coverage on transactions: {len(tx_enriched)-missing_acctid:,}/{len(tx_enriched):,} "
      f"({100*(len(tx_enriched)-missing_acctid)/max(1,len(tx_enriched)):.1f}%)")

# Now transactions have both TP_UserID and AccountID (from TP). Merge with Account for person fields
final = tx_enriched.merge(
    acct, on='AccountID', how='left', suffixes=('_tx', '_acct')
)

print(f"    Rows after enrichment (pre-filter): {len(final):,}")

# =========================
# FILTERS (business logic)
# =========================
print("[5/7] Applying business filters...")
before = len(final)
# Only Deposit Approval
if 'lv_transactioncaseidName' in final.columns:
    final = final[final['lv_transactioncaseidName'].astype(str) == KEEP_TRANSACTIONCASE]

# Exclude Purchases in Lv_TempName
if 'Lv_TempName' in final.columns:
    final = final[~final['Lv_TempName'].astype(str).str.contains(EXCLUDE_TEMP_CONTAINS, na=False)]

after = len(final)
print(f"    Kept {after:,}/{before:,} rows after filters.")

# =========================
# PLAN / PLAN_SB extraction
# =========================
if 'lv_AdditionalInfo' in final.columns:
    final[['Plan', 'Plan_SB']] = final.apply(extract_plan_fields, axis=1)
else:
    final['Plan'] = np.nan
    final['Plan_SB'] = np.nan

# =========================
# OPTIONAL: SIRIX API enrichment
# =========================
if ENABLE_SIRIX_API:
    print("[6/7] Enriching with SiRiX balances (this may take time)...")
    balances = []
    for i, row in final.iterrows():
        uid = clean_tp_id(row.get('TP_UserID'))
        if uid:
            res = fetch_sirix_bal(uid)
        else:
            res = {"Balance": None, "Equity": None, "OpenPnL": None}
        balances.append(res)
        time.sleep(SIRIX_SLEEP)

    bal_df = pd.DataFrame(balances)
    final = pd.concat([final.reset_index(drop=True), bal_df], axis=1)
else:
    # placeholders
    final['Balance'] = np.nan
    final['Equity']  = np.nan
    final['OpenPnL'] = np.nan

# =========================
# DATE CONVERSION
# =========================
to_datetime_inplace(final, DATE_COLS)

# =========================
# OUTPUTS
# =========================
print("[7/7] Writing outputs...")

# 1) Full results (all columns) for auditing
final.to_excel(OUT_FULL_XLSX, index=False)
print(f"    [OK] Full XLSX: {OUT_FULL_XLSX}")

# 2) Dashboard JSON (only needed columns, clean names)
# Mapping (as requested)
def pick(x, key):
    return x.get(key) if key in x else None

dash_cols = {
    "Customer Name":       "Name",                      # from Account
    "Customer ID":         "lv_maintpaccountidName",    # from Account
    "Account ID":          "TP_UserID",                 # TP/Lv_name normalized
    "Email":               "EMailAddress1",             # from Account
    "Phone Code":          "Lv_Phone1CountryCode",      # from Account
    "Phone Number":        "Lv_Phone1Phone",            # from Account
    "Country":             "lv_countryidName",          # from Account
    "Affiliate":           "Lv_SubAffiliate",           # from Account
    "Tag":                 "Lv_Tag1",                   # from Account
    "Plan":                "Plan",                      # from AdditionalInfo
    "Plan SB":             "Plan_SB",                   # from AdditionalInfo
    "Balance":             "Balance",                   # from API or placeholder
    "Equity":              "Equity",                    # from API or placeholder
    "OpenPnL":             "OpenPnL"                    # from API or placeholder
}

dash_df = pd.DataFrame({
    out_col: final[in_col] if in_col in final.columns else np.nan
    for out_col, in_col in dash_cols.items()
})

# Keep transaction grain (multiple rows per person is expected here).
dash_df.to_json(OUT_DASHBOARD_JSON, orient="records")
print(f"    [OK] Dashboard JSON: {OUT_DASHBOARD_JSON}")

# 3) Merge report (checks + row counts)
with pd.ExcelWriter(OUT_REPORT_XLSX) as wr:
    # Summary sheet
    summary = pd.DataFrame({
        "Metric": [
            "Account.AccountID unique?",
            "Lv_tpaccount.TP_UserID unique?",
            "TP→Account coverage",
            "Tx→Account coverage",
            "Tx→TP coverage",
            "Rows (TP)",
            "Rows (Tx)",
            "Rows (Account)",
            "Rows after filters (Final)"
        ],
        "Value": [
            str(report['Account_AccountID_unique']),
            str(report['TP_TP_UserID_unique']),
            report['TP→Account coverage'],
            report['Tx→Account coverage'],
            report['Tx→TP coverage'],
            f"{len(tp):,}",
            f"{len(tx):,}",
            f"{len(acct):,}",
            f"{len(final):,}"
        ]
    })
    summary.to_excel(wr, sheet_name="Summary", index=False)

    # Sample top 1000 (all columns) for manual comparison
    sample = final.head(1000)
    sample.to_excel(wr, sheet_name="SampleTop1000_AllCols", index=False)

print("[DONE] Build complete.")
