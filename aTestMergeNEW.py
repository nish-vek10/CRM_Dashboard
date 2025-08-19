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
SAVED_DIR.mkdir(parents=True, exist_ok=True)

FILE_TP       = BASE_DIR / "Lv_tpaccount.xlsx"
FILE_TX       = BASE_DIR / "Lv_monetarytransaction.xlsx"
FILE_ACCOUNT  = BASE_DIR / "Account.xlsx"

OUT_REPORT_XLSX    = SAVED_DIR / "merge_reportNEW.xlsx"
OUT_FULL_XLSX      = SAVED_DIR / "merged_full_resultsNEW.xlsx"
OUT_DASHBOARD_JSON = SAVED_DIR / "merged_data_full_enrichedNEW.json"  # <- feed this to React

# Filters (business logic)
KEEP_TRANSACTIONCASE = "Deposit Approval"
EXCLUDE_TEMP_CONTAINS = "Purchases"  # case-insensitive

# SiRiX API enrichment
ENABLE_SIRIX_API = True
SIRIX_API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
SIRIX_TOKEN   = "t1_a7xeQOJPnfBzuCncH60yjLFu"
SIRIX_TIMEOUT = 12   # seconds
SIRIX_SLEEP   = 0.20 # seconds between calls (throttle)

# Date columns to convert if present
DATE_COLS = [
    "CreatedOn", "CreatedOn_y", "CreatedOn_x",
    "Lv_DateOfBirth", "lv_DateofFTD",
    "lv_FTDDateru_Date", "lv_LastDepositDate_Date",
    "Lv_ApprovedOn"
]

# =========================
# UTILS
# =========================
def clean_tp_id(s: Any) -> Optional[str]:
    """Normalize TP user id to a clean string (strip, drop .0 if present)."""
    if pd.isna(s):
        return None
    s = str(s).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s

def to_datetime_inplace(df: pd.DataFrame, cols):
    """Convert date-like columns to datetime safely."""
    for col in cols:
        if col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                valid = df[col] > 1_000_000_000  # treat as ms timestamps
                df[col] = df[col].astype("object")
                df.loc[valid, col] = pd.to_datetime(df.loc[valid, col], unit="ms", errors="coerce")
                df.loc[~valid, col] = pd.NaT
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce")

def pct(n, d) -> str:
    if d == 0:
        return "0.0%"
    return f"{(n/d)*100:,.1f}%"

def extract_plan_fields(row) -> pd.Series:
    try:
        info_raw = row.get("lv_AdditionalInfo", "{}")
        info = json.loads(info_raw or "{}")
        plan = info.get("name")
        plan_sb = (info.get("challenges") or {}).get("funding")
    except Exception:
        plan = None
        plan_sb = None
    return pd.Series([plan, plan_sb])

def fetch_sirix_bal(user_id: str, *, max_retries: int = 3) -> Dict[str, Optional[float]]:
    """Call SiRiX once for Balance/Equity/OpenPnL with retries and compact logging."""
    # Clean / validate id
    if user_id is None:
        print("[API] Skipping: user_id is None")
        return {"Balance": None, "Equity": None, "OpenPnL": None}

    uid = str(user_id).strip()
    if uid.endswith(".0"):
        uid = uid[:-2]
    if uid == "" or uid.lower() in {"nan", "none", "null"}:
        print(f"[API] Skipping: invalid id '{user_id}'")
        return {"Balance": None, "Equity": None, "OpenPnL": None}

    headers = {
        "Authorization": f"Bearer {SIRIX_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "UserID": uid,
        "GetOpenPositions": False,
        "GetPendingPositions": False,
        "GetClosePositions": False,
        "GetMonetaryTransactions": False,
    }

    backoff = 1.5
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(SIRIX_API_URL, headers=headers, json=payload, timeout=SIRIX_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                bal = (data.get("UserData") or {}).get("AccountBalance") or {}
                if not bal:
                    print(f"[API] {uid} → 200 but no AccountBalance")
                return {
                    "Balance": bal.get("Balance"),
                    "Equity": bal.get("Equity"),
                    "OpenPnL": bal.get("OpenPnL"),
                }

            body_snip = r.text[:200].replace("\n", " ")
            print(f"[API] {uid} → HTTP {r.status_code} (attempt {attempt}/{max_retries}) :: {body_snip}")

            if r.status_code in (429,) or 500 <= r.status_code < 600:
                time.sleep(backoff ** attempt)
                continue

            return {"Balance": None, "Equity": None, "OpenPnL": None}

        except requests.Timeout:
            print(f"[API] {uid} → Timeout (attempt {attempt}/{max_retries})")
            time.sleep(backoff ** attempt)
        except Exception as e:
            print(f"[API] {uid} → Error: {e} (attempt {attempt}/{max_retries})")
            time.sleep(backoff ** attempt)

    return {"Balance": None, "Equity": None, "OpenPnL": None}

# =========================
# 1) LOAD
# =========================
print("[1/7] Loading Excel files...")
tp = pd.read_excel(FILE_TP)
tx = pd.read_excel(FILE_TX)
acct = pd.read_excel(FILE_ACCOUNT)

print(f"    Lv_tpaccount rows: {len(tp):,}")
print(f"    Lv_monetarytransaction rows: {len(tx):,}")
print(f"    Account rows: {len(acct):,}")

# =========================
# 2) NORMALISE KEYS
# =========================
print("[2/7] Normalising keys...")

# Ensure AccountID column exists on Account (case-insensitive fallback)
if "AccountID" not in acct.columns:
    cand = [c for c in acct.columns if c.strip().lower() == "accountid"]
    if not cand:
        raise KeyError("Account.xlsx must contain AccountID column (or a case/space variant).")
    acct = acct.rename(columns={cand[0]: "AccountID"})

# Bring AccountID into TP & TX if present as lv_accountid
if "lv_accountid" in tp.columns:
    tp["AccountID"] = tp["lv_accountid"]
if "lv_accountid" in tx.columns:
    tx["AccountID"] = tx["lv_accountid"]

# Normalized trading platform user id
tp["TP_UserID"] = tp["Lv_name"].apply(clean_tp_id) if "Lv_name" in tp.columns else None
tx["TP_UserID"] = tx["lv_tpaccountidName"].apply(clean_tp_id) if "lv_tpaccountidName" in tx.columns else None

# Ensure AccountID textual
for df_ in (tp, tx, acct):
    if "AccountID" in df_.columns:
        df_["AccountID"] = df_["AccountID"].astype(str).str.strip()

# =========================
# 3) CHECKS: uniqueness & coverage
# =========================
print("[3/7] Running uniqueness & coverage checks...")

report = {}
report["Account_AccountID_unique"] = acct["AccountID"].is_unique if "AccountID" in acct.columns else False
report["TP_TP_UserID_unique"] = tp["TP_UserID"].dropna().is_unique if "TP_UserID" in tp.columns else False

cov_tp_to_acct = tp["AccountID"].isin(acct["AccountID"]).sum() if "AccountID" in tp.columns else 0
cov_tx_to_acct = tx["AccountID"].isin(acct["AccountID"]).sum() if "AccountID" in tx.columns else 0
cov_tx_to_tp   = tx["TP_UserID"].isin(tp["TP_UserID"]).sum() if "TP_UserID" in tx.columns else 0

report["TP→Account coverage"] = f"{cov_tp_to_acct:,}/{len(tp):,} ({pct(cov_tp_to_acct, len(tp))})"
report["Tx→Account coverage"] = f"{cov_tx_to_acct:,}/{len(tx):,} ({pct(cov_tx_to_acct, len(tx))})"
report["Tx→TP coverage"]      = f"{cov_tx_to_tp:,}/{len(tx):,} ({pct(cov_tx_to_tp, len(tx))})"

print("    Uniqueness:")
print(f"      Account.AccountID unique? {report['Account_AccountID_unique']}")
print(f"      Lv_tpaccount.TP_UserID unique? {report['TP_TP_UserID_unique']}")
print("    Coverage:")
print(f"      TP→Account: {report['TP→Account coverage']}")
print(f"      Tx→Account: {report['Tx→Account coverage']}")
print(f"      Tx→TP     : {report['Tx→TP coverage']}")

# =========================
# 4) SAFE 2-STAGE JOIN (transaction grain)
# =========================
print("[4/7] Performing safe 2-stage join... (transaction grain)")

# Stage A (reference): TP + Account (not used for grain but handy for debugging)
tp_acct = tp.merge(acct, on="AccountID", how="left", suffixes=("_tp", "_acct"))

# Stage B: carry AccountID onto transactions using TP_UserID if AccountID missing/wrong
tx_enriched = tx.merge(tp[["TP_UserID", "AccountID"]], on="TP_UserID", how="left", suffixes=("_tx", "_from_tp"))

# Prefer AccountID from tx if present, else take from TP linkage
if "AccountID_tx" in tx_enriched.columns and "AccountID_from_tp" in tx_enriched.columns:
    tx_enriched["AccountID"] = tx_enriched["AccountID_tx"].where(
        tx_enriched["AccountID_tx"].notna(), tx_enriched["AccountID_from_tp"]
    )
    tx_enriched = tx_enriched.drop(columns=["AccountID_tx", "AccountID_from_tp"])
elif "AccountID_tx" in tx_enriched.columns:
    tx_enriched = tx_enriched.rename(columns={"AccountID_tx": "AccountID"})
elif "AccountID_from_tp" in tx_enriched.columns:
    tx_enriched = tx_enriched.rename(columns={"AccountID_from_tp": "AccountID"})
else:
    tx_enriched["AccountID"] = np.nan

missing_acctid = tx_enriched["AccountID"].isna().sum()
print(
    f"    AccountID coverage on transactions: {len(tx_enriched)-missing_acctid:,}/{len(tx_enriched):,} "
    f"({100*(len(tx_enriched)-missing_acctid)/max(1,len(tx_enriched)):.1f}%)"
)

# Now attach Account fields
final = tx_enriched.merge(acct, on="AccountID", how="left", suffixes=("_tx", "_acct"))
print(f"    Rows after enrichment (pre-filter): {len(final):,}")

# =========================
# 5) FILTERS
# =========================
print("[5/7] Applying business filters...")
before = len(final)

# Keep only Deposit Approval (exact match after trim)
if "lv_transactioncaseidName" in final.columns:
    col = final["lv_transactioncaseidName"].fillna("").str.strip()
    final = final[col.eq(KEEP_TRANSACTIONCASE)]

# Exclude 'Purchases' in Lv_TempName (case-insensitive contains)
if "Lv_TempName" in final.columns:
    final = final[~final["Lv_TempName"].fillna("").str.contains(EXCLUDE_TEMP_CONTAINS, case=False, na=False)]

after = len(final)
print(f"    Kept {after:,}/{before:,} rows after filters.")

# =========================
# 6) PLAN / PLAN_SB extraction
# =========================
if "lv_AdditionalInfo" in final.columns:
    final[["Plan", "Plan_SB"]] = final.apply(extract_plan_fields, axis=1)
else:
    final["Plan"] = np.nan
    final["Plan_SB"] = np.nan

# =========================
# 7) SIRIX API enrichment (Balance/Equity/OpenPnL)
# =========================
if ENABLE_SIRIX_API:
    print("[6/7] Enriching with SiRiX balances (this may take time)...")

    # Quick sanity on IDs
    print("[DEBUG] TP_UserID dtype:", final["TP_UserID"].dtype if "TP_UserID" in final.columns else "MISSING")
    print("[DEBUG] Example TP_UserIDs:", final["TP_UserID"].dropna().astype(str).head(5).tolist() if "TP_UserID" in final.columns else [])
    print(f"[DEBUG] Rows missing TP_UserID: {final['TP_UserID'].isna().sum() if 'TP_UserID' in final.columns else 'N/A'}")

    # Get all unique IDs first (cleaned)
    unique_ids = sorted(set(
        str(x).strip().removesuffix(".0")
        for x in final["TP_UserID"].dropna().astype(str)
        if str(x).strip() not in {"", "nan", "none", "null"}
    ))
    total = len(unique_ids)
    print(f"    Unique users to query: {total:,}")
    print("[DEBUG] First 20 user IDs:", unique_ids[:20])

    results_map: Dict[str, Dict[str, Optional[float]]] = {}
    ok, fail = 0, 0

    for i, uid in enumerate(unique_ids, start=1):
        print(f"[LIVE] ({i}/{total}) Fetching balances for UserID: {uid}")
        res = fetch_sirix_bal(uid)
        results_map[uid] = res

        if any(v is not None for v in res.values()):
            ok += 1
        else:
            fail += 1

        time.sleep(SIRIX_SLEEP)

    print(f"[OK] Balance fetch summary → success: {ok:,}, failed: {fail:,}")

    # Map back to every row (transaction grain) with normalized keys
    final["Balance"] = final["TP_UserID"].map(lambda x: (results_map.get(str(x).strip().removesuffix(".0")) or {}).get("Balance"))
    final["Equity"]  = final["TP_UserID"].map(lambda x: (results_map.get(str(x).strip().removesuffix(".0")) or {}).get("Equity"))
    final["OpenPnL"] = final["TP_UserID"].map(lambda x: (results_map.get(str(x).strip().removesuffix(".0")) or {}).get("OpenPnL"))
else:
    final["Balance"] = np.nan
    final["Equity"]  = np.nan
    final["OpenPnL"] = np.nan

# =========================
# 8) DATE CONVERSION
# =========================
to_datetime_inplace(final, DATE_COLS)

# =========================
# 9) OUTPUTS
# =========================
print("[7/7] Writing outputs...")

# 1) Full results (all columns) for auditing
final.to_excel(OUT_FULL_XLSX, index=False)
print(f"    [OK] Full XLSX: {OUT_FULL_XLSX}")

# 2) Dashboard JSON (exact columns)
dash_cols = {
    "Customer Name":       "Name",                      # Account
    "Customer ID":         "lv_maintpaccountidName",    # Account
    "Account ID":          "TP_UserID",                 # normalised Lv_name
    "Email":               "EMailAddress1",             # Account
    "Phone Code":          "Lv_Phone1CountryCode",      # Account
    "Phone Number":        "Lv_Phone1Phone",            # Account
    "Country":             "lv_countryidName",          # Account
    "Affiliate":           "Lv_SubAffiliate",           # Account
    "Tag":                 "Lv_Tag1",                   # Account
    "Plan":                "Plan",                      # from AdditionalInfo
    "Plan SB":             "Plan_SB",                   # from AdditionalInfo
    "Balance":             "Balance",                   # from SIRIX
    "Equity":              "Equity",                    # from SIRIX
    "OpenPnL":             "OpenPnL"                    # from SIRIX
}
dash_df = pd.DataFrame({
    out_col: final[in_col] if in_col in final.columns else np.nan
    for out_col, in_col in dash_cols.items()
})
dash_df.to_json(OUT_DASHBOARD_JSON, orient="records")
print(f"    [OK] Dashboard JSON: {OUT_DASHBOARD_JSON}")

# 3) Merge report + sample
with pd.ExcelWriter(OUT_REPORT_XLSX) as wr:
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
            str(report["Account_AccountID_unique"]),
            str(report["TP_TP_UserID_unique"]),
            report["TP→Account coverage"],
            report["Tx→Account coverage"],
            report["Tx→TP coverage"],
            f"{len(tp):,}",
            f"{len(tx):,}",
            f"{len(acct):,}",
            f"{len(final):,}"
        ]
    })
    summary.to_excel(wr, sheet_name="Summary", index=False)

    sample = final.head(1000)
    sample.to_excel(wr, sheet_name="SampleTop1000_AllCols", index=False)

print("[DONE] Build complete.")
