# one_off_newly_added_pull.py
import pandas as pd
import requests
from pathlib import Path
from time import sleep, time
from datetime import datetime
from collections import Counter

# === Input: your newly-added IDs file ===
INPUT_XLSX = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\finalCleanOutput\comparison\newly_added.xlsx")

# === Output: saved in the same folder as INPUT_XLSX ===
OUTPUT_XLSX = INPUT_XLSX.parent / "newly_added_enriched.xlsx"

# === Sirix API config ===
API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"  # <- update if needed

# === Column mapping (case-insensitive) ===
# Expecting columns like: lv_name (account id), lv_accountidname (customer name), lv_tempname (status label)
COL_ID = "lv_name"
COL_NAME = "lv_accountidname"
COL_TEMP = "lv_tempname"

# --- helpers ---
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase/strip all column headers to allow case-insensitive access."""
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def get_col(df: pd.DataFrame, col_lower: str, default=None):
    return df[col_lower] if col_lower in df.columns else pd.Series([default] * len(df))

def clean_user_id(value):
    """Coerce to string account id without decimals/spaces; keep None if missing."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    # If it's like "183623.0", make it "183623"
    try:
        s = str(int(float(s)))
    except Exception:
        pass
    return s

def fetch_sirix_data(user_id: str):
    """Fetch Country, Plan, Balance, Equity, OpenPnL, GroupName & BlownUp flag for one account."""
    try:
        if not user_id:
            return None

        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "UserID": user_id,
            "GetOpenPositions": False,
            "GetPendingPositions": False,
            "GetClosePositions": False,
            "GetMonetaryTransactions": True
        }
        resp = requests.post(API_URL, headers=headers, json=payload, timeout=25)
        if resp.status_code != 200:
            print(f"[!] API error {resp.status_code} for {user_id}")
            return None
        data = resp.json() or {}

        user_data = data.get("UserData") or {}
        details = (user_data.get("UserDetails") or {})
        bal     = (user_data.get("AccountBalance") or {})
        group   = (user_data.get("GroupInfo") or {})

        # Extract fields
        country  = details.get("Country")
        balance  = bal.get("Balance")
        equity   = bal.get("Equity")
        open_pnl = bal.get("OpenPnL")
        group_name = group.get("GroupName")

        # BlownUp: any MonetaryTransactions comment containing "Zero Balance"
        txns = data.get("MonetaryTransactions") or []
        blown_up = any("zero balance" in str(t.get("Comment", "")).lower() for t in txns)

        # Plan: first txn whose comment starts with "Initial balance"
        plan = None
        for t in txns:
            if str(t.get("Comment", "")).lower().startswith("initial balance"):
                plan = t.get("Amount")
                break

        is_purchase_group = "purchase" in str(group_name or "").lower()

        return {
            "Country": country,
            "Plan": plan,
            "Balance": balance,
            "Equity": equity,
            "OpenPnL": open_pnl,
            "GroupName": group_name,
            "IsPurchaseGroup": is_purchase_group,
            "BlownUp": blown_up,
        }
    except Exception as e:
        print(f"[!] Exception for UserID {user_id}: {e}")
        return None

def main():
    print("[SERVICE] One-off pull for newly added IDs started.")
    t0 = time()

    # Load input
    if not INPUT_XLSX.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_XLSX}")

    df_src = pd.read_excel(INPUT_XLSX)
    df_src = normalize_columns(df_src)

    # Basic checks
    for col in (COL_ID, COL_NAME, COL_TEMP):
        if col not in df_src.columns:
            print(f"[WARN] Column '{col}' not found in input. Available columns: {list(df_src.columns)}")
    # Build rows
    ids = get_col(df_src, COL_ID)
    names = get_col(df_src, COL_NAME)
    temps = get_col(df_src, COL_TEMP)

    # Iterate IDs
    results_active = []
    results_blown  = []
    results_purch  = []
    results_50k    = []
    baseline_rows  = []

    seen_ids = []

    total = len(df_src)
    print(f"[INFO] Loaded {total} rows from: {INPUT_XLSX}")

    for i in range(total):
        raw_id = ids.iloc[i]
        user_id = clean_user_id(raw_id)
        cust_name = names.iloc[i] if i < len(names) else None
        tempname  = temps.iloc[i] if i < len(temps) else None

        if not user_id:
            print(f"[{i+1}/{total}] Skipping empty/malformed ID: {raw_id!r}")
            continue

        print(f"[{i+1}/{total}] Fetching UserID: {user_id} ...")
        data = fetch_sirix_data(user_id)

        # Default structure if missing
        if not data:
            data = {"Country": None, "Plan": None, "Balance": None, "Equity": None, "OpenPnL": None,
                    "GroupName": None, "IsPurchaseGroup": False, "BlownUp": False}

        # Capture baseline (for these new IDs, baseline = current equity)
        baseline_rows.append({
            "ACCOUNT ID": user_id,
            "BaselineEquity": data.get("Equity")
        })

        row_common = {
            "CUSTOMER NAME": cust_name,
            "ACCOUNT ID": user_id,
            "TempName": tempname,
            "Country": data.get("Country"),
            "Plan": data.get("Plan"),
            "Balance": data.get("Balance"),
            "Equity": data.get("Equity"),
            "OpenPnL": data.get("OpenPnL"),
        }

        # Routing
        if data.get("BlownUp"):
            print(f"    ↳ [BLOWN-UP] {user_id} -> BlownUp sheet.")
            results_blown.append({**row_common, "PctChange": None})
        elif data.get("IsPurchaseGroup"):
            print(f"    ↳ [PURCHASES(API)] {user_id} -> Purchases_API sheet (GroupName='{data.get('GroupName')}').")
            results_purch.append({**row_common, "GroupName": data.get("GroupName"), "PctChange": None})
        else:
            # Optional special sheet for Plan == 50000
            plan_val = None
            try:
                plan_raw = data.get("Plan")
                plan_val = float(plan_raw) if plan_raw is not None else None
            except Exception:
                pass

            if plan_val is not None and abs(plan_val - 50000.0) < 1e-6:
                print(f"    ↳ [PLAN=50000] {user_id} -> Plan50000 sheet.")
                results_50k.append({**row_common, "PctChange": None})
            else:
                results_active.append({**row_common, "PctChange": None})

        seen_ids.append(user_id)
        sleep(0.2)  # polite rate limit

    # Build DataFrames
    active_df    = pd.DataFrame(results_active)
    blown_df     = pd.DataFrame(results_blown)
    purchases_df = pd.DataFrame(results_purch)
    plan50k_df   = pd.DataFrame(results_50k)
    baseline_df  = pd.DataFrame(baseline_rows)

    # Save to one Excel file with multiple sheets
    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
        active_df.to_excel(xw, sheet_name="Active", index=False)
        blown_df.to_excel(xw, sheet_name="BlownUp", index=False)
        purchases_df.to_excel(xw, sheet_name="Purchases_API", index=False)
        plan50k_df.to_excel(xw, sheet_name="Plan50000", index=False)
        baseline_df.to_excel(xw, sheet_name="Baseline", index=False)

    # Summary
    dup_counts = Counter(seen_ids)
    duplicates = {uid: cnt for uid, cnt in dup_counts.items() if cnt > 1}

    print("\n[OK] Saved:")
    print(f" - {OUTPUT_XLSX}")
    print("\n===== SUMMARY =====")
    print(f"Total processed: {len(seen_ids)}")
    print(f"Unique IDs     : {len(dup_counts)}")
    print(f"Duplicates     : {len(duplicates)}")
    if duplicates:
        for uid, cnt in duplicates.items():
            print(f" - {uid} ({cnt} times)")
    print(f"Blown-up       : {len(results_blown)} (sheet: BlownUp)")
    print(f"Purchases(API) : {len(results_purch)} (sheet: Purchases_API)")
    print(f"Plan=50000     : {len(results_50k)} (sheet: Plan50000)")
    print(f"Active (final) : {len(results_active)} (sheet: Active)")

    mm, ss = divmod(int(time() - t0), 60)
    print(f"[PROCESS COMPLETE] Run time: {mm:02d}:{ss:02d} (MM:SS)")

if __name__ == "__main__":
    main()
