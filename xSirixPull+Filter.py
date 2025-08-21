import pandas as pd
import requests
import json
from pathlib import Path
from time import sleep, time
from collections import Counter

# === Config ===
INPUT_XLSX = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/finalCleanOutput/Lv_tpaccount.xlsx"
OUTPUT_DIR = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/finalCleanOutput")
OUTPUT_JSON = OUTPUT_DIR / "crm_sirix_enriched.json"
OUTPUT_XLSX = OUTPUT_DIR / "crm_sirix_enriched.xlsx"

API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"

# === 1. Load CRM Data ===
df = pd.read_excel(INPUT_XLSX)
print(f"[INFO] Loaded {len(df):,} CRM rows.")

# === 2. Apply filter (remove 'Purchases') ===
before = len(df)
df = df[~df['Lv_TempName'].fillna('').str.contains('Purchases', case=False)]
print(f"[FILTER] Removed Purchases -> {len(df):,} rows (from {before:,}).")

# Normalize the index so progress counts are 1..N
df = df.reset_index(drop=True)
total = len(df)

# === 3. API fetch function ===
def fetch_sirix_data(user_id):
    """Fetch Country, Plan, Balance, Equity, OpenPnL for one account."""
    try:
        if pd.isna(user_id):
            return None

        clean_user_id = str(int(float(user_id))).strip()
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "UserID": clean_user_id,
            "GetOpenPositions": False,
            "GetPendingPositions": False,
            "GetClosePositions": False,
            "GetMonetaryTransactions": True  # needed for Plan
        }

        resp = requests.post(API_URL, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            print(f"[!] API error {resp.status_code} for {clean_user_id}")
            return None
        data = resp.json()

        # Country
        country = (data.get("UserData") or {}).get("UserDetails", {}).get("Country")

        # Account Balance
        bal = (data.get("UserData") or {}).get("AccountBalance") or {}
        balance = bal.get("Balance")
        equity = bal.get("Equity")
        open_pnl = bal.get("OpenPnL")

        # Plan: look for MonetaryTransaction with Comment starting "Initial balance"
        plan = None
        for t in data.get("MonetaryTransactions") or []:
            if str(t.get("Comment", "")).lower().startswith("initial balance"):
                plan = t.get("Amount")
                break

        return {
            "Country": country,
            "Plan": plan,
            "Balance": balance,
            "Equity": equity,
            "OpenPnL": open_pnl
        }

    except Exception as e:
        print(f"[!] Exception for UserID {user_id}: {e}")
        return None

# === 4. Loop through accounts ===
results = []
seen_ids = []
total = len(df)

print(f"[START] Pulling data for {total} accounts...\n")

start_time = time()  # start timer

for i, row in df.iterrows():
    user_id = row.get("Lv_name")
    print(f"[{i+1}/{total}] Fetching UserID: {user_id} ...")

    sirix_data = fetch_sirix_data(user_id)
    if sirix_data:
        plan = sirix_data["Plan"]
        balance = sirix_data["Balance"]
        # % Change calculation
        pct_change = None
        if plan not in (None, 0) and balance is not None:
            try:
                pct_change = ((balance - plan) / plan) * 100
            except Exception:
                pct_change = None
        sirix_data["PctChange"] = pct_change
    else:
        sirix_data = {"Country": None, "Plan": None, "Balance": None, "Equity": None, "OpenPnL": None, "PctChange": None}

    results.append({
        "CUSTOMER NAME": row.get("lv_accountidName"),
        "ACCOUNT ID": row.get("Lv_name"),
        **sirix_data
    })
    seen_ids.append(user_id)

    sleep(0.2)  # rate limit

# === 5. Save outputs ===
enriched_df = pd.DataFrame(results)
enriched_df.to_excel(OUTPUT_XLSX, index=False)
enriched_df.to_json(OUTPUT_JSON, orient="records")

print(f"\n[OK] Enriched data saved to:\n - {OUTPUT_XLSX}\n - {OUTPUT_JSON}")

# === 6. Final summary ===
dup_counts = Counter(seen_ids)
duplicates = {uid: cnt for uid, cnt in dup_counts.items() if cnt > 1}

print("\n===== SUMMARY =====")
print(f"Total processed: {len(seen_ids)}")
print(f"Unique IDs     : {len(dup_counts)}")
print(f"Duplicates     : {len(duplicates)}")
if duplicates:
    print("Duplicate IDs:")
    for uid, cnt in duplicates.items():
        print(f" - {uid} ({cnt} times)")
print("===================")

# === 7. Elapsed time ===
elapsed = int(time() - start_time)
mm, ss = divmod(elapsed, 60)
print(f"\n[PROCESS COMPLETE] Total time: {mm:02d}:{ss:02d} (MM:SS)")
