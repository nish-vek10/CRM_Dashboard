import pandas as pd
import json
import requests
from datetime import datetime
from time import sleep
import numpy as np

# === Config ===
INPUT_JSON = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/merged_clean_dataNEW.json"
OUTPUT_JSON = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/merged_data_full_enrichedNEW.json"
OUTPUT_XLSX = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/merged_data_full_enrichedNEW.xlsx"

API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"

# === 1. Load raw data ===
df = pd.read_json(INPUT_JSON)
print(f"[INFO] Loaded {len(df):,} rows from source.")

# === 2. Apply filters ===
# Keep ONLY lv_transactioncaseidName == "Deposit Approval"
# AND exclude any Lv_TempName that contains "Purchases" (case-insensitive)
before = len(df)
df = df[
    df['lv_transactioncaseidName'].fillna('').str.strip().eq('Deposit Approval') &
    ~df['Lv_TempName'].fillna('').str.contains('Purchases', case=False, na=False)
]
print(f"[FILTER] Deposit Approval + exclude Purchases -> {len(df):,} rows (from {before:,}).")

# # Limit to first 200 for testing
# df = df.head(200)

# === 3. Extract Plan and Plan_SB from lv_AdditionalInfo ===
def extract_plan_fields(row):
    try:
        info = json.loads(row.get('lv_AdditionalInfo', '{}'))
        plan = info.get('name')
        plan_sb = info.get('challenges', {}).get('funding')
    except (ValueError, TypeError, json.JSONDecodeError):
        plan = None
        plan_sb = None
    return pd.Series([plan, plan_sb])

df[['Plan', 'Plan_SB']] = df.apply(extract_plan_fields, axis=1)

# === 4. Define API fetch function ===
def fetch_balance_data(user_id):
    try:
        if pd.isna(user_id):
            return {"Balance": None, "Equity": None, "OpenPnL": None}

        # Ensure proper formatting of user_id (remove .0)
        clean_user_id = str(int(float(user_id))).strip()

        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        payload = {
            "UserID": clean_user_id,
            "GetOpenPositions": False,
            "GetPendingPositions": False,
            "GetClosePositions": False,
            "GetMonetaryTransactions": False
        }

        response = requests.post(API_URL, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            data = response.json()
            bal = data.get("UserData", {}).get("AccountBalance")
            if bal:
                print(f"[OK] Collected data for UserID {clean_user_id}")
                return {
                    "Balance": bal.get("Balance"),
                    "Equity": bal.get("Equity"),
                    "OpenPnL": bal.get("OpenPnL")
                }
            else:
                print(f"[!] No AccountBalance for UserID: {clean_user_id}")
        else:
            print(f"[!] API error for UserID {clean_user_id} — Status: {response.status_code}")
    except Exception as e:
        print(f"[!] Exception for UserID {user_id}: {e}")

    return {"Balance": None, "Equity": None, "OpenPnL": None}

# Helper: pick the right ID column (Lv_name vs lv_name)
ID_COL = 'Lv_name' if 'Lv_name' in df.columns else ('lv_name' if 'lv_name' in df.columns else None)
if not ID_COL:
    raise KeyError("Neither 'Lv_name' nor 'lv_name' exists in the dataset to fetch balances.")

# === 5. Fetch balances from API ===
print("[...] Fetching balances from API...")
balance_data = []
for _, row in df.iterrows():
    user_id = row.get(ID_COL)
    if not user_id or pd.isna(user_id):
        balance_data.append({"Balance": None, "Equity": None, "OpenPnL": None})
        continue
    result = fetch_balance_data(user_id)
    balance_data.append(result)
    sleep(0.2)

balance_df = pd.DataFrame(balance_data)
df = pd.concat([df.reset_index(drop=True), balance_df], axis=1)

# === 6. Log fetch success rate ===
filled = balance_df.dropna(subset=["Balance", "Equity", "OpenPnL"])
print(f"[OK] Successfully pulled balance for {len(filled)} of {len(balance_df)} users.")

# === 7. Convert all date/time columns ===
date_columns = [
    'CreatedOn', 'CreatedOn_y',
    'Lv_DateOfBirth',
    'lv_DateofFTD',
    'lv_FTDDateru_Date',
    'lv_LastDepositDate_Date',
    'Lv_ApprovedOn'
]

for col in date_columns:
    if col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            valid = df[col] > 1000000000    # crude ms-vs-not check
            df[col] = df[col].astype('object')
            df.loc[valid, col] = pd.to_datetime(df.loc[valid, col], unit='ms', errors='coerce')
            df.loc[~valid, col] = pd.NaT
        else:
            df[col] = pd.to_datetime(df[col], errors='coerce')

# === 8. Save final result ===
df.to_json(OUTPUT_JSON, orient='records')
df.to_excel(OUTPUT_XLSX, index=False)

print("\n[OK] All done!")
print(f">> JSON saved to: {OUTPUT_JSON}")
print(f">> Excel saved to: {OUTPUT_XLSX}")
