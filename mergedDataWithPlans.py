import pandas as pd
import json

# === 1. Load the existing JSON data ===
INPUT_PATH = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/merged_clean_dataNEW.json"
OUTPUT_JSON = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/merged_data_with_plansNEW.json"
OUTPUT_EXCEL = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/merged_data_with_plansNEW.xlsx"

df = pd.read_json(INPUT_PATH)

# === 2. Function to safely extract Plan and Plan_SB ===
def extract_plan_fields(row):
    try:
        info = json.loads(row.get('lv_AdditionalInfo', '{}'))
        plan = info.get('name')
        plan_sb = info.get('challenges', {}).get('funding')
    except (ValueError, TypeError, json.JSONDecodeError):
        plan = None
        plan_sb = None
    return pd.Series([plan, plan_sb])

# === 3. Apply function and add new columns ===
df[['Plan', 'Plan_SB']] = df.apply(extract_plan_fields, axis=1)

# === 4. Convert date/time fields ===
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
        # Check if it's numeric — then it's likely a timestamp in ms
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce', unit='ms')
        else:
            df[col] = pd.to_datetime(df[col], errors='coerce')

# === 5. Save to JSON and Excel ===
df.to_json(OUTPUT_JSON, orient='records')
df.to_excel(OUTPUT_EXCEL, index=False)

print(f"[OK] Extracted Plan and Plan_SB.")
print(f"[OK] JSON saved to: {OUTPUT_JSON}")
print(f"[OK] Excel saved to: {OUTPUT_EXCEL}")


