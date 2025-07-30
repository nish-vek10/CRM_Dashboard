import pandas as pd
import json

# === 1. Load the existing JSON data ===
INPUT_PATH = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Equity_Snapshot.json"
OUTPUT_JSON = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Filtered_by_GROUP.json"
OUTPUT_EXCEL = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Filtered_by_GROUP.xlsx"

print("[OK] Reading data from JSON file...")
df = pd.read_json(INPUT_PATH)

# === 2. Initial record count ===
initial_count = len(df)
print(f"[INFO] Total records loaded: {initial_count}")

# === 3. Filter the data safely ===
if 'GROUP' in df.columns:
    free_trial_count = (df['GROUP'] == 'Free Trial').sum()
    test_count = (df['GROUP'] == 'Test').sum()

    # Use .copy() to avoid SettingWithCopyWarning
    filtered_df = df.loc[~df['GROUP'].isin(['Free Trial', 'Test'])].copy()

    removed_count = initial_count - len(filtered_df)
    print(f"[INFO] Records with 'Free Trial': {free_trial_count}")
    print(f"[INFO] Records with 'Test': {test_count}")
    print(f"[INFO] Total records removed: {removed_count}")
    print(f"[INFO] Records remaining after filter: {len(filtered_df)}")
else:
    print("[WARNING] Column 'GROUP' not found in dataset. No filtering applied.")
    filtered_df = df.copy()

# === 4. Convert timestamps safely ===
for col in filtered_df.columns:
    if pd.api.types.is_numeric_dtype(filtered_df[col]):
        # Check if timestamps are in milliseconds range
        if filtered_df[col].max() > 1e12:
            # Explicitly convert to datetime (avoiding future dtype issues)
            converted_col = pd.to_datetime(filtered_df[col], errors='coerce', unit='ms')
            filtered_df[col] = converted_col.astype('datetime64[ns]')

# === 5. Save filtered data ===
filtered_df.to_json(OUTPUT_JSON, orient='records', date_format='iso')
filtered_df.to_excel(OUTPUT_EXCEL, index=False)

print("[OK] Timestamp Conversion Completed.")
print("[OK] Filtering Data Completed.")
print(f"[OK] Filtered JSON saved to: {OUTPUT_JSON}")
print(f"[OK] Filtered Excel saved to: {OUTPUT_EXCEL}")
