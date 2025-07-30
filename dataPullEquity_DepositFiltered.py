import pandas as pd
import sys

# === 1. Setup logging to both terminal and log file ===
LOG_FILE = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Filtered_by_DEPOSIT_log.txt"

class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open(LOG_FILE, "w", encoding="utf-8")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass

# Redirect all prints to terminal + log file
sys.stdout = Logger()

# === 2. File paths ===
INPUT_PATH = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Filtered_by_GROUP.json"
OUTPUT_JSON = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Filtered_by_DEPOSIT.json"
OUTPUT_EXCEL = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Filtered_by_DEPOSIT.xlsx"

# Allowed DEPOSIT values
ALLOWED_DEPOSITS = [0, 1000, 2500, 5000, 10000, 20000, 40000, 60000, 80000, 100000]

print("[OK] Reading data from JSON file...")
df = pd.read_json(INPUT_PATH)

# === 3. Initial record count ===
initial_count = len(df)
print(f"[INFO] Total records loaded: {initial_count}")

# === 4. Filter the data by DEPOSIT ===
if 'DEPOSIT' in df.columns:
    # Show individual counts for each allowed deposit
    print("[INFO] Counts for each allowed DEPOSIT value:")
    for val in ALLOWED_DEPOSITS:
        count = (df['DEPOSIT'] == val).sum()
        print(f"   - {val}: {count}")

    # Total matching records
    matching_count = df['DEPOSIT'].isin(ALLOWED_DEPOSITS).sum()
    non_matching_count = initial_count - matching_count

    # Apply filter
    filtered_df = df.loc[df['DEPOSIT'].isin(ALLOWED_DEPOSITS)].copy()

    print(f"[INFO] Records matching allowed DEPOSIT values (Total): {matching_count}")
    print(f"[INFO] Records removed (non-matching): {non_matching_count}")
    print(f"[INFO] Records remaining after filter: {len(filtered_df)}")
else:
    print("[WARNING] Column 'DEPOSIT' not found in dataset. No filtering applied.")
    filtered_df = df.copy()

# === 5. Convert timestamps safely ===
for col in filtered_df.columns:
    if pd.api.types.is_numeric_dtype(filtered_df[col]):
        if filtered_df[col].max() > 1e12:
            converted_col = pd.to_datetime(filtered_df[col], errors='coerce', unit='ms')
            filtered_df[col] = converted_col.astype('datetime64[ns]')

# === 6. Save filtered data ===
filtered_df.to_json(OUTPUT_JSON, orient='records', date_format='iso')
filtered_df.to_excel(OUTPUT_EXCEL, index=False)

print("[OK] Timestamp Conversion Completed.")
print("[OK] Filtering Data Completed.")
print(f"[OK] Filtered JSON saved to: {OUTPUT_JSON}")
print(f"[OK] Filtered Excel saved to: {OUTPUT_EXCEL}")
print(f"[OK] Log file saved to: {LOG_FILE}")
