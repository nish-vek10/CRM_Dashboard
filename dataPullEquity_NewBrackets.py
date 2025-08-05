import pandas as pd
import numpy as np
from datetime import datetime

# === 1. File Paths ===
INPUT_PATH = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Filtered_by_DEPOSIT.json"
OUTPUT_EXCEL = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Trader_Return_Analysis_March2025.xlsx"
LOG_FILE = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Return_Analysis_Log_March2025.txt"

# Allowed deposits
ALLOWED_DEPOSITS = [1000, 2500, 5000, 10000, 20000, 40000, 60000, 80000, 100000]

# Define old detailed bucket ranges
RETURN_BUCKETS_OLD = [(-5, -4.01), (-4, -3.01), (-3, -2.01), (-2, -1.01), (-1, -0.01),
                      (0, 0.99), (1, 1.99), (2, 2.99), (3, 3.99), (4, 4.99),
                      (5, 5.99), (6, 6.99), (7, 7.99), (8, 8.99), (9, 9.99), (10, 1000)]

# Define new bucket ranges for 3rd sheet
RETURN_BUCKETS_NEW = {
    "Less than 5%": (-1000, 4.99),
    "5% to 7.49%": (5, 7.49),
    "7.5% to 9.99%": (7.5, 9.99),
    "10% and More": (10, 1000)
}

# === 2. Load Data ===
print("[OK] Reading data from JSON file...")
df = pd.read_json(INPUT_PATH)
print(f"[INFO] Total records loaded: {len(df)}")

# Ensure required columns
required_cols = ['LOGIN', 'DEPOSIT', 'EQUITY', 'TIME']
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# === 3. Filter date range ===
df['TIME'] = pd.to_datetime(df['TIME'], errors='coerce')
start_date = pd.Timestamp("2025-03-01 00:00:00")
end_date = pd.Timestamp(datetime.now())
df_filtered_date = df[(df['TIME'] >= start_date) & (df['TIME'] <= end_date)].copy()
print(f"[INFO] Records after date filter (from {start_date.date()}): {len(df_filtered_date)}")

# Sort data
df_filtered_date = df_filtered_date.sort_values(by=['LOGIN', 'TIME'])

# === 4. Calculate per trader ===
results = []
skipped_no_deposit = 0
skipped_no_equity = 0

for login, group in df_filtered_date.groupby('LOGIN'):
    deposits = group.loc[group['DEPOSIT'] > 0, 'DEPOSIT']
    if deposits.empty:
        skipped_no_deposit += 1
        continue
    initial_deposit = deposits.iloc[0]
    if initial_deposit not in ALLOWED_DEPOSITS:
        continue

    max_equity = group['EQUITY'].max()
    if max_equity <= 0:
        skipped_no_equity += 1
        continue

    max_return_pct = ((max_equity - initial_deposit) / initial_deposit) * 100
    results.append([login, initial_deposit, max_equity, max_return_pct])

df_results = pd.DataFrame(results, columns=['LOGIN', 'Initial_Deposit', 'Max_Equity', 'Max_Return_%'])

print(f"[INFO] Traders processed: {len(df_results)}")
print(f"[INFO] Traders skipped (no deposit): {skipped_no_deposit}")
print(f"[INFO] Traders skipped (no equity): {skipped_no_equity}")

# === 5. Old bucket summary ===
summary_old = []
for deposit in ALLOWED_DEPOSITS:
    sub = df_results[df_results['Initial_Deposit'] == deposit]
    if sub.empty:
        continue
    bucket_counts = []
    for low, high in RETURN_BUCKETS_OLD:
        count = sub[(sub['Max_Return_%'] >= low) & (sub['Max_Return_%'] <= high)].shape[0]
        bucket_counts.append(count)
    avg_return = sub['Max_Return_%'].mean()
    trader_count = len(sub)
    summary_old.append([deposit] + bucket_counts + [avg_return, trader_count])

bucket_labels_old = [f"{low}% to {high}%" for low, high in RETURN_BUCKETS_OLD]
columns_old = ['Deposit'] + bucket_labels_old + ['Avg Return %', 'Trader Count']
df_summary_old = pd.DataFrame(summary_old, columns=columns_old)

# === 6. New bucket summary (3rd sheet) ===
summary_new = []
for deposit in ALLOWED_DEPOSITS:
    sub = df_results[df_results['Initial_Deposit'] == deposit]
    if sub.empty:
        continue
    counts = {}
    for label, (low, high) in RETURN_BUCKETS_NEW.items():
        counts[label] = sub[(sub['Max_Return_%'] >= low) & (sub['Max_Return_%'] <= high)].shape[0]
    trader_count = len(sub)
    summary_new.append([deposit, trader_count] + [counts[label] for label in RETURN_BUCKETS_NEW.keys()])

columns_new = ['Deposit', 'Trader Count'] + list(RETURN_BUCKETS_NEW.keys())
df_summary_new = pd.DataFrame(summary_new, columns=columns_new)

# === 7. Save Excel output with 3 sheets ===
with pd.ExcelWriter(OUTPUT_EXCEL, engine='openpyxl') as writer:
    df_filtered_date.to_excel(writer, sheet_name="Filtered_Data_March_2025", index=False)
    df_results.to_excel(writer, sheet_name="Detailed_Trader_Results", index=False)
    df_summary_new.to_excel(writer, sheet_name="Return_Bucket_Summary_New", index=False)

# === 8. Save logs ===
with open(LOG_FILE, 'w') as log:
    log.write("=== Trader Return Analysis Log (March 2025 Filter) ===\n")
    log.write(f"Total records loaded: {len(df)}\n")
    log.write(f"Records after date filter: {len(df_filtered_date)}\n")
    log.write(f"Traders processed: {len(df_results)}\n")
    log.write(f"Traders skipped (no deposit): {skipped_no_deposit}\n")
    log.write(f"Traders skipped (no equity): {skipped_no_equity}\n\n")
    log.write("--- New Bucket Summary ---\n")
    log.write(df_summary_new.to_string(index=False))
    log.write("\n")

print("[OK] Analysis completed successfully with March 2025 filter.")
print(f"[OK] Excel saved to: {OUTPUT_EXCEL}")
print(f"[OK] Log saved to: {LOG_FILE}")
