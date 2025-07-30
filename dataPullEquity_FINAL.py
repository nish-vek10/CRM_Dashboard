import pandas as pd
import numpy as np
import json
from pathlib import Path

# === 1. File Paths ===
INPUT_PATH = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Filtered_by_DEPOSIT.json"
OUTPUT_EXCEL = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Trader_Return_Analysis.xlsx"
LOG_FILE = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots/Return_Analysis_Log.txt"

# Allowed deposits
ALLOWED_DEPOSITS = [1000, 2500, 5000, 10000, 20000, 40000, 60000, 80000, 100000]

# Define bucket ranges
RETURN_BUCKETS = [(-5, -4.01), (-4, -3.01), (-3, -2.01), (-2, -1.01), (-1, -0.01),
                  (0, 0.99), (1, 1.99), (2, 2.99), (3, 3.99), (4, 4.99),
                  (5, 5.99), (6, 6.99), (7, 7.99), (8, 8.99), (9, 9.99), (10, 1000)]

# === 2. Load Data ===
print("[OK] Reading data from JSON file...")
df = pd.read_json(INPUT_PATH)
print(f"[INFO] Total records loaded: {len(df)}")

# Ensure required columns
required_cols = ['LOGIN', 'DEPOSIT', 'EQUITY', 'TIME']
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# === 3. Preprocess ===
df['TIME'] = pd.to_datetime(df['TIME'], errors='coerce')
df = df.sort_values(by=['LOGIN', 'TIME'])

# === 4. Calculate per trader ===
results = []
skipped_no_deposit = 0
skipped_no_equity = 0

for login, group in df.groupby('LOGIN'):
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

# Convert results to DataFrame
df_results = pd.DataFrame(results, columns=['LOGIN', 'Initial_Deposit', 'Max_Equity', 'Max_Return_%'])

print(f"[INFO] Traders processed: {len(df_results)}")
print(f"[INFO] Traders skipped (no deposit): {skipped_no_deposit}")
print(f"[INFO] Traders skipped (no equity): {skipped_no_equity}")

# === 5. Bucket results ===
summary = []

for deposit in ALLOWED_DEPOSITS:
    sub = df_results[df_results['Initial_Deposit'] == deposit]
    if sub.empty:
        continue
    bucket_counts = []
    for low, high in RETURN_BUCKETS:
        count = sub[(sub['Max_Return_%'] >= low) & (sub['Max_Return_%'] <= high)].shape[0]
        bucket_counts.append(count)
    avg_return = sub['Max_Return_%'].mean()
    trader_count = len(sub)
    summary.append([deposit] + bucket_counts + [avg_return, trader_count])

# Build column names
bucket_labels = [f"{low}% to {high}%" for low, high in RETURN_BUCKETS]
columns = ['Deposit'] + bucket_labels + ['Avg Return %', 'Trader Count']

df_summary = pd.DataFrame(summary, columns=columns)

# === 6. Save outputs ===
with pd.ExcelWriter(OUTPUT_EXCEL, engine='openpyxl') as writer:
    df_results.to_excel(writer, sheet_name="Detailed_Trader_Results", index=False)
    df_summary.to_excel(writer, sheet_name="Return_Bucket_Summary", index=False)

# === 7. Logging ===
with open(LOG_FILE, 'w') as log:
    log.write("=== Trader Return Analysis Log ===\n")
    log.write(f"Total records loaded: {len(df)}\n")
    log.write(f"Traders processed: {len(df_results)}\n")
    log.write(f"Traders skipped (no deposit): {skipped_no_deposit}\n")
    log.write(f"Traders skipped (no equity): {skipped_no_equity}\n")
    log.write("\n--- Bucket Summary ---\n")
    log.write(df_summary.to_string(index=False))
    log.write("\n")

print("[OK] Analysis completed successfully.")
print(f"[OK] Excel saved to: {OUTPUT_EXCEL}")
print(f"[OK] Log saved to: {LOG_FILE}")
