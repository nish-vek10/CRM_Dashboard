import pandas as pd
import itertools
import os
from tqdm import tqdm

# ---------------- CONFIGURATION ----------------
folder_path = r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\temp"
files_to_check = [
    "Lv_monetarytransaction.xlsx",
    "account.xlsx",
    "Lv_tpaccount.xlsx"
]
output_file = os.path.join(folder_path, "Matching_Analysis.xlsx")

# ---------------- STEP 1: LOAD ALL FILES ----------------
print("[...] Loading files...")
dataframes = {}
columns_per_file = {}

for file in files_to_check:
    file_path = os.path.join(folder_path, file)
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file}")
        continue
    print(f"[-] Reading file: {file}")
    df = pd.read_excel(file_path)
    dataframes[file] = df
    columns_per_file[file] = df.columns.tolist()

print("[OK] All files loaded successfully.\n")

# ---------------- STEP 2: PREPARE STORAGE FOR RESULTS ----------------
results = []

# ---------------- STEP 3: PAIRWISE MATCHING ----------------
print("[#] Checking pairwise column matches...")
pairwise_combinations = list(itertools.combinations(dataframes.items(), 2))
total_steps = sum(len(df1.columns) * len(df2.columns) for _, df1 in dataframes.items() for _, df2 in dataframes.items() if _ != _)

for (file1, df1), (file2, df2) in tqdm(pairwise_combinations, desc="Pairwise comparisons", unit="pair"):
    for col1 in df1.columns:
        for col2 in df2.columns:
            set1 = set(df1[col1].dropna().astype(str))
            set2 = set(df2[col2].dropna().astype(str))
            intersection = set1.intersection(set2)
            if intersection:
                count_match = len(intersection)
                min_unique = min(len(set1), len(set2))
                overlap_pct = (count_match / min_unique) * 100 if min_unique > 0 else 0
                results.append({
                    "Files_Compared": f"{file1} ↔ {file2}",
                    "Column_1": col1,
                    "Column_2": col2,
                    "Column_3": "",
                    "Match_Type": "Pairwise",
                    "Matching_Records": count_match,
                    "%_Overlap": round(overlap_pct, 2)
                })
    print(f"    [OK] Finished comparing '{file1}' with '{file2}'.\n")

# ---------------- STEP 4: THREE-WAY MATCHING ----------------
print("[#] Checking three-way column matches...")
threeway_combinations = list(itertools.combinations(dataframes.items(), 3))

for (file1, df1), (file2, df2), (file3, df3) in tqdm(threeway_combinations, desc="Three-way comparisons", unit="triplet"):
    for col1 in df1.columns:
        for col2 in df2.columns:
            for col3 in df3.columns:
                set1 = set(df1[col1].dropna().astype(str))
                set2 = set(df2[col2].dropna().astype(str))
                set3 = set(df3[col3].dropna().astype(str))
                intersection = set1.intersection(set2).intersection(set3)
                if intersection:
                    count_match = len(intersection)
                    min_unique = min(len(set1), len(set2), len(set3))
                    overlap_pct = (count_match / min_unique) * 100 if min_unique > 0 else 0
                    results.append({
                        "Files_Compared": f"{file1} ↔ {file2} ↔ {file3}",
                        "Column_1": col1,
                        "Column_2": col2,
                        "Column_3": col3,
                        "Match_Type": "Three-way",
                        "Matching_Records": count_match,
                        "%_Overlap": round(overlap_pct, 2)
                    })
    print(f"    [OK] Finished comparing '{file1}', '{file2}', and '{file3}'.\n")

# ---------------- STEP 5: SAVE RESULTS ----------------
print("[OK] SAVING ANALYSIS TO EXCEL...")

overview_df = pd.DataFrame([
    {"File": file, "Columns": ", ".join(cols)}
    for file, cols in columns_per_file.items()
])
results_df = pd.DataFrame(results)

with pd.ExcelWriter(output_file) as writer:
    overview_df.to_excel(writer, sheet_name="Files_And_Columns", index=False)
    results_df.to_excel(writer, sheet_name="Matching_Results", index=False)

print(f"[DONE] MATCHING ANALYSIS COMPLETED! Results saved to: {output_file}")
