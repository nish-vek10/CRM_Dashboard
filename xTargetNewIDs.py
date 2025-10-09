# compare_excel.py
import pandas as pd
from pathlib import Path

# 🔹 Change these paths to your actual files
file_a = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\finalCleanOutput\comparison\Lv_tpaccountOLD.xlsx")
file_b = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\finalCleanOutput\comparison\Lv_tpaccountNEW.xlsx")
output_file = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\finalCleanOutput\comparison\comparison_result.xlsx")

# 🔹 Column to compare on (account ID)
KEY_COL = "lv_name"

def main():
    # Read Excel files
    df_a = pd.read_excel(file_a)
    df_b = pd.read_excel(file_b)

    # Ensure the ID column is string (to avoid int/str mismatches)
    df_a[KEY_COL] = df_a[KEY_COL].astype(str).str.strip()
    df_b[KEY_COL] = df_b[KEY_COL].astype(str).str.strip()

    # Merge IDs to find differences
    merged = df_a[[KEY_COL]].merge(
        df_b[[KEY_COL]],
        on=KEY_COL,
        how="outer",
        indicator=True
    )

    ids_only_in_a = merged.loc[merged["_merge"] == "left_only", KEY_COL]
    ids_only_in_b = merged.loc[merged["_merge"] == "right_only", KEY_COL]
    ids_in_both   = merged.loc[merged["_merge"] == "both", KEY_COL]

    only_in_a = df_a[df_a[KEY_COL].isin(ids_only_in_a)]
    only_in_b = df_b[df_b[KEY_COL].isin(ids_only_in_b)]
    in_both   = df_a[df_a[KEY_COL].isin(ids_in_both)]  # take rows from A for common IDs

    # Save to one Excel file with multiple sheets
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        only_in_a.to_excel(writer, sheet_name="only_in_a", index=False)
        only_in_b.to_excel(writer, sheet_name="only_in_b", index=False)
        in_both.to_excel(writer, sheet_name="in_both", index=False)

    print(f"Comparison done. Results saved to: {output_file}")

if __name__ == "__main__":
    main()
