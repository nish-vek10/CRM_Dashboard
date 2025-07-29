import pandas as pd
import os

# --- CONFIGURATION ---
folder_path = r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\temp"

# Files and their matching fields
file_match_fields = {
    "Lv_monetarytransaction.xlsx": "lv_tpaccountidName",
    "Account.xlsx": "lv_maintpaccountidName",
    "Lv_tpaccount.xlsx": "Lv_name"
}

# User IDs for data extraction
USER_IDS = [
    "132506",
    "182253",
    "134380",
    "167398",
    "105190",
    "112116",
    "155887",
    "105188",
    "180826"
]

# Output combined file
output_file = os.path.join(folder_path, "Filtered_Data_All_Users_NEW.xlsx")
missing_log = os.path.join(folder_path, "missing_ids.txt")

# Final dataframe for all results
final_data = []

print("\n=== Starting Data Extraction ===")

# Open missing log file
with open(missing_log, "w") as log:
    log.write("IDs with no data found in ANY file:\n")

# --- STEP 1: Process each user ---
for user_id in USER_IDS:
    print(f"\n[OK] Processing data for user: {user_id} ...")
    user_data_found = False

    # --- STEP 2: Loop through all files and extract matching rows ---
    for file_name, match_field in file_match_fields.items():
        file_path = os.path.join(folder_path, file_name)

        if not os.path.exists(file_path):
            print(f"   [#] Skipping {file_name}: File not found.")
            continue

        try:
            df = pd.read_excel(file_path)

            if match_field not in df.columns:
                print(f"   [#] Skipping {file_name}: Column '{match_field}' not found.")
                continue

            # Convert field to string and strip spaces
            df[match_field] = df[match_field].astype(str).str.strip()

            # Filter for current user
            filtered_df = df[df[match_field] == str(user_id).strip()]

            # If matches found, add Source_File column and append
            if not filtered_df.empty:
                filtered_df.insert(0, "Source_File", file_name)
                final_data.append(filtered_df)
                user_data_found = True

            print(f"   [INFO] {file_name}: {len(filtered_df)} rows matched.")

        except Exception as e:
            print(f"   [#] Error reading {file_name}: {e}")

    # Log users with no data found in any file
    if not user_data_found:
        with open(missing_log, "a") as log:
            log.write(user_id + "\n")

# --- STEP 3: Save all results into one Excel sheet ---
if final_data:
    combined_df = pd.concat(final_data, ignore_index=True)
    combined_df.to_excel(output_file, sheet_name="Filtered_Data", index=False)
    print(f"\n[DONE] Data extraction completed! All data saved in: {output_file}")
else:
    print("\n[ERROR] No matching data found for any user ID.")

print(f"[INFO] Missing IDs logged to: {missing_log}")
