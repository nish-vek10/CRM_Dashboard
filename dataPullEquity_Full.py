import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
import time

# ===== STEP 1: Output folder path - for the data to be saved
output_folder = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots")
output_folder.mkdir(parents=True, exist_ok=True)

# ===== STEP 2: Table/view to export
table_name = "etwotprop_daily_view"
schema_name = "platform-real3"

# ===== STEP 3: MySQL connection parameters
user = "etwotprop"
password = "MrFHSv1vma"
host = "reports3.leveratetech.com"
port = 3306
database = "platform-real3"

# SQLAlchemy connection string
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

try:
    print(f"[INFO] Connecting to MySQL server at {host}:{port} ...")

    # === 4.1 Get column names first ===
    print("[INFO] Fetching column names from the table...")
    column_query = f"SELECT * FROM {table_name} LIMIT 1"
    sample_df = pd.read_sql(column_query, engine)
    columns = list(sample_df.columns)

    # Print column names
    print(f"[COLUMNS FOUND] {len(columns)} columns:")
    for col in columns:
        print(f"   - {col}")

    # Save column names to a text file
    fields_file = output_folder / "Equity_Snapshot_FieldsName.txt"
    with open(fields_file, "w", encoding="utf-8") as f:
        f.write(f"Field names for table: {table_name}\n")
        f.write("=" * 40 + "\n")
        for col in columns:
            f.write(f"{col}\n")
    print(f"[INFO] Field names saved to: {fields_file}")

    # === 4.2 Start data extraction in chunks ===
    query = f"SELECT * FROM {table_name}"
    print(f"\n[INFO] Starting data extraction from '{table_name}'...")
    start_time = time.time()

    chunks = pd.read_sql(query, engine, chunksize=10000)
    df_list = []
    total_rows = 0

    for idx, chunk in enumerate(chunks, start=1):
        row_count = len(chunk)
        total_rows += row_count
        print(f"[FETCHING] → Chunk {idx} fetched ({row_count} rows) | Total so far: {total_rows}")
        df_list.append(chunk)

    # Combine all chunks
    df = pd.concat(df_list, ignore_index=True)

    elapsed_time = round(time.time() - start_time, 2)
    print(f"[INFO] ### Data extraction complete. Total rows fetched: {total_rows} in {elapsed_time} seconds. ###")

    # === 5. Save to Excel ===
    output_file = output_folder / "Equity_Snapshot.xlsx"
    df.to_excel(output_file, index=False)
    print(f"[SUCCESS] Data successfully saved to: {output_file}")

except Exception as e:
    print(f"[ERROR] Failed to extract data: {e}")

finally:
    engine.dispose()

print("[DONE] ~ Equity data pull complete.")
