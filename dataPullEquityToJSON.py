import pandas as pd
from pathlib import Path

# === 1. Define base directory ===
base_path = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/Equity Snapshots")

# === 2. Input/Output Files ===
input_excel = base_path / "Equity_Snapshot.xlsx"      # Source Excel file
output_json = base_path / "Equity_Snapshot.json"      # Output JSON file

# === 3. Load Excel and export to JSON ===
print("[OK] Reading data from Excel...")
df = pd.read_excel(input_excel)  # Automatically reads the only sheet

# Export ALL records to JSON with indentation for readability
df.to_json(output_json, orient="records", indent=2)

print(f"[OK] Exported {len(df)} records to JSON: {output_json}")
