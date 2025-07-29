import pandas as pd
from pathlib import Path

# === 1. Define base directory ===
base_path = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard")

# === 2. Input/Output Files ===
input_excel = base_path / "merged_clean_output.xlsx"
output_json = base_path / "merged_clean_data.json"

# === 3. Load Excel and export to JSON ===
df = pd.read_excel(input_excel)

# Export ALL records to JSON with indentation
df.to_json(output_json, orient="records", indent=2)

print(f"[OK] Exported {len(df)} records to JSON: {output_json}")
