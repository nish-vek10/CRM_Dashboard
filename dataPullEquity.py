import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, inspect, text
import urllib

# === Output folder ===
output_folder = Path("C:/Users/anish/Desktop/Anish/CRM API/CRM Dashboard/SecondDB_Debug")
output_folder.mkdir(parents=True, exist_ok=True)

# === Connection parameters — to confirm ===
server = "crmrepl3.LEVERATETECH.COM"  # Confirm this is the correct server
database = "etwotprop_mscrm"          # Confirm this is the correct database
username = "Repl_UsereTwotprop"
password = "WeyxeOWWjctYoBF"

print("[WARNING] Please confirm if the server, port, and database name are correct.")
print(f" - Server: {server}")
print(f" - Database: {database}")
print("If these are incorrect, update them before proceeding.\n")

# === Create SQLAlchemy engine ===
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;TrustServerCertificate=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
inspector = inspect(engine)

# === Print available schemas ===
try:
    print("[...] Fetching available schemas...")
    schemas = inspector.get_schema_names()
    print(f"[DEBUG] Schemas found: {schemas}")
except Exception as e:
    print(f"[ERROR] Could not retrieve schemas: {e}")

# === 1. Try INFORMATION_SCHEMA.TABLES ===
try:
    print("\n[TEST] Querying INFORMATION_SCHEMA.TABLES...")
    df_info = pd.read_sql("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", engine)
    print(f"[OK] Found {len(df_info)} tables in INFORMATION_SCHEMA.")
    print(df_info.head())
    df_info.to_excel(output_folder / "info_schema_tables.xlsx", index=False)
except Exception as e:
    print(f"[ERROR] Failed to query INFORMATION_SCHEMA.TABLES: {e}")

# === 2. Try sys.tables ===
try:
    print("\n[TEST] Querying sys.tables and related metadata...")
    query = """
    SELECT 
        s.name AS schema_name,
        t.name AS table_name
    FROM 
        sys.tables t
    INNER JOIN 
        sys.schemas s ON t.schema_id = s.schema_id
    ORDER BY 
        s.name, t.name
    """
    df_sys = pd.read_sql(text(query), engine)
    print(f"[OK] Found {len(df_sys)} tables in sys.tables.")
    print(df_sys.head())
    df_sys.to_excel(output_folder / "sys_tables.xlsx", index=False)
except Exception as e:
    print(f"[ERROR] Failed to query sys.tables: {e}")

print("\n[DONE] Debug queries completed.")
