import pandas as pd
from sqlalchemy import create_engine
import urllib

# Server and credentials
server = "crmrepl3.LEVERATETECH.COM, 1433"
uid = "Repl_UsereTwotprop"
pwd = "WeyxeOWWjctYoBF"

candidate_dbs = ['sirix_replication', 'trading_platform', 'leverate_trading', 'sirix_snapshot', 'etwotprop_mscrm', 'etwotprop_platform', 'leveratecrm', 'replication_db', 'accounts_data', 'e2t_trading', 'crm_snapshot']
keywords = ['equity', 'snapshot', 'balance', 'account', 'history', 'pnl']

for db in candidate_dbs:
    try:
        print(f"\n[TRY] Connecting to database: {db}")
        params = urllib.parse.quote_plus(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={db};"
            f"UID={uid};"
            f"PWD={pwd};"
            f"Encrypt=yes;TrustServerCertificate=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

        df = pd.read_sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", engine)
        match_df = df[df['TABLE_NAME'].str.lower().str.contains('|'.join(keywords))]

        if not match_df.empty:
            print(f"[MATCH] Found relevant tables in {db}:")
            print(match_df)
        else:
            print(f"[SKIP] No matching tables found in {db}.")
    except Exception as e:
        print(f"[ERROR] Could not connect to {db}: {e}")
