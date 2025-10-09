# build_monday_baseline_from_closed.py
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from time import sleep

# ========= INPUT / OUTPUT =========
INPUT_XLSX = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\finalCleanOutput\comparison\newly_added_enriched.xlsx")
INPUT_SHEET = "Active"   # where we read IDs from; supports either "ACCOUNT ID" or "lv_name"
OUTPUT_XLSX = INPUT_XLSX.parent / "newly_added_baseline_from_closed.xlsx"

# ========= SIRIX API CONFIG =========
BASE_URL = "https://restapi-real3.sirixtrader.com"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"  # <-- update if needed

# Endpoints
USER_TXN_ENDPOINT = "/api/UserStatus/GetUserTransactions"
CLOSED_POS_ENDPOINT = "/api/ManagementService/GetClosedPositionsForUser"

# ========= BEHAVIOR =========
# Only PnL component to sum from closed positions
INCLUDE_ROLLOVER = False
INCLUDE_COMMISSION = False

# ========= HELPERS =========
def clean_user_id(value):
    """Coerce to canonical string ID like '183623'."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    try:
        s = str(int(float(s)))
    except Exception:
        pass
    return s

def monday_noon_london_for_week(dt: datetime) -> datetime:
    """Return Monday 12:00 (Europe/London) for the week containing dt."""
    london = ZoneInfo("Europe/London")
    dt_l = dt.astimezone(london)
    monday = dt_l - timedelta(days=dt_l.weekday())
    return monday.replace(hour=12, minute=0, second=0, microsecond=0)

def to_iso_z(dt: datetime) -> str:
    """Timezone-aware datetime -> RFC3339 Z string."""
    dt_utc = dt.astimezone(ZoneInfo("UTC"))
    return dt_utc.isoformat().replace("+00:00", "Z")

def post_json(path: str, payload: dict):
    url = BASE_URL + path
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    return r

def get_user_transactions(user_id: str):
    # This endpoint bundles UserData + Closed + Monetary + Open + Pending (based on provider)
    payload = {
        "UserID": user_id,
        "GetOpenPositions": False,
        "GetPendingPositions": False,
        "GetClosePositions": False,
        "GetMonetaryTransactions": True
    }
    r = post_json(USER_TXN_ENDPOINT, payload)
    if r.status_code != 200:
        raise RuntimeError(f"UserTransactions {user_id}: HTTP {r.status_code} - {r.text[:200]}")
    return r.json() or {}

def get_closed_positions(user_id: str, start_iso_utc: str, end_iso_utc: str):
    payload = {
        "userID": user_id,
        "startTime": start_iso_utc,
        "endTime": end_iso_utc
    }
    r = post_json(CLOSED_POS_ENDPOINT, payload)
    if r.status_code != 200:
        raise RuntimeError(f"ClosedPositions {user_id}: HTTP {r.status_code} - {r.text[:200]}")
    obj = r.json() or {}
    return obj.get("ClosedPositions", []) or []

def extract_fullname_creation_plan(user_obj: dict):
    """From GetUserTransactions response, pull FullName, CreationTime, Plan (Initial balance ... Amount)."""
    user_data = user_obj.get("UserData") or {}
    details = user_data.get("UserDetails") or {}
    fullname = details.get("FullName")

    # Prefer UserDetails.CreationTime if present; fallback to earliest MonetaryTransactions Time
    creation_iso = details.get("CreationTime")
    if not creation_iso:
        mt = user_obj.get("MonetaryTransactions") or []
        if mt:
            # Take min Time as creation proxy
            try:
                creation_iso = min((m.get("Time") for m in mt if m.get("Time")), default=None)
            except Exception:
                creation_iso = None

    # Plan: Comment startswith "Initial balance" (case-insensitive); take Amount
    plan = None
    for t in (user_obj.get("MonetaryTransactions") or []):
        comment = str(t.get("Comment", "")).lower()
        if comment.startswith("initial balance"):
            plan = t.get("Amount")
            break

    # Normalize types/strings
    plan_val = None
    try:
        plan_val = float(plan) if plan is not None else None
    except Exception:
        plan_val = None

    # Ensure creation time is timezone-aware for safety (assumed ISO with Z/offset)
    creation_dt = None
    if creation_iso:
        try:
            creation_dt = datetime.fromisoformat(creation_iso.replace("Z", "+00:00"))
            if creation_dt.tzinfo is None:
                creation_dt = creation_dt.replace(tzinfo=ZoneInfo("UTC"))
        except Exception:
            creation_dt = None

    return fullname, creation_dt, plan_val, creation_iso

def sum_closed_pnl(closed_positions: list):
    """Sum Profit (and optionally rollover/commission) by your switches."""
    total = 0.0
    roll = 0.0
    comm = 0.0
    for c in closed_positions:
        total += float(c.get("ProfitInAccountCurrency") or 0)
        if INCLUDE_ROLLOVER:
            roll += float(c.get("RolloverInAccountCurrency") or 0)
        if INCLUDE_COMMISSION:
            comm += float(c.get("CommissionInAccountCurrency") or 0)
    return total + roll + comm

# ========= MAIN =========
def main():
    # Load IDs from the Active sheet (accept ACCOUNT ID or lv_name)
    if not INPUT_XLSX.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX, sheet_name=INPUT_SHEET)
    cols = {c.strip().lower(): c for c in df.columns}
    id_col = cols.get("lv_name") or cols.get("account id") or cols.get("account_id")
    if not id_col:
        raise KeyError(f"Could not find 'lv_name' or 'ACCOUNT ID' column in sheet '{INPUT_SHEET}'. "
                       f"Columns present: {list(df.columns)}")

    # Compute this week's Monday 12:00 Europe/London as common endTime
    now = datetime.now(ZoneInfo("Europe/London"))
    monday_noon_local = monday_noon_london_for_week(now)
    end_iso_utc = to_iso_z(monday_noon_local)

    ids = []
    for v in df[id_col].tolist():
        cid = clean_user_id(v)
        if cid:
            ids.append(cid)

    ids_unique = list(dict.fromkeys(ids))
    print(f"[INFO] Accounts to process: {len(ids_unique)}")
    print(f"[INFO] endTime (Europe/London): {monday_noon_local:%Y-%m-%d %H:%M}")
    print(f"[INFO] endTime (UTC): {end_iso_utc}\n")

    out_rows = []

    for i, uid in enumerate(ids_unique, 1):
        print(f"[{i}/{len(ids_unique)}] UserID {uid} → fetching user transactions …")
        try:
            utx = get_user_transactions(uid)
        except Exception as e:
            print(f"    [!] Failed GetUserTransactions: {e}")
            continue

        fullname, creation_dt, plan_val, creation_iso_raw = extract_fullname_creation_plan(utx)

        if not creation_dt:
            print("    [!] Missing CreationTime. Skipping closed PnL sum.")
            row = {
                "lv_name": uid,
                "lv_accountidname": fullname,
                "plan": plan_val,
                "startTime": None,
                "endTime": end_iso_utc,
                "TotalClosedPnL": None,
                "baseline_equity": None if plan_val is None else plan_val
            }
            out_rows.append(row)
            sleep(0.2)
            continue

        # startTime = account CreationTime (as per your design)
        start_iso_utc = to_iso_z(creation_dt)

        # If the account was created after Monday noon, you'll get a shorter window (that’s OK)
        print(f"    CreationTime (startTime) UTC: {start_iso_utc}")
        print(f"    endTime UTC                : {end_iso_utc}")
        print(f"    Plan amount               : {plan_val}")

        try:
            closed = get_closed_positions(uid, start_iso_utc, end_iso_utc)
        except Exception as e:
            print(f"    [!] Failed GetClosedPositions: {e}")
            closed = []

        total_closed = sum_closed_pnl(closed)
        baseline_equity = (plan_val or 0.0) + (total_closed or 0.0)

        print(f"    Closed positions fetched  : {len(closed)}")
        print(f"    TotalClosedPnL            : {round(total_closed, 2)}")
        print(f"    baseline_equity (= plan + PnL): {round(baseline_equity, 2)}")

        row = {
            "lv_name": uid,
            "lv_accountidname": fullname,
            "plan": round(plan_val, 2) if plan_val is not None else None,
            "startTime": start_iso_utc,
            "endTime": end_iso_utc,
            "TotalClosedPnL": round(total_closed, 2),
            "baseline_equity": round(baseline_equity, 2)
        }
        out_rows.append(row)
        sleep(0.2)  # politeness / rate limit

    out_df = pd.DataFrame(out_rows, columns=[
        "lv_name", "lv_accountidname", "plan", "startTime", "endTime", "TotalClosedPnL", "baseline_equity"
    ])

    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
        out_df.to_excel(xw, sheet_name="Baseline_ClosedPnL", index=False)

    print("\n[OK] Saved:", OUTPUT_XLSX)
    nones = out_df["baseline_equity"].isna().sum()
    print(f"[SUMMARY] Rows: {len(out_df)} | Baselines computed: {len(out_df) - nones} | Missing: {nones}")

if __name__ == "__main__":
    main()
