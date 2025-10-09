# build_monday_baseline_from_closed_USERSTATUS_only.py
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from time import sleep

# ====== INPUT / OUTPUT ======
INPUT_XLSX  = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard\finalCleanOutput\comparison\newly_added_enriched.xlsx")
INPUT_SHEET = "Active"   # read IDs from here; accepts "lv_name" or "ACCOUNT ID"
OUTPUT_XLSX = INPUT_XLSX.parent / "newly_added_baseline_from_closedNEW.xlsx"

# ====== SIRIX API CONFIG ======
API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN   = "t1_a7xeQOJPnfBzuCncH60yjLFu"

# ====== HELPERS ======
def clean_id(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    try:
        s = str(int(float(s)))
    except Exception:
        pass
    return s

def london_monday_noon_for_week(dt: datetime) -> datetime:
    """Return Monday 12:00 in Europe/London for the week containing dt."""
    london = ZoneInfo("Europe/London")
    dt_l = dt.astimezone(london)
    monday = dt_l - timedelta(days=dt_l.weekday())
    return monday.replace(hour=12, minute=0, second=0, microsecond=0)

def to_dt(s: str) -> datetime | None:
    """Parse ISO 8601 (handles trailing 'Z'). Returns timezone-aware dt (UTC if no tz)."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt
    except Exception:
        return None

def to_iso_z(dt: datetime) -> str:
    return dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")

def fetch_userstatus(user_id: str) -> dict | None:
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "UserID": str(user_id),
        "GetOpenPositions": True,
        "GetPendingPositions": True,
        "GetClosePositions": True,
        "GetMonetaryTransactions": True,
    }
    try:
        r = requests.post(API_URL, headers=headers, json=payload, timeout=60)
    except Exception as e:
        print(f"[!] Network error for {user_id}: {e}")
        return None

    if r.status_code != 200:
        print(f"[!] HTTP {r.status_code} for {user_id}: {r.text[:200]}")
        return None
    try:
        return r.json() or {}
    except Exception as e:
        print(f"[!] JSON decode error for {user_id}: {e}")
        return None

def extract_fields(obj: dict):
    """Return (full_name, creation_dt, plan_amount, closed_positions:list, creation_iso_raw)."""
    ud = obj.get("UserData") or {}
    det = ud.get("UserDetails") or {}
    full_name = det.get("FullName")

    creation_iso = det.get("CreationTime")
    creation_dt = to_dt(creation_iso) if creation_iso else None

    # Fallback: earliest MonetaryTransactions Time if CreationTime missing
    if not creation_dt:
        mt = obj.get("MonetaryTransactions") or []
        times = [to_dt(m.get("Time")) for m in mt if m.get("Time")]
        times = [t for t in times if t is not None]
        if times:
            creation_dt = min(times)
            creation_iso = to_iso_z(creation_dt)

    # Plan from MonetaryTransactions where Comment startswith "Initial balance"
    plan_amt = None
    for m in (obj.get("MonetaryTransactions") or []):
        c = str(m.get("Comment", "")).lower()
        if c.startswith("initial balance"):
            try:
                plan_amt = float(m.get("Amount")) if m.get("Amount") is not None else None
            except Exception:
                plan_amt = None
            break

    closed = obj.get("ClosedPositions") or []
    return full_name, creation_dt, plan_amt, closed, creation_iso

def sum_totalprofit_in_window(closed_rows: list, start_dt: datetime, end_dt: datetime) -> float:
    total = 0.0
    for c in closed_rows:
        ct = to_dt(c.get("CloseTime"))
        if ct is None:
            continue
        if ct < start_dt or ct > end_dt:
            continue
        total += float(c.get("TotalProfit") or 0.0)
    return total

# ====== MAIN ======
def main():
    if not INPUT_XLSX.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_XLSX}")

    # IDs from Active sheet
    df = pd.read_excel(INPUT_XLSX, sheet_name=INPUT_SHEET)
    cols = {c.strip().lower(): c for c in df.columns}
    id_col = cols.get("lv_name") or cols.get("account id") or cols.get("account_id")
    if not id_col:
        raise KeyError(f"Could not find 'lv_name' or 'ACCOUNT ID' in sheet '{INPUT_SHEET}'. Columns: {list(df.columns)}")

    raw_ids = [clean_id(v) for v in df[id_col].tolist()]
    ids = [i for i in raw_ids if i]
    ids = list(dict.fromkeys(ids))  # unique, preserve order

    # endTime = this week's Monday 12:00 (Europe/London)
    now_london = datetime.now(ZoneInfo("Europe/London"))
    end_local = london_monday_noon_for_week(now_london)
    end_utc = end_local.astimezone(ZoneInfo("UTC"))
    end_iso = to_iso_z(end_local)

    print(f"[INFO] Processing {len(ids)} accounts")
    print(f"[INFO] endTime (Europe/London): {end_local:%Y-%m-%d %H:%M}")
    print(f"[INFO] endTime (UTC)          : {end_iso}\n")

    out_rows = []

    for i, uid in enumerate(ids, 1):
        print(f"[{i}/{len(ids)}] UserID {uid} → GetUserTransactions …")
        data = fetch_userstatus(uid)
        if not data:
            print("    [!] No data. Skipping.")
            continue

        full_name, creation_dt, plan_amt, closed_rows, creation_iso_raw = extract_fields(data)

        if not creation_dt:
            print("    [!] Missing CreationTime; cannot compute window. Row will have nulls.")
            out_rows.append({
                "lv_name": uid,
                "lv_accountidname": full_name,
                "plan": None if plan_amt is None else round(plan_amt, 2),
                "startTime": None,
                "endTime": end_iso,
                "TotalClosedPnL": None,
                "baseline_equity": None
            })
            sleep(0.2)
            continue

        start_dt = creation_dt.astimezone(ZoneInfo("UTC"))
        # If account created after endTime, window is empty (we’ll get zero)
        total_closed = sum_totalprofit_in_window(closed_rows, start_dt, end_utc)
        baseline_equity = (plan_amt or 0.0) + (total_closed or 0.0)

        print(f"    FullName         : {full_name}")
        print(f"    startTime (UTC)  : {to_iso_z(start_dt)}")
        print(f"    endTime (UTC)    : {end_iso}")
        print(f"    Plan             : {plan_amt}")
        print(f"    Closed in window : {len(closed_rows)} rows (filtered)")
        print(f"    TotalClosedPnL   : {round(total_closed, 2)}")
        print(f"    baseline_equity  : {round(baseline_equity, 2)}")

        out_rows.append({
            "lv_name": uid,
            "lv_accountidname": full_name,
            "plan": None if plan_amt is None else round(plan_amt, 2),
            "startTime": to_iso_z(start_dt),
            "endTime": end_iso,
            "TotalClosedPnL": round(total_closed, 2),
            "baseline_equity": round(baseline_equity, 2)
        })
        sleep(0.2)  # politeness

    out_df = pd.DataFrame(out_rows, columns=[
        "lv_name", "lv_accountidname", "plan", "startTime", "endTime", "TotalClosedPnL", "baseline_equity"
    ])

    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
        out_df.to_excel(xw, sheet_name="Baseline_ClosedPnL", index=False)

    print(f"\n[OK] Saved: {OUTPUT_XLSX}")
    print(f"[SUMMARY] Rows: {len(out_df)} | Computed baselines: {(out_df['baseline_equity'].notna()).sum()}")

if __name__ == "__main__":
    main()
