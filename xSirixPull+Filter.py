import pandas as pd
import requests
import json
import sys
import json as _json
from pathlib import Path
from time import sleep, time
from datetime import datetime, timedelta
from collections import Counter
import os

# === Config ===
INPUT_XLSX = "C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/finalCleanOutput/Lv_tpaccount.xlsx"
OUTPUT_DIR = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/finalCleanOutput")
OUTPUT_JSON = OUTPUT_DIR / "crm_sirix_enrichedNEW.json"
OUTPUT_XLSX = OUTPUT_DIR / "crm_sirix_enrichedNEW.xlsx"
BASELINE_JSON = OUTPUT_DIR / "baseline_equityNEW.json"

# ---------------- Baseline helpers ----------------
def load_baseline():
    """Return (baseline_at_dt, baseline_map) or (None, {})."""
    try:
        if BASELINE_JSON.exists():
            obj = _json.loads(BASELINE_JSON.read_text(encoding="utf-8"))
            ts = obj.get("baseline_at")
            base = obj.get("equity", {})
            baseline_at = datetime.fromisoformat(ts) if ts else None
            return baseline_at, base
    except Exception as e:
        print(f"[WARN] Failed to read baseline: {e}")
    return None, {}

def save_baseline(baseline_at_dt, baseline_map):
    try:
        BASELINE_JSON.write_text(
            _json.dumps({
                "baseline_at": baseline_at_dt.isoformat(timespec="seconds"),
                "equity": baseline_map
            }, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        print(f"[OK] Baseline saved at {baseline_at_dt}.")
    except Exception as e:
        print(f"[ERROR] Failed to save baseline: {e}")

def need_new_week(baseline_at_dt, now_dt):
    """
    True if no baseline or it's from a previous competition window.
    Competition window starts each Monday at 12:00 local.
    """
    if baseline_at_dt is None:
        return True
    # compute this week's Monday 12:00
    monday_noon = get_monday_noon(now_dt)
    # if baseline is before this week's Monday 12:00 -> new week
    return baseline_at_dt < monday_noon

def get_monday_noon(dt):
    """Return datetime for Monday 12:00 of week containing dt (Monday=0)."""
    monday = dt - timedelta(days=dt.weekday())
    monday_noon = datetime(monday.year, monday.month, monday.day, 12, 0, 0)
    return monday_noon

def next_monday_noon_on_or_after(dt):
    """Return next Monday 12:00 that is >= dt."""
    mn = get_monday_noon(dt)
    if dt <= mn:
        return mn
    # go to next week
    return mn + timedelta(days=7)

def next_4h_tick_wallclock(now_dt):
    """Return the next wall-clock 4h boundary (00, 04, 08, 12, 16, 20)."""
    # round hour up to next multiple of 4
    next_hour = ((now_dt.hour // 4) + 1) * 4
    day = now_dt.date()
    if next_hour >= 24:
        next_hour -= 24
        day = day + timedelta(days=1)
    return datetime.combine(day, datetime.min.time()).replace(hour=next_hour)

def next_2h_tick_wallclock(now_dt):
    """Return the next wall-clock 2h boundary (00, 02, 04, ..., 22)."""
    next_hour = ((now_dt.hour // 2) + 1) * 2
    day = now_dt.date()
    if next_hour >= 24:
        next_hour -= 24
        day = day + timedelta(days=1)
    return datetime.combine(day, datetime.min.time()).replace(hour=next_hour)



API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"

# TESTING SWITCH
TEST_MODE = True   # set to False for real weekly behavior

# One-off kick: run a fetch immediately on startup, then continue the 2h schedule
RUN_NOW_ON_START = True


# === Weekly reset reminder (non-blocking) ===
def weekly_reset_reminder():
    now = datetime.now()
    if BASELINE_JSON.exists():
        mtime = datetime.fromtimestamp(BASELINE_JSON.stat().st_mtime)
        print(
            f"[REMINDER] Weekly reset uses 'baseline_equityNEW.json'. "
            f"File FOUND (last updated: {mtime:%Y-%m-%d %H:%M}).\n"
            f"          If this is the Monday 12:00 reset run, DELETE this file first."
        )
    else:
        print("[INFO] 'baseline_equityNEW.json' not found — fresh weekly baselines will be seeded this run.")

weekly_reset_reminder()


# # === 1. Load CRM Data ===
# df = pd.read_excel(INPUT_XLSX)
# print(f"[INFO] Loaded {len(df):,} CRM rows.")
#
#
# # === 2. Apply filter (remove 'Purchases') ===
# before = len(df)
# df = df[~df['Lv_TempName'].fillna('').str.contains('Purchases', case=False)]
# print(f"[FILTER] Removed Purchases -> {len(df):,} rows (from {before:,}).")
#
# # Normalize the index so progress counts are 1..N
# df = df.reset_index(drop=True)
# total = len(df)



# === Load (or initialise) weekly baseline ===
baseline = {}
if BASELINE_JSON.exists():
    try:
        baseline = json.loads(BASELINE_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[WARN] Could not read baseline file: {e}. Starting fresh.")
        baseline = {}
# normalize keys to strings
baseline = {str(k): v for k, v in baseline.items()}
baseline_updated = False  # will set True when we seed new baselines mid-week


# === 3. API fetch function ===
def fetch_sirix_data(user_id):
    """Fetch Country, Plan, Balance, Equity, OpenPnL for one account + blown-up flag."""
    try:
        if pd.isna(user_id):
            return None

        clean_user_id = str(int(float(user_id))).strip()
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "UserID": clean_user_id,
            "GetOpenPositions": False,
            "GetPendingPositions": False,
            "GetClosePositions": False,
            "GetMonetaryTransactions": True  # needed for Plan + accounts blown detection
        }

        resp = requests.post(API_URL, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            print(f"[!] API error {resp.status_code} for {clean_user_id}")
            return None
        data = resp.json()

        # Country
        country = (data.get("UserData") or {}).get("UserDetails", {}).get("Country")

        # Account Balance
        bal = (data.get("UserData") or {}).get("AccountBalance") or {}
        balance = bal.get("Balance")
        equity = bal.get("Equity")
        open_pnl = bal.get("OpenPnL")

        # GroupName (API-side "Purchases" safety filter)
        group_name = ((data.get("UserData") or {}).get("GroupInfo") or {}).get("GroupName")
        is_purchase_group = "purchase" in str(group_name or "").lower()

        # Monetary transactions
        txns = data.get("MonetaryTransactions") or []

        # Detect blown-up: any transaction with comment containing "Zero Balance"
        blown_up = any("zero balance" in str(t.get("Comment", "")).lower() for t in txns)

        # Plan: look for MonetaryTransaction with Comment starting "Initial balance"
        plan = None
        for t in data.get("MonetaryTransactions") or []:
            if str(t.get("Comment", "")).lower().startswith("initial balance"):
                plan = t.get("Amount")
                break

        return {
            "Country": country,
            "Plan": plan,
            "Balance": balance,
            "Equity": equity,
            "OpenPnL": open_pnl,
            "BlownUp": blown_up,
            "GroupName": group_name,
            "IsPurchaseGroup": is_purchase_group,
        }

    except Exception as e:
        print(f"[!] Exception for UserID {user_id}: {e}")
        return None


def run_once(mode, baseline_map, baseline_at_dt):
    """
    mode: 'baseline' | 'update'
      - 'baseline': seed baseline_map from current equity (no PctChange)
      - 'update'  : compute PctChange = (equity - baseline_equity) / baseline_equity * 100
    baseline_map: dict[str(account_id)] -> float(equity at baseline)
    baseline_at_dt: datetime of when baseline was created (informational)
    """
    print(f"[RUN] Mode = {mode} | Baseline at = {baseline_at_dt}")

    results = []  # Active
    blown_results = []  # Blown-up
    purchases_results = []  # API GroupName == Purchases
    plan50k_results = []  # Plan == 50000
    seen_ids = []

    start_time_local = time()

    # === Load + filter CRM (your existing Step 1–2 logic reused) ===
    df = pd.read_excel(INPUT_XLSX)
    before = len(df)
    df = df[~df['Lv_TempName'].fillna('').str.contains('Purchases', case=False)]
    df = df.reset_index(drop=True)
    total = len(df)
    print(f"[INFO] Loaded {before:,} CRM rows -> {len(df):,} after filter.")

    # === Iterate accounts ===
    for i, row in df.iterrows():
        user_id = row.get("Lv_name")
        print(f"[{i+1}/{total}] Fetching UserID: {user_id} ...")
        sirix_data = fetch_sirix_data(user_id)

        if sirix_data and sirix_data.get("BlownUp"):  # you added this flag earlier
            print(f"    ↳ [BLOWN-UP] UserID {user_id} -> BlownUp sheet.")
            blown_results.append({
                "CUSTOMER NAME": row.get("lv_accountidName"),
                "ACCOUNT ID": row.get("Lv_name"),
                "Country": sirix_data.get("Country"),
                "Plan": sirix_data.get("Plan"),
                "Balance": sirix_data.get("Balance"),
                "Equity": sirix_data.get("Equity"),
                "OpenPnL": sirix_data.get("OpenPnL"),
                "PctChange": None
            })
            seen_ids.append(user_id)
            sleep(0.2)
            continue

        # Secondary filter: API GroupName says "Purchases" -> route to Purchases sheet
        if sirix_data and sirix_data.get("IsPurchaseGroup"):
            purchases_results.append({
                "CUSTOMER NAME": row.get("lv_accountidName"),
                "ACCOUNT ID": row.get("Lv_name"),
                "Country": sirix_data.get("Country"),
                "Plan": sirix_data.get("Plan"),
                "Balance": sirix_data.get("Balance"),
                "Equity": sirix_data.get("Equity"),
                "OpenPnL": sirix_data.get("OpenPnL"),
                "GroupName": sirix_data.get("GroupName"),
                "PctChange": None
            })
            print(
                f"    ↳ [PURCHASES(API)] UserID {user_id} -> Purchases sheet (GroupName='{sirix_data.get('GroupName')}').")
            seen_ids.append(user_id)
            sleep(0.2)
            continue

        # Plan == 50000 -> route to Plan50000 sheet
        if sirix_data:
            plan_raw = sirix_data.get("Plan")
            try:
                plan_val = float(plan_raw) if plan_raw is not None else None
            except (TypeError, ValueError):
                plan_val = None

            if plan_val is not None and abs(plan_val - 50000.0) < 1e-6:
                plan50k_results.append({
                    "CUSTOMER NAME": row.get("lv_accountidName"),
                    "ACCOUNT ID": row.get("Lv_name"),
                    "Country": sirix_data.get("Country"),
                    "Plan": sirix_data.get("Plan"),
                    "Balance": sirix_data.get("Balance"),
                    "Equity": sirix_data.get("Equity"),
                    "OpenPnL": sirix_data.get("OpenPnL"),
                    "PctChange": None
                })
                print(f"    ↳ [PLAN=50000] UserID {user_id} -> Plan50000 sheet.")
                seen_ids.append(user_id)
                sleep(0.2)
                continue

        if not sirix_data:
            sirix_data = {"Country": None, "Plan": None, "Balance": None, "Equity": None, "OpenPnL": None}
            pct_change = None
        else:
            equity = sirix_data.get("Equity")
            # --- compute PctChange against baseline equity (for updates only) ---
            if mode == "update":
                base_eq = baseline_map.get(str(user_id))
                pct_change = None
                if base_eq not in (None, 0) and equity not in (None,):
                    try:
                        pct_change = ((equity - base_eq) / base_eq) * 100
                    except Exception:
                        pct_change = None
            else:
                pct_change = None  # baseline run doesn't compute % yet

        # if baseline mode, capture baseline equity for this account
        if mode == "baseline":
            eq = sirix_data.get("Equity")
            if eq is not None:
                baseline_map[str(user_id)] = float(eq)

        results.append({
            "CUSTOMER NAME": row.get("lv_accountidName"),
            "ACCOUNT ID": row.get("Lv_name"),
            "Country": sirix_data.get("Country"),
            "Plan": sirix_data.get("Plan"),
            "Balance": sirix_data.get("Balance"),
            "Equity": sirix_data.get("Equity"),
            "OpenPnL": sirix_data.get("OpenPnL"),
            "PctChange": pct_change
        })
        seen_ids.append(user_id)
        sleep(0.2)  # rate limit

    # === Save outputs ===
    enriched_df = pd.DataFrame(results)             # Active
    blown_df = pd.DataFrame(blown_results)          # Blown-up
    purchases_df = pd.DataFrame(purchases_results)  # Purchases
    plan50k_df = pd.DataFrame(plan50k_results)      # 50K Balance

    # --- NEW: sort Active by PctChange descending (only in 'update' mode) ---
    if mode == "update":
        pre_len = len(enriched_df)
        # Coerce to numeric for safe sorting; None/invalid -> NaN
        enriched_df["_sort"] = pd.to_numeric(enriched_df["PctChange"], errors="coerce")

        # Stable sort so equal % keep their original relative order; NaN goes last
        enriched_df = (
            enriched_df
            .sort_values(by="_sort", ascending=False, na_position="last", kind="mergesort")
            .drop(columns="_sort")
        )

        same_len = (len(enriched_df) == pre_len)
        top3 = enriched_df["PctChange"].head(3).tolist()
        print(f"[SORT] Active sheet sorted by PctChange (desc, NaN/None last). "
              f"Rows unchanged: {same_len}. Top3 PctChange: {top3}")
    else:
        print("[SORT] Baseline mode: PctChange not computed — skipping sort for Active.")

    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as writer:
        enriched_df.to_excel(writer, index=False, sheet_name="Active")
        blown_df.to_excel(writer, index=False, sheet_name="BlownUp")
        purchases_df.to_excel(writer, index=False, sheet_name="Purchases_API")
        plan50k_df.to_excel(writer, index=False, sheet_name="Plan50000")

        # Also store the baseline (for visibility) if we have it
        if baseline_map:
            pd.DataFrame(
                [{"ACCOUNT ID": k, "BaselineEquity": v} for k, v in baseline_map.items()]
            ).to_excel(writer, index=False, sheet_name="Baseline")

    # atomic JSON write (Active only)
    _tmp = OUTPUT_JSON.with_suffix(".json.tmp")
    enriched_df.to_json(_tmp, orient="records")
    _tmp.replace(OUTPUT_JSON)

    # === Summary ===
    dup_counts = Counter(seen_ids)
    duplicates = {uid: cnt for uid, cnt in dup_counts.items() if cnt > 1}

    print(f"\n[OK] Saved:\n - {OUTPUT_XLSX}\n - {OUTPUT_JSON} (Active only)")
    print("\n===== SUMMARY =====")
    print(f"Total processed: {len(seen_ids)}")
    print(f"Unique IDs     : {len(dup_counts)}")
    print(f"Duplicates     : {len(duplicates)}")
    if duplicates:
        for uid, cnt in duplicates.items():
            print(f" - {uid} ({cnt} times)")
    print(f"Blown-up       : {len(blown_results)} (sheet: BlownUp)")
    print(f"Purchases(API) : {len(purchases_results)} (sheet: Purchases_API)")
    print(f"Plan=50000     : {len(plan50k_results)} (sheet: Plan50000)")
    print(f"Active (final) : {len(results)} (sheet: Active)")

    elapsed = int(time() - start_time_local)
    mm, ss = divmod(elapsed, 60)
    print(f"[PROCESS COMPLETE] Run time: {mm:02d}:{ss:02d} (MM:SS)")


if __name__ == "__main__":
    print("[SERVICE] E2T weekly loop started. Ctrl+C to stop.")
    while True:
        now = datetime.now()

        # Load baseline (if present)
        baseline_at_dt, baseline_map = load_baseline()

        # --- TEST vs REAL scheduling ---
        if TEST_MODE:
            # TEST: if no baseline yet (e.g., mid-week), seed baseline immediately.
            if baseline_at_dt is None or need_new_week(baseline_at_dt, now):
                print("[TEST MODE] Forcing baseline seeding now (ignoring Monday 12:00 rule).")
                baseline_map = {}
                baseline_at_dt = now
                run_once(mode="baseline", baseline_map=baseline_map, baseline_at_dt=baseline_at_dt)
                save_baseline(baseline_at_dt, baseline_map)
            # After baseline (new or existing), schedule next 2h/4h tick
            next_run = next_2h_tick_wallclock(datetime.now())
        else:
            # REAL weekly behavior
            if need_new_week(baseline_at_dt, now):
                # No baseline for this week -> wait until Monday 12:00 (or run now if we just passed it)
                target = next_monday_noon_on_or_after(now)
                if now >= target:
                    print("[SCHED] Seeding new weekly baseline now.")
                    baseline_map = {}
                    baseline_at_dt = now
                    run_once(mode="baseline", baseline_map=baseline_map, baseline_at_dt=baseline_at_dt)
                    save_baseline(baseline_at_dt, baseline_map)
                    next_run = next_2h_tick_wallclock(datetime.now())
                else:
                    secs = (target - now).total_seconds()
                    print(f"[SCHED] Waiting until Monday 12:00 to seed baseline (~{int(secs // 3600)}h).")
                    sleep(max(5.0, secs))
                    continue
            else:
                next_run = next_2h_tick_wallclock(now)

        # --- One-off immediate run (optional) ---
        if RUN_NOW_ON_START:
            print("[RUN-NOW] Performing one immediate fetch now (then resume 2h schedule).")
            # Reload baseline in case it changed just now
            baseline_at_dt, baseline_map = load_baseline()

            if TEST_MODE and (baseline_at_dt is None or need_new_week(baseline_at_dt, datetime.now())):
                # In TEST_MODE we allow seeding baseline immediately if missing/outdated
                print("[RUN-NOW] TEST_MODE: baseline missing/outdated -> seeding baseline now.")
                baseline_map = {}
                baseline_at_dt = datetime.now()
                run_once(mode="baseline", baseline_map=baseline_map, baseline_at_dt=baseline_at_dt)
                save_baseline(baseline_at_dt, baseline_map)
            else:
                # Respect real Monday-12:00 rule; if baseline is missing, this update will just have PctChange=None
                if need_new_week(baseline_at_dt, datetime.now()):
                    print("[RUN-NOW] Baseline missing/outdated; running update anyway (PctChange may be NaN).")
                run_once(mode="update", baseline_map=baseline_map, baseline_at_dt=baseline_at_dt)

            # After the immediate run, reset the schedule to the next even-hour boundary
            next_run = next_2h_tick_wallclock(datetime.now())
            RUN_NOW_ON_START = False

        # Sleep until the next run time
        now = datetime.now()
        if next_run > now:
            secs = (next_run - now).total_seconds()
            hh = int(secs // 3600)
            mm = int((secs % 3600) // 60)
            ss = int(secs % 60)
            print(f"[SCHED] Next update at {next_run} (in {hh:02d}:{mm:02d}:{ss:02d}).")
            sleep(secs)

        # On wake, run update with current baseline
        # Reload baseline in case you manually replaced it while sleeping
        baseline_at_dt, baseline_map = load_baseline()

        if need_new_week(baseline_at_dt, datetime.now()):
            # Edge case: someone deleted or replaced baseline while sleeping
            print("[SCHED] Baseline missing/outdated on wake; switching to baseline seeding.")
            baseline_map = {}
            baseline_at_dt = datetime.now()
            run_once(mode="baseline", baseline_map=baseline_map, baseline_at_dt=baseline_at_dt)
            save_baseline(baseline_at_dt, baseline_map)
        else:
            # Regular 2-hour update
            run_once(mode="update", baseline_map=baseline_map, baseline_at_dt=baseline_at_dt)