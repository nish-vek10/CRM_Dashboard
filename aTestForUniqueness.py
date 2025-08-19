# -*- coding: utf-8 -*-
"""
Diagnostics + Full Merge (All Fields) + Dashboard Preview
--------------------------------------------------------
Reads 3 Excel exports (Lv_tpaccount.xlsx, Lv_monetarytransaction.xlsx, Account.xlsx),
runs key-health diagnostics, and writes:

  1) RowCounts / NullsBlanks / Duplicates / OverlapSummary / TrimMismatches / GuidSanity
  2) ColumnMapping sheet (for dashboard columns)
  3) MergedPreview (dashboard-shaped)
  4) MergedFull_allFields_topN (all fields from all sources, prefixed)
  5) JoinShape (rows before/after joins)
  6) SourceColumns (list of columns per file)

Usage:
- Adjust paths if needed.
- Set TOP_N to how many rows of the full all-fields merge to export.
"""

import json
import pandas as pd
from pathlib import Path
from uuid import UUID

# ============ CONFIG ============
DATA_DIR = Path(r"C:\Users\anish\OneDrive\Desktop\Anish\CRM API\CRM Dashboard")
FILE_TP  = DATA_DIR / "Lv_tpaccount.xlsx"
FILE_MT  = DATA_DIR / "Lv_monetarytransaction.xlsx"
FILE_AC  = DATA_DIR / "Account.xlsx"

OUTPUT_XLSX = DATA_DIR / "AA-diagnostics_preview_and_full.xlsx"

# How many rows to export in the big all-fields sheet (keep Excel responsive)
TOP_N = 1000

# Candidate key columns
TP_GUID_COL   = "lv_accountid"          # in Lv_tpaccount
MT_GUID_COL   = "lv_accountid"          # in Lv_monetarytransaction
AC_GUID_COL   = "AccountID"             # in Account

TP_USERID_COL = "Lv_name"               # in Lv_tpaccount
MT_USERID_COL = "lv_tpaccountidName"    # in Lv_monetarytransaction

MT_INFO_COL   = "lv_AdditionalInfo"     # JSON for Plan/Plan_SB

# Dashboard mapping
DASHBOARD_COLUMNS = [
    ("Customer Name",        ("Account", "Name")),
    ("Customer ID",          ("Account", "lv_maintpaccountidName")),
    ("Account ID",           ("Lv_tpaccount", "Lv_name")),
    ("Email",                ("Account", "EMailAddress1")),
    ("Phone Code",           ("Account", "Lv_Phone1CountryCode")),
    ("Phone Number",         ("Account", "Lv_Phone1Phone")),
    ("Country",              ("Account", "lv_countryidName")),
    ("Affiliate",            ("Account", "Lv_SubAffiliate")),
    ("Tag",                  ("Account", "Lv_Tag1")),
    ("Plan",                 ("_derived", "Plan")),
    ("Plan SB",              ("_derived", "Plan_SB")),
    ("Balance",              ("_api", "Balance")),
    ("Equity",               ("_api", "Equity")),
    ("OpenPnL",              ("_api", "OpenPnL")),
]

# ============ HELPERS ============
def norm_str(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None

def is_guid_like(x):
    try:
        if pd.isna(x):
            return False
        UUID(str(x))
        return True
    except Exception:
        return False

def coverage(left_series, right_series, normalize=False):
    if normalize:
        L = pd.Series([norm_str(v) for v in left_series if pd.notna(v)])
        R = pd.Series([norm_str(v) for v in right_series if pd.notna(v)])
    else:
        L = pd.Series([v for v in left_series if pd.notna(v)])
        R = pd.Series([v for v in right_series if pd.notna(v)])

    Ld = set(L.dropna().unique())
    Rd = set(R.dropna().unique())
    matched = Ld & Rd
    left_cov = round(100.0 * len(matched) / len(Ld), 2) if Ld else 0.0
    return len(Ld), len(Rd), len(matched), left_cov

def parse_plan_fields(val):
    if pd.isna(val):
        return pd.Series([None, None])
    try:
        info = json.loads(val)
        plan = info.get("name")
        plan_sb = None
        ch = info.get("challenges") or {}
        if isinstance(ch, dict):
            plan_sb = ch.get("funding")
        return pd.Series([plan, plan_sb])
    except Exception:
        return pd.Series([None, None])

# ============ LOAD ============
print("[1/7] Loading Excel files...")
tp = pd.read_excel(FILE_TP)
mt = pd.read_excel(FILE_MT)
ac = pd.read_excel(FILE_AC)

# Keep source column listings
source_cols = pd.DataFrame({
    "Lv_tpaccount": pd.Series(tp.columns),
    "Lv_monetarytransaction": pd.Series(mt.columns),
    "Account": pd.Series(ac.columns),
})

# ============ DIAGNOSTICS ============
print("[2/7] Running diagnostics...")

row_counts = pd.DataFrame({
    "Table": ["Lv_tpaccount", "Lv_monetarytransaction", "Account"],
    "RowCount": [len(tp), len(mt), len(ac)]
})

def null_blank_rate(df, col, label):
    if col not in df.columns:
        return {"Column": label, "NullPct": None, "Exists": False}
    s = df[col]
    nulls = s.isna().sum()
    blanks = s.astype(str).str.strip().eq("").sum() if s.dtype == object or s.dtype.name.startswith("string") else 0
    rate = 100.0 * (nulls + blanks) / len(df) if len(df) else 0.0
    return {"Column": label, "NullPct": round(rate,2), "Exists": True}

nulls_blanks = pd.DataFrame([
    null_blank_rate(ac, AC_GUID_COL, f"Account.{AC_GUID_COL}"),
    null_blank_rate(tp, TP_GUID_COL, f"Lv_tpaccount.{TP_GUID_COL}"),
    null_blank_rate(mt, MT_GUID_COL, f"Lv_monetarytransaction.{MT_GUID_COL}"),
    null_blank_rate(tp, TP_USERID_COL, f"Lv_tpaccount.{TP_USERID_COL}"),
    null_blank_rate(mt, MT_USERID_COL, f"Lv_monetarytransaction.{MT_USERID_COL}"),
])

def dup_count(df, col, label, threshold=None):
    if col not in df.columns:
        return {"Column": label, "DuplicateGroups": None, "Exists": False}
    g = df.groupby(col, dropna=True).size()
    if threshold is None:
        dups = (g[g > 1].shape[0])
    else:
        dups = (g[g > threshold].shape[0])
    return {"Column": label, "DuplicateGroups": int(dups), "Exists": True}

dups = pd.DataFrame([
    dup_count(ac, AC_GUID_COL, f"Account.{AC_GUID_COL}"),
    dup_count(tp, TP_GUID_COL, f"Lv_tpaccount.{TP_GUID_COL}"),
    dup_count(tp, TP_USERID_COL, f"Lv_tpaccount.{TP_USERID_COL}"),
    dup_count(mt, MT_GUID_COL, f"Lv_monetarytransaction.{MT_GUID_COL}", threshold=1000),
    dup_count(mt, MT_USERID_COL, f"Lv_monetarytransaction.{MT_USERID_COL}", threshold=1000),
])

ov_rows = []
if all(c in df.columns for df, c in [(tp, TP_GUID_COL), (ac, AC_GUID_COL)]):
    L,R,M,P = coverage(tp[TP_GUID_COL], ac[AC_GUID_COL], normalize=False)
    ov_rows.append(["Lv_tpaccount.lv_accountid -> Account.AccountID", L, R, M, P])

if all(c in df.columns for df, c in [(mt, MT_GUID_COL), (ac, AC_GUID_COL)]):
    L,R,M,P = coverage(mt[MT_GUID_COL], ac[AC_GUID_COL], normalize=False)
    ov_rows.append(["Lv_monetarytransaction.lv_accountid -> Account.AccountID", L, R, M, P])

if all(c in df.columns for df, c in [(mt, MT_USERID_COL), (tp, TP_USERID_COL)]):
    L,R,M,P = coverage(mt[MT_USERID_COL], tp[TP_USERID_COL], normalize=True)
    ov_rows.append(["Lv_monetarytransaction.lv_tpaccountidName -> Lv_tpaccount.Lv_name (trimmed)", L, R, M, P])

overlap = pd.DataFrame(ov_rows, columns=["Link", "LeftDistinct", "RightDistinct", "MatchedDistinct", "LeftCoveragePct"])

# Trim mismatch samples
trim_mismatches = pd.DataFrame()
if MT_USERID_COL in mt.columns and TP_USERID_COL in tp.columns:
    m_ids = pd.DataFrame({"m_raw": mt[MT_USERID_COL].astype(str, errors="ignore")})
    t_ids = pd.DataFrame({"t_raw": tp[TP_USERID_COL].astype(str, errors="ignore")})
    m_ids["m_norm"] = m_ids["m_raw"].map(norm_str)
    t_ids["t_norm"] = t_ids["t_raw"].map(norm_str)
    mm = m_ids.merge(t_ids, left_on="m_norm", right_on="t_norm", how="inner")
    trim_mismatches = mm[(mm["m_raw"] != mm["t_raw"]) & (mm["m_norm"].notna())].head(50)[["m_raw","t_raw","m_norm"]]
    trim_mismatches = trim_mismatches.rename(columns={"m_raw":"MonetaryIdRaw","t_raw":"TpNameRaw","m_norm":"TrimmedMatch"})

def guid_sanity(df, col):
    if col not in df.columns:
        return pd.DataFrame(columns=[col])
    s = df[col]
    bad = s[~s.isna()].astype(str).map(is_guid_like).eq(False)
    out = s[bad].drop_duplicates().head(20).to_frame()
    return out

guid_bad_tp = guid_sanity(tp, TP_GUID_COL)
guid_bad_mt = guid_sanity(mt, MT_GUID_COL)

# ============ PREP FOR MERGES ============
print("[3/7] Preparing joins / derived fields...")

# Normalized helper keys
tp["_guid"]   = tp[TP_GUID_COL] if TP_GUID_COL in tp.columns else pd.Series([None]*len(tp))
tp["_userid"] = tp[TP_USERID_COL].map(norm_str) if TP_USERID_COL in tp.columns else pd.Series([None]*len(tp))

mt["_guid"]   = mt[MT_GUID_COL] if MT_GUID_COL in mt.columns else pd.Series([None]*len(mt))
mt["_userid"] = mt[MT_USERID_COL].map(norm_str) if MT_USERID_COL in mt.columns else pd.Series([None]*len(mt))

ac["_guid"]   = ac[AC_GUID_COL] if AC_GUID_COL in ac.columns else pd.Series([None]*len(ac))

# Derived Plan fields from monetary
if MT_INFO_COL in mt.columns:
    mt[["Plan","Plan_SB"]] = mt[MT_INFO_COL].apply(parse_plan_fields)
else:
    mt["Plan"] = None
    mt["Plan_SB"] = None

# ============ MERGE (Dashboard preview) ============
print("[4/7] Building dashboard-shaped preview (one row per TP record)...")

# Start from TP
preview = tp.copy()

# Join Account by GUID (left)
preview = preview.merge(ac.add_prefix("AC_"), left_on="_guid", right_on="AC__guid", how="left")

# For preview: choose latest monetary per _userid if date columns exist
mt_for_join = mt.copy()
date_cols = [c for c in ["CreatedOn","CreatedOn_x","CreatedOn_y","Lv_ApprovedOn","Time"] if c in mt_for_join.columns]
if date_cols:
    date_col = date_cols[0]
    mt_for_join["_sort_date"] = pd.to_datetime(mt_for_join[date_col], errors="coerce")
    mt_for_join = mt_for_join.sort_values("_sort_date").drop_duplicates(subset=["_userid"], keep="last")
else:
    mt_for_join = mt_for_join.drop_duplicates(subset=["_userid"], keep="last")

preview = preview.merge(mt_for_join.add_prefix("MT_"), left_on="_userid", right_on="MT__userid", how="left")

# Assemble dashboard columns
data = {}
for disp, (source, field) in DASHBOARD_COLUMNS:
    if source == "Account":
        colname = f"AC_{field}"
    elif source == "Lv_tpaccount":
        colname = field
    elif source == "_derived":
        colname = f"MT_{field}"
    elif source == "_api":
        data[disp] = ""
        continue
    else:
        colname = None

    data[disp] = preview[colname] if (colname and colname in preview.columns) else ""

preview_dashboard = pd.DataFrame(data)

# ============ FULL MERGE (All fields; may expand rows) ============
print("[5/7] Building full all-fields merge (can be one-to-many)...")

# Prefix source cols to avoid collisions
tp_pref = tp.add_prefix("TP_")
mt_pref = mt.add_prefix("MT_")
ac_pref = ac.add_prefix("AC_")

# Keep the normalized keys in prefixed frames too
tp_pref["TP__guid"] = tp["_guid"]
tp_pref["TP__userid"] = tp["_userid"]

mt_pref["MT__guid"] = mt["_guid"]
mt_pref["MT__userid"] = mt["_userid"]

ac_pref["AC__guid"] = ac["_guid"]

# First join TP + AC on GUID
full_merge = tp_pref.merge(ac_pref, left_on="TP__guid", right_on="AC__guid", how="left")

rows_after_tp_ac = len(full_merge)

# Then join MT on user id (this can create multiple rows per TP if multiple MT rows share the same userid)
full_merge = full_merge.merge(mt_pref, left_on="TP__userid", right_on="MT__userid", how="left")

rows_after_full = len(full_merge)

join_shape = pd.DataFrame({
    "Stage": ["TP only", "TP + AC (GUID left-join)", "TP + AC + MT (UserID left-join)"],
    "Rows": [len(tp_pref), rows_after_tp_ac, rows_after_full]
})

# For the very large all-fields output, cap to TOP_N rows (sorted by a reasonable date if available)
sort_candidates = [c for c in full_merge.columns if c.endswith("CreatedOn") or c.endswith("Time") or "ApprovedOn" in c]
if sort_candidates:
    full_sorted = full_merge.sort_values(by=sort_candidates[0], ascending=False, na_position="last")
else:
    full_sorted = full_merge

full_topn = full_sorted.head(TOP_N).reset_index(drop=True)

# ============ COLUMN MAPPING ============
mapping_rows = []
for disp, (source, field) in DASHBOARD_COLUMNS:
    mapping_rows.append({
        "Dashboard Column": disp,
        "Source Table": source,
        "Source Field": field
    })
mapping_df = pd.DataFrame(mapping_rows)

# ============ SAVE ============
print("[6/7] Writing Excel workbook...")

with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as xw:
    row_counts.to_excel(xw, sheet_name="RowCounts", index=False)
    nulls_blanks.to_excel(xw, sheet_name="NullsBlanks", index=False)
    dups.to_excel(xw, sheet_name="Duplicates", index=False)
    overlap.to_excel(xw, sheet_name="OverlapSummary", index=False)
    if not trim_mismatches.empty:
        trim_mismatches.to_excel(xw, sheet_name="TrimMismatches", index=False)
    guid_pages = []
    if not guid_sanity(tp, TP_GUID_COL).empty:
        gs_tp = guid_sanity(tp, TP_GUID_COL).rename(columns={TP_GUID_COL: "Lv_tpaccount.bad_guid"})
        guid_pages.append(gs_tp)
    if not guid_sanity(mt, MT_GUID_COL).empty:
        gs_mt = guid_sanity(mt, MT_GUID_COL).rename(columns={MT_GUID_COL: "Lv_monetarytransaction.bad_guid"})
        guid_pages.append(gs_mt)
    if guid_pages:
        pd.concat(guid_pages, axis=1).to_excel(xw, sheet_name="GuidSanity", index=False)

    mapping_df.to_excel(xw, sheet_name="ColumnMapping", index=False)
    source_cols.to_excel(xw, sheet_name="SourceColumns", index=False)
    join_shape.to_excel(xw, sheet_name="JoinShape", index=False)

    # Dashboard preview (one row per TP)
    preview_dashboard.to_excel(xw, sheet_name="MergedPreview", index=False)

    # All fields (top N)
    full_topn.to_excel(xw, sheet_name="MergedFull_allFields_topN", index=False)

print(f"[7/7] Done. Saved to: {OUTPUT_XLSX}")
print(f"[INFO] Full merge total rows: {rows_after_full:,}  |  Exported top {len(full_topn):,} rows.")
