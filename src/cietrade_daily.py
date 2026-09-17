"""Daily aggregate rows from cieTrade converting jobs plus the End of Shift app.

The hand-built weekly workbooks stopped after 2026-08-21. From CIETRADE_FROM_DATE
on, this module produces the rows the workbook parser used to produce — one per
(date, shift, machine) — so every dashboard keeps working unchanged:

  * pounds come from cieTrade (cietrade_model: posted jobs plus the poller's
    open-job snapshots), per line, mapped to the dashboard's machine names;
  * machine hours, crew hours, operators, material and comments come from
    data/labor_entries.xlsx (the End of Shift web app) when a matching entry
    exists; otherwise the row carries output only;
  * Guillotine follows the workbook convention: rolls in as Actual_Input and no
    Actual_Output, which the dashboard's "with Guillotine support" view fills.

Rows carry Source = "cietrade". Every run regenerates all cieTrade weeks, so a
posting or a late End of Shift entry is picked up next time.

    python3 src/cietrade_daily.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd

from config import (
    CIETRADE_FROM_DATE, CIETRADE_LINE_TO_MACHINE, DATA_DIR, DEFAULT_AGGREGATED_DATA,
    DEFAULT_AGGREGATED_NOTES, LABOR_RATE,
)
from aggregate_daily_data import _categorize_note, dedup_daily, merge_incremental
from atomic import write_atomic_excel, write_with_snapshot
from labor_entries import LABOR_ENTRIES_PATH, load_entries
import cietrade_model as model

LOG = logging.getLogger("cietrade_daily")
DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
AGG_COLUMNS = [
    "Date", "Day_of_Week", "Week_Start", "Week_End", "Shift", "Machine_Name", "Input_Item", "Actual_Input",
    "Output_Product", "Actual_Output", "Machine_Hours", "Man_Hours", "Operator", "Comment", "Output_per_Hour",
    "Labor_Cost", "Total_Expense", "Cost_per_Pound", "Has_Machine_Hours", "Has_Man_Hours", "Has_Output",
    "Has_Comment", "Data_Quality_Score", "Date_Corrected", "Source",
]
PROVENANCE = {"exact": "", "spread": "cieTrade: averaged over a multi-day job", "partial": "cieTrade: shift in progress at the last poll"}


def _week(d: pd.Timestamp) -> tuple[str, str]:
    mon = d - timedelta(days=d.weekday())
    return mon.strftime("%Y-%m-%d"), (mon + timedelta(days=4)).strftime("%Y-%m-%d")


def _labor_index(labor_path: Path) -> tuple[dict, pd.DataFrame]:
    """(Date, Shift, Machine_Name) -> entry dict, plus the shift notes."""
    if not Path(labor_path).exists():
        return {}, pd.DataFrame()
    entries, notes = load_entries(labor_path)
    idx = {}
    for r in entries.itertuples(index=False):
        if not r.Date or not r.Shift or not r.Machine_Name:
            continue
        idx[(str(r.Date)[:10], str(r.Shift), str(r.Machine_Name))] = r._asdict()
    return idx, notes


def _num(v) -> float:
    try:
        f = float(v)
        return 0.0 if f != f else f
    except (TypeError, ValueError):
        return 0.0


def _text(v) -> str:
    return "" if v is None or (isinstance(v, float) and v != v) else str(v).strip()


def daily_rows(res: dict, labor_path: Path = LABOR_ENTRIES_PATH, from_date: str = CIETRADE_FROM_DATE,
               hourly_rate: float = LABOR_RATE) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Aggregate-schema rows and notes for every cieTrade shift-day from ``from_date``."""
    warnings: list[str] = []
    cov = res["cov"]
    keep = cov[(cov["Date"] >= pd.Timestamp(from_date)) & cov["mode"].isin(["exact", "spread", "partial"]) & cov["lbs"].notna()].copy()
    keep["Machine_Name"] = keep["line"].map(CIETRADE_LINE_TO_MACHINE)
    lost = keep[keep["Machine_Name"].isna()]
    if len(lost):
        warnings.append("cieTrade lines without a dashboard machine (dropped): " + ", ".join(sorted(lost["line"].unique())))
        keep = keep.dropna(subset=["Machine_Name"])
    labor, shift_notes = _labor_index(labor_path)
    rows, notes = [], []
    rank = {"partial": 3, "spread": 2, "exact": 1}
    for (date, shift, machine), g in keep.groupby(["Date", "Shift", "Machine_Name"]):
        lbs = round(float(g["lbs"].sum()), 1)
        mode = max(g["mode"], key=lambda x: rank[x])
        jobs = ", ".join(str(int(j)) for j in sorted(g["job"].dropna().unique()))
        key = (date.strftime("%Y-%m-%d"), shift, machine)
        lab = labor.get(key, {})
        machine_hours, man_hours = _num(lab.get("Machine_Hours")), _num(lab.get("Man_Hours"))
        operator, material, comment = _text(lab.get("Operator")), _text(lab.get("Material")), _text(lab.get("Comment"))
        downtime = _num(lab.get("Downtime_Minutes"))
        if downtime:
            comment = (comment + " · " if comment else "") + f"down {downtime:.0f} min" + (f" ({_text(lab.get('Downtime_Reason'))})" if _text(lab.get("Downtime_Reason")) else "")
        prov = PROVENANCE[mode]
        full_comment = " · ".join(x for x in (comment, prov) if x)
        guillotine = machine == "GUILLOTINE"
        actual_input, actual_output = (lbs, 0.0) if guillotine else (lbs, lbs)
        has_mh, has_man, has_out = machine_hours > 0, man_hours > 0, actual_output > 0
        labor_cost = man_hours * hourly_rate
        week_start, week_end = _week(date)
        rows.append({
            "Date": key[0], "Day_of_Week": DOW[date.weekday()], "Week_Start": week_start, "Week_End": week_end,
            "Shift": shift, "Machine_Name": machine, "Input_Item": f"cieTrade job {jobs}" if jobs else "cieTrade",
            "Actual_Input": actual_input, "Output_Product": material, "Actual_Output": actual_output,
            "Machine_Hours": machine_hours, "Man_Hours": man_hours, "Operator": operator, "Comment": full_comment,
            "Output_per_Hour": actual_output / machine_hours if machine_hours > 0 else float("nan"),
            "Labor_Cost": labor_cost, "Total_Expense": labor_cost,
            "Cost_per_Pound": labor_cost / actual_output if actual_output > 0 else float("nan"),
            "Has_Machine_Hours": has_mh, "Has_Man_Hours": has_man, "Has_Output": has_out, "Has_Comment": bool(comment),
            "Data_Quality_Score": has_mh * 25 + has_man * 25 + has_out * 40 + (10 if has_mh == has_out else 0),
            "Date_Corrected": False, "Source": "cietrade",
        })
        if comment:
            notes.append({"Date": key[0], "Shift": shift, "Machine_Name": machine, "Input_Item": material or "",
                          "Operator": operator, "Note": comment, "Category": _categorize_note(comment)})
    if len(shift_notes):
        for r in shift_notes.itertuples(index=False):
            if str(r.Date)[:10] >= from_date and _text(r.Note):
                notes.append({"Date": str(r.Date)[:10], "Shift": _text(r.Shift), "Machine_Name": "", "Input_Item": "",
                              "Operator": "", "Note": _text(r.Note), "Category": _categorize_note(_text(r.Note))})
    rows_df = pd.DataFrame(rows, columns=AGG_COLUMNS)
    return rows_df, pd.DataFrame(notes, columns=["Date", "Shift", "Machine_Name", "Input_Item", "Operator", "Note", "Category"]), warnings


def update_aggregate(rows: pd.DataFrame, notes: pd.DataFrame, agg_path: Path = DEFAULT_AGGREGATED_DATA,
                     notes_path: Path = DEFAULT_AGGREGATED_NOTES, from_date: str = CIETRADE_FROM_DATE,
                     dry_run: bool = False) -> dict:
    """Replace the cieTrade weeks of the aggregate with ``rows``; keep everything else."""
    existing = pd.read_excel(agg_path) if Path(agg_path).exists() else pd.DataFrame(columns=AGG_COLUMNS)
    if "Source" not in existing.columns:
        existing["Source"] = "workbook"
    existing["Source"] = existing["Source"].fillna("workbook")
    workbook_late = existing[(existing["Source"] != "cietrade") & (existing["Date"].astype(str) >= from_date)]
    summary = {"rows": int(len(rows)), "days": int(rows["Date"].nunique()) if len(rows) else 0,
               "with_hours": int((rows["Machine_Hours"] > 0).sum()) if len(rows) else 0,
               "through": rows["Date"].max() if len(rows) else None, "pre_snapshot": None, "records": int(len(existing)),
               "skipped_weeks": []}
    if len(workbook_late):
        # a workbook exists for these weeks after all — it wins, cieTrade rows step aside
        wk = set(workbook_late["Week_Start"].astype(str))
        summary["skipped_weeks"] = sorted(wk)
        rows = rows[~rows["Week_Start"].isin(wk)]
    merged = merge_incremental(existing, rows) if len(rows) else existing
    merged, _ = dedup_daily(merged)
    merged = merged.sort_values(["Date", "Shift", "Machine_Name"], kind="stable").reset_index(drop=True)
    summary["records"] = int(len(merged))
    if dry_run:
        return summary
    r = write_with_snapshot(Path(agg_path), lambda tmp: merged.to_excel(tmp, index=False),
                            snapshot_dir=DATA_DIR / "snapshots", new_row_count=len(merged))
    summary["pre_snapshot"] = r.get("snapshot")
    old_notes = pd.read_excel(notes_path) if Path(notes_path).exists() else pd.DataFrame(columns=notes.columns)
    kept = old_notes[old_notes["Date"].astype(str) < from_date]
    write_atomic_excel(pd.concat([kept, notes], ignore_index=True), Path(notes_path), index=False)
    return summary


def run(dry_run: bool = False, verbose: bool = True) -> dict:
    res = model.run(verbose=verbose)
    rows, notes, warnings = daily_rows(res)
    summary = update_aggregate(rows, notes, dry_run=dry_run)
    summary["warnings"] = warnings + list(res["meta"].get("warnings", []))
    summary["last_poll"] = res["meta"].get("last_poll")
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="compute the rows, write nothing")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    s = run(dry_run=args.dry_run)
    print(f"cieTrade rows: {s['rows']} over {s['days']} days through {s['through']} ({s['with_hours']} with hours) "
          f"-> aggregate {s['records']} records" + (" [dry run]" if args.dry_run else ""))
    for w in s["warnings"]:
        print("warning:", w)
