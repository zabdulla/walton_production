"""
Daily Processing Dashboard Builder - Redesigned.

Generates an interactive HTML dashboard for daily production data with:
- Week/Month period selector with navigation
- Calendar-style grid showing data completeness per day
- Clear handling of missing data (supervisor forgot to input)
- Notes displayed as indicators on days, not separate section
- Focused visualizations for selected time period

Usage:
    python build_daily_dashboard.py
"""

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from config import PROJECT_ROOT, DEFAULT_AGGREGATED_DATA, DEFAULT_AGGREGATED_NOTES, CHART_PALETTE
from daily_template import (  # template extracted 2026-07
    build_dashboard_html,
    compute_machine_baselines,  # re-export: tests + callers use this path
)

DEFAULT_DAILY_INPUT = DEFAULT_AGGREGATED_DATA
DEFAULT_NOTES_INPUT = DEFAULT_AGGREGATED_NOTES
DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "daily.html"


def load_data(daily_path: Path, notes_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load daily data and notes from Excel files."""
    daily = pd.read_excel(daily_path)
    daily["Date"] = pd.to_datetime(daily["Date"])
    daily = daily.sort_values(["Date", "Machine_Name"])

    notes = pd.DataFrame()
    if notes_path.exists():
        notes = pd.read_excel(notes_path)
        notes["Date"] = pd.to_datetime(notes["Date"])
        notes = notes.sort_values("Date", ascending=False)

    return daily, notes


def prepare_daily_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Create day-level summary across all machines."""
    summary = (
        daily.groupby("Date")
        .agg(
            Total_Output=("Actual_Output", "sum"),
            Total_Machine_Hours=("Machine_Hours", "sum"),
            Total_Man_Hours=("Man_Hours", "sum"),
            Records=("Date", "count"),
            Avg_Quality=("Data_Quality_Score", "mean"),
            Has_Hours=("Has_Machine_Hours", "any"),
            Has_Output=("Has_Output", "any"),
            Machines_Active=("Machine_Name", "nunique"),
        )
        .reset_index()
    )
    summary["Date_Str"] = summary["Date"].dt.strftime("%Y-%m-%d")
    summary["Day_Name"] = summary["Date"].dt.day_name().str[:3]
    summary["Week_Start"] = summary["Date"] - pd.to_timedelta(summary["Date"].dt.dayofweek, unit="d")
    summary["Week_Start_Str"] = summary["Week_Start"].dt.strftime("%Y-%m-%d")
    summary["Month"] = summary["Date"].dt.to_period("M").astype(str)

    # Data status: "complete", "partial", "missing"
    summary["Status"] = "complete"
    summary.loc[(summary["Total_Output"] == 0) | (~summary["Has_Output"]), "Status"] = "partial"
    summary.loc[(summary["Total_Machine_Hours"] == 0) & (summary["Total_Man_Hours"] == 0), "Status"] = "missing"

    return summary


def prepare_machine_daily(daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate data by date and machine for charts."""
    agg = (
        daily.groupby(["Date", "Machine_Name"])
        .agg(
            Actual_Output=("Actual_Output", "sum"),
            Machine_Hours=("Machine_Hours", "sum"),
            Man_Hours=("Man_Hours", "sum"),
            Avg_Quality=("Data_Quality_Score", "mean"),
        )
        .reset_index()
    )
    agg["Date_Str"] = agg["Date"].dt.strftime("%Y-%m-%d")
    agg["Week_Start"] = agg["Date"] - pd.to_timedelta(agg["Date"].dt.dayofweek, unit="d")
    agg["Week_Start_Str"] = agg["Week_Start"].dt.strftime("%Y-%m-%d")
    agg["Month"] = agg["Date"].dt.to_period("M").astype(str)
    return agg


def prepare_notes_by_date(notes: pd.DataFrame) -> dict:
    """Create a dict of date -> list of notes."""
    if notes.empty:
        return {}

    notes_dict = {}
    for _, row in notes.iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d")
        note_info = {
            "machine": row.get("Machine_Name", ""),
            "category": row.get("Category", "operational"),
            "note": row.get("Note", ""),
            "operator": row.get("Operator", ""),
            "shift": row.get("Shift", ""),
        }
        if date_str not in notes_dict:
            notes_dict[date_str] = []
        notes_dict[date_str].append(note_info)

    return notes_dict


def get_weeks_list(daily_summary: pd.DataFrame) -> list[dict]:
    """Get list of unique weeks with their date ranges."""
    weeks = []
    for week_start_str in sorted(daily_summary["Week_Start_Str"].unique()):
        week_data = daily_summary[daily_summary["Week_Start_Str"] == week_start_str]
        week_start = pd.to_datetime(week_start_str)
        week_end = week_start + pd.Timedelta(days=6)
        weeks.append({
            "start": week_start_str,
            "end": week_end.strftime("%Y-%m-%d"),
            "label": f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}",
            "days": len(week_data),
        })
    return weeks


def get_months_list(daily_summary: pd.DataFrame) -> list[dict]:
    """Get list of unique months."""
    months = []
    for month_str in sorted(daily_summary["Month"].unique()):
        month_data = daily_summary[daily_summary["Month"] == month_str]
        months.append({
            "value": month_str,
            "label": pd.to_datetime(month_str).strftime("%B %Y"),
            "days": len(month_data),
        })
    return months


def main(daily_path: Path, notes_path: Path, output_path: Path) -> None:
    """Main entry point."""
    print(f"Loading data from {daily_path}...")
    daily, notes = load_data(daily_path, notes_path)

    print("Preparing summaries...")
    daily_summary = prepare_daily_summary(daily)
    machine_daily = prepare_machine_daily(daily)
    notes_by_date = prepare_notes_by_date(notes)

    weeks_list = get_weeks_list(daily_summary)
    months_list = get_months_list(daily_summary)
    machines = sorted(daily["Machine_Name"].unique().tolist())

    print(f"Building dashboard with {len(weeks_list)} weeks, {len(months_list)} months...")
    html = build_dashboard_html(
        daily_summary,
        machine_daily,
        notes_by_date,
        weeks_list,
        months_list,
        machines,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    from atomic import write_atomic_text
    write_atomic_text(output_path, html)
    print(f"Wrote daily dashboard to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build daily processing dashboard.")
    parser.add_argument("--input", type=Path, default=DEFAULT_DAILY_INPUT,
                        help="Path to aggregated_daily_data.xlsx")
    parser.add_argument("--notes", type=Path, default=DEFAULT_NOTES_INPUT,
                        help="Path to aggregated_notes.xlsx")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Path to write HTML dashboard")
    args = parser.parse_args()
    main(args.input, args.notes, args.output)
