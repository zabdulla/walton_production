"""Unit tests for src/build_daily_dashboard.py data-prep functions.

These cover the math/status logic that feeds the published daily dashboard;
the HTML rendering itself is covered by the smoke tests.
"""
from __future__ import annotations

import pandas as pd
import pytest

from build_daily_dashboard import (
    get_weeks_list,
    prepare_daily_summary,
    prepare_machine_daily,
    prepare_notes_by_date,
)


def _daily_df(rows: list[dict]) -> pd.DataFrame:
    base = {
        "Actual_Output": 1000.0, "Machine_Hours": 8.0, "Man_Hours": 8.0,
        "Data_Quality_Score": 90, "Has_Machine_Hours": True, "Has_Output": True,
        "Machine_Name": "EXTRUDER",
    }
    df = pd.DataFrame([{**base, **r} for r in rows])
    df["Date"] = pd.to_datetime(df["Date"])
    return df


def test_summary_totals_and_machine_count() -> None:
    df = _daily_df([
        {"Date": "2026-01-05", "Machine_Name": "EXTRUDER", "Actual_Output": 1000},
        {"Date": "2026-01-05", "Machine_Name": "GRINDER", "Actual_Output": 500},
    ])
    s = prepare_daily_summary(df)
    assert len(s) == 1
    row = s.iloc[0]
    assert row["Total_Output"] == 1500
    assert row["Machines_Active"] == 2
    assert row["Status"] == "complete"


def test_summary_status_partial_when_no_output() -> None:
    df = _daily_df([{"Date": "2026-01-05", "Actual_Output": 0, "Has_Output": False}])
    assert prepare_daily_summary(df).iloc[0]["Status"] == "partial"


def test_summary_status_missing_when_no_hours_at_all() -> None:
    df = _daily_df([{
        "Date": "2026-01-05", "Actual_Output": 0, "Has_Output": False,
        "Machine_Hours": 0.0, "Man_Hours": 0.0, "Has_Machine_Hours": False,
    }])
    assert prepare_daily_summary(df).iloc[0]["Status"] == "missing"


def test_week_start_is_monday() -> None:
    # 2026-01-07 is a Wednesday; its week starts Monday 2026-01-05
    df = _daily_df([{"Date": "2026-01-07"}])
    assert prepare_daily_summary(df).iloc[0]["Week_Start_Str"] == "2026-01-05"


def test_machine_daily_groups_by_date_and_machine() -> None:
    df = _daily_df([
        {"Date": "2026-01-05", "Machine_Name": "EXTRUDER", "Actual_Output": 100},
        {"Date": "2026-01-05", "Machine_Name": "EXTRUDER", "Actual_Output": 200},
        {"Date": "2026-01-06", "Machine_Name": "EXTRUDER", "Actual_Output": 300},
    ])
    agg = prepare_machine_daily(df)
    assert len(agg) == 2
    jan5 = agg[agg["Date_Str"] == "2026-01-05"].iloc[0]
    assert jan5["Actual_Output"] == 300


def test_notes_by_date_grouping() -> None:
    notes = pd.DataFrame([
        {"Date": pd.Timestamp("2026-01-05"), "Machine_Name": "EXTRUDER",
         "Category": "downtime", "Note": "belt broke", "Operator": "Z", "Shift": "1st"},
        {"Date": pd.Timestamp("2026-01-05"), "Machine_Name": "GRINDER",
         "Category": "operational", "Note": "ok", "Operator": "Y", "Shift": "2nd"},
    ])
    d = prepare_notes_by_date(notes)
    assert list(d) == ["2026-01-05"]
    assert len(d["2026-01-05"]) == 2
    assert d["2026-01-05"][0]["category"] == "downtime"


def test_notes_by_date_empty() -> None:
    assert prepare_notes_by_date(pd.DataFrame()) == {}


def test_get_weeks_list_ranges() -> None:
    df = _daily_df([{"Date": "2026-01-05"}, {"Date": "2026-01-07"}])
    weeks = get_weeks_list(prepare_daily_summary(df))
    assert len(weeks) == 1
    assert weeks[0]["start"] == "2026-01-05"
    assert weeks[0]["end"] == "2026-01-11"
    assert weeks[0]["days"] == 2
