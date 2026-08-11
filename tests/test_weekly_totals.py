"""Output totals must never depend on whether hours were recorded.

Reported by the plant on 2026-08-11: the dashboard showed EXTRUDER at
~34,000 lbs for the week of 2026-08-03 when hand-adding the three shifts
gave 73,781. The weekly rollup was fed a frame pre-filtered to
``Man_Hours > 0 & Machine_Hours > 0``, so six rows that recorded output but
no hours were dropped along with their tonnage. Plant-wide that hid 77,951
of 239,995 lbs (32.5%) that week, and the share had been climbing for a
month as hours capture degraded.

Tonnage is ground truth. Rates are derived, and must never cost us tonnage.
"""
from __future__ import annotations

import pandas as pd
import pytest

from build_interactive_dashboard import aggregate_weekly


def _rows(specs: list[dict]) -> pd.DataFrame:
    base = {
        "Machine_Name": "EXTRUDER", "Week_Start": pd.Timestamp("2026-08-03"),
        "Actual_Output": 5_000.0, "Machine_Hours": 8.0, "Man_Hours": 16.0,
        "Labor_Cost": 400.0, "Total_Expense": 400.0,
    }
    return pd.DataFrame([{**base, **s} for s in specs])


def _one(df: pd.DataFrame) -> pd.Series:
    out = aggregate_weekly(df)
    assert len(out) == 1
    return out.iloc[0]


# ---------------------------------------------------------------------------
# The bug
# ---------------------------------------------------------------------------

def test_output_counts_when_hours_are_missing() -> None:
    """The reported case: rows with output but zero hours keep their tonnage."""
    row = _one(_rows([
        {"Actual_Output": 34_646.0, "Machine_Hours": 8.0, "Man_Hours": 16.0},
        {"Actual_Output": 39_135.0, "Machine_Hours": 0.0, "Man_Hours": 0.0},
    ]))
    assert row["Actual_Output"] == 73_781.0


def test_rate_excludes_output_whose_hours_were_never_recorded() -> None:
    """Only the output that has hours may be divided by those hours."""
    row = _one(_rows([
        {"Actual_Output": 8_000.0, "Machine_Hours": 10.0, "Man_Hours": 10.0},
        {"Actual_Output": 92_000.0, "Machine_Hours": 0.0, "Man_Hours": 0.0},
    ]))
    assert row["Actual_Output"] == 100_000.0
    # 8,000 / 10 — NOT 100,000 / 10, which would invent a tenfold rate
    assert row["Output_per_Hour"] == pytest.approx(800.0)


def test_all_hours_missing_gives_no_rate_but_keeps_output() -> None:
    row = _one(_rows([{"Actual_Output": 12_000.0, "Machine_Hours": 0.0, "Man_Hours": 0.0}]))
    assert row["Actual_Output"] == 12_000.0
    assert pd.isna(row["Output_per_Hour"])
    assert pd.isna(row["Output_per_Man_Hour"])


# ---------------------------------------------------------------------------
# Impossible hours
# ---------------------------------------------------------------------------

def test_impossible_machine_hours_are_treated_as_unrecorded() -> None:
    """2026-08-07 EXTRUDER recorded 713.25 machine-hours in one day.

    Believing it would drag the week's rate from ~680 to ~63. Each row is
    one machine-day, so the week's real hours arrive as several valid rows.
    """
    row = _one(_rows([
        {"Actual_Output": 17_323.0, "Machine_Hours": 20.0, "Man_Hours": 16.0},
        {"Actual_Output": 17_323.0, "Machine_Hours": 20.0, "Man_Hours": 16.0},
        {"Actual_Output": 13_287.0, "Machine_Hours": 713.25, "Man_Hours": 0.0},
    ]))
    assert row["Actual_Output"] == 47_933.0, "output still counts"
    assert row["Total_Machine_Hours"] == pytest.approx(40.0), "typo excluded from hours"
    assert row["Output_per_Hour"] == pytest.approx(34_646.0 / 40.0)


def test_a_single_row_over_24_hours_is_rejected_not_summed() -> None:
    """One machine-day cannot exceed 24 hours however it is written."""
    row = _one(_rows([{"Actual_Output": 1_000.0, "Machine_Hours": 50.95}]))
    assert row["Actual_Output"] == 1_000.0
    assert row["Total_Machine_Hours"] == 0.0
    assert pd.isna(row["Output_per_Hour"])


def test_exactly_24_machine_hours_is_allowed() -> None:
    row = _one(_rows([{"Actual_Output": 2_400.0, "Machine_Hours": 24.0}]))
    assert row["Total_Machine_Hours"] == 24.0
    assert row["Output_per_Hour"] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Cost per pound spans all output
# ---------------------------------------------------------------------------

def test_cost_per_pound_uses_total_output() -> None:
    """Labour was paid to make every pound, including unhoured ones."""
    row = _one(_rows([
        {"Actual_Output": 5_000.0, "Total_Expense": 500.0},
        {"Actual_Output": 5_000.0, "Total_Expense": 0.0, "Machine_Hours": 0.0, "Man_Hours": 0.0},
    ]))
    assert row["Actual_Output"] == 10_000.0
    assert row["Production_Cost_per_Pound"] == pytest.approx(0.05)


def test_multiple_machines_stay_independent() -> None:
    df = _rows([
        {"Machine_Name": "EXTRUDER", "Actual_Output": 10_000.0, "Machine_Hours": 0.0},
        {"Machine_Name": "GRINDER", "Actual_Output": 8_000.0, "Machine_Hours": 10.0},
    ])
    out = aggregate_weekly(df).set_index("Machine_Name")
    assert out.loc["EXTRUDER", "Actual_Output"] == 10_000.0
    assert pd.isna(out.loc["EXTRUDER", "Output_per_Hour"])
    assert out.loc["GRINDER", "Output_per_Hour"] == pytest.approx(800.0)
