"""Labor efficiency and operator capture sections on the published dashboard.

These read row-level labour data rather than the weekly rollup, so they are
the only published sections that depend on crew-hour semantics and on the
Operator column being populated.
"""
from __future__ import annotations

import pandas as pd
import pytest

from build_interactive_dashboard import (
    build_labor_efficiency_html,
    build_operator_capture_html,
)


def _rows(specs: list[dict]) -> pd.DataFrame:
    base = {
        "Date": pd.Timestamp("2026-07-20"), "Shift": "1st",
        "Machine_Name": "EXTRUDER", "Output_Product": "PP resin",
        "Actual_Output": 10_000.0, "Man_Hours": 10.0, "Machine_Hours": 8.0,
        "Operator": "Alice",
    }
    return pd.DataFrame([{**base, **s} for s in specs])


# ---------------------------------------------------------------------------
# Labor efficiency
# ---------------------------------------------------------------------------

def test_labor_efficiency_computes_rate_and_cost() -> None:
    # 20,000 lbs over 200 man-hours = 100 lbs/man-hour; at $25/hr that is
    # $5,000 of labour over 20,000 lbs = $0.25/lb.
    df = _rows([{"Actual_Output": 20_000.0, "Man_Hours": 200.0}])
    html = build_labor_efficiency_html(df)
    assert "100" in html
    assert "$0.2500" in html


def test_labor_efficiency_ranks_best_first() -> None:
    df = _rows([
        {"Shift": "1st", "Actual_Output": 30_000.0, "Man_Hours": 100.0},  # 300
        {"Shift": "2nd", "Actual_Output": 10_000.0, "Man_Hours": 100.0},  # 100
    ])
    html = build_labor_efficiency_html(df)
    assert html.index("1st") < html.index("2nd")


def test_labor_efficiency_omits_thinly_staffed_machines() -> None:
    """A machine with a handful of hours produces a meaningless rate."""
    df = _rows([
        {"Machine_Name": "EXTRUDER", "Man_Hours": 500.0},
        {"Machine_Name": "SMALL GRINDER", "Man_Hours": 3.0},
    ])
    html = build_labor_efficiency_html(df)
    assert "EXTRUDER" in html
    assert "SMALL GRINDER" not in html


def test_labor_efficiency_survives_zero_output() -> None:
    """Cost per pound is undefined with no output — must not raise."""
    df = _rows([{"Actual_Output": 0.0, "Man_Hours": 200.0}])
    html = build_labor_efficiency_html(df)
    assert "—" in html


def test_labor_efficiency_handles_empty_input() -> None:
    assert "No labor data" in build_labor_efficiency_html(pd.DataFrame())


# ---------------------------------------------------------------------------
# Operator capture
# ---------------------------------------------------------------------------

def test_capture_measures_output_share_not_row_share() -> None:
    """One huge blank row matters more than many small named ones."""
    df = _rows([
        {"Operator": "", "Actual_Output": 9_000.0},
        {"Operator": "Alice", "Actual_Output": 500.0},
        {"Operator": "Bob", "Actual_Output": 500.0},
    ])
    html = build_operator_capture_html(df)
    assert "90.0%" in html


def test_capture_is_quiet_when_everything_is_named() -> None:
    df = _rows([{"Operator": "Alice"}, {"Operator": "Bob"}])
    html = build_operator_capture_html(df)
    assert "0.0%" in html
    assert "Rows that are blank again and again" not in html


def test_capture_lists_repeat_signatures_by_frequency() -> None:
    df = _rows(
        [{"Operator": "", "Machine_Name": "EXTRUDER", "Output_Product": "PP resin"}] * 5
        + [{"Operator": "", "Machine_Name": "GRINDER", "Output_Product": "LD bricks"}] * 2
    )
    html = build_operator_capture_html(df)
    assert "Rows that are blank again and again" in html
    assert html.index("EXTRUDER") < html.index("GRINDER"), "most frequent first"


def test_capture_treats_whitespace_operator_as_blank() -> None:
    df = _rows([{"Operator": "   ", "Actual_Output": 1_000.0}])
    html = build_operator_capture_html(df)
    assert "100.0%" in html


def test_capture_handles_empty_input() -> None:
    assert "No operator data" in build_operator_capture_html(pd.DataFrame())
