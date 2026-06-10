"""Tests for src/validate_data.py — pure-function checks."""
from __future__ import annotations

import pandas as pd

from validate_data import (
    _check_anomalous_values,
    _check_duplicates,
    _check_missing_operators,
    _check_unmapped_products,
    gating_decision,
)


# ---------------------------------------------------------------------------
# _check_unmapped_products
# ---------------------------------------------------------------------------

def test_unmapped_products_empty_when_all_mapped() -> None:
    df = pd.DataFrame({
        "Output_Product": ["PP Bales", "PP Bales", "LD Bales", None, ""],
    })
    result = _check_unmapped_products(df)
    assert result == []


def test_unmapped_products_returns_unknown_with_counts() -> None:
    df = pd.DataFrame({
        "Output_Product": ["Brand New Stuff", "Brand New Stuff", "Mystery", "PP Bales"],
    })
    result = _check_unmapped_products(df)
    products = {item["product"]: item["count"] for item in result}
    assert "Brand New Stuff" in products
    assert products["Brand New Stuff"] == 2
    assert "Mystery" in products
    assert "PP Bales" not in products  # this one is mapped


def test_unmapped_products_applies_typo_map() -> None:
    # "PP Resin" is a typo entry that gets corrected to "PP resin" (which is mapped)
    df = pd.DataFrame({"Output_Product": ["PP Resin", "PP Resin"]})
    assert _check_unmapped_products(df) == []


# ---------------------------------------------------------------------------
# _check_duplicates
# ---------------------------------------------------------------------------

def _make_row(date="2026-01-01", shift="1st", machine="EXTRUDER",
              product="PP resin", output=1000):
    return {
        "Date": pd.Timestamp(date), "Shift": shift, "Machine_Name": machine,
        "Output_Product": product, "Actual_Output": output,
    }


def test_check_duplicates_no_duplicates() -> None:
    df = pd.DataFrame([_make_row(date="2026-01-01"), _make_row(date="2026-01-02")])
    result = _check_duplicates(df)
    assert result["count"] == 0
    assert result["examples"] == []


def test_check_duplicates_one_pair() -> None:
    df = pd.DataFrame([_make_row(), _make_row(), _make_row(date="2026-02-01")])
    result = _check_duplicates(df)
    # Two identical rows: 1 "extra"
    assert result["count"] == 1
    assert len(result["examples"]) == 2  # both members of the group shown


def test_check_duplicates_triplet() -> None:
    df = pd.DataFrame([_make_row(), _make_row(), _make_row(),
                       _make_row(date="2026-02-01")])
    result = _check_duplicates(df)
    # Three identical rows: 2 "extras"
    assert result["count"] == 2


# ---------------------------------------------------------------------------
# _check_missing_operators
# ---------------------------------------------------------------------------

def test_missing_operators_all_present() -> None:
    df = pd.DataFrame({
        "Operator": ["Alice", "Bob"], "Machine_Name": ["EXTRUDER", "GUILLOTINE"],
    })
    assert _check_missing_operators(df) == {}


def test_missing_operators_by_machine() -> None:
    df = pd.DataFrame({
        "Operator": ["Alice", None, "", "Bob"],
        "Machine_Name": ["EXTRUDER", "EXTRUDER", "GUILLOTINE", "GUILLOTINE"],
    })
    result = _check_missing_operators(df)
    assert result.get("EXTRUDER") == 1
    assert result.get("GUILLOTINE") == 1


# ---------------------------------------------------------------------------
# _check_anomalous_values
# ---------------------------------------------------------------------------

def test_anomalous_flags_high_output() -> None:
    df = pd.DataFrame({
        "Date": [pd.Timestamp("2026-01-01")],
        "Actual_Output": [100_000],  # > 50,000 threshold
        "Machine_Hours": [8],
        "Man_Hours": [16],
        "Output_per_Hour": [1000],
        "Machine_Name": ["EXTRUDER"],
        "Shift": ["1st"],
    })
    flags = _check_anomalous_values(df)
    rules = [f["rule"] for f in flags]
    assert any("Actual_Output" in r for r in rules)


def test_anomalous_flags_unrealistic_hours() -> None:
    df = pd.DataFrame({
        "Date": [pd.Timestamp("2026-01-01")],
        "Actual_Output": [100],
        "Machine_Hours": [30],  # > 24
        "Man_Hours": [10],
        "Output_per_Hour": [10],
        "Machine_Name": ["EXTRUDER"],
        "Shift": ["1st"],
    })
    flags = _check_anomalous_values(df)
    rules = [f["rule"] for f in flags]
    assert any("Machine_Hours" in r for r in rules)


def test_anomalous_normal_values_no_flags() -> None:
    df = pd.DataFrame({
        "Date": [pd.Timestamp("2026-01-01")],
        "Actual_Output": [5000],
        "Machine_Hours": [8],
        "Man_Hours": [16],
        "Output_per_Hour": [625],
        "Machine_Name": ["EXTRUDER"],
        "Shift": ["1st"],
    })
    assert _check_anomalous_values(df) == []


# ---------------------------------------------------------------------------
# gating_decision — the publish-blocking logic
# ---------------------------------------------------------------------------

def test_gating_clean_results_does_not_block() -> None:
    results = {
        "unmapped_products": [],
        "duplicates_count": 0,
        "payroll": {"status": "ok"},
    }
    blocked, reasons = gating_decision(results)
    assert not blocked
    assert reasons == []


def test_gating_blocks_on_significant_unmapped_products() -> None:
    results = {
        "unmapped_products": [{"product": "Foo", "count": 5}],
        "duplicates_count": 0,
        "payroll": {"status": "ok"},
    }
    blocked, reasons = gating_decision(results)
    assert blocked
    assert any("unmapped" in r.lower() for r in reasons)


def test_gating_allows_few_unmapped_rows() -> None:
    # Below the 5-row threshold — likely a typo, warn-only
    results = {
        "unmapped_products": [{"product": "Foo", "count": 2},
                              {"product": "Bar", "count": 1}],
        "duplicates_count": 0,
        "payroll": {"status": "ok"},
    }
    blocked, reasons = gating_decision(results)
    assert not blocked


def test_gating_blocks_on_any_duplicates() -> None:
    results = {
        "unmapped_products": [],
        "duplicates_count": 1,
        "payroll": {"status": "ok"},
    }
    blocked, reasons = gating_decision(results)
    assert blocked
    assert any("duplicate" in r.lower() for r in reasons)


def test_gating_blocks_on_unrostered_employees() -> None:
    results = {
        "unmapped_products": [],
        "duplicates_count": 0,
        "payroll": {
            "status": "ok",
            "unrostered_employees": ["Jane Doe"],
        },
    }
    blocked, reasons = gating_decision(results)
    assert blocked
    assert any("roster" in r.lower() or "Jane Doe" in r for r in reasons)


def test_gating_skips_payroll_when_data_missing() -> None:
    # status="missing_data" means no payroll file at all — don't block on it
    results = {
        "unmapped_products": [],
        "duplicates_count": 0,
        "payroll": {"status": "missing_data", "unrostered_employees": ["Anyone"]},
    }
    blocked, reasons = gating_decision(results)
    assert not blocked


def test_gating_does_not_block_on_warnings_only() -> None:
    # Missing operators / weeks / anomalous values are NOT blocking
    results = {
        "unmapped_products": [],
        "duplicates_count": 0,
        "payroll": {"status": "ok"},
        "missing_weeks": ["2025-01-01"],
        "missing_operators": {"EXTRUDER": 100},
        "anomalous_values": [{"rule": "x", "value": 50000, "Date": "2026-01-01",
                              "Machine_Name": "E", "Shift": "1"}] * 100,
    }
    blocked, reasons = gating_decision(results)
    assert not blocked


# ---------------------------------------------------------------------------
# _check_latest_week_shifts — missed-report alarm
# ---------------------------------------------------------------------------

def _shift_df(rows: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame({
        "Date": [r[0] for r in rows],
        "Shift": [r[1] for r in rows],
        "Machine_Name": "EXTRUDER",
        "Actual_Output": 1000.0,
    })


def test_latest_week_all_shifts_present() -> None:
    from validate_data import _check_latest_week_shifts
    df = _shift_df([("2026-06-01", "1st"), ("2026-06-02", "2nd"), ("2026-06-03", "3rd")])
    res = _check_latest_week_shifts(df)
    assert res["week_start"] == "2026-06-01"
    assert res["missing_shifts"] == []


def test_latest_week_missing_shift_detected() -> None:
    from validate_data import _check_latest_week_shifts
    df = _shift_df([
        # prior week complete
        ("2026-05-25", "1st"), ("2026-05-25", "2nd"), ("2026-05-25", "3rd"),
        # latest week: 3rd shift report never arrived
        ("2026-06-01", "1st"), ("2026-06-02", "2nd"),
    ])
    res = _check_latest_week_shifts(df)
    assert res["week_start"] == "2026-06-01"
    assert res["missing_shifts"] == ["3rd"]


# ---------------------------------------------------------------------------
# _check_weekly_output_anomalies — rolling 2-sigma per machine
# ---------------------------------------------------------------------------

def _weekly_df(outputs: list[float], machine: str = "EXTRUDER") -> pd.DataFrame:
    """One row per week (Mondays), given weekly output totals."""
    start = pd.Timestamp("2026-01-05")
    return pd.DataFrame({
        "Date": [start + pd.Timedelta(weeks=i) for i in range(len(outputs))],
        "Shift": "1st",
        "Machine_Name": machine,
        "Actual_Output": outputs,
    })


def test_output_anomaly_flags_collapse() -> None:
    from validate_data import _check_weekly_output_anomalies
    # Stable ~10k/week with slight variation, then a collapse to 1k
    df = _weekly_df([10_000, 10_500, 9_800, 10_200, 9_900, 10_100, 1_000])
    anomalies = _check_weekly_output_anomalies(df)
    assert len(anomalies) >= 1
    worst = anomalies[-1]
    assert worst["machine"] == "EXTRUDER"
    assert worst["output"] == 1000.0
    assert worst["deviation_sigma"] >= 2.0


def test_output_anomaly_quiet_on_stable_data() -> None:
    from validate_data import _check_weekly_output_anomalies
    df = _weekly_df([10_000, 10_500, 9_800, 10_200, 9_900, 10_100, 10_300])
    assert _check_weekly_output_anomalies(df) == []


def test_output_anomaly_ignores_old_weeks() -> None:
    from validate_data import _check_weekly_output_anomalies
    # Anomaly at week 7 of 30 — far outside the recent window
    outputs = [10_000, 10_500, 9_800, 10_200, 9_900, 10_100, 1_000] + [10_000] * 23
    df = _weekly_df(outputs)
    anomalies = _check_weekly_output_anomalies(df, recent_weeks=8)
    assert all(a["week_start"] >= "2026-05-25" for a in anomalies)


def test_output_anomaly_skips_short_history() -> None:
    from validate_data import _check_weekly_output_anomalies
    df = _weekly_df([10_000, 1_000])
    assert _check_weekly_output_anomalies(df) == []
