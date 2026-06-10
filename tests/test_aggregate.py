"""Tests for src/aggregate_daily_data.py — date parsing, shift, notes."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from aggregate_daily_data import (
    _categorize_note,
    _extract_date_from_sheet,
    _parse_date_range,
    _parse_shift,
    _safe_float,
)


# ---------------------------------------------------------------------------
# _safe_float
# ---------------------------------------------------------------------------

def test_safe_float_numeric() -> None:
    assert _safe_float(10) == 10.0
    assert _safe_float(3.14) == 3.14
    assert _safe_float("42") == 42.0


def test_safe_float_none_returns_default() -> None:
    assert _safe_float(None) == 0.0
    assert _safe_float(None, default=99) == 99


def test_safe_float_nan_returns_default() -> None:
    import math
    assert _safe_float(math.nan) == 0.0
    assert _safe_float(pd.NA) == 0.0


def test_safe_float_garbage_returns_default() -> None:
    assert _safe_float("not a number") == 0.0


# ---------------------------------------------------------------------------
# _parse_shift
# ---------------------------------------------------------------------------

def test_parse_shift_each_label() -> None:
    assert _parse_shift("1st shift processing weights 01-01-26 to 01-05-26.xlsx") == "1st"
    assert _parse_shift("2nd shift processing weights 01-01-26 to 01-05-26.xlsx") == "2nd"
    assert _parse_shift("3rd shift processing weights 01-01-26 to 01-05-26.xlsx") == "3rd"


def test_parse_shift_case_insensitive() -> None:
    assert _parse_shift("1ST SHIFT processing.xlsx") == "1st"


def test_parse_shift_unspecified_when_no_match() -> None:
    assert _parse_shift("processing weights 01-01-26 to 01-05-26.xlsx") == "unspecified"
    assert _parse_shift("random.xlsx") == "unspecified"


# ---------------------------------------------------------------------------
# _parse_date_range
# ---------------------------------------------------------------------------

def test_parse_date_range_two_digit_year() -> None:
    start, end = _parse_date_range("1st shift processing weights 04-13-26 to 04-17-26.xlsx")
    assert start == "2026-04-13"
    assert end == "2026-04-17"


def test_parse_date_range_four_digit_year() -> None:
    start, end = _parse_date_range("processing weights 04-13-2026 to 04-17-2026.xlsx")
    assert start == "2026-04-13"
    assert end == "2026-04-17"


def test_parse_date_range_raises_when_no_match() -> None:
    with pytest.raises(ValueError):
        _parse_date_range("processing weights no dates here.xlsx")


# ---------------------------------------------------------------------------
# _categorize_note
# ---------------------------------------------------------------------------

def test_categorize_note_empty() -> None:
    assert _categorize_note("") == ""


def test_categorize_note_downtime_keywords() -> None:
    assert _categorize_note("machine was down all morning") == "downtime"
    assert _categorize_note("Belt broken, waiting for repair") == "downtime"


def test_categorize_note_material_keywords() -> None:
    assert _categorize_note("waiting for material") == "material"
    assert _categorize_note("ran out of feedstock") == "material"


def test_categorize_note_quality_keywords() -> None:
    assert _categorize_note("weights not entered") == "quality"
    assert _categorize_note("missing data") == "quality"


def test_categorize_note_fallback_operational() -> None:
    assert _categorize_note("normal day") == "operational"
    assert _categorize_note("trained new operator") == "operational"


# ---------------------------------------------------------------------------
# _extract_date_from_sheet
# ---------------------------------------------------------------------------

def _sheet_with_date_at_0_9(value) -> pd.DataFrame:
    """Build a minimal DataFrame with `value` at row 0, column 9 (COL_DATE)."""
    rows = [[None] * 10]  # 10 columns
    rows[0][9] = value
    return pd.DataFrame(rows)


def test_extract_date_from_sheet_datetime() -> None:
    df = _sheet_with_date_at_0_9(datetime(2026, 4, 13))
    assert _extract_date_from_sheet(df) == "2026-04-13"


def test_extract_date_from_sheet_timestamp() -> None:
    df = _sheet_with_date_at_0_9(pd.Timestamp("2026-04-13"))
    assert _extract_date_from_sheet(df) == "2026-04-13"


def test_extract_date_from_sheet_iso_string() -> None:
    df = _sheet_with_date_at_0_9("2026-04-13")
    assert _extract_date_from_sheet(df) == "2026-04-13"


def test_extract_date_from_sheet_us_format() -> None:
    df = _sheet_with_date_at_0_9("04-13-26")
    assert _extract_date_from_sheet(df) == "2026-04-13"


def test_extract_date_from_sheet_slashes() -> None:
    df = _sheet_with_date_at_0_9("04/13/2026")
    assert _extract_date_from_sheet(df) == "2026-04-13"


def test_extract_date_from_sheet_missing_returns_none() -> None:
    df = _sheet_with_date_at_0_9(None)
    assert _extract_date_from_sheet(df) is None


def test_extract_date_from_sheet_garbage_returns_none() -> None:
    df = _sheet_with_date_at_0_9("not a date")
    assert _extract_date_from_sheet(df) is None


# ---------------------------------------------------------------------------
# dedup_daily — duplicate-row removal must not collapse distinct operators
# ---------------------------------------------------------------------------

def _dup_row(**overrides) -> dict:
    base = {
        "Date": "2026-01-05", "Shift": "1st", "Machine_Name": "EXTRUDER",
        "Output_Product": "PP resin", "Actual_Output": 2000.0,
        "Operator": "Alice", "Machine_Hours": 8.0, "Man_Hours": 8.0,
    }
    base.update(overrides)
    return base


def test_dedup_drops_exact_duplicates() -> None:
    from aggregate_daily_data import dedup_daily
    df = pd.DataFrame([_dup_row(), _dup_row()])
    out, dropped = dedup_daily(df)
    assert len(out) == 1
    assert dropped == 1


def test_dedup_preserves_distinct_operators() -> None:
    """Two operators hitting the same output number on the same machine/shift
    are real rows, not duplicates (regression for over-broad dedup key)."""
    from aggregate_daily_data import dedup_daily
    df = pd.DataFrame([_dup_row(Operator="Alice"), _dup_row(Operator="Bob")])
    out, dropped = dedup_daily(df)
    assert len(out) == 2
    assert dropped == 0


def test_dedup_preserves_distinct_hours() -> None:
    from aggregate_daily_data import dedup_daily
    df = pd.DataFrame([_dup_row(Machine_Hours=8.0), _dup_row(Machine_Hours=4.0)])
    out, dropped = dedup_daily(df)
    assert len(out) == 2
    assert dropped == 0


# ---------------------------------------------------------------------------
# merge_incremental — replace (Week_Start, Shift) slices, keep the rest
# ---------------------------------------------------------------------------

def _agg_row(week: str, shift: str, machine: str = "EXTRUDER", output: float = 1000.0) -> dict:
    return {
        "Date": week, "Week_Start": week, "Shift": shift,
        "Machine_Name": machine, "Output_Product": "PP resin",
        "Actual_Output": output, "Operator": "Z",
        "Machine_Hours": 8.0, "Man_Hours": 8.0,
    }


def test_merge_incremental_replaces_matching_slice() -> None:
    from aggregate_daily_data import merge_incremental
    existing = pd.DataFrame([
        _agg_row("2026-01-05", "1st", output=1000),
        _agg_row("2026-01-12", "1st", output=2000),
    ])
    new = pd.DataFrame([_agg_row("2026-01-12", "1st", output=2500)])
    out = merge_incremental(existing, new)
    assert len(out) == 2
    week2 = out[out["Week_Start"] == "2026-01-12"]
    assert week2["Actual_Output"].tolist() == [2500]
    # untouched week preserved
    assert out[out["Week_Start"] == "2026-01-05"]["Actual_Output"].tolist() == [1000]


def test_merge_incremental_keeps_other_shifts_of_same_week() -> None:
    from aggregate_daily_data import merge_incremental
    existing = pd.DataFrame([
        _agg_row("2026-01-12", "1st", output=1000),
        _agg_row("2026-01-12", "2nd", output=2000),
    ])
    new = pd.DataFrame([_agg_row("2026-01-12", "1st", output=1500)])
    out = merge_incremental(existing, new)
    assert len(out) == 2
    assert out[out["Shift"] == "2nd"]["Actual_Output"].tolist() == [2000]
    assert out[out["Shift"] == "1st"]["Actual_Output"].tolist() == [1500]


def test_merge_incremental_is_idempotent() -> None:
    from aggregate_daily_data import merge_incremental
    existing = pd.DataFrame([_agg_row("2026-01-05", "1st")])
    new = pd.DataFrame([_agg_row("2026-01-12", "1st")])
    once = merge_incremental(existing, new)
    twice = merge_incremental(once, new)
    assert len(once) == len(twice) == 2


def test_merge_incremental_empty_existing_returns_new() -> None:
    from aggregate_daily_data import merge_incremental
    new = pd.DataFrame([_agg_row("2026-01-05", "1st")])
    out = merge_incremental(pd.DataFrame(), new)
    assert len(out) == 1


def test_merge_incremental_backfills_date_corrected_flag() -> None:
    from aggregate_daily_data import merge_incremental
    existing = pd.DataFrame([_agg_row("2026-01-05", "1st")])  # no Date_Corrected col
    new_row = _agg_row("2026-01-12", "1st")
    new_row["Date_Corrected"] = True
    out = merge_incremental(existing, pd.DataFrame([new_row]))
    assert out["Date_Corrected"].tolist() == [False, True]
