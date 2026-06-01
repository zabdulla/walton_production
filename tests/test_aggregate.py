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
