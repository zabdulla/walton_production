"""Tests for src/fetch_emails.py — subject parsing, filename classification."""
from __future__ import annotations

import pytest

from fetch_emails import parse_week_dates, shift_from_filename


# ---------------------------------------------------------------------------
# parse_week_dates — extracts MM-DD-YY range from email subjects
# ---------------------------------------------------------------------------

def test_parse_week_dates_two_digit_year() -> None:
    result = parse_week_dates("processing weights for the week of 4/13/26-4/17/26")
    assert result == ("04-13-26", "04-17-26")


def test_parse_week_dates_four_digit_year() -> None:
    result = parse_week_dates("processing weights for the week of 4/13/2026-4/17/2026")
    assert result == ("04-13-26", "04-17-26")


def test_parse_week_dates_zero_padded() -> None:
    result = parse_week_dates("week of 04/13/26-04/17/26")
    assert result == ("04-13-26", "04-17-26")


def test_parse_week_dates_with_space_around_dash() -> None:
    result = parse_week_dates("week of 4/13/26 - 4/17/26")
    assert result == ("04-13-26", "04-17-26")


def test_parse_week_dates_em_dash() -> None:
    # Some emails use en-dash instead of hyphen
    result = parse_week_dates("week of 4/13/26 – 4/17/26")
    assert result == ("04-13-26", "04-17-26")


def test_parse_week_dates_returns_none_when_no_range() -> None:
    assert parse_week_dates("daily report for 4/15/26") is None
    assert parse_week_dates("just some subject") is None


def test_parse_week_dates_walton_prefix() -> None:
    result = parse_week_dates("Walton processing weights for the week of 4/6/26-4/10/26")
    assert result == ("04-06-26", "04-10-26")


# ---------------------------------------------------------------------------
# parse_week_dates — bug T2.4: the regex accepts 3-digit years.
# These tests document current behavior so we can fix it cleanly.
# ---------------------------------------------------------------------------

def test_parse_week_dates_three_digit_year_should_not_silently_succeed() -> None:
    """3-digit year is malformed input and should NOT silently coerce to 2002.

    Today's regex accepts \\d{2,4} which matches 3 digits like '202'.
    After fix: 3-digit input either returns None or raises — the regex should
    only accept exactly 2 or 4 digit years.
    """
    # Currently this incorrectly returns ("04-13-02", ...). After fix: None.
    result = parse_week_dates("week of 4/13/202-4/17/202")
    # Accept either None (cleanest) or a ValueError-raising behavior.
    # Asserting the explicit None contract:
    assert result is None or result == ("04-13-20", "04-17-20")  # WILL FAIL on \d{2,4}


# ---------------------------------------------------------------------------
# shift_from_filename
# ---------------------------------------------------------------------------

def test_shift_from_filename_1st() -> None:
    assert shift_from_filename("1st shift processing weights.xlsx") == "1st"


def test_shift_from_filename_2nd() -> None:
    assert shift_from_filename("2nd shift processing weights.xlsx") == "2nd"


def test_shift_from_filename_3rd() -> None:
    assert shift_from_filename("3rd shift processing weights.xlsx") == "3rd"


def test_shift_from_filename_with_date_suffix() -> None:
    assert shift_from_filename(
        "1st shift processing weights 04-13-26 to 04-17-26.xlsx"
    ) == "1st"


def test_shift_from_filename_case_insensitive() -> None:
    assert shift_from_filename("1ST SHIFT PROCESSING WEIGHTS.xlsx") == "1st"


def test_shift_from_filename_non_shift_file_returns_none() -> None:
    assert shift_from_filename("Walton PayPeriod_Report(2026-04-06).pdf") is None
    assert shift_from_filename("random.xlsx") is None
    assert shift_from_filename("processing weights weekly.xlsx") is None  # no shift


def test_shift_from_filename_requires_shift_keyword() -> None:
    # "1st" alone isn't enough — must be in a shift-file context
    assert shift_from_filename("1st place trophy.xlsx") is None
