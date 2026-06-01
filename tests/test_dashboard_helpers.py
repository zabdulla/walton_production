"""Tests for shared dashboard helpers in build_interactive_dashboard.py."""
from __future__ import annotations

import pandas as pd

from build_interactive_dashboard import (
    _fmt_num,
    _pct_change_html,
    clean_product_names,
)


# ---------------------------------------------------------------------------
# _fmt_num
# ---------------------------------------------------------------------------

def test_fmt_num_int_default() -> None:
    assert _fmt_num(1234) == "1,234"
    assert _fmt_num(1234, "int") == "1,234"


def test_fmt_num_currency_no_decimals() -> None:
    assert _fmt_num(1234.5, "currency") == "$1,234"
    assert _fmt_num(1234.99, "currency") == "$1,235"


def test_fmt_num_currency_4_decimals() -> None:
    assert _fmt_num(0.1234, "currency4") == "$0.1234"
    assert _fmt_num(1.0, "currency4") == "$1.0000"


def test_fmt_num_float1() -> None:
    assert _fmt_num(3.14159, "float1") == "3.1"


def test_fmt_num_float2() -> None:
    assert _fmt_num(3.14159, "float2") == "3.14"


def test_fmt_num_nan_returns_em_dash() -> None:
    assert _fmt_num(pd.NA) == "—"
    assert _fmt_num(float("nan")) == "—"


def test_fmt_num_large_int() -> None:
    assert _fmt_num(1_000_000) == "1,000,000"


# ---------------------------------------------------------------------------
# _pct_change_html
# ---------------------------------------------------------------------------

def test_pct_change_positive() -> None:
    html = _pct_change_html(110, 100)
    assert "+10.0%" in html
    assert "trend-up" in html


def test_pct_change_negative() -> None:
    html = _pct_change_html(90, 100)
    assert "-10.0%" in html
    assert "trend-down" in html


def test_pct_change_flat() -> None:
    html = _pct_change_html(100, 100)
    assert "0%" in html
    assert "trend-flat" in html


def test_pct_change_zero_previous_returns_empty() -> None:
    # Avoid divide-by-zero
    assert _pct_change_html(100, 0) == ""


def test_pct_change_nan_returns_empty() -> None:
    assert _pct_change_html(pd.NA, 100) == ""
    assert _pct_change_html(100, pd.NA) == ""


# ---------------------------------------------------------------------------
# clean_product_names
# ---------------------------------------------------------------------------

def test_clean_product_names_applies_typo_map() -> None:
    df = pd.DataFrame({
        "Output Product": ["PP Resin", "PP Shreds", "Tisue bales"],
    })
    cleaned = clean_product_names(df)
    # Typos corrected: "PP Resin" → "PP resin", etc.
    assert "PP resin" in cleaned["Output Product"].values
    assert "PP shreds" in cleaned["Output Product"].values
    assert "Tissue bales" in cleaned["Output Product"].values


def test_clean_product_names_adds_category_column() -> None:
    df = pd.DataFrame({"Output Product": ["PP resin", "LD Bales"]})
    cleaned = clean_product_names(df)
    assert "Product Category" in cleaned.columns
    assert cleaned.loc[cleaned["Output Product"] == "PP resin", "Product Category"].iloc[0] == "PP - Resin"
    assert cleaned.loc[cleaned["Output Product"] == "LD Bales", "Product Category"].iloc[0] == "LDPE - Bales"


def test_clean_product_names_other_for_unknown() -> None:
    df = pd.DataFrame({"Output Product": ["Never Heard Of It"]})
    cleaned = clean_product_names(df)
    assert cleaned["Product Category"].iloc[0] == "Other"


def test_clean_product_names_no_op_when_column_absent() -> None:
    df = pd.DataFrame({"foo": [1, 2, 3]})
    cleaned = clean_product_names(df)
    # Same shape, no Product Category column
    assert "Product Category" not in cleaned.columns


def test_clean_product_names_does_not_mutate_input() -> None:
    df = pd.DataFrame({"Output Product": ["PP Resin"]})
    original = df.copy()
    clean_product_names(df)
    pd.testing.assert_frame_equal(df, original)
