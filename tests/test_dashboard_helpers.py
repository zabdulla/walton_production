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


# ---------------------------------------------------------------------------
# target_miss_counts / RAG status
# ---------------------------------------------------------------------------

def _weekly_frame(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    """rows: (machine, week_start, output)"""
    df = pd.DataFrame(rows, columns=["Machine Name", "Week Start", "Actual_Output"])
    df["Week Start"] = pd.to_datetime(df["Week Start"])
    return df


def test_target_miss_counts_counts_hits_and_misses() -> None:
    from build_interactive_dashboard import target_miss_counts
    # EXTRUDER target is 100,000
    df = _weekly_frame([
        ("EXTRUDER", "2026-01-05", 120_000),  # hit
        ("EXTRUDER", "2026-01-12", 80_000),   # miss
        ("EXTRUDER", "2026-01-19", 100_000),  # hit (>= target)
    ])
    counts = target_miss_counts(df, lookback=8)
    assert counts["EXTRUDER"]["misses"] == 1
    assert counts["EXTRUDER"]["n_weeks"] == 3
    assert [w["hit"] for w in counts["EXTRUDER"]["weeks"]] == [True, False, True]


def test_target_miss_counts_idle_week_is_a_miss() -> None:
    from build_interactive_dashboard import target_miss_counts
    # GRINDER (target 80k) has no row in the week of Jan 12, but the dataset
    # does (via EXTRUDER) — that idle week counts as a miss.
    df = _weekly_frame([
        ("GRINDER", "2026-01-05", 90_000),
        ("EXTRUDER", "2026-01-12", 120_000),
        ("GRINDER", "2026-01-19", 85_000),
    ])
    counts = target_miss_counts(df, lookback=8)
    assert counts["GRINDER"]["misses"] == 1
    assert counts["GRINDER"]["n_weeks"] == 3


def test_target_miss_counts_respects_lookback_window() -> None:
    from build_interactive_dashboard import target_miss_counts
    rows = [("EXTRUDER", f"2026-01-{d:02d}", 0) for d in (5, 12, 19, 26)]
    rows += [("EXTRUDER", "2026-02-02", 150_000), ("EXTRUDER", "2026-02-09", 150_000)]
    counts = target_miss_counts(_weekly_frame(rows), lookback=2)
    # Only the last 2 weeks considered, both hits
    assert counts["EXTRUDER"]["misses"] == 0
    assert counts["EXTRUDER"]["n_weeks"] == 2


def test_target_miss_counts_skips_untracked_and_absent_machines() -> None:
    from build_interactive_dashboard import target_miss_counts
    df = _weekly_frame([("SHREDDER", "2026-01-05", 50_000)])  # no target defined
    counts = target_miss_counts(df, lookback=8)
    assert counts == {}


def test_target_miss_counts_sums_multiple_rows_per_week() -> None:
    from build_interactive_dashboard import target_miss_counts
    # Two rows (e.g. two shifts) in the same week sum to a hit.
    df = _weekly_frame([
        ("EXTRUDER", "2026-01-05", 60_000),
        ("EXTRUDER", "2026-01-05", 50_000),
    ])
    counts = target_miss_counts(df, lookback=8)
    assert counts["EXTRUDER"]["misses"] == 0


def test_rag_status_thresholds() -> None:
    from build_interactive_dashboard import _rag_status
    assert _rag_status(0) == ("#22c55e", "On target")
    assert _rag_status(1) == ("#22c55e", "On target")
    assert _rag_status(2) == ("#f59e0b", "Needs attention")
    assert _rag_status(3) == ("#f59e0b", "Needs attention")
    assert _rag_status(4) == ("#ef4444", "Below target")
    assert _rag_status(8) == ("#ef4444", "Below target")


def test_build_target_rag_html_renders_cards() -> None:
    from build_interactive_dashboard import build_target_rag_html
    df = _weekly_frame([
        ("EXTRUDER", "2026-01-05", 120_000),
        ("EXTRUDER", "2026-01-12", 80_000),
    ])
    html = build_target_rag_html(df)
    assert "EXTRUDER" in html
    assert "1 / 2" in html
    assert "rag-dot" in html
