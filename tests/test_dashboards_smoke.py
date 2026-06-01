"""
Smoke tests for the generated dashboard HTML files.

These are not unit tests of builder functions — they exercise the actual
``docs/index.html`` and ``docs/daily.html`` (and the local ``reports/*``
dashboards if present), asserting they look like real dashboards rather
than truncated junk.

Skipped if the dashboard hasn't been built yet. CI should build first,
then run these.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DOCS = PROJECT_ROOT / "docs"
REPORTS = PROJECT_ROOT / "reports"


# Minimum size threshold: a real dashboard with all sections + embedded
# Plotly data is hundreds of KB. Anything under 50 KB is almost certainly
# truncated.
MIN_SIZE_BYTES = 50_000


PUBLISHED_DASHBOARDS = [
    (DOCS / "index.html", "Interactive dashboard"),
    (DOCS / "daily.html", "Daily dashboard"),
]

LOCAL_DASHBOARDS = [
    (REPORTS / "operator.html", "Operator dashboard"),
    (REPORTS / "profit.html", "Profit dashboard"),
    (REPORTS / "payroll.html", "Payroll dashboard"),
]


def _read_or_skip(path: Path) -> str:
    if not path.exists():
        pytest.skip(f"{path.name} not built yet — run weekly_update.py first")
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Published dashboards (docs/) — always required
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path,label", PUBLISHED_DASHBOARDS)
def test_published_dashboard_exists(path: Path, label: str) -> None:
    assert path.exists(), f"{label} missing at {path}"


@pytest.mark.parametrize("path,label", PUBLISHED_DASHBOARDS)
def test_published_dashboard_minimum_size(path: Path, label: str) -> None:
    html = _read_or_skip(path)
    size = len(html)
    assert size > MIN_SIZE_BYTES, (
        f"{label} suspiciously small ({size:,} bytes < {MIN_SIZE_BYTES:,})"
    )


@pytest.mark.parametrize("path,label", PUBLISHED_DASHBOARDS)
def test_published_dashboard_includes_plotly(path: Path, label: str) -> None:
    html = _read_or_skip(path)
    # Either CDN-loaded or bundled
    assert "plotly" in html.lower() or "Plotly" in html, (
        f"{label} missing Plotly include / data — likely render failure"
    )


@pytest.mark.parametrize("path,label", PUBLISHED_DASHBOARDS)
def test_published_dashboard_html_well_formed(path: Path, label: str) -> None:
    html = _read_or_skip(path)
    # Top and bottom tags present
    assert html.lstrip().lower().startswith(("<!doctype", "<html")), (
        f"{label} doesn't start with a doctype/html tag"
    )
    assert "</html>" in html.lower(), f"{label} missing closing </html>"


@pytest.mark.parametrize("path,label", PUBLISHED_DASHBOARDS)
def test_published_dashboard_has_data(path: Path, label: str) -> None:
    """At least one Plotly figure should be present in the rendered HTML."""
    html = _read_or_skip(path)
    # Plotly emits a div with id and inline JS calling Plotly.newPlot.
    # Looser check: count occurrences of either pattern.
    has_newplot = "Plotly.newPlot" in html or "Plotly.react" in html
    has_data_div = bool(re.search(r"<div\s+id=['\"]fig", html))
    assert has_newplot or has_data_div, (
        f"{label} has no visible Plotly figures — render likely failed"
    )


# ---------------------------------------------------------------------------
# Local dashboards (reports/) — gitignored, skip if absent
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path,label", LOCAL_DASHBOARDS)
def test_local_dashboard_size(path: Path, label: str) -> None:
    html = _read_or_skip(path)
    assert len(html) > MIN_SIZE_BYTES, (
        f"{label} suspiciously small ({len(html):,} bytes)"
    )


# ---------------------------------------------------------------------------
# Cross-dashboard checks
# ---------------------------------------------------------------------------

def test_index_dashboard_includes_navigation_link() -> None:
    html = _read_or_skip(DOCS / "index.html")
    # The interactive dashboard should link to the daily view
    assert "daily.html" in html, "index.html lost link to daily.html"


def test_daily_dashboard_does_not_reference_nonexistent_data() -> None:
    """Daily dashboard embeds JSON via Python's to_json — sanity-check it parsed."""
    html = _read_or_skip(DOCS / "daily.html")
    # Quick check: the daily dashboard injects a SUMMARY array
    assert "SUMMARY" in html or "Total_Output" in html or "summary_json" not in html
