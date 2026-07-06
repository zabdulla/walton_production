"""Tests for src/last_run_status.py — log parsing and relative-time text.

Regression context: on 2026-07-02 the orchestrator's logger format dropped
millisecond timestamps. The status parser required them, so every newer run
was invisible and the report claimed the last run was weeks old.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

import last_run_status


OLD_FORMAT_RUN = """\
2026-06-15 12:00:04,532 [INFO] === START 2026-06-15 12:00:04 ===
2026-06-15 12:00:30,120 [INFO] fetching
2026-06-15 12:01:05,900 [INFO] === END runtime=61.0s ===
"""

NEW_FORMAT_RUN = """\
2026-07-06 12:00:05 [INFO] === START 2026-07-06 12:00:05 ===
2026-07-06 12:00:30 [INFO] aggregating
2026-07-06 12:00:57 [INFO] === END runtime=51.9s ===
"""


def _parse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str) -> list[dict]:
    log = tmp_path / "weekly_update.log"
    log.write_text(text, encoding="utf-8")
    monkeypatch.setattr(last_run_status, "LOG_FILE", log)
    return last_run_status.parse_runs()


def test_parse_runs_old_format_with_milliseconds(tmp_path, monkeypatch) -> None:
    runs = _parse(tmp_path, monkeypatch, OLD_FORMAT_RUN)
    assert len(runs) == 1
    assert runs[0]["start"] == "2026-06-15 12:00:04"
    assert runs[0]["runtime"] == 61.0


def test_parse_runs_new_format_without_milliseconds(tmp_path, monkeypatch) -> None:
    """The current logger writes no milliseconds — these runs must parse."""
    runs = _parse(tmp_path, monkeypatch, NEW_FORMAT_RUN)
    assert len(runs) == 1
    assert runs[0]["start"] == "2026-07-06 12:00:05"
    assert runs[0]["runtime"] == 51.9


def test_parse_runs_mixed_formats_sees_every_run(tmp_path, monkeypatch) -> None:
    """A log spanning the format change must report the NEWEST run as latest,
    not glue new-format runs onto the last old-format entry."""
    runs = _parse(tmp_path, monkeypatch, OLD_FORMAT_RUN + NEW_FORMAT_RUN)
    assert len(runs) == 2
    assert runs[-1]["start"] == "2026-07-06 12:00:05"
    assert runs[-1]["end"] is not None


# ---------------------------------------------------------------------------
# human_age — past and future
# ---------------------------------------------------------------------------

def test_human_age_past() -> None:
    assert last_run_status.human_age(datetime.now() - timedelta(days=3)) == "3 days ago"
    assert last_run_status.human_age(datetime.now() - timedelta(hours=2)) == "2 hours ago"
    assert last_run_status.human_age(datetime.now() - timedelta(seconds=10)) == "just now"


def test_human_age_future() -> None:
    """timedelta normalization made future dates read as short past ages
    ("1 hour ago" for next Monday). Future must say "in ..."."""
    assert last_run_status.human_age(datetime.now() + timedelta(days=6, hours=23)) == "in 6 days"
    assert last_run_status.human_age(datetime.now() + timedelta(hours=3, minutes=5)) == "in 3 hours"
    assert last_run_status.human_age(datetime.now() + timedelta(seconds=30)) == "shortly"
