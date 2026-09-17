"""Week-at-a-glance / daily-chart payload and header status line for docs/index.html."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import dashboard_daily_sections as ds  # noqa: E402


def _df() -> pd.DataFrame:
    return pd.DataFrame([
        dict(Date="2026-09-14", Shift="1st", Machine_Name="EXTRUDER", Actual_Input=8639.0, Actual_Output=8639.0, Basis="exact"),
        dict(Date="2026-09-14", Shift="1st", Machine_Name="GUILLOTINE", Actual_Input=3441.0, Actual_Output=0.0, Basis="partial"),
        dict(Date="2026-09-14", Shift="2nd", Machine_Name="EXTRUDER", Actual_Input=100.0, Actual_Output=100.0, Basis="averaged"),
        dict(Date="2026-09-14", Shift="2nd", Machine_Name="EXTRUDER", Actual_Input=50.0, Actual_Output=50.0, Basis=None),
        dict(Date="2026-08-20", Shift="1st", Machine_Name="AUTO TIE BALER", Actual_Input=9000.0, Actual_Output=9000.0, Basis=None),
    ])


def test_payload_rows_are_compact_and_carry_both_output_bases() -> None:
    p = ds.build_daily_payload(_df(), {"awaiting": [["2026-09-15", "1st", "EXTRUDER"]], "last_poll": "2026-09-17T13:26:41"})
    assert p["machines"] == ["AUTO TIE BALER", "EXTRUDER", "GUILLOTINE"]          # ordered by volume incl. support
    rows = {(r[0], r[1], p["machines"][r[2]]): r for r in p["rows"]}
    g = rows[("2026-09-14", "1st", "GUILLOTINE")]
    assert g[3] == 0 and g[4] == 3441 and g[5] == "p"                             # standard 0, support fills, in progress
    e2 = rows[("2026-09-14", "2nd", "EXTRUDER")]
    assert e2[3] == 150 and e2[5] == "a"                                          # two rows summed; averaged wins over exact
    assert rows[("2026-08-20", "1st", "AUTO TIE BALER")][5] == "e"               # workbook rows read as exact
    assert p["status"]["awaiting"] == [["2026-09-15", "1st", "EXTRUDER"]]
    js = ds.daily_script(p)
    assert js.startswith("  <script>") and json.dumps(p, separators=(",", ":")) in js and "__DAILY_PAYLOAD__" not in js


def test_payload_without_basis_column_or_status() -> None:
    p = ds.build_daily_payload(_df().drop(columns=["Basis"]), None)
    assert {r[5] for r in p["rows"]} == {"e"} and p["status"] == {}


def test_status_line_mentions_poller_and_open_jobs() -> None:
    line = ds.status_line_html({"last_poll": "2026-09-17T13:26:41", "open_jobs": 14, "open_lbs": 226408}, 88, "2026-09-17")
    assert "Data through <b>Thu Sep 17</b>" in line and "last Thu Sep 17, 1:26 PM" in line
    assert "14 open jobs, 226,408 lbs not yet posted" in line and "88 weeks of data" in line
    assert "polled" not in ds.status_line_html(None, 5, "2026-08-21")


def test_load_status_tolerates_missing_file(tmp_path) -> None:
    assert ds.load_status(tmp_path / "none.json") is None
    (tmp_path / "s.json").write_text('{"open_jobs": 3}')
    assert ds.load_status(tmp_path / "s.json") == {"open_jobs": 3}
