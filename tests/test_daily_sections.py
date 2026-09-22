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
    assert "Data through <b>Thu Sep 17</b>" in line and "rebuilt Thu Sep 17, 1:26 PM" in line and "88 weeks of data" in line
    assert "cieTrade" not in ds.status_line_html(None, 5, "2026-08-21")


def test_load_status_tolerates_missing_file(tmp_path) -> None:
    assert ds.load_status(tmp_path / "none.json") is None
    (tmp_path / "s.json").write_text('{"open_jobs": 3}')
    assert ds.load_status(tmp_path / "s.json") == {"open_jobs": 3}


def test_end_of_shift_card_shows_filed_and_missing_shifts() -> None:
    from dashboard_daily_sections import end_of_shift_html
    status = {"end_of_shift": {"as_of": "2026-09-21", "filed": 1, "entries": 3, "last_filed": "2026-09-21",
                               "days": [{"date": "2026-09-21", "shifts": {"1st": {"filed": True, "by": "Tim", "machines": 5, "machine_hours": 32.0, "man_hours": 39.0, "downtime_min": 120},
                                                                          "2nd": {"filed": False}, "3rd": {"filed": False}}},
                                        {"date": "2026-09-20", "shifts": {"1st": {"filed": False}, "2nd": {"filed": False}, "3rd": {"filed": False}}}],
                               "recent": [{"date": "2026-09-21", "shift": "1st", "machine": "AUTO TIE BALER", "operator": "Tony", "hours": 5.25, "downtime_min": 120, "reason": "Other", "comment": "track fix"}],
                               "notes": [{"date": "2026-09-21", "shift": "2nd", "note": "Steven A was unloading"}]}}
    html = end_of_shift_html(status)
    assert "Tim" in html and "5 machines" in html and "120 min down" in html and html.count("pill await\">missing") == 2   # Sat 20th shows — not missing
    assert "Steven A was unloading" in html and "track fix" in html
    assert "No submissions" in end_of_shift_html({"end_of_shift": None}) and "No submissions" in end_of_shift_html(None)


def test_submitted_forms_render_every_machine_row() -> None:
    from dashboard_daily_sections import submitted_forms_html
    eos = {"as_of": "2026-09-21", "reports": [
        {"date": "2026-09-21", "shift": "1st", "by": "Tim", "filed_at": "9/21/2026", "source": "form",
         "machines": [{"machine": "EXTRUDER", "machine_hours": 7.0, "man_hours": 14.5, "operators": "Steven, Daniel", "material": "BOPP resin", "downtime_min": 60, "reason": "Waiting on material", "comment": ""},
                      {"machine": "GUILLOTINE", "machine_hours": 7.25, "man_hours": 7.25, "operators": "Vince", "material": "Ricoh Slabs/BOPP", "downtime_min": 0, "reason": "", "comment": ""}],
         "notes": ["Steven A was unloading"]},
        {"date": "2026-09-18", "shift": "3rd", "by": "Connor", "filed_at": "", "source": "form", "machines": [], "notes": []}]}
    html = submitted_forms_html(eos)
    assert html.count("<details") == 2 and "Monday Sep 21" in html and "Friday Sep 18" in html and "1st shift" in html and "3rd shift" in html
    assert "EXTRUDER" in html and "Steven, Daniel" in html and "Waiting on material" in html and "Ricoh Slabs/BOPP" in html
    assert "2 machines" in html and "14.25 machine h" in html and "21.75 man h" in html and "60 min down" in html and "Steven A was unloading" in html
    assert submitted_forms_html({"as_of": "2026-09-21", "reports": []}) == ""
