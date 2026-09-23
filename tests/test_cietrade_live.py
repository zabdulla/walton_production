"""Live feed: change windows, production-day boundaries, tiles and curves."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import cietrade_live as live  # noqa: E402

COLS = ["Snapshot", "Job No", "Job Date", "Warehouse", "Warehouse Status", "Machine", "Status", "Post Date", "Start-Time",
        "End-Time", "Elapsed-Time", "UOM", "Input Qty", "Output Qty", "Yield Loss Qty", "Department", "Description",
        "Input Value", "Output Value", "Expenses", "User Defined 1", "UserDefined2", "Input Units", "Output Units",
        "Operator", "Finished Product"]


def _row(ts, job, machine, qty, units, job_date="2026-09-17", start="12:00PM"):
    r = {c: "" for c in COLS}
    r.update({"Snapshot": ts, "Job No": job, "Job Date": job_date, "Warehouse": "Plus Monroe Warehouse", "Machine": machine,
              "Status": "Work", "Start-Time": start, "UOM": "LBS", "Input Qty": 0, "Output Qty": qty, "Input Units": 0, "Output Units": units})
    return r


def _api_dir(tmp_path: Path) -> Path:
    d = tmp_path / "cietrade"; (d / "snapshots").mkdir(parents=True)
    rows = [
        _row("2026-09-17T13:00:00", 1, "EXTRUDER (1ST SHIFT)", 100, 0, job_date="2026-09-16", start="9:00AM"),  # older than the previous poll: history unknown
        _row("2026-09-17T13:20:00", 1, "EXTRUDER (1ST SHIFT)", 250, 0),                      # +150 between the 13:10 and 13:20 polls
        _row("2026-09-17T13:20:00", 2, "AUTO-TIE BALER (1ST SHIFT)", 300, 1, start="1:15PM"),  # created 13:15, first seen: +300, +1 bale
        _row("2026-09-17T13:30:00", 1, "EXTRUDER (1ST SHIFT)", 200, 0),                      # correction −50
        _row("2026-09-17T13:30:00", 2, "AUTO-TIE BALER (1ST SHIFT)", 300, 1),
        _row("2026-09-16T10:00:00", 9, "GUILLOTINE (1ST SHIFT)", 0, 0, job_date="2026-09-16", start="9:55AM"),
        _row("2026-09-16T10:10:00", 9, "GUILLOTINE (1ST SHIFT)", 900, 0, job_date="2026-09-16", start="9:55AM"),  # yesterday +900
    ]
    pd.DataFrame(rows, columns=COLS).to_csv(d / "snapshots" / "all.csv", index=False)
    polls = ["2026-09-16T09:50:00", "2026-09-16T10:00:00", "2026-09-16T10:10:00",
             "2026-09-17T13:00:00", "2026-09-17T13:10:00", "2026-09-17T13:20:00", "2026-09-17T13:30:00"]
    (d / "polls.jsonl").write_text("".join(json.dumps({"ts": t, "ok": True, "changed": True}) + "\n" for t in polls))
    return d


def test_changes_use_the_previous_poll_as_the_window_start(tmp_path) -> None:
    d = _api_dir(tmp_path)
    ch = live.changes(live.load_snapshots(d), live.load_polls(d))
    today = ch[ch["t1"] >= "2026-09-17"]
    assert [(str(r.t0)[11:16], str(r.t1)[11:16], r.machine, r.lbs, r.units, r.correction) for r in today.itertuples()] == [
        ("13:15", "13:20", "AUTO TIE BALER", 300, 1, False),   # window starts when the job was opened
        ("13:10", "13:20", "EXTRUDER", 150, 0, False),
        ("13:20", "13:30", "EXTRUDER", -50, 0, True),
    ]


def test_production_day_and_current_shift_follow_the_shift_clock() -> None:
    assert live.production_day(pd.Timestamp("2026-09-17 05:59")) == pd.Timestamp("2026-09-16")
    assert live.production_day(pd.Timestamp("2026-09-17 06:00")) == pd.Timestamp("2026-09-17")
    assert [live.current_shift(pd.Timestamp(f"2026-09-17 {h}")) for h in ("06:00", "13:59", "14:00", "21:59", "22:00", "02:30")] == \
        ["1st", "1st", "2nd", "2nd", "3rd", "3rd"]


def test_build_live_tiles_curves_and_feed(tmp_path) -> None:
    d = _api_dir(tmp_path)
    L = live.build_live(now=pd.Timestamp("2026-09-17 14:35:00"), api_dir=d)
    assert L["day"] == "2026-09-17" and L["current_shift"] == "2nd" and L["lbs_today"] == 400 and L["changes_today"] == 3
    tiles = {m["m"]: m for m in L["machines"]}
    assert tiles["AUTO TIE BALER"]["lbs"] == 300 and tiles["EXTRUDER"]["lbs"] == 100 and tiles["EXTRUDER"]["by_shift"] == {"1st": 100}
    assert tiles["EXTRUDER"]["quiet_min"] == 65 and tiles["EXTRUDER"]["flag"] is False      # produced on 1st, quiet during 2nd: no flag
    ext = L["series"]["EXTRUDER"]
    assert ext[0] == [0, 0] and ext[-1][1] == 100 and ext[-1][0] == 7 * 60 + 30          # flat to the last poll (13:30)
    assert L["yesterday"]["lbs"] == 900 and L["yesterday"]["total"][-1] == [24 * 60, 900]
    assert [c["lbs"] for c in L["changes"]] == [-50, 300, 150, 900] and L["changes"][0]["correction"]
    out = live.write_live(tmp_path / "live.json", now=pd.Timestamp("2026-09-17 14:35:00"), api_dir=d)
    assert json.loads((tmp_path / "live.json").read_text())["lbs_today"] == out["lbs_today"] == 400


def test_quiet_flag_only_for_machines_that_ran_this_shift(tmp_path) -> None:
    d = _api_dir(tmp_path)
    L = live.build_live(now=pd.Timestamp("2026-09-17 13:59:00"), api_dir=d)   # still 1st shift, 29 min after the last change
    assert all(not m["flag"] for m in L["machines"])
    assert {m["m"]: m["quiet_min"] for m in L["machines"]} == {"AUTO TIE BALER": 39, "EXTRUDER": 29}


def test_build_live_with_no_data(tmp_path) -> None:
    d = tmp_path / "empty"; d.mkdir()
    L = live.build_live(now=pd.Timestamp("2026-09-19 09:00:00"), api_dir=d)     # Saturday: no shift scheduled
    assert L["machines"] == [] and L["changes"] == [] and L["current_shift"] is None and L["last_poll"] is None


def test_outage_names_the_first_failed_poll_after_the_last_good_one(tmp_path) -> None:
    d = _api_dir(tmp_path)
    with (d / "polls.jsonl").open("a") as fh:
        fh.write(json.dumps({"ts": "2026-09-17T13:40:00", "ok": False, "changed": False, "error": "HTTPError: HTTP Error 404: Not Found"}) + "\n")
        fh.write(json.dumps({"ts": "2026-09-17T13:50:00", "ok": False, "changed": False, "error": "HTTPError: HTTP Error 404: Not Found"}) + "\n")
    L = live.build_live(now=pd.Timestamp("2026-09-17 13:55:00"), api_dir=d)
    assert not L["poll_ok"] and L["last_ok_poll"] == "2026-09-17T13:30:00" and L["api_down_since"] == "2026-09-17T13:40:00"
    assert L["failed_polls"] == 2 and "404" in L["last_error"]
    assert L["lbs_today"] == 400 and L["changes_today"] == 3          # figures through the last good poll are kept
    ok = live.build_live(now=pd.Timestamp("2026-09-17 13:35:00"), api_dir=_api_dir(tmp_path / "ok"))
    assert ok["poll_ok"] and ok["api_down_since"] is None and ok["failed_polls"] == 0 and ok["last_error"] == ""
