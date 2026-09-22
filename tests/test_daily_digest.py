"""Daily digest: day and week figures, End of Shift roll-up, trend, once-a-day gate. No network."""
from __future__ import annotations

import json
from datetime import date, datetime

import pandas as pd

import daily_digest as dd


def _df():
    rows = []
    for wk in range(6):                                   # six Mondays back to the digest day's week
        for dow in range(5):
            d = pd.Timestamp("2026-09-21") - pd.Timedelta(weeks=wk) + pd.Timedelta(days=dow)
            if d > pd.Timestamp("2026-09-21"):
                continue
            for sh in ("1st", "2nd"):
                rows.append({"Date": d, "Shift": sh, "Machine_Name": "EXTRUDER", "Actual_Output": 5000 + 100 * wk, "Actual_Input": 0, "Machine_Hours": 7, "Man_Hours": 14})
                rows.append({"Date": d, "Shift": sh, "Machine_Name": "GUILLOTINE", "Actual_Output": 0, "Actual_Input": 6000, "Machine_Hours": 0, "Man_Hours": 0})
    rows.append({"Date": pd.Timestamp("2026-09-22"), "Shift": "1st", "Machine_Name": "EXTRUDER", "Actual_Output": 9999, "Actual_Input": 0, "Machine_Hours": 1, "Man_Hours": 1})
    return pd.DataFrame(rows)


STATUS = {"last_poll": "2026-09-22T05:55:00", "awaiting": [], "end_of_shift": {"reports": [
    {"date": "2026-09-21", "shift": "1st", "by": "Tim", "machines": [
        {"machine": "EXTRUDER", "machine_hours": 7.0, "man_hours": 14.0, "operators": "Steven", "material": "BOPP", "downtime_min": 60, "reason": "Blades", "comment": ""}], "notes": ["clean-up"]},
    {"date": "2026-09-18", "shift": "3rd", "by": "Connor", "machines": [], "notes": []}]}}


def test_pick_day_falls_back_to_latest_day_with_rows() -> None:
    df = _df()
    assert dd.pick_day(df, date(2026, 9, 21)) == date(2026, 9, 21)
    assert dd.pick_day(df, date(2026, 9, 20)) == date(2026, 9, 18)      # weekend -> Friday


def test_build_digest_day_week_trend_and_eos() -> None:
    dg = dd.build_digest(_df(), STATUS, date(2026, 9, 21))
    ex = next(r for r in dg["rows"] if r["machine"] == "EXTRUDER")
    assert ex["shifts"] == [5000, 5000, 0] and ex["total"] == 10000 and ex["machine_h"] == 14 and ex["lbs_per_mh"] == 714
    assert ex["avg4"] == 10500                                            # previous four Mondays: 5100..5400 per shift, two shifts
    gu = next(r for r in dg["rows"] if r["machine"] == "GUILLOTINE")
    assert gu["total"] == 12000                                           # rolls counted in when no output is booked
    assert dg["day_total"] == 22000 and dg["week"]["days"] == ["Mon 21"] and dg["week"]["total"] == 22000
    assert next(r for r in dg["week"]["rows"] if r["machine"] == "EXTRUDER")["pace"] == 20000
    assert dg["trend"][-1]["partial"] and dg["trend"][-1]["lbs"] == 22000     # today's rows (Sep 22) excluded
    assert len(dg["trend"]) == 6 and dg["trend"][-2]["avg4"] is not None
    assert dg["eos"]["filed"] == {"1st": "Tim", "2nd": None, "3rd": None} and dg["eos"]["downtime_total"] == 60 and dg["eos"]["notes"] == [{"shift": "1st", "note": "clean-up"}]


def test_render_email_and_chart(tmp_path) -> None:
    dg = dd.build_digest(_df(), STATUS, date(2026, 9, 21))
    html = dd.render_email(dg, image_src="cid:trend.png")
    assert "Monday, September 21" in html and "cid:trend.png" in html and dd.DASHBOARD_URL in html and "missing" in html and "Blades" in html
    png = dd.draw_trend_png(dg["trend"], tmp_path / "t.png")
    assert png.exists() and png.stat().st_size > 1000


def test_send_gate_once_per_day_after_hour(tmp_path) -> None:
    st = tmp_path / "state.json"
    assert dd.due_now(st, datetime(2026, 9, 22, 5, 59)) == (False, "before 06:00")
    assert dd.due_now(st, datetime(2026, 9, 22, 6, 4)) == (True, "due")
    dd.mark_sent(st, datetime(2026, 9, 22, 6, 4))
    assert json.loads(st.read_text())["last_sent"] == "2026-09-22"
    assert dd.due_now(st, datetime(2026, 9, 22, 18, 0)) == (False, "already sent today")
    assert dd.due_now(st, datetime(2026, 9, 23, 6, 4)) == (True, "due")
