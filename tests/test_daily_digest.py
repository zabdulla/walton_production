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
    df = pd.DataFrame(rows)
    df["Week_Start"] = df["Date"] - pd.to_timedelta(df["Date"].dt.weekday, unit="D")      # the aggregate's own columns
    df["Labor_Cost"] = df["Man_Hours"] * 20.0
    df["Total_Expense"] = df["Labor_Cost"]
    return df


STATUS = {"last_poll": "2026-09-22T05:55:00", "awaiting": [], "end_of_shift": {"reports": [
    {"date": "2026-09-21", "shift": "1st", "by": "Tim", "machines": [
        {"machine": "EXTRUDER", "machine_hours": 7.0, "man_hours": 14.0, "operators": "Steven", "material": "BOPP", "downtime_min": 60, "reason": "Blades", "comment": ""}], "notes": ["clean-up"]},
    {"date": "2026-09-18", "shift": "3rd", "by": "Connor", "machines": [], "notes": []}]}}


def test_pick_day_falls_back_to_latest_day_with_rows() -> None:
    df = _df()
    assert dd.pick_day(df, date(2026, 9, 21)) == date(2026, 9, 21)
    assert dd.pick_day(df, date(2026, 9, 20)) == date(2026, 9, 18)      # weekend -> Friday


def test_build_digest_week_by_shift_and_reports() -> None:
    dg = dd.build_digest(_df(), STATUS, date(2026, 9, 21))
    assert dg["day_total"] == 22000 and dg["week"]["days"] == ["Mon 21"] and dg["week"]["plant_wtd"] == 22000
    first = dg["week"]["by_shift"][0]
    assert first["shift"] == "1st" and first["wtd"] == 11000 and first["day"] == 11000
    assert [r["machine"] for r in first["rows"]] == ["EXTRUDER", "GUILLOTINE"]          # reading order; Guillotine's rolls count as output
    assert next(r for r in first["rows"] if r["machine"] == "EXTRUDER")["days"] == [5000]
    assert dg["week"]["by_shift"][2]["rows"] == [] and dg["week"]["by_shift"][2]["wtd"] == 0
    reps = {r["shift"]: r for r in dg["reports"]}
    assert reps["1st"]["filed"] and reps["1st"]["by"] == "Tim" and reps["1st"]["downtime_min"] == 60 and reps["1st"]["notes"] == ["clean-up"]
    assert reps["1st"]["machines"][0]["reason"] == "Blades" and not reps["2nd"]["filed"] and not reps["3rd"]["filed"]


def test_render_email() -> None:
    dg = dd.build_digest(_df(), STATUS, date(2026, 9, 21))
    html = dd.render_email(dg)
    assert "Monday, September 21" in html and dd.DASHBOARD_URL in html
    assert html.count("Classic dashboard") == 2
    assert html.index("Week at a glance") < html.index("End of Shift reports")
    assert "No End of Shift report was filed" in html and "Blades" in html and "clean-up" in html
    assert "<img" not in html and "<style" not in html and "table-layout:fixed" in html   # no images; inline styles only
    msg = dd.build_message(["a@x.com"], "s", html)
    assert "raw" in msg
    import base64, email
    raw = email.message_from_bytes(base64.urlsafe_b64decode(dd.build_message(["a@x.com", "b@x.com"], "s", html, bcc=["c@y.com"])["raw"]))
    assert raw["To"] == "a@x.com, b@x.com" and raw["Bcc"] == "c@y.com"


def test_monday_email_covers_friday_and_the_complete_week() -> None:
    df = _df()
    day = dd.pick_day(df, date(2026, 9, 20))                              # Monday's target is Sunday -> Friday
    dg = dd.build_digest(df, STATUS, day)
    assert dg["date"] == "2026-09-18" and dg["week"]["days"] == ["Mon 14", "Tue 15", "Wed 16", "Thu 17", "Fri 18"]
    assert {r["shift"]: r["filed"] for r in dg["reports"]} == {"1st": False, "2nd": False, "3rd": True}   # Connor's Friday 3rd
    html = dd.render_email(dg)
    assert "complete week" in html and "Friday, September 18" in html


def test_send_gate_skips_weekends(tmp_path) -> None:
    st = tmp_path / "state.json"
    assert dd.due_now(st, datetime(2026, 9, 26, 9, 0)) == (False, "weekend")    # Saturday
    assert dd.due_now(st, datetime(2026, 9, 27, 9, 0)) == (False, "weekend")    # Sunday
    assert dd.due_now(st, datetime(2026, 9, 28, 7, 2)) == (True, "due")          # Monday


def test_send_gate_once_per_day_after_hour(tmp_path) -> None:
    st = tmp_path / "state.json"
    assert dd.due_now(st, datetime(2026, 9, 22, 5, 59), send_hour=6) == (False, "before 06:00")
    assert dd.due_now(st, datetime(2026, 9, 22, 6, 4), send_hour=6) == (True, "due")
    dd.mark_sent(st, datetime(2026, 9, 22, 6, 4))
    assert json.loads(st.read_text())["last_sent"] == "2026-09-22"
    assert dd.due_now(st, datetime(2026, 9, 22, 18, 0)) == (False, "already sent today")
    assert dd.due_now(st, datetime(2026, 9, 23, 6, 4), send_hour=6) == (True, "due")


def test_email_lists_the_clock_corrections_for_the_week() -> None:
    status = dict(STATUS, shift_flags=[
        {"date": "2026-09-21", "shift": "3rd", "from_shift": "2nd", "kind": "reassigned", "machine": "GRINDER", "job": 17140, "lbs": 4269, "windows": 1, "first": "2026-09-22T05:13", "last": "2026-09-22T05:22"},
        {"date": "2026-09-19", "shift": "1st", "from_shift": "1st", "kind": "weekend", "machine": "EXTRUDER", "job": 17145, "lbs": 5900, "windows": 4, "first": "2026-09-19T07:36", "last": "2026-09-19T12:06"},
        {"date": "2026-09-10", "shift": "3rd", "from_shift": "2nd", "kind": "reassigned", "machine": "GRINDER", "job": 17140, "lbs": 10, "windows": 1, "first": "2026-09-11T05:00", "last": "2026-09-11T05:10"}])
    dg = dd.build_digest(_df(), status, date(2026, 9, 21))
    assert [f["date"] for f in dg["flags"]] == ["2026-09-21"]                       # only this week's days
    html = dd.render_email(dg)
    assert "Shift notes from the cieTrade clock" in html
    assert "4,269 lbs entered on the 2nd-shift job at 5:13 AM–5:22 AM, during 3rd shift hours; counted as 3rd." in html
    assert "cieTrade clock" not in dd.render_email(dd.build_digest(_df(), STATUS, date(2026, 9, 21)))


def test_email_links_the_dashboard_and_the_classic_site() -> None:
    html = dd.render_email(dd.build_digest(_df(), STATUS, date(2026, 9, 21)))
    assert html.count('href="https://zabdulla.github.io/walton_production/"') == 2          # top and bottom banners
    assert html.count('href="https://zabdulla.github.io/walton_production/classic/"') == 2
    assert "Production dashboard" in html and "Classic dashboard" in html and "beta" not in html.lower()


def test_report_rows_follow_the_machine_reading_order() -> None:
    """A form filed Guillotine-first still reads Extruder, Grinder, Auto Tie Baler, ..., Guillotine in the email."""
    import copy
    status = copy.deepcopy(STATUS)
    status["end_of_shift"]["reports"][0]["machines"] = [
        {"machine": "GUILLOTINE", "machine_hours": 7, "man_hours": 7}, {"machine": "AUTO TIE BALER", "machine_hours": 7, "man_hours": 7},
        {"machine": "EXTRUDER", "machine_hours": 7, "man_hours": 14}, {"machine": "REPACKAGING", "machine_hours": 1, "man_hours": 1}]
    dg = dd.build_digest(_df(), status, date(2026, 9, 21))
    r = next(x for x in dg["reports"] if x["shift"] == "1st")
    assert [m["machine"] for m in r["machines"]] == ["EXTRUDER", "AUTO TIE BALER", "GUILLOTINE", "REPACKAGING"]
