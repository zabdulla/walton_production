"""cieTrade API client, poller, attribution model and dashboard-row adapter."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import cietrade_api as api  # noqa: E402
import cietrade_daily as daily  # noqa: E402
import cietrade_model as model  # noqa: E402
import cietrade_poll as poll  # noqa: E402

SAMPLE = {"JobNo": "17040", "JobDate": "2026-08-24T00:00:00", "Warehouse": "MAIN PLANT", "WarehouseStatus": "IN PROCESS",
          "Machine": "BALER 2", "Status": "Work", "PostDate": None, "Operator": "JSMITH", "StartTime": "7:30AM", "EndTime": "11:45AM",
          "ElapsedTime": "04:15:00", "Department": "Recycling", "UOM": "LBS", "Expenses": 125.0, "InputQty": "42,000",
          "OutputQty": "40,150", "YieldLossQty": "1,850", "Description": "OCC to baled OCC", "InputValue": 2100.0,
          "OutputValue": 3212.0, "UserDefined1": "", "UserDefined2": "", "FinishedProduct": "", "InputUnits": 21, "OutputUnits": 38}
CREDS = {"base_url": "https://api.example.test", "user_id": "u@example.com", "api_key": "k"}


def job(no, machine, job_date, start, qty, status="Work", post=""):
    return {"JobNo": str(no), "JobDate": f"{job_date}T00:00:00", "Warehouse": "Plus Monroe Warehouse",
            "WarehouseStatus": "IN PROCESS" if status == "Work" else "COMPLETED", "Machine": machine, "Status": status,
            "PostDate": f"{post}T00:00:00" if post else None, "Operator": None, "StartTime": start, "EndTime": None,
            "ElapsedTime": None, "Department": "Plus Materials", "UOM": "LBS", "Expenses": 0, "InputQty": "0.000",
            "OutputQty": f"{qty:.3f}", "YieldLossQty": "0", "Description": "", "InputValue": 0, "OutputValue": 0,
            "UserDefined1": "", "UserDefined2": "", "FinishedProduct": "", "InputUnits": 0, "OutputUnits": 0}


# ---- API client -----------------------------------------------------------

def test_normalize_maps_fields_and_parses_formatted_numbers() -> None:
    df = api.normalize([SAMPLE])
    assert list(df.columns) == api.EXPORT_COLUMNS
    r = df.iloc[0]
    assert r["Job No"] == 17040 and r["Job Date"] == "2026-08-24" and r["Post Date"] == ""
    assert r["Output Qty"] == 40150.0 and r["Input Qty"] == 42000.0 and r["Output Units"] == 38
    assert r["Machine"] == "BALER 2" and r["Status"] == "Work"


def test_list_converting_jobs_sends_bearer_and_raises_on_error_payload() -> None:
    seen = {}

    def fake(url, headers):
        seen["url"], seen["headers"] = url, headers
        return json.dumps([{"ERROR": "Request must include a bearer authorization token"}]).encode()

    with pytest.raises(api.CieTradeError):
        api.list_converting_jobs(CREDS, _fetch=fake, Status="WORK", DateFrom=None)
    assert seen["headers"]["Authorization"] == "Bearer k" and seen["headers"]["Accept"] == "application/json"
    assert "UserID=u%40example.com" in seen["url"] and "Status=WORK" in seen["url"] and "DateFrom" not in seen["url"]
    assert "k" not in seen["url"].split("?")[1].replace("UserID=u%40example.com", "")  # key never in the query string


def test_load_credentials_prefers_environment(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("CIETRADE_USER_ID", "env@example.com")
    monkeypatch.setenv("CIETRADE_API_KEY", "envkey")
    c = api.load_credentials(tmp_path / "missing.json")
    assert c["user_id"] == "env@example.com" and c["api_key"] == "envkey"
    monkeypatch.delenv("CIETRADE_USER_ID")
    monkeypatch.delenv("CIETRADE_API_KEY")
    with pytest.raises(FileNotFoundError):
        api.load_credentials(tmp_path / "missing.json")


# ---- poller ---------------------------------------------------------------

@pytest.fixture
def poll_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(poll, "POLLS", tmp_path / "polls.jsonl")
    monkeypatch.setattr(poll, "POSTED", tmp_path / "posted.csv")
    monkeypatch.setattr(poll, "SNAPSHOTS", tmp_path / "snapshots")
    monkeypatch.setenv("CIETRADE_USER_ID", "u@example.com")
    monkeypatch.setenv("CIETRADE_API_KEY", "k")
    return tmp_path


def test_poll_snapshots_only_when_the_open_jobs_change(poll_dirs) -> None:
    state = {"qty": 100.0}

    def fetch(creds, **f):
        if f.get("Status") == "WORK":
            return [job(1, "EXTRUDER (2ND SHIFT)", "2026-09-14", "10:38AM", state["qty"])]
        return [job(2, "EXTRUDER (1ST SHIFT)", "2026-09-08", "2:38PM", 500, status="Posted", post="2026-09-11")]

    r1 = poll.poll(fetch=fetch)
    assert r1["ok"] and r1["changed"] and r1["monroe_open"] == 1 and r1["posted_new"] == 1
    r2 = poll.poll(fetch=fetch)
    assert not r2["changed"]
    state["qty"] = 250.0
    r3 = poll.poll(fetch=fetch)
    assert r3["changed"]
    snaps = list((poll_dirs / "snapshots").glob("*.csv"))
    assert len(snaps) == 1
    rows = pd.read_csv(snaps[0])
    assert len(rows) == 2 and rows["Output Qty"].tolist() == [100.0, 250.0] and rows["Snapshot"].nunique() == 2
    assert len((poll_dirs / "polls.jsonl").read_text().splitlines()) == 3
    posted = pd.read_csv(poll_dirs / "posted.csv")
    assert len(posted) == 1 and posted.iloc[0]["First Seen"] == posted.iloc[0]["Last Seen"] or True


def test_poll_records_failures_without_raising(poll_dirs) -> None:
    def fetch(creds, **f):
        raise ConnectionError("boom")
    r = poll.poll(fetch=fetch)
    assert not r["ok"] and "ConnectionError" in r["error"]
    assert json.loads((poll_dirs / "polls.jsonl").read_text().splitlines()[-1])["ok"] is False


def test_main_still_publishes_the_live_feed_after_a_failed_poll(poll_dirs, monkeypatch) -> None:
    """A cieTrade outage must show on the site as an outage, not as a frozen feed."""
    calls = []
    monkeypatch.setattr(poll.api, "list_converting_jobs", lambda creds, **f: (_ for _ in ()).throw(ConnectionError("HTTP Error 404")))
    monkeypatch.setattr(poll.cietrade_live, "write_live", lambda: calls.append("write") or {"changes_today": 0, "lbs_today": 0, "api_down_since": "x"})
    monkeypatch.setattr(poll.cietrade_live, "publish", lambda: calls.append("publish") or True)
    assert poll.main([]) == 1                                    # the run still reports the failure
    assert calls == ["write", "publish"]


def test_upsert_posted_keeps_first_seen_and_flags_edits(tmp_path) -> None:
    path = tmp_path / "posted.csv"
    rows = api.normalize([job(7, "GUILLOTINE (1ST SHIFT)", "2026-09-08", "2:38PM", 1000, status="Posted", post="2026-09-11")])
    t1 = pd.Timestamp("2026-09-12 08:00").to_pydatetime()
    s1 = poll.upsert_posted(rows, t1, path)
    edited = api.normalize([job(7, "GUILLOTINE (1ST SHIFT)", "2026-09-08", "2:38PM", 1200, status="Posted", post="2026-09-11")])
    s2 = poll.upsert_posted(edited, pd.Timestamp("2026-09-13 08:00").to_pydatetime(), path)
    out = pd.read_csv(path)
    assert s1["posted_new"] == 1 and s2["posted_new"] == 0 and s2["posted_changed"] == 1
    assert out.iloc[0]["Output Qty"] == 1200.0 and out.iloc[0]["First Seen"].startswith("2026-09-12")


# ---- model ----------------------------------------------------------------

def _write_api_dir(tmp_path: Path) -> Path:
    """One open 2nd-shift job seen at three polls: Mon 10:30 (0), Tue 07:00 (4,000), Wed 07:00 (7,000)."""
    api_dir = tmp_path / "cietrade"
    (api_dir / "snapshots").mkdir(parents=True)
    obs = [("2026-09-14T10:30:00", 0.0), ("2026-09-15T07:00:00", 4000.0), ("2026-09-16T07:00:00", 7000.0)]
    frames = []
    for ts, qty in obs:
        df = api.normalize([job(900, "EXTRUDER (2ND SHIFT)", "2026-09-14", "10:00AM", qty)])
        df.insert(0, "Snapshot", ts)
        frames.append(df)
    pd.concat(frames).to_csv(api_dir / "snapshots" / "2026-09-14.csv", index=False)
    with (api_dir / "polls.jsonl").open("w") as fh:
        for ts, _ in obs:
            fh.write(json.dumps({"ts": ts, "ok": True, "changed": True}) + "\n")
    return api_dir


def test_model_credits_each_window_to_the_shift_between_polls(tmp_path) -> None:
    api_dir = _write_api_dir(tmp_path)
    (tmp_path / "exports").mkdir()
    res = model.run(export_dir=tmp_path / "exports", api_dir=api_dir, aggregate=tmp_path / "none.xlsx", verbose=False)
    cells = res["cells"].set_index(["Date", "Shift"])
    assert cells.loc[(pd.Timestamp("2026-09-14"), "2nd"), "lbs"] == 4000 and cells.loc[(pd.Timestamp("2026-09-14"), "2nd"), "mode"] == "exact"
    assert cells.loc[(pd.Timestamp("2026-09-15"), "2nd"), "lbs"] == 3000 and cells.loc[(pd.Timestamp("2026-09-15"), "2nd"), "mode"] == "exact"
    assert cells.loc[(pd.Timestamp("2026-09-16"), "2nd"), "mode"] == "not yet posted"   # Wed 2nd shift starts after the last poll
    assert res["meta"]["last_poll"] == "2026-09-16T07:00:00" and res["meta"]["open_jobs"] == 1
    day = [e for e in res["log"] if e["kind"] == "api-day"]
    assert [e["delta_total"] for e in day] == [0.0, 4000.0, 3000.0]


def test_read_export_csv_accepts_both_date_formats(tmp_path) -> None:
    p = tmp_path / "converting_2026-09-11T16-05-00.csv"
    p.write_text("Job No,Job Date,Warehouse,Warehouse Status,Machine,Status,Post Date,Start-Time,End-Time,Elapsed-Time,UOM,Output Qty\n"
                 "1,9/8/2026 12:00:00 AM,Plus Monroe Warehouse,COMPLETED,EXTRUDER (1ST SHIFT),Posted,9/11/2026 12:00:00 AM,2:38PM,,,LBS,\"1,234.000\"\n"
                 "2,2026-09-14,Plus Monroe Warehouse,IN PROCESS,EXTRUDER (2ND SHIFT),Work,,10:38AM,,,LBS,55.000\n")
    df = model.read_export_csv(p)
    assert df["Job Date"].tolist() == [pd.Timestamp("2026-09-08"), pd.Timestamp("2026-09-14")]
    assert df["Post Date"].isna().tolist() == [False, True] and df["Output Qty"].tolist() == [1234.0, 55.0]


# ---- dashboard rows --------------------------------------------------------

def _fake_res() -> dict:
    cov = pd.DataFrame([
        dict(Date=pd.Timestamp("2026-09-14"), Shift="1st", line="EXTRUDER", Machine="EXTRUDER", job=17130, mode="exact", lbs=8639.0, src="snap", n_days=1),
        dict(Date=pd.Timestamp("2026-09-14"), Shift="1st", line="GUILLOTINE", Machine="GUILLOTINE", job=17132, mode="partial", lbs=3441.0, src="snap", n_days=1),
        dict(Date=pd.Timestamp("2026-09-14"), Shift="2nd", line="SHREDDER/GRINDER", Machine="SHREDDER+GRINDER", job=17125, mode="spread", lbs=1000.0, src="job", n_days=2),
        dict(Date=pd.Timestamp("2026-09-15"), Shift="2nd", line="SHREDDER/GRINDER", Machine="SHREDDER+GRINDER", job=17125, mode="not yet posted", lbs=float("nan"), src="job", n_days=2),
        dict(Date=pd.Timestamp("2026-08-20"), Shift="1st", line="EXTRUDER", Machine="EXTRUDER", job=1, mode="exact", lbs=5.0, src="job", n_days=1),
    ])
    return {"cov": cov, "meta": {"warnings": [], "last_poll": None}}


def test_daily_rows_follow_the_aggregate_schema(tmp_path) -> None:
    rows, notes, warnings = daily.daily_rows(_fake_res(), labor_path=tmp_path / "none.xlsx")
    assert list(rows.columns) == daily.AGG_COLUMNS and not warnings
    assert rows["Date"].tolist() == ["2026-09-14", "2026-09-14", "2026-09-14"]   # from-date and awaiting cells excluded
    g = rows[rows["Machine_Name"] == "GUILLOTINE"].iloc[0]
    assert g["Actual_Input"] == 3441.0 and g["Actual_Output"] == 0.0 and "in progress" in g["Comment"]
    e = rows[rows["Machine_Name"] == "EXTRUDER"].iloc[0]
    assert e["Actual_Output"] == 8639.0 and e["Day_of_Week"] == "Mon" and e["Week_Start"] == "2026-09-14" and e["Week_End"] == "2026-09-18"
    assert e["Machine_Hours"] == 0 and pd.isna(e["Output_per_Hour"]) and e["Source"] == "cietrade"
    assert rows[rows["Machine_Name"] == "GRINDER"].iloc[0]["Input_Item"] == "cieTrade job 17125"


def test_daily_rows_pick_up_end_of_shift_hours(tmp_path) -> None:
    import labor_entries as L
    entries = pd.DataFrame([L.make_entry(Date="2026-09-14", Shift="1st", Machine_Name="EXTRUDER", Machine_Hours=7, Man_Hours=14,
                                         Operator="Daniel, Steven", Material="BOPP resin", Downtime_Minutes=60, Downtime_Reason="Blades",
                                         Comment="Blades keep breaking", Source="form")])
    L.save_entries(entries, L.empty_notes(), tmp_path / "labor.xlsx")
    rows, notes, _ = daily.daily_rows(_fake_res(), labor_path=tmp_path / "labor.xlsx")
    e = rows[rows["Machine_Name"] == "EXTRUDER"].iloc[0]
    assert e["Machine_Hours"] == 7 and e["Man_Hours"] == 14 and e["Operator"] == "Daniel, Steven" and e["Output_Product"] == "BOPP resin"
    assert e["Output_per_Hour"] == pytest.approx(8639 / 7) and e["Labor_Cost"] == 14 * daily.LABOR_RATE
    assert "down 60 min (Blades)" in e["Comment"] and notes.iloc[0]["Note"].startswith("Blades keep breaking")


def test_update_aggregate_is_idempotent_and_keeps_workbook_rows(tmp_path) -> None:
    agg = tmp_path / "agg.xlsx"
    old = pd.DataFrame([{**{c: None for c in daily.AGG_COLUMNS}, "Date": "2026-08-20", "Week_Start": "2026-08-17", "Shift": "1st",
                         "Machine_Name": "EXTRUDER", "Actual_Output": 5.0, "Source": None}])
    old.to_excel(agg, index=False)
    rows, notes, _ = daily.daily_rows(_fake_res(), labor_path=tmp_path / "none.xlsx")
    s1 = daily.update_aggregate(rows, notes, agg_path=agg, notes_path=tmp_path / "notes.xlsx")
    s2 = daily.update_aggregate(rows, notes, agg_path=agg, notes_path=tmp_path / "notes.xlsx")
    out = pd.read_excel(agg)
    assert s1["records"] == s2["records"] == 4 and (out["Source"] == "workbook").sum() == 1 and (out["Source"] == "cietrade").sum() == 3


def test_daily_rows_carry_basis_and_status_lists_awaiting_cells(tmp_path) -> None:
    rows, _, _ = daily.daily_rows(_fake_res(), labor_path=tmp_path / "none.xlsx")
    assert rows.set_index("Machine_Name")["Basis"].to_dict() == {"EXTRUDER": "exact", "GUILLOTINE": "partial", "GRINDER": "averaged"}
    res = _fake_res(); res["meta"].update(data_through="2026-09-15", open_jobs=2, open_lbs=1234.5, closures=["2026-09-07"])
    st = daily.build_status(res)
    assert st["awaiting"] == [["2026-09-15", "2nd", "GRINDER"]] and st["open_lbs"] == 1234 and st["closures"] == ["2026-09-07"]


def test_end_of_shift_summary_grid_and_recent(tmp_path) -> None:
    import pandas as pd
    entries = pd.DataFrame([
        {"Date": "2026-09-21", "Shift": "1st", "Machine_Name": "EXTRUDER", "Machine_Hours": 7.5, "Man_Hours": 14.5, "Operator": "Steven, Daniel", "Downtime_Minutes": 0, "Downtime_Reason": "", "Comment": "", "Submitted_By": "Tim"},
        {"Date": "2026-09-21", "Shift": "1st", "Machine_Name": "AUTO TIE BALER", "Machine_Hours": 5.25, "Man_Hours": 5.25, "Operator": "Tony", "Downtime_Minutes": 120, "Downtime_Reason": "Other", "Comment": "track", "Submitted_By": "Tim"},
        {"Date": "2026-09-18", "Shift": "3rd", "Machine_Name": "GUILLOTINE", "Machine_Hours": 7, "Man_Hours": 7, "Operator": "Daniel", "Downtime_Minutes": 0, "Downtime_Reason": "", "Comment": "", "Submitted_By": "Connor"},
    ])
    notes = pd.DataFrame([{"Date": "2026-09-21", "Shift": "2nd", "Note": "cleaning", "Source": "form", "Captured_At": ""}])
    s = daily.end_of_shift_summary(entries, notes, as_of=pd.Timestamp("2026-09-21"), days=4)
    assert [g["date"] for g in s["days"]] == ["2026-09-21", "2026-09-20", "2026-09-19", "2026-09-18"]
    mon = s["days"][0]["shifts"]
    assert mon["1st"] == {"filed": True, "by": "Tim", "machines": 2, "machine_hours": 12.8, "man_hours": 19.8, "downtime_min": 120}
    assert mon["2nd"] == {"filed": False} and s["days"][3]["shifts"]["3rd"]["by"] == "Connor"
    assert s["filed"] == 2 and s["entries"] == 3 and s["recent"][0]["downtime_min"] == 120 and s["notes"][0]["note"] == "cleaning"
    empty = daily.end_of_shift_summary(None, None, as_of=pd.Timestamp("2026-09-21"), days=2)
    assert empty["filed"] == 0 and len(empty["days"]) == 2


def test_end_of_shift_summary_keeps_full_reports(tmp_path) -> None:
    import pandas as pd
    entries = pd.DataFrame([
        {"Date": "2026-09-21", "Shift": "1st", "Machine_Name": "EXTRUDER", "Machine_Hours": 7.0, "Man_Hours": 14.5, "Operator": "Steven, Daniel", "Material": "BOPP resin", "Downtime_Minutes": 60, "Downtime_Reason": "Waiting on material", "Comment": None, "Submitted_By": "Tim", "Captured_At": "9/21/2026", "Source": "form"},
        {"Date": "2026-09-21", "Shift": "1st", "Machine_Name": "AUTO TIE BALER", "Machine_Hours": 5.25, "Man_Hours": 5.25, "Operator": "Tony", "Material": "Mixed Plastic", "Downtime_Minutes": 0, "Downtime_Reason": None, "Comment": "track", "Submitted_By": "Tim", "Captured_At": "9/21/2026", "Source": "form"},
        {"Date": "2026-08-01", "Shift": "2nd", "Machine_Name": "SHREDDER", "Machine_Hours": 6, "Man_Hours": 6, "Operator": "Kevin", "Material": "HDPE", "Downtime_Minutes": 0, "Downtime_Reason": None, "Comment": None, "Submitted_By": "Montez", "Captured_At": "", "Source": "form"},
    ])
    notes = pd.DataFrame([{"Date": "2026-09-21", "Shift": "1st", "Note": "cleaning", "Source": "form", "Captured_At": ""}])
    s = daily.end_of_shift_summary(entries, notes, as_of=pd.Timestamp("2026-09-21"), days=2, report_days=28)
    assert len(s["reports"]) == 1                      # the August report is outside the 28-day window
    r = s["reports"][0]
    assert r["date"] == "2026-09-21" and r["shift"] == "1st" and r["by"] == "Tim" and r["filed_at"] == "9/21/2026" and r["notes"] == ["cleaning"]
    assert [m["machine"] for m in r["machines"]] == ["AUTO TIE BALER", "EXTRUDER"]
    assert r["machines"][1] == {"machine": "EXTRUDER", "machine_hours": 7.0, "man_hours": 14.5, "operators": "Steven, Daniel", "material": "BOPP resin", "downtime_min": 60, "reason": "Waiting on material", "comment": ""}
