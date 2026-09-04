"""End-of-Shift labor capture: photo extraction normalisation, form reader, landing file.

The two JSON fixtures are hand-transcribed from real report photos (2nd shift
2026-09-01, 3rd shift 2026-09-03) and stand in for the vision model's output, so
everything downstream of the API call is tested without credentials.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import labor_entries as L
from labor_sheet import map_columns, responses_to_entries
from shift_report_ocr import ShiftReport, report_to_entries

FIX = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Vocabulary normalisation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("label,expected", [
    ("Auto tie", "AUTO TIE BALER"), ("Auto tie baler", "AUTO TIE BALER"),
    ("Baler - 1", "BALER 1"), ("Baler 2", "BALER 2"),
    ("Big densifier", "AVANGUARD DENSIFIER (OLD)"), ("Big densifier (Avanguard)", "AVANGUARD DENSIFIER (OLD)"),
    ("Densifier", "GREEN MAX DENSIFIER (NEW)"), ("New Densifier", "GREEN MAX DENSIFIER (NEW)"),
    ("New densifier (Green Max)", "GREEN MAX DENSIFIER (NEW)"),
    ("Extruder", "EXTRUDER"), ("Guillotine", "GUILLOTINE"), ("Shredder", "SHREDDER"),
    ("Shredder/Grinder", "GRINDER"), ("ShredderGrinder", "GRINDER"), ("Small grinder", "SMALL GRINDER"),
])
def test_every_paper_row_label_maps(label, expected) -> None:
    assert L.normalize_machine(label) == expected


def test_unknown_machine_is_none_not_guessed() -> None:
    assert L.normalize_machine("Forklift") is None
    assert L.normalize_machine("") is None


@pytest.mark.parametrize("value,expected", [
    ("Second Shift", "2nd"), ("3rd shift", "3rd"), ("First", "1st"), ("2", "2nd"), ("1st", "1st"), ("", None),
])
def test_shift_labels(value, expected) -> None:
    assert L.normalize_shift(value) == expected


def test_material_in_brackets_is_not_a_person() -> None:
    assert L.split_operator_material("Montez, Tevin [BOPP]") == (["Montez", "Tevin"], "BOPP")
    assert L.split_operator_material("Darius (Foil Bags)") == (["Darius"], "Foil Bags")
    assert L.split_operator_material("Steven B. [Mixed Plastic]") == (["Steven B"], "Mixed Plastic")
    assert L.split_operator_material("Andrew") == (["Andrew"], None)


def test_crew_splitting_handles_separators() -> None:
    assert L.parse_operators("Connor, James") == ["Connor", "James"]
    assert L.parse_operators("Connor / James & Rohan and Daniel") == ["Connor", "James", "Rohan", "Daniel"]


# ---------------------------------------------------------------------------
# Photo extraction -> landing rows
# ---------------------------------------------------------------------------

def _report(name: str) -> ShiftReport:
    return ShiftReport.model_validate_json((FIX / name).read_text())


def test_clean_report_lands_every_written_row() -> None:
    e, n = report_to_entries(_report("shift_report_2nd_2026-09-01.json"), source="image:test.jpg")
    assert len(e) == 4
    assert set(e["Machine_Name"]) == {"AUTO TIE BALER", "EXTRUDER", "GUILLOTINE", "GRINDER"}
    assert (e["Date"] == "2026-09-01").all() and (e["Shift"] == "2nd").all()
    ext = e[e["Machine_Name"] == "EXTRUDER"].iloc[0]
    assert ext["Machine_Hours"] == 7.0 and ext["Man_Hours"] == 14.0
    assert ext["Operator"] == "Montez, Tevin" and ext["Material"] == "BOPP"
    assert not e["Needs_Review"].any()
    # the margin note is captured as a shift note, not lost
    assert n["Note"].tolist() == ["Steven A. was unloading / dumping trash"]


def test_ambiguous_date_flags_every_row_for_review() -> None:
    e, _ = report_to_entries(_report("shift_report_3rd_2026-09-03.json"), source="image:test.jpg")
    assert len(e) == 2
    assert e["Needs_Review"].all()
    assert "2026-08-03" in e.iloc[0]["Comment"]          # the alternative reading is surfaced
    assert e.iloc[0]["Confidence"] <= 0.6                  # capped by the date confidence


def test_unknown_row_label_is_kept_but_flagged() -> None:
    rep = _report("shift_report_2nd_2026-09-01.json")
    rep.rows[0].machine_label = "Mystery machine"
    e, _ = report_to_entries(rep, source="image:x.jpg")
    row = e.iloc[0]
    assert row["Machine_Name"] == "Mystery machine" and row["Needs_Review"]
    assert "unknown machine label" in row["Comment"]


# ---------------------------------------------------------------------------
# Google Form responses -> landing rows
# ---------------------------------------------------------------------------

def test_form_headers_map_by_prefix() -> None:
    m = map_columns(["Timestamp", "Date", "Shift", "Machine", "Machine hours operated",
                     "Total man hours", "Operator(s)", "Material run", "Comments", "Shift notes (anything else)"])
    assert m["Machine hours operated"] == "machine_hours" and m["Machine"] == "machine"
    assert m["Total man hours"] == "man_hours" and m["Shift notes (anything else)"] == "shift_note"


def test_form_csv_lands_like_the_photo_route() -> None:
    e, n = responses_to_entries(pd.read_csv(FIX / "form_responses_sample.csv"))
    assert len(e) == 4 and (e["Source"] == "form").all()
    assert e.iloc[1]["Operator"] == "Montez, Tevin" and e.iloc[1]["Man_Hours"] == 14.0
    assert e.iloc[3]["Machine_Name"] == "GREEN MAX DENSIFIER (NEW)" and e.iloc[3]["Date"] == "2026-09-03"
    assert not e["Needs_Review"].any()
    assert n["Note"].tolist() == ["Steven A. was unloading / dumping trash"]


# ---------------------------------------------------------------------------
# Landing file: merge, re-submission, round trip
# ---------------------------------------------------------------------------

def test_resubmission_replaces_rather_than_duplicates(tmp_path) -> None:
    path = tmp_path / "labor.xlsx"
    e1, n1 = responses_to_entries(pd.read_csv(FIX / "form_responses_sample.csv"))
    r1 = L.record(e1, n1, path=path)
    assert r1["total_entries"] == 4 and path.exists()
    corrected = e1.iloc[[1]].copy(); corrected["Man_Hours"] = 15.0
    r2 = L.record(corrected, None, path=path)
    assert r2["total_entries"] == 4, "same day/shift/machine/source must replace"
    entries, notes = L.load_entries(path)
    assert entries.loc[entries["Machine_Name"] == "EXTRUDER", "Man_Hours"].iloc[0] == 15.0
    assert len(notes) == 1


def test_photo_and_form_rows_for_same_machine_both_kept(tmp_path) -> None:
    """During the transition both routes may report the same shift; keep both, keyed by source."""
    path = tmp_path / "labor.xlsx"
    e_img, _ = report_to_entries(_report("shift_report_2nd_2026-09-01.json"), source="image:a.jpg")
    e_form, _ = responses_to_entries(pd.read_csv(FIX / "form_responses_sample.csv"))
    L.record(e_img, None, path=path)
    r = L.record(e_form, None, path=path)
    assert r["total_entries"] == 8


def test_dry_run_writes_nothing(tmp_path) -> None:
    path = tmp_path / "labor.xlsx"
    e, _ = responses_to_entries(pd.read_csv(FIX / "form_responses_sample.csv"))
    r = L.record(e, None, path=path, dry_run=True)
    assert r["total_entries"] == 4 and not path.exists()


# ---------------------------------------------------------------------------
# The model call itself, with the client faked (no credentials needed)
# ---------------------------------------------------------------------------

def _fake_client(stop_reason: str = "end_turn", captured: dict | None = None):
    import shift_report_ocr as O

    class FakeResp:
        pass
    resp = FakeResp()
    resp.stop_reason = stop_reason
    resp.stop_details = None
    resp.parsed_output = O.ShiftReport.model_validate_json((FIX / "shift_report_2nd_2026-09-01.json").read_text())

    class FakeMessages:
        def parse(self, **kw):
            if captured is not None:
                captured.update(kw)
            return resp

    class FakeClient:
        messages = FakeMessages()
    return FakeClient()


def test_extract_sends_an_upright_downscaled_jpeg_and_the_schema(tmp_path) -> None:
    import base64, io
    from PIL import Image
    import shift_report_ocr as O
    img = tmp_path / "sheet.png"
    Image.new("RGB", (3024, 4032), "white").save(img)   # phone-sized
    captured: dict = {}
    rep = O.extract_report(img, client=_fake_client(captured=captured))
    assert rep.shift == "2nd"
    assert captured["model"] == "claude-opus-5" and captured["output_format"] is O.ShiftReport
    block = captured["messages"][0]["content"][0]
    assert block["type"] == "image" and block["source"]["media_type"] == "image/jpeg"
    decoded = Image.open(io.BytesIO(base64.b64decode(block["source"]["data"])))
    assert max(decoded.size) <= O.MAX_EDGE


def test_refusal_is_an_error_not_silent_garbage(tmp_path) -> None:
    from PIL import Image
    import shift_report_ocr as O
    img = tmp_path / "sheet.jpg"
    Image.new("RGB", (100, 100), "white").save(img)
    with pytest.raises(RuntimeError):
        O.extract_report(img, client=_fake_client(stop_reason="refusal"))
