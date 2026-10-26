"""Microsoft 365 adapters: Graph calls shaped like the Gmail code expects, with no network."""
import io
import json
from datetime import date

import pandas as pd
import pytest

from src import config, daily_digest as dd, eos_workbook, fetch_emails, graph_mail, labor_sheet
from src.graph_client import GraphClient, GraphError


class FakeClient:
    """Answers Graph paths from a table; records what was posted."""
    app_only = True
    mailbox_path = "/users/ops@plusmaterials.com"

    def __init__(self, routes: dict, raw: dict | None = None):
        self.routes, self.raw, self.posts, self.calls = routes, raw or {}, [], []

    def get(self, path, params=None):
        self.calls.append((path, params or {}))
        for key, val in self.routes.items():
            if path.endswith(key):
                return val
        raise GraphError(404, "no route", path)

    def get_all(self, path, params=None, limit=None):
        return self.get(path, params).get("value", [])

    def get_bytes(self, path, params=None):
        self.calls.append((path, params or {}))
        for key, val in self.raw.items():
            if path.endswith(key):
                return val
        raise GraphError(404, "no raw route", path)

    def post(self, path, body):
        self.posts.append((path, body)); return {}

    def whoami(self):
        return "ops@plusmaterials.com"


MSG = {"id": "m1", "subject": "Walton processing weights for the week of 8/17/26-8/21/26",
       "receivedDateTime": "2026-08-24T09:21:53Z", "from": {"emailAddress": {"name": "Carl Maddox", "address": "carl@plusmaterials.com"}},
       "hasAttachments": True}
ATTS = {"value": [
    {"@odata.type": "#microsoft.graph.fileAttachment", "id": "a1", "name": "1st shift processing weights.xlsx", "size": 1200, "contentType": "application/vnd.ms-excel"},
    {"@odata.type": "#microsoft.graph.fileAttachment", "id": "a2", "name": "Walton PayPeriod_Report(2026-08-20).pdf", "size": 900, "contentType": "application/pdf"},
    {"@odata.type": "#microsoft.graph.fileAttachment", "id": "a3", "name": "logo.png", "size": 10, "contentType": "image/png", "isInline": True},
]}


def _mailbox():
    client = FakeClient({"/messages": {"value": [MSG]}, "/messages/m1/attachments": ATTS, "/messages/m1": MSG},
                        raw={"/attachments/a1/$value": b"xlsx-bytes"})
    return graph_mail.GraphMailbox(client), client


def test_gmail_query_becomes_a_graph_filter() -> None:
    mb, client = _mailbox()
    stubs = mb.list_messages(fetch_emails.PROCESSING_WEIGHTS_QUERY + " newer_than:14d")
    assert [s["id"] for s in stubs] == ["m1"]
    path, params = client.calls[0]
    assert path.endswith("/messages") and "from/emailAddress/address eq 'carl@plusmaterials.com'" in params["$filter"]
    assert "hasAttachments eq true" in params["$filter"] and "contains(subject,'processing weights for the week of')" in params["$filter"]
    assert "receivedDateTime ge 20" in params["$filter"] and "$search" not in params


def test_filename_query_keeps_only_messages_carrying_such_a_file() -> None:
    mb, _ = _mailbox()
    assert [s["id"] for s in mb.list_messages("from:carl@plusmaterials.com filename:PayPeriod has:attachment newer_than:90d")] == ["m1"]
    assert mb.list_messages("from:carl@plusmaterials.com filename:Nothing has:attachment") == []


def test_message_has_the_gmail_layout_the_fetchers_walk() -> None:
    mb, _ = _mailbox()
    msg = mb.get_message("m1")
    assert fetch_emails.header_value(msg, "Subject").startswith("Walton processing weights")
    assert fetch_emails.header_value(msg, "Date") == "Mon, 24 Aug 2026 09:21:53 +0000"
    names = [p["filename"] for p in fetch_emails.iter_attachments(msg["payload"])]
    assert names == ["1st shift processing weights.xlsx", "Walton PayPeriod_Report(2026-08-20).pdf"]   # inline image dropped
    assert fetch_emails.download_attachment_bytes(mb, "m1", "a1") == b"xlsx-bytes"
    assert fetch_emails.list_messages(mb, "from:carl@plusmaterials.com has:attachment")[0]["id"] == "m1"


def test_fetch_processing_weights_runs_on_the_graph_mailbox(tmp_path, monkeypatch) -> None:
    mb, _ = _mailbox()
    monkeypatch.setattr(fetch_emails, "REPORTS_DIR", tmp_path)
    saved = fetch_emails.fetch_processing_weights(mb, days_back=14)
    assert [p.name for p in saved] == ["1st shift processing weights 08-17-26 to 08-21-26.xlsx"] and saved[0].read_bytes() == b"xlsx-bytes"


def test_send_posts_a_graph_message_with_html_and_bcc() -> None:
    mb, client = _mailbox()
    assert mb.send(["a@x.com", "b@x.com"], "Subj", "<b>hi</b>", bcc=["c@x.com"]) == "sent"
    path, body = client.posts[0]
    assert path == "/users/ops@plusmaterials.com/sendMail" and body["saveToSentItems"] is True
    m = body["message"]
    assert m["subject"] == "Subj" and m["body"] == {"contentType": "HTML", "content": "<b>hi</b>"}
    assert [r["emailAddress"]["address"] for r in m["toRecipients"]] == ["a@x.com", "b@x.com"]
    assert [r["emailAddress"]["address"] for r in m["bccRecipients"]] == ["c@x.com"]


def test_digest_send_goes_through_graph_when_selected(monkeypatch) -> None:
    sent = {}
    class Box:
        def send(self, to, subject, html, text=None, bcc=None):
            sent.update(to=to, subject=subject, html=html, text=text, bcc=bcc); return "sent"
    monkeypatch.setenv("MAIL_PROVIDER", "microsoft")
    import importlib
    gm = importlib.import_module("graph_mail")                  # the module the digest imports (src/ is on sys.path)
    monkeypatch.setattr(gm.GraphMailbox, "from_env", classmethod(lambda cls: Box()))
    assert dd.send(["me@x.com"], "S", "<p>x</p>", bcc=["q@x.com"]) == "sent"
    assert sent["to"] == ["me@x.com"] and sent["bcc"] == ["q@x.com"] and "dashboard" in sent["text"]


def test_providers_default_to_google(monkeypatch) -> None:
    monkeypatch.delenv("MAIL_PROVIDER", raising=False); monkeypatch.delenv("EOS_SOURCE", raising=False)
    assert config.mail_provider() == "google" and config.eos_source() == "google"
    monkeypatch.setenv("MAIL_PROVIDER", "Microsoft "); monkeypatch.setenv("EOS_SOURCE", "nonsense")
    assert config.mail_provider() == "microsoft" and config.eos_source() == "google"


def _workbook_bytes() -> bytes:
    entries = pd.DataFrame([
        ["2026-10-20 15:02", "20261020-150200-1st", "2026-10-20", "1st", "Extruder", 7.5, 14.5, "Steven, Daniel", "BOPP resin", None, "", "", "Tim"],
        ["2026-10-20 15:02", "20261020-150200-1st", "2026-10-20", "1st", "Guillotine", 7.25, 7.25, "Vince", "BOPP slabs", 30, "Blades", "dull", "Tim"],
    ], columns=["Timestamp", "Submission ID", "Date", "Shift", "Machine", "Machine hours operated", "Total man hours",
                "Operator(s)", "Material run", "Downtime minutes", "Downtime reason", "Comments", "Submitted by"])
    subs = pd.DataFrame([["2026-10-20 15:02", "20261020-150200-1st", "2026-10-20", "1st", "Tim", "clean-up at 2", 2, ""]],
                        columns=["Timestamp", "Submission ID", "Date", "Shift", "Submitted by", "Shift notes", "Machines reported", "Replaces earlier"])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf) as xw:
        entries.to_excel(xw, sheet_name="Entries", index=False); subs.to_excel(xw, sheet_name="Submissions", index=False)
    return buf.getvalue()


def test_workbook_from_a_sharing_link_feeds_the_sheet_reader(monkeypatch) -> None:
    url = "https://plusmaterials.sharepoint.com/:x:/s/ops/EOS?e=abc"
    client = FakeClient({}, raw={f"/shares/{eos_workbook.share_token(url)}/driveItem/content": _workbook_bytes()})
    assert eos_workbook.share_token(url).startswith("u!") and "=" not in eos_workbook.share_token(url)
    entries_raw, subs_raw = eos_workbook.fetch(client, {"url": url})
    entries, notes = labor_sheet.responses_to_entries(entries_raw)
    notes = pd.concat([notes, labor_sheet.submissions_to_notes(subs_raw)], ignore_index=True)
    assert list(entries["Machine_Name"]) == ["EXTRUDER", "GUILLOTINE"] and float(entries["Machine_Hours"].iloc[0]) == 7.5
    assert entries["Downtime_Minutes"].iloc[1] == 30 and entries["Needs_Review"].sum() == 0
    assert list(notes["Note"]) == ["clean-up at 2"]


def test_workbook_by_drive_and_path_and_missing_config() -> None:
    client = FakeClient({}, raw={"/drives/d1/root:/Walton/End of Shift (log).xlsx:/content": _workbook_bytes()})
    entries, _ = eos_workbook.fetch(client, {"drive_id": "d1", "path": "/Walton/End of Shift (log).xlsx"})
    assert len(entries) == 2
    with pytest.raises(FileNotFoundError):
        eos_workbook.download_workbook(client, {})
    assert eos_workbook.configured({"url": "x"}) and not eos_workbook.configured({"drive_id": "d"})


def test_graph_client_mailbox_path_and_env() -> None:
    assert GraphClient("t", "c", "secret", mailbox="ops@x.com").mailbox_path == "/users/ops@x.com"
    assert GraphClient("t", "c", None).mailbox_path == "/me"
    assert GraphClient("t", "c", None, mailbox="me@x.com").mailbox_path == "/users/me@x.com"


def test_graph_client_retries_then_raises(monkeypatch) -> None:
    class R:
        def __init__(self, status, body=None, headers=None):
            self.status_code, self._body, self.headers, self.content, self.text = status, body, headers or {}, b"x", "err"
        def json(self): return self._body
    class S:
        def __init__(self): self.n = 0
        def request(self, *a, **k):
            self.n += 1
            return R(503, headers={"Retry-After": "0"}) if self.n < 3 else R(400, {"error": {"message": "bad filter"}})
    c = GraphClient("t", "c", "s", mailbox="m@x.com", session=S())
    monkeypatch.setattr(c, "token", lambda: "tok")
    with pytest.raises(GraphError) as e:
        c.get("/users/m@x.com/messages")
    assert e.value.status == 400 and "bad filter" in str(e.value)


def test_floor_page_posts_the_excel_columns(tmp_path) -> None:
    """docs/eos/index.html: the static End of Shift page for the Power Automate flow."""
    from pathlib import Path
    page = Path(__file__).resolve().parents[1].joinpath("docs", "eos", "index.html").read_text()
    assert "var FLOW_URL = " in page and "google.script" not in page
    for col in ("'Machine hours operated'", "'Total man hours'", "'Operator(s)'", "'Downtime minutes'", "'Submitted by'", "'Shift notes'", "'Machines reported'"):
        assert col in page
    gs = Path(__file__).resolve().parents[1].joinpath("scripts", "end_of_shift_app", "Code.gs").read_text()
    import re
    labels = re.findall(r"\['([^']+)', '[A-Z0-9 ()/]+'\]", gs.split("var MACHINES")[1].split("];")[0])
    assert all(f"'{m}'" in page for m in labels) and len(labels) == 9
