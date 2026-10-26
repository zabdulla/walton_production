"""Microsoft 365 adapters: Graph calls shaped like the Gmail code expects, with no network."""
import io
import json
from datetime import date

import pandas as pd
import pytest

from src import config, daily_digest as dd, eos_workbook, graph_mail, labor_sheet
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









def test_send_posts_a_graph_message_with_html_and_bcc() -> None:
    client = FakeClient({}); mb = graph_mail.GraphMailbox(client)
    assert mb.send(["a@x.com", "b@x.com"], "Subj", "<b>hi</b>", bcc=["c@x.com"]).startswith("via Graph")
    path, body = client.posts[0]
    assert path == "/users/ops@plusmaterials.com/sendMail" and body["saveToSentItems"] is True
    m = body["message"]
    assert m["subject"] == "Subj" and m["body"] == {"contentType": "HTML", "content": "<b>hi</b>"}
    assert [r["emailAddress"]["address"] for r in m["toRecipients"]] == ["a@x.com", "b@x.com"]
    assert [r["emailAddress"]["address"] for r in m["bccRecipients"]] == ["c@x.com"]


def test_digest_send_goes_through_graph_when_selected(monkeypatch) -> None:
    sent = {}
    class Box:
        def send(self, to, subject, html, bcc=None):
            sent.update(to=to, subject=subject, html=html, bcc=bcc); return "sent"
    monkeypatch.setenv("MAIL_PROVIDER", "microsoft")
    import importlib
    gm = importlib.import_module("graph_mail")                  # the module the digest imports (src/ is on sys.path)
    monkeypatch.setattr(gm.GraphMailbox, "from_env", classmethod(lambda cls: Box()))
    assert dd.send(["me@x.com"], "S", "<p>x</p>", bcc=["q@x.com"]) == "sent"
    assert sent["to"] == ["me@x.com"] and sent["bcc"] == ["q@x.com"] and sent["html"] == "<p>x</p>"


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
    client2 = FakeClient({}, raw={"/drives/d1/items/i9/content": _workbook_bytes()})
    assert len(eos_workbook.fetch(client2, {"drive_id": "d1", "item_id": "i9"})[0]) == 2
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


ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
CODE_GS = ROOT / "scripts" / "end_of_shift_app" / "Code.gs"
WORKFLOWS = ROOT / ".github" / "workflows"

NODE_SHIM = r"""
const vm = require('vm'); const fs = require('fs');
const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const pad = n => String(n).padStart(2, '0');
const g = {
  Utilities: { formatDate(d, tz, fmt) {
    if (tz !== 'UTC') throw new Error('shim formats UTC only, got ' + tz);
    const h12 = (d.getUTCHours() % 12) || 12, ap = d.getUTCHours() < 12 ? 'AM' : 'PM';
    if (fmt === 'yyyy-MM-dd') return d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate());
    if (fmt === 'MMM d, h:mm a') return MON[d.getUTCMonth()] + ' ' + d.getUTCDate() + ', ' + h12 + ':' + pad(d.getUTCMinutes()) + ' ' + ap;
    throw new Error('shim: unknown format ' + fmt);
  } },
  Session: { getScriptTimeZone: () => 'UTC' },
  console, module: {}, require: undefined,
};
vm.createContext(g);
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), g);
const out = {};
for (const expr of JSON.parse(process.argv[3])) out[expr] = vm.runInContext(expr, g);
process.stdout.write(JSON.stringify(out));
"""


def run_gs(exprs: list[str]) -> dict:
    """Evaluate expressions against Code.gs under node with a tiny Apps Script shim."""
    import shutil, subprocess, tempfile
    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed here")
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(NODE_SHIM)
    r = subprocess.run([node, f.name, str(CODE_GS), json.dumps(exprs)], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


def test_relay_reads_excel_date_serials_as_dates() -> None:
    """Graph hands the dates the script wrote back as Excel serial numbers; the duplicate
    check and the 'filed at' time must read those, text, and Date objects alike."""
    out = run_gs([
        "normDate_(46321)",                       # 2026-10-26 as Excel stores it
        "normDate_(46321.5)",                     # the time of day does not change the date
        "normDate_('2026-10-26')",                # text as written (if Excel ever keeps it)
        "normDate_('2026-10-26 13:17:16')",
        "normDate_(new Date(Date.UTC(2026, 9, 26)))",
        "normDate_('')", "normDate_(null)",
        "fmtTime_(46321 + (13 * 60 + 17) / 1440)",  # 1:17 PM on the 26th
        "fmtTime_('2026-10-26 13:17:16')",
        "isSerial_('46321')", "isSerial_(0)", "isSerial_(46321)",
    ])
    assert out["normDate_(46321)"] == "2026-10-26" == out["normDate_(46321.5)"]
    assert out["normDate_('2026-10-26')"] == "2026-10-26" == out["normDate_('2026-10-26 13:17:16')"]
    assert out["normDate_(new Date(Date.UTC(2026, 9, 26)))"] == "2026-10-26"
    assert out["normDate_('')"] == "" == out["normDate_(null)"]
    assert out["fmtTime_(46321 + (13 * 60 + 17) / 1440)"] == "Oct 26, 1:17 PM"
    assert out["fmtTime_('2026-10-26 13:17:16')"] == "Oct 26, 1:17 PM"
    assert out["isSerial_('46321')"] is False and out["isSerial_(0)"] is False and out["isSerial_(46321)"] is True


def test_relay_duplicate_check_matches_serial_rows() -> None:
    """getExisting over rows as Graph returns them: the Submissions row for the same date and
    shift is found whether its cells are serials or text, and the latest one wins."""
    rows = ("[[46321.55, '20261026-131716-1st', 46321, '1st', 'Test', 'Test', 0, ''],"
            " ['2026-10-26 14:00:00', '20261026-140000-1st', '2026-10-26', '1st', 'Tim', '', 4, 'yes'],"
            " [46321.6, '20261026-142400-2nd', 46321, '2nd', 'Brandon', '', 3, '']]")
    out = run_gs([
        f"(function(){{ tableValues_ = function(){{ return {rows}; }}; return getExisting('2026-10-26', '1st'); }})()",
        f"(function(){{ tableValues_ = function(){{ return {rows}; }}; return getExisting('2026-10-26', '2nd'); }})()",
        f"(function(){{ tableValues_ = function(){{ return {rows}; }}; return getExisting('2026-10-27', '1st'); }})()",
    ])
    first, second, none = out.values()
    assert first == {"at": "Oct 26, 2:00 PM", "by": "Tim", "machines": 4}
    assert second == {"at": "Oct 26, 2:24 PM", "by": "Brandon", "machines": 3}
    assert none is None


def test_relay_machine_list_matches_the_pipeline_order() -> None:
    import re
    gs = CODE_GS.read_text()
    labels = re.findall(r"\['([^']+)', '[A-Z0-9 ()/]+'\]", gs.split("var MACHINES")[1].split("];")[0])
    assert len(labels) == 9 and "Extruder" in labels and "Guillotine" in labels
    assert "doPost" in gs and "docs/eos" not in gs and not (ROOT / "docs" / "eos").exists()


def test_retry_after_header_as_seconds_or_http_date() -> None:
    from src.graph_client import retry_after_seconds
    assert retry_after_seconds("3", 0) == 3.0
    assert retry_after_seconds(None, 2) == 4.0 and retry_after_seconds("", 0) == 1.0
    assert retry_after_seconds("600", 0) == 60.0                               # capped
    now = 1_800_000_000.0
    import email.utils
    at = email.utils.formatdate(now + 7, usegmt=True)
    assert round(retry_after_seconds(at, 0, now=now)) == 7
    assert retry_after_seconds(email.utils.formatdate(now - 100, usegmt=True), 1, now=now) == 0.0   # in the past: go now
    assert retry_after_seconds("garbage", 1) == 2.0                             # unparseable: backoff


def test_retry_waits_for_an_http_date_then_succeeds(monkeypatch) -> None:
    class R:
        def __init__(self, status, body=None, headers=None):
            self.status_code, self._body, self.headers, self.content, self.text = status, body, headers or {}, b"x", "err"
        def json(self): return self._body
    class S:
        def __init__(self): self.n = 0
        def request(self, *a, **k):
            self.n += 1
            return R(429, headers={"Retry-After": "Mon, 01 Jan 2001 00:00:00 GMT"}) if self.n == 1 else R(200, {"value": [1]})
    slept = []
    monkeypatch.setattr("src.graph_client.time.sleep", lambda s: slept.append(s))
    c = GraphClient("t", "c", "s", mailbox="m@x.com", session=S())
    monkeypatch.setattr(c, "token", lambda: "tok")
    assert c.get("/x") == {"value": [1]} and slept == [0.0]


def test_401_refresh_gets_a_fresh_token_past_the_msal_cache(monkeypatch) -> None:
    """MSAL's app-only client returns its cached token until it expires; after a 401 the
    client must build a new one so the retry carries a different bearer."""
    import sys, types
    made = []
    class App:
        def __init__(self, client_id, authority=None, client_credential=None):
            made.append(self)
        def acquire_token_for_client(self, scopes):
            return {"access_token": f"tok{len(made)}", "expires_in": 3600}
    monkeypatch.setitem(sys.modules, "msal", types.SimpleNamespace(ConfidentialClientApplication=App))
    class R:
        def __init__(self, status, body=None):
            self.status_code, self._body, self.headers, self.content, self.text = status, body, {}, b"x", "err"
        def json(self): return self._body
    seen = []
    class S:
        def request(self, method, url, params=None, json=None, headers=None, timeout=None):
            seen.append(headers["Authorization"])
            return R(401) if len(seen) == 1 else R(200, {"ok": True})
    c = GraphClient("t", "c", "secret", mailbox="m@x.com", session=S())
    assert c.get("/x") == {"ok": True}
    assert seen == ["Bearer tok1", "Bearer tok2"] and len(made) == 2
    assert c.get("/x") == {"ok": True} and len(made) == 2          # no 401: the token is reused


def test_from_env_needs_no_mailbox_to_read_a_workbook(monkeypatch) -> None:
    for k in ("GRAPH_TENANT_ID", "GRAPH_CLIENT_ID", "GRAPH_CLIENT_SECRET"):
        monkeypatch.setenv(k, "x")
    monkeypatch.delenv("GRAPH_MAILBOX", raising=False)
    c = GraphClient.from_env()
    assert c.app_only and c.mailbox is None
    with pytest.raises(FileNotFoundError, match="GRAPH_MAILBOX"):
        c.mailbox_path                                            # only a mailbox call needs it
    monkeypatch.setenv("GRAPH_MAILBOX", "ops@x.com")
    assert GraphClient.from_env().mailbox_path == "/users/ops@x.com"
    monkeypatch.delenv("GRAPH_CLIENT_ID")
    with pytest.raises(FileNotFoundError, match="GRAPH_CLIENT_ID"):
        GraphClient.from_env()


def test_workflow_gates_check_drive_and_item_ids_and_the_fallback_pull_is_non_fatal() -> None:
    gate = '[ -z "$EOS_WORKBOOK_URL" ] && { [ -z "$EOS_WORKBOOK_DRIVE_ID" ] || [ -z "$EOS_WORKBOOK_ITEM_ID" ]; }'
    for name in ("cietrade-poll.yml", "digest-sample.yml", "weekly-cloud.yml"):
        assert gate in (WORKFLOWS / name).read_text(), name
    weekly = (WORKFLOWS / "weekly-cloud.yml").read_text()
    pull = weekly.split("Pull End of Shift entries")[1].split("- name:")[0]
    assert "continue-on-error: true" in pull
    pipeline = weekly.split("Run weekly update pipeline")[1].split("- name:")[0]
    assert "GRAPH_CLIENT_SECRET" not in pipeline and "EOS_WORKBOOK" not in pipeline   # pulled once, in the step above


def test_sheet_reader_blanks_nan_material_and_comment() -> None:
    df = pd.DataFrame([{"Timestamp": "2026-10-26 13:00:00", "Submission ID": "x", "Date": "2026-10-26", "Shift": "1st",
                        "Machine": "EXTRUDER", "Machine hours operated": 7, "Total man hours": 14, "Operator(s)": "A, B",
                        "Material run": float("nan"), "Downtime minutes": "", "Downtime reason": None,
                        "Comments": " nan ", "Submitted by": "Tim"}])
    rows, _notes = labor_sheet.responses_to_entries(df, source="test")
    assert len(rows) == 1
    r = rows.iloc[0]
    assert pd.isna(r["Material"]) and pd.isna(r["Comment"]) and r["Machine_Name"] == "EXTRUDER"
