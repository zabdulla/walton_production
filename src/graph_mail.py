"""Outlook mail through Microsoft Graph, shaped like the Gmail code expects.

`GraphMailbox` answers the same four calls the fetchers make against the Gmail
service (`list_messages`, `get_message`, `download_attachment_bytes` and the
headers/parts layout `header_value` and `iter_attachments` walk), so
fetch_emails.py and shift_report_ocr.py run unchanged on either provider. The
Gmail-style query strings they build (from:, subject:"...", filename:, has:attachment,
newer_than:Nd) are translated here into a Graph $filter plus a client-side
attachment-name check, which is where Gmail's `filename:` lives in Graph terms.

It also sends: `send(to, subject, html, text, bcc)` for the daily email.
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from graph_client import GraphClient, GraphError  # noqa: E402,F401

_TOKEN_RE = re.compile(r'(\w+):(?:"([^"]*)"|(\S+))|(\S+)')


def parse_gmail_query(query: str) -> dict[str, Any]:
    """'from:a@b subject:"x y" filename:Pay has:attachment newer_than:14d' -> fields."""
    out: dict[str, Any] = {"sender": None, "subject": None, "filename": None, "has_attachment": False, "days_back": None, "text": []}
    for m in _TOKEN_RE.finditer(query or ""):
        key, quoted, bare, loose = m.groups()
        val = quoted if quoted is not None else bare
        if key is None:
            out["text"].append(loose)
        elif key == "from":
            out["sender"] = val
        elif key == "subject":
            out["subject"] = val
        elif key == "filename":
            out["filename"] = val
        elif key == "has" and val == "attachment":
            out["has_attachment"] = True
        elif key == "newer_than":
            n = re.match(r"(\d+)([dwm]?)", val)
            if n:
                mult = {"": 1, "d": 1, "w": 7, "m": 30}[n.group(2)]
                out["days_back"] = int(n.group(1)) * mult
        else:
            out["text"].append(f"{key}:{val}")
    return out


def _odata_str(s: str) -> str:
    return s.replace("'", "''")


class GraphMailbox:
    is_graph = True

    def __init__(self, client: GraphClient):
        self.client = client
        self._attachments: dict[str, list[dict]] = {}
        self._meta: dict[str, dict] = {}

    @classmethod
    def from_env(cls) -> "GraphMailbox":
        return cls(GraphClient.from_env())

    @property
    def prefix(self) -> str:
        return self.client.mailbox_path

    # ---------------------------------------------------------------- read
    def list_messages(self, query: str, max_results: int = 50) -> list[dict]:
        q = parse_gmail_query(query)
        parts = []
        if q["sender"]:
            parts.append(f"from/emailAddress/address eq '{_odata_str(q['sender'])}'")
        if q["has_attachment"] or q["filename"]:
            parts.append("hasAttachments eq true")
        if q["days_back"]:
            since = (datetime.now(timezone.utc) - timedelta(days=q["days_back"])).strftime("%Y-%m-%dT%H:%M:%SZ")
            parts.append(f"receivedDateTime ge {since}")
        if q["subject"]:
            parts.append(f"contains(subject,'{_odata_str(q['subject'])}')")
        params = {"$select": "id,subject,receivedDateTime,from,hasAttachments", "$top": str(min(max(max_results, 1), 100))}
        if parts:
            params["$filter"] = " and ".join(parts)
        if q["text"]:
            params["$search"] = '"' + " ".join(q["text"]).replace('"', "") + '"'
            params.pop("$filter", None)                 # Graph refuses $search together with $filter
        msgs = self.client.get_all(self.prefix + "/messages", params, limit=max_results * 4 if q["filename"] else max_results)
        msgs.sort(key=lambda m: m.get("receivedDateTime", ""), reverse=True)
        out = []
        for m in msgs:
            self._meta[m["id"]] = m
            if q["filename"] and not any(q["filename"].lower() in a["name"].lower() for a in self._list_attachments(m["id"])):
                continue                                # Gmail's filename: keeps only messages carrying such a file
            out.append({"id": m["id"], "threadId": m.get("conversationId", "")})
            if len(out) >= max_results:
                break
        return out

    def _list_attachments(self, msg_id: str) -> list[dict]:
        if msg_id not in self._attachments:
            items = self.client.get_all(f"{self.prefix}/messages/{msg_id}/attachments",
                                        {"$select": "id,name,size,contentType,isInline"})
            self._attachments[msg_id] = [a for a in items if a.get("@odata.type", "").endswith("fileAttachment") and not a.get("isInline")]
        return self._attachments[msg_id]

    def get_message(self, msg_id: str) -> dict:
        """A message in the Gmail 'full' layout the fetchers walk: headers plus attachment parts."""
        meta = self._meta.get(msg_id) or self.client.get(f"{self.prefix}/messages/{msg_id}",
                                                         {"$select": "id,subject,receivedDateTime,from,hasAttachments"})
        received = meta.get("receivedDateTime", "")
        try:
            date_hdr = format_datetime(datetime.strptime(received[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc))
        except ValueError:
            date_hdr = received
        frm = (meta.get("from") or {}).get("emailAddress", {})
        headers = [{"name": "Subject", "value": meta.get("subject", "")},
                   {"name": "From", "value": f"{frm.get('name', '')} <{frm.get('address', '')}>".strip()},
                   {"name": "Date", "value": date_hdr}]
        parts = [{"filename": a["name"], "mimeType": a.get("contentType", ""), "body": {"attachmentId": a["id"], "size": a.get("size", 0)}}
                 for a in self._list_attachments(msg_id)]
        return {"id": msg_id, "payload": {"headers": headers, "parts": parts}, "internalDate": received}

    def download_attachment_bytes(self, msg_id: str, attachment_id: str) -> bytes:
        return self.client.get_bytes(f"{self.prefix}/messages/{msg_id}/attachments/{attachment_id}/$value")

    # ---------------------------------------------------------------- send
    def send(self, to: list[str], subject: str, html: str, text: str | None = None, bcc: list[str] | None = None) -> str:
        msg: dict[str, Any] = {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html},
            "toRecipients": [{"emailAddress": {"address": a}} for a in to],
        }
        if bcc:
            msg["bccRecipients"] = [{"emailAddress": {"address": a}} for a in bcc]
        self.client.post(f"{self.prefix}/sendMail", {"message": msg, "saveToSentItems": True})
        return "sent"                                    # Graph returns no message id from sendMail

    def whoami(self) -> str:
        return self.client.whoami()
