"""Outlook mail through Microsoft Graph: the daily email's sender.

`GraphMailbox.send(to, subject, html, text, bcc)` posts to /sendMail for the
mailbox the client acts for (GRAPH_MAILBOX with app-only credentials, or the
signed-in person). Reading mail is not needed any more: production comes from
the cieTrade poll log and the End of Shift log, nothing arrives by email.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from graph_client import GraphClient, GraphError  # noqa: E402,F401


class GraphMailbox:
    is_graph = True

    def __init__(self, client: GraphClient):
        self.client = client

    @classmethod
    def from_env(cls) -> "GraphMailbox":
        return cls(GraphClient.from_env())

    @property
    def prefix(self) -> str:
        return self.client.mailbox_path

    # ---------------------------------------------------------------- send
    def send(self, to: list[str], subject: str, html: str, bcc: list[str] | None = None) -> str:
        """One HTML message. Graph's sendMail carries a single body, so there is no plain-text
        alternative to attach (the Gmail path builds a multipart message instead)."""
        msg: dict[str, Any] = {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html},
            "toRecipients": [{"emailAddress": {"address": a}} for a in to],
        }
        if bcc:
            msg["bccRecipients"] = [{"emailAddress": {"address": a}} for a in bcc]
        self.client.post(f"{self.prefix}/sendMail", {"message": msg, "saveToSentItems": True})
        return f"via Graph from {getattr(self.client, 'mailbox', None) or 'me'}"   # Graph returns no message id from sendMail

    def whoami(self) -> str:
        return self.client.whoami()
