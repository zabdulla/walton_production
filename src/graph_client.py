"""Microsoft Graph access for the pipeline: one token, one HTTP helper.

Two ways in, chosen by what is configured:

  app-only    GRAPH_TENANT_ID + GRAPH_CLIENT_ID + GRAPH_CLIENT_SECRET: an Entra app
              registration with application permissions (setup/MICROSOFT_365_SETUP.md).
              Nothing to re-consent and no refresh token to lapse, so this is what the
              GitHub runner uses. Mailbox calls go to /users/{GRAPH_MAILBOX}.
  delegated   GRAPH_CLIENT_ID (+ GRAPH_TENANT_ID) without a secret: a one-time device-code
              sign-in in the terminal, cached in ~/.config/walton/graph_token_cache.json,
              for a Mac. Mailbox calls go to /me.

Only `requests` and `msal` are needed. Every call carries a timeout and retries on
429 / 5xx with the Retry-After the service asks for.
"""
from __future__ import annotations

import json
import os
import sys
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import WALTON_CONFIG_DIR  # noqa: E402

GRAPH = "https://graph.microsoft.com/v1.0"
APP_SCOPES = ["https://graph.microsoft.com/.default"]
DELEGATED_SCOPES = ["Mail.Send", "Files.Read.All", "Sites.Read.All"]
TOKEN_CACHE_PATH = WALTON_CONFIG_DIR / "graph_token_cache.json"
TIMEOUT_SEC = 60
RETRIES = 3


def retry_after_seconds(header: str | None, attempt: int, now: float | None = None) -> float:
    """Seconds to wait before retrying: the Retry-After header as a number or an HTTP-date
    (both are legal), else exponential backoff; never more than a minute."""
    wait: float | None = None
    if header:
        try:
            wait = float(header)
        except ValueError:
            try:
                wait = parsedate_to_datetime(header).timestamp() - (now if now is not None else time.time())
            except (TypeError, ValueError):
                wait = None
    if wait is None:
        wait = float(2 ** attempt)
    return min(max(wait, 0.0), 60.0)


class GraphError(RuntimeError):
    def __init__(self, status: int, message: str, path: str = ""):
        super().__init__(f"Graph {status} on {path}: {message}" if path else f"Graph {status}: {message}")
        self.status, self.message, self.path = status, message, path


class GraphClient:
    def __init__(self, tenant: str | None, client_id: str | None, client_secret: str | None = None,
                 mailbox: str | None = None, cache_path: Path = TOKEN_CACHE_PATH, session: Any = None):
        self.tenant, self.client_id, self.client_secret = tenant, client_id, client_secret
        self.mailbox = mailbox
        self.cache_path = Path(cache_path)
        self.session = session or requests.Session()
        self._app: Any = None
        self._cache: Any = None
        self._token: str | None = None
        self._token_expires = 0.0
        self._refresh = False                       # set after a 401: the next token() must not come from a cache

    # ---------------------------------------------------------------- setup
    @classmethod
    def from_env(cls) -> "GraphClient":
        c = cls(tenant=os.environ.get("GRAPH_TENANT_ID"), client_id=os.environ.get("GRAPH_CLIENT_ID"),
                client_secret=os.environ.get("GRAPH_CLIENT_SECRET") or None, mailbox=os.environ.get("GRAPH_MAILBOX") or None)
        if not c.client_id:
            raise FileNotFoundError("GRAPH_CLIENT_ID is not set (see setup/MICROSOFT_365_SETUP.md)")
        return c                                    # GRAPH_MAILBOX is checked when a mailbox call is made, not here

    @property
    def app_only(self) -> bool:
        return bool(self.client_secret)

    @property
    def mailbox_path(self) -> str:
        """The URL prefix for mailbox calls: a named user for app-only, /me for a signed-in person.
        Reading a workbook needs no mailbox, so its absence only matters here."""
        if self.app_only and not self.mailbox:
            raise FileNotFoundError("GRAPH_MAILBOX is required with app-only credentials (the mailbox to send from)")
        return f"/users/{self.mailbox}" if (self.app_only or self.mailbox) else "/me"

    # ---------------------------------------------------------------- tokens
    def token(self) -> str:
        if self._token and time.time() < self._token_expires - 60:
            return self._token
        import msal
        authority = f"https://login.microsoftonline.com/{self.tenant or 'organizations'}"
        if self.app_only:
            if self._app is None or self._refresh:   # a fresh MSAL app has an empty cache, so this really re-requests
                self._app = msal.ConfidentialClientApplication(self.client_id, authority=authority, client_credential=self.client_secret)
            result = self._app.acquire_token_for_client(scopes=APP_SCOPES)
        else:
            if self._app is None:
                self._cache = msal.SerializableTokenCache()
                if self.cache_path.exists():
                    self._cache.deserialize(self.cache_path.read_text())
                self._app = msal.PublicClientApplication(self.client_id, authority=authority, token_cache=self._cache)
            accounts = self._app.get_accounts()
            result = self._app.acquire_token_silent(DELEGATED_SCOPES, account=accounts[0], force_refresh=self._refresh) if accounts else None
            if not result:
                flow = self._app.initiate_device_flow(scopes=DELEGATED_SCOPES)
                if "user_code" not in flow:
                    raise GraphError(0, f"device flow could not start: {flow.get('error_description', flow)}")
                print(flow["message"], flush=True)          # "To sign in, use a web browser to open ... and enter the code ..."
                result = self._app.acquire_token_by_device_flow(flow)
            if self._cache.has_state_changed:
                self.cache_path.parent.mkdir(parents=True, exist_ok=True)
                self.cache_path.write_text(self._cache.serialize())
                self.cache_path.chmod(0o600)
        if "access_token" not in result:
            raise GraphError(0, f"token request failed: {result.get('error')}: {result.get('error_description')}")
        self._token = result["access_token"]
        self._token_expires = time.time() + float(result.get("expires_in", 3600))
        self._refresh = False
        return self._token

    # ---------------------------------------------------------------- http
    def request(self, method: str, path: str, *, params: dict | None = None, json_body: Any = None,
                raw: bool = False, headers: dict | None = None) -> Any:
        url = path if path.startswith("http") else GRAPH + path
        last: GraphError | None = None
        for attempt in range(RETRIES + 1):
            h = {"Authorization": f"Bearer {self.token()}", **(headers or {})}
            r = self.session.request(method, url, params=params, json=json_body, headers=h, timeout=TIMEOUT_SEC)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < RETRIES:
                time.sleep(retry_after_seconds(r.headers.get("Retry-After"), attempt))
                continue
            if r.status_code == 401 and attempt == 0:
                self._token, self._refresh = None, True  # one real refresh (past any cache), then give up
                continue
            if r.status_code >= 400:
                try:
                    msg = r.json().get("error", {}).get("message", r.text)
                except ValueError:
                    msg = r.text
                last = GraphError(r.status_code, msg, path)
                break
            if r.status_code == 204 or not r.content:
                return b"" if raw else {}
            return r.content if raw else r.json()
        raise last or GraphError(0, "no response", path)

    def get(self, path: str, params: dict | None = None) -> dict:
        return self.request("GET", path, params=params)

    def get_bytes(self, path: str, params: dict | None = None) -> bytes:
        return self.request("GET", path, params=params, raw=True)

    def post(self, path: str, body: Any) -> dict:
        return self.request("POST", path, json_body=body)

    def get_all(self, path: str, params: dict | None = None, limit: int | None = None) -> list[dict]:
        """Follow @odata.nextLink; stop after `limit` items when given."""
        out: list[dict] = []
        page = self.get(path, params)
        while True:
            out.extend(page.get("value", []))
            nxt = page.get("@odata.nextLink")
            if not nxt or (limit and len(out) >= limit):
                return out[:limit] if limit else out
            page = self.get(nxt)

    def whoami(self) -> str:
        """The mailbox the client acts for (its address), for --auth style checks."""
        me = self.get(self.mailbox_path, {"$select": "mail,userPrincipalName"})
        return me.get("mail") or me.get("userPrincipalName") or self.mailbox or "?"


def settings_summary() -> str:
    mode = "app-only" if os.environ.get("GRAPH_CLIENT_SECRET") else "delegated (device sign-in)"
    return f"Graph {mode}, mailbox {os.environ.get('GRAPH_MAILBOX') or 'me'}, tenant {os.environ.get('GRAPH_TENANT_ID') or 'organizations'}"


if __name__ == "__main__":                              # python3 src/graph_client.py -> sign in / check
    c = GraphClient.from_env()
    print(settings_summary())
    print("acting as", c.whoami())
    print(json.dumps({"cache": str(c.cache_path) if not c.app_only else None}))
