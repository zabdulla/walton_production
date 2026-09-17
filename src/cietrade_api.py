"""cieTrade .NET API client — ListConvertingJobs (Converting Job Inquiry).

Reference: cieTrade "ListConvertingJobs endpoint reference" v1.0 (2026-09-16) and
https://cietrade.helpscoutdocs.com/article/1689-list-converting-jobs

    GET https://api.cietrade.net/ListConvertingJobs?UserID=<email>&<filters>
    Authorization: Bearer <API key>        Accept: application/json

Filters (all optional, case-insensitive): JobNo, DateType (JOB|POST), DateFrom,
DateTo, Warehouse, WarehouseStatus (SCHEDULED|IN PROCESS|COMPLETED),
Status (WORK|POSTED|ALL), Machine, Dept, Operator, FinishedProduct.
Errors come back as HTTP 200 with ``[{"ERROR": "..."}]``.

``normalize`` reshapes the JSON rows into the column layout of the manual
Converting Inquiry CSV export, so everything downstream reads one format.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

from config import CIETRADE_BASE_URL, CIETRADE_CONFIG_PATH

TIMEOUT_S = 180  # the API's own database timeout

# API field -> CSV export column (order matches the export file)
FIELD_MAP = [
    ("JobNo", "Job No"), ("JobDate", "Job Date"), ("Warehouse", "Warehouse"),
    ("WarehouseStatus", "Warehouse Status"), ("Machine", "Machine"), ("Status", "Status"),
    ("PostDate", "Post Date"), ("StartTime", "Start-Time"), ("EndTime", "End-Time"),
    ("ElapsedTime", "Elapsed-Time"), ("UOM", "UOM"), ("InputQty", "Input Qty"),
    ("OutputQty", "Output Qty"), ("YieldLossQty", "Yield Loss Qty"), ("Department", "Department"),
    ("Description", "Description"), ("InputValue", "Input Value"), ("OutputValue", "Output Value"),
    ("Expenses", "Expenses"), ("UserDefined1", "User Defined 1"), ("UserDefined2", "UserDefined2"),
    ("InputUnits", "Input Units"), ("OutputUnits", "Output Units"),
    ("Operator", "Operator"), ("FinishedProduct", "Finished Product"),
]
EXPORT_COLUMNS = [c for _, c in FIELD_MAP]
NUMERIC = {"Input Qty", "Output Qty", "Yield Loss Qty", "Input Value", "Output Value", "Expenses"}
DATES = {"Job Date", "Post Date"}


class CieTradeError(RuntimeError):
    """The API answered, but with an error payload."""


def load_credentials(path: Path = CIETRADE_CONFIG_PATH) -> dict[str, str]:
    """Environment first (CI / one-off shells), then the local config file."""
    user, key = os.environ.get("CIETRADE_USER_ID"), os.environ.get("CIETRADE_API_KEY")
    if user and key:
        return {"base_url": os.environ.get("CIETRADE_BASE_URL", CIETRADE_BASE_URL), "user_id": user, "api_key": key}
    if not path.exists():
        raise FileNotFoundError(
            f"cieTrade credentials not found. Create {path} as "
            '{"user_id": "<cieTrade login email>", "api_key": "<API token from Settings > Integration>"} '
            "with permissions 600, or set CIETRADE_USER_ID and CIETRADE_API_KEY.")
    cfg = json.loads(path.read_text())
    missing = [k for k in ("user_id", "api_key") if not cfg.get(k)]
    if missing:
        raise KeyError(f"{path} is missing {', '.join(missing)}")
    cfg.setdefault("base_url", CIETRADE_BASE_URL)
    return cfg


def _request(url: str, headers: dict[str, str]) -> bytes:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        return resp.read()


def list_converting_jobs(creds: dict[str, str], _fetch=_request, **filters: Any) -> list[dict]:
    """Call ListConvertingJobs with the given filters; return the JSON rows.

    ``_fetch`` is injectable for tests. Empty/None filter values are dropped
    (the API treats a missing parameter as "all").
    """
    params = {"UserID": creds["user_id"]}
    params.update({k: str(v) for k, v in filters.items() if v not in (None, "")})
    url = f"{creds['base_url'].rstrip('/')}/ListConvertingJobs?{urllib.parse.urlencode(params)}"
    headers = {"Authorization": f"Bearer {creds['api_key']}", "Accept": "application/json"}
    body = _fetch(url, headers)
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        raise CieTradeError(f"non-JSON response ({len(body)} bytes): {body[:120]!r}") from e
    if isinstance(data, dict):
        data = [data]
    if data and isinstance(data[0], dict) and "ERROR" in data[0]:
        raise CieTradeError(str(data[0]["ERROR"]))
    return data


def _num(v: Any) -> float:
    if v is None or v == "":
        return float("nan")
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", "").strip())
    except ValueError:
        return float("nan")


def normalize(rows: list[dict]) -> pd.DataFrame:
    """JSON rows -> DataFrame in the manual export's column layout.

    Quantities become floats (the API sends formatted strings), dates become
    ISO ``YYYY-MM-DD`` strings (null -> empty), job numbers become ints.
    """
    out: list[dict] = []
    for r in rows:
        rec: dict[str, Any] = {}
        for api, col in FIELD_MAP:
            v = r.get(api)
            if col in NUMERIC:
                rec[col] = _num(v)
            elif col in DATES:
                rec[col] = str(v)[:10] if v else ""
            elif col == "Job No":
                rec[col] = int(str(v).strip()) if v not in (None, "") else None
            elif col in ("Input Units", "Output Units"):
                rec[col] = int(_num(v)) if v not in (None, "") and _num(v) == _num(v) else 0
            else:
                rec[col] = "" if v is None else str(v).strip()
        out.append(rec)
    df = pd.DataFrame(out, columns=EXPORT_COLUMNS)
    return df.sort_values(["Job Date", "Job No"], kind="stable").reset_index(drop=True) if len(df) else df
