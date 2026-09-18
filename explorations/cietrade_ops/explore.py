"""Probe the cieTrade API beyond ListConvertingJobs and keep samples locally.

cieTrade exposes screens as GET https://api.cietrade.net/List<Screen>?UserID=...
(Bearer API key in the header). Only ListConvertingJobs is documented in this
repo; this probe tries the other screen names we would expect (receiving,
inventory, purchase/sales orders, shipments, tickets) and records what answers.

    python3 explorations/cietrade_ops/explore.py                  # probe every candidate, save samples
    python3 explorations/cietrade_ops/explore.py --days 120       # wider date window
    python3 explorations/cietrade_ops/explore.py --endpoint ListReceipts --param Warehouse="Plus Monroe Warehouse"
    python3 explorations/cietrade_ops/explore.py --list           # print the candidates and exit

Everything lands in out/ (gitignored): out/discovery.json (what answered, columns,
row counts) and out/samples/<Endpoint>.json (the rows, local only — cieTrade data
never goes to the repo from here). Then build.py renders the pilot page.
Nothing is written to cieTrade.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
SAMPLES = OUT / "samples"
sys.path.insert(0, str(HERE.parents[1] / "src"))
import cietrade_api as api  # noqa: E402

# Documented read endpoints (explorations/cietrade_ops/API_REFERENCE.md) with the
# parameters each one needs beyond UserID and the date window. Probed 2026-09-18:
# all of these answer. Unknown names come back HTTP 404 {"Message": ...}.
CANDIDATES: list[tuple[str, dict]] = [
    ("ListConvertingJobs", {}),
    ("ListInventory", {}),                                   # lots: PR-* receipts, CJ-* job outputs; DateFrom/DateTo required
    ("ListOrders", {"Source": "PO"}), ("ListOrders", {"Source": "SO"}),
    ("ListOrderDetails", {"Source": "PO"}), ("ListOrderDetails", {"Source": "SO"}),
    ("ListWorksheets", {"Status": "ALL"}), ("ListWorksheetDetails", {"Status": "ALL"}), ("ListWorksheetExpenses", {"Status": "ALL"}),
    ("TradingInquiry", {"Status": "ALL"}),
    ("ListAdjustments", {"Type": "P"}), ("ListAdjustments", {"Type": "S"}),
    ("ListDispatchJobs", {}),
    ("ListAccounts", {}), ("ListAccountLocations", {}), ("ListContacts", {}),
    ("ListAccountsReceivable", {}), ("ListCustomerLedger", {}), ("ListBillingSheets", {}), ("ListBillingSheetCharges", {}),
    ("VoucherInquiry", {}), ("PostedPayables", {}), ("ListServices", {}), ("ListServiceExpenses", {}), ("SystemLog", {}),
]


def call(creds: dict, endpoint: str, params: dict, timeout: int = 120) -> dict:
    """One GET. Returns a record describing what came back; never raises."""
    q = {"UserID": creds["user_id"]}
    q.update({k: str(v) for k, v in params.items() if v not in (None, "")})
    url = f"{creds['base_url'].rstrip('/')}/{endpoint}?{urllib.parse.urlencode(q)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {creds['api_key']}", "Accept": "application/json"})
    rec = {"endpoint": endpoint, "params": {k: v for k, v in params.items() if v not in (None, "")}, "http": None,
           "kind": "unknown", "rows": 0, "columns": [], "error": None, "ms": 0, "sample": None}
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body, rec["http"] = resp.read(), resp.status
    except urllib.error.HTTPError as e:
        rec["http"], rec["kind"], rec["error"] = e.code, "http-error", f"HTTP {e.code} {e.reason}"
        body = e.read()
    except Exception as e:  # noqa: BLE001
        rec["kind"], rec["error"] = "transport-error", f"{e.__class__.__name__}: {str(e)[:160]}"
        body = b""
    rec["ms"] = int((time.time() - t0) * 1000)
    if not body:
        return rec
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        rec["kind"] = rec["kind"] if rec["error"] else "non-json"
        rec["error"] = rec["error"] or f"non-JSON ({len(body)} bytes): {body[:100]!r}"
        return rec
    if rec["error"]:                       # HTTP 4xx/5xx with a JSON body ({"Message": ...}) is still an error
        return rec
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        rec["kind"], rec["error"] = "unexpected", f"JSON {type(data).__name__}"
        return rec
    if data and isinstance(data[0], dict) and ("ERROR" in data[0] or "INPUT ERROR" in data[0]):
        rec["kind"], rec["error"] = "api-error", str(data[0].get("ERROR") or data[0].get("INPUT ERROR"))[:300]
        return rec
    rec["kind"], rec["rows"] = "ok", len(data)
    cols: list[str] = []
    for r in data[:200]:
        for k in (r.keys() if isinstance(r, dict) else []):
            if k not in cols:
                cols.append(k)
    rec["columns"] = cols
    rec["sample"] = data
    return rec


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--days", type=int, default=60, help="DateFrom = today minus this many days (default 60)")
    ap.add_argument("--endpoint", help="probe one endpoint only")
    ap.add_argument("--param", action="append", default=[], help="extra query parameter, k=v (repeatable)")
    ap.add_argument("--no-date", action="store_true", help="omit DateFrom/DateTo")
    ap.add_argument("--list", action="store_true", help="print the candidate endpoints and exit")
    args = ap.parse_args(argv)
    if args.list:
        print("\n".join(f"{ep} {p or ''}".rstrip() for ep, p in CANDIDATES))
        return 0
    creds = api.load_credentials()
    since = (datetime.now() - timedelta(days=args.days)).strftime("%m/%d/%Y")
    base: dict = {} if args.no_date else {"DateFrom": since, "DateTo": datetime.now().strftime("%m/%d/%Y")}
    for kv in args.param:
        k, _, v = kv.partition("=")
        base[k] = v
    targets = [(args.endpoint, {})] if args.endpoint else CANDIDATES
    OUT.mkdir(exist_ok=True)
    SAMPLES.mkdir(exist_ok=True)
    disc_path = OUT / "discovery.json"
    discovery = json.loads(disc_path.read_text()) if disc_path.exists() else {"probed_at": None, "endpoints": {}}
    print(f"{'endpoint':28} {'http':>4} {'kind':16} {'rows':>6}  columns / error")
    for ep, extra in targets:
        rec = call(creds, ep, {**base, **extra})
        if rec["kind"] == "api-error" and not args.no_date and "date" in (rec["error"] or "").lower():
            rec = call(creds, ep, {k: v for k, v in base.items() if k not in ("DateFrom", "DateTo")})   # some screens take no dates
        detail = ", ".join(rec["columns"][:12]) + (" …" if len(rec["columns"]) > 12 else "") if rec["kind"] == "ok" else (rec["error"] or "")
        print(f"{ep:28} {str(rec['http'] or '-'):>4} {rec['kind']:16} {rec['rows']:>6}  {detail[:110]}")
        sample = rec.pop("sample")
        # one file per (endpoint, parameters) so ListOrders?Source=PO and ?Source=SO coexist
        tag = "_".join(f"{k}-{v}" for k, v in sorted(rec["params"].items()) if k not in ("DateFrom", "DateTo"))
        key = f"{ep}[{tag}]" if tag else ep
        if sample is not None:
            (SAMPLES / f"{key}.json").write_text(json.dumps(sample, indent=1))
            rec["sample_file"] = f"samples/{key}.json"
        discovery["endpoints"][key] = rec
        time.sleep(0.3)
    discovery["probed_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    discovery["window_from"] = None if args.no_date else since
    disc_path.write_text(json.dumps(discovery, indent=1))
    ok = [e for e, r in discovery["endpoints"].items() if r["kind"] == "ok"]
    print(f"\n{len(ok)} endpoint(s) answered with rows: {', '.join(ok) or '—'}")
    print(f"wrote {disc_path}; now: python3 {HERE.relative_to(HERE.parents[1])}/build.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
