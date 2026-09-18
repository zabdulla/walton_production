"""Render the local cieTrade operations pilot page from what explore.py found.

    python3 explorations/cietrade_ops/build.py            # -> out/cietrade_ops_pilot.html (open in a browser)

For every endpoint that answered with rows the page shows: a column profile (fill
rate, distinct values, samples), rows per week on the first date-like column,
totals of numeric columns by warehouse/site-like column, and a preview table.
No external libraries, nothing leaves the machine; out/ is gitignored.
"""
from __future__ import annotations

import html
import json
import re
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}|^\d{1,2}/\d{1,2}/\d{4}")


def _num(v):
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    try:
        s = str(v).replace(",", "").replace("$", "").strip()
        return float(s) if s and s not in ("-", "") else None
    except ValueError:
        return None


def _date(v):
    s = str(v or "").strip()
    if not DATE_RE.match(s):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:19] if "T" in s else s, fmt)
        except ValueError:
            continue
    return None


def profile(rows: list[dict], columns: list[str]) -> list[dict]:
    n = max(len(rows), 1)
    out = []
    for c in columns:
        vals = [r.get(c) for r in rows]
        filled = [v for v in vals if v not in (None, "", "0", 0) or (isinstance(v, (int, float)) and v != 0)]
        distinct = {str(v) for v in vals if v not in (None, "")}
        nums = [x for x in (_num(v) for v in vals if v not in (None, "")) if x is not None]
        dates = [d for d in (_date(v) for v in vals if v not in (None, "")) if d]
        kind = "date" if dates and len(dates) >= 0.8 * max(len([v for v in vals if v not in (None, "")]), 1) else \
               "number" if nums and len(nums) >= 0.8 * max(len([v for v in vals if v not in (None, "")]), 1) else "text"
        samples = sorted(distinct, key=lambda s: (-sum(1 for v in vals if str(v) == s), s))[:6]
        out.append({"name": c, "kind": kind, "fill": round(100 * len(filled) / n), "distinct": len(distinct),
                    "samples": samples, "sum": round(sum(nums), 1) if kind == "number" else None,
                    "min": min(dates).strftime("%Y-%m-%d") if kind == "date" else None,
                    "max": max(dates).strftime("%Y-%m-%d") if kind == "date" else None})
    return out


def site_column(columns: list[str]) -> str | None:
    for pat in ("warehouse", "site", "location", "plant", "yard"):
        for c in columns:
            if pat in c.lower() and "status" not in c.lower():
                return c
    return None


def section(ep: str, rec: dict, rows: list[dict]) -> str:
    cols = rec["columns"]
    prof = profile(rows, cols)
    esc = html.escape
    h = [f'<section class="card" id="{esc(ep)}"><h2>{esc(ep)} <span class="muted">{rec["rows"]:,} rows · {len(cols)} columns · {rec["ms"]} ms</span></h2>']
    if rec.get("params"):
        h.append(f'<p class="muted">query: {esc(", ".join(f"{k}={v}" for k, v in rec["params"].items()))}</p>')
    # column profile
    h.append('<h3>Columns</h3><table class="prof"><tr><th>column</th><th>kind</th><th>filled</th><th>distinct</th><th>range / total</th><th>most common</th></tr>')
    for p in prof:
        rng = f'{p["min"]} → {p["max"]}' if p["kind"] == "date" else (f'Σ {p["sum"]:,}' if p["kind"] == "number" else "")
        h.append(f'<tr><td><code>{esc(p["name"])}</code></td><td>{p["kind"]}</td><td>{p["fill"]}%</td><td>{p["distinct"]}</td><td>{esc(rng)}</td><td class="small">{esc(" · ".join(p["samples"]))}</td></tr>')
    h.append("</table>")
    # rows per week on first date column
    dcol = next((p["name"] for p in prof if p["kind"] == "date"), None)
    if dcol:
        weeks: dict[str, int] = {}
        for r in rows:
            d = _date(r.get(dcol))
            if d:
                wk = (d - __import__("datetime").timedelta(days=d.weekday())).strftime("%Y-%m-%d")
                weeks[wk] = weeks.get(wk, 0) + 1
        if weeks:
            mx = max(weeks.values())
            h.append(f'<h3>Rows per week <span class="muted">by {esc(dcol)}</span></h3><div class="bars">')
            for wk in sorted(weeks):
                h.append(f'<div class="bar"><span class="lbl">{wk}</span><i style="width:{100 * weeks[wk] / mx:.0f}%"></i><span class="val">{weeks[wk]}</span></div>')
            h.append("</div>")
    # numeric totals by site
    scol = site_column(cols)
    ncols = [p["name"] for p in prof if p["kind"] == "number"][:6]
    if scol and ncols:
        by: dict[str, dict[str, float]] = {}
        cnt: dict[str, int] = {}
        for r in rows:
            s = str(r.get(scol) or "—")
            cnt[s] = cnt.get(s, 0) + 1
            for c in ncols:
                x = _num(r.get(c))
                if x is not None:
                    by.setdefault(s, {})[c] = by.setdefault(s, {}).get(c, 0.0) + x
        h.append(f'<h3>By {esc(scol)}</h3><table class="prof"><tr><th>{esc(scol)}</th><th>rows</th>' + "".join(f"<th>Σ {esc(c)}</th>" for c in ncols) + "</tr>")
        for s in sorted(cnt, key=lambda k: -cnt[k]):
            h.append(f"<tr><td>{esc(s)}</td><td>{cnt[s]:,}</td>" + "".join(f'<td>{by.get(s, {}).get(c, 0):,.0f}</td>' for c in ncols) + "</tr>")
        h.append("</table>")
    # preview
    show = cols[:14]
    h.append(f'<h3>Preview <span class="muted">first {min(50, len(rows))} rows, {len(show)} of {len(cols)} columns</span></h3><div class="scroll"><table class="prev"><tr>' + "".join(f"<th>{esc(c)}</th>" for c in show) + "</tr>")
    for r in rows[:50]:
        h.append("<tr>" + "".join(f'<td>{esc(str(r.get(c, "") if r.get(c) is not None else ""))[:60]}</td>' for c in show) + "</tr>")
    h.append("</table></div></section>")
    return "".join(h)


CSS = """
:root{--bg:#f6f7f9;--card:#fff;--text:#1a1d21;--muted:#66707c;--border:#e3e6ea;--accent:#1f6feb;--ok:#1a7f37;--bad:#b42318}
@media(prefers-color-scheme:dark){:root{--bg:#111418;--card:#181c22;--text:#e8ebef;--muted:#9aa4b0;--border:#2a3037;--accent:#6ea8ff;--ok:#3fb950;--bad:#f0645d}}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding:16px}
main{max-width:1200px;margin:0 auto}h1{font-size:22px;margin:0 0 4px}h2{font-size:17px;margin:0 0 8px}h3{font-size:13px;margin:16px 0 6px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted)}
.muted{color:var(--muted);font-weight:400;font-size:12px}.small{font-size:12px}.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;margin:14px 0}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:5px 8px;border-bottom:1px solid var(--border);vertical-align:top}th{font-size:12px;color:var(--muted)}
.prev td{white-space:nowrap;font-size:12px}.scroll{overflow:auto;max-height:420px}code{font-size:12px}
.ok{color:var(--ok);font-weight:600}.bad{color:var(--bad)}.bars .bar{display:flex;align-items:center;gap:8px;margin:2px 0}.bar .lbl{width:84px;font-size:12px;color:var(--muted)}
.bar i{display:block;height:12px;background:var(--accent);border-radius:3px;min-width:2px}.bar .val{font-size:12px}
nav a{margin-right:12px;font-size:13px;color:var(--accent);text-decoration:none}
"""


def main() -> None:
    disc_path = OUT / "discovery.json"
    if not disc_path.exists():
        raise SystemExit("no out/discovery.json — run explore.py first")
    disc = json.loads(disc_path.read_text())
    eps = disc["endpoints"]
    ok = [e for e, r in eps.items() if r["kind"] == "ok"]
    esc = html.escape
    parts = [f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>cieTrade ops pilot</title><style>{CSS}</style></head><body><main>',
             f'<h1>cieTrade operations pilot</h1><p class="muted">probed {esc(str(disc.get("probed_at")))} · window from {esc(str(disc.get("window_from") or "no dates"))} · local only, nothing here is published</p>']
    parts.append('<section class="card"><h2>What answered</h2><table class="prof"><tr><th>endpoint</th><th>http</th><th>result</th><th>rows</th><th>columns / error</th></tr>')
    for e, r in sorted(eps.items(), key=lambda kv: (kv[1]["kind"] != "ok", kv[0])):
        cls = "ok" if r["kind"] == "ok" else ("bad" if r["kind"] in ("api-error",) else "muted")
        detail = ", ".join(r["columns"]) if r["kind"] == "ok" else (r.get("error") or "")
        link = f'<a href="#{esc(e)}">{esc(e)}</a>' if r["kind"] == "ok" else esc(e)
        parts.append(f'<tr><td>{link}</td><td>{esc(str(r.get("http") or "-"))}</td><td class="{cls}">{esc(r["kind"])}</td><td>{r["rows"]:,}</td><td class="small">{esc(detail[:220])}</td></tr>')
    parts.append("</table></section>")
    if ok:
        parts.append("<nav>" + "".join(f'<a href="#{esc(e)}">{esc(e)}</a>' for e in ok) + "</nav>")
    for e in ok:
        rows = json.loads((OUT / eps[e]["sample_file"]).read_text())
        parts.append(section(e, eps[e], rows))
    parts.append("</main></body></html>")
    OUT.mkdir(exist_ok=True)
    out = OUT / "cietrade_ops_pilot.html"
    out.write_text("".join(parts))
    print(f"wrote {out} ({len(ok)} endpoint section(s))")


if __name__ == "__main__":
    main()
