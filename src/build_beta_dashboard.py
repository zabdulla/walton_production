"""Beta production dashboard: docs/beta/index.html.

One page, five views (Today · Week · Shifts · Machines · More), a sidebar on desktop
that becomes a bottom tab bar on a phone. Built from the same inputs as the current
site: data/aggregated_daily_data.xlsx (pounds per shift-day-machine),
data/cietrade_status.json (End of Shift reports, freshness) and the live feed gist
(fetched client-side). Charts are plain inline SVG in the Financial Times manner:
message titles, hairline horizontal grid, no legends, the latest point labelled.
See design/DESIGN.md.

    python3 src/build_beta_dashboard.py               # -> docs/beta/index.html
"""
from __future__ import annotations

import argparse
import html as _h
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATA_DIR, DEFAULT_WEEKS, LIVE_FEED_URL, PROJECT_ROOT, RUNNING_AVG_WINDOW, SHIFT_HOURS  # noqa: E402

DEFAULT_INPUT = DATA_DIR / "aggregated_daily_data.xlsx"
STATUS_PATH = DATA_DIR / "cietrade_status.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "beta" / "index.html"
SHIFTS = ("1st", "2nd", "3rd")
# Fixed categorical slot per machine (validated order, see design/DESIGN.md). Colour follows the entity.
MACHINE_SLOT = {"EXTRUDER": 1, "AUTO TIE BALER": 2, "GRINDER": 3, "GUILLOTINE": 4, "SHREDDER": 5, "GREEN MAX DENSIFIER (NEW)": 6}
SLOT_HEX = {1: "#2a78d6", 2: "#eb6834", 3: "#1baf7a", 4: "#eda100", 5: "#e87ba4", 6: "#008300", 7: "#4a3aa7"}
PRETTY = {"AUTO TIE BALER": "Auto Tie Baler", "GREEN MAX DENSIFIER (NEW)": "Green Max Densifier", "AVANGUARD DENSIFIER (OLD)": "Avanguard Densifier",
          "SMALL GRINDER": "Small Grinder", "BALER 1": "Baler 1", "BALER 2": "Baler 2"}

e = _h.escape


def pretty(m: str) -> str:
    return PRETTY.get(m, m.title())


def slot_color(m: str) -> str:
    return SLOT_HEX[MACHINE_SLOT.get(m, 7)]


def n(v) -> str:
    return "0" if v in (None, "") else f"{int(round(float(v))):,}"


def compact(v: float) -> str:
    v = float(v)
    return f"{v/1_000_000:.2f}M" if abs(v) >= 1_000_000 else (f"{v/1000:.1f}K" if abs(v) >= 10_000 else f"{int(round(v)):,}")


# ---------------------------------------------------------------- data

def load(agg_path: Path = DEFAULT_INPUT, status_path: Path = STATUS_PATH) -> tuple[pd.DataFrame, dict]:
    df = pd.read_excel(agg_path)
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    out = pd.to_numeric(df["Actual_Output"], errors="coerce").fillna(0.0)
    inp = pd.to_numeric(df["Actual_Input"], errors="coerce").fillna(0.0) if "Actual_Input" in df.columns else 0.0
    guillotine = df["Machine_Name"].astype(str).str.contains("GUILLOTINE", case=False, na=False)
    df["lbs"] = out.where(~(guillotine & (out == 0)), inp)          # the support basis: Guillotine's rolls count when it books no output
    df["Shift"] = df["Shift"].astype(str)
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    return df, status


def model(df: pd.DataFrame, status: dict, today: date | None = None) -> dict:
    today = today or date.today()
    have = df.loc[df["Date"] <= pd.Timestamp(today), "Date"]
    last = have.max() if len(have) else pd.Timestamp(today)
    yesterday_like = df.loc[df["Date"] <= pd.Timestamp(today - timedelta(days=1)), "Date"].max()
    day = yesterday_like if pd.notna(yesterday_like) else last                     # the last complete production day
    monday = day - pd.Timedelta(days=day.weekday())
    wdays = [monday + pd.Timedelta(days=i) for i in range(5)]
    # yesterday vs the 4-week same-weekday average
    d_rows = df[df["Date"] == day]
    prev = df[(df["Date"] < day) & (df["Date"].dt.weekday == day.weekday()) & (df["Date"] >= day - pd.Timedelta(weeks=5))]
    prev_days = sorted(prev["Date"].unique())[-4:]
    avg4 = float(prev[prev["Date"].isin(prev_days)]["lbs"].sum() / len(prev_days)) if prev_days else 0.0
    # week to date vs last week at the same point
    wk = df[(df["Date"] >= monday) & (df["Date"] <= day)]
    lw = df[(df["Date"] >= monday - pd.Timedelta(weeks=1)) & (df["Date"] <= day - pd.Timedelta(weeks=1))]
    # week table: machine x day, per shift and all
    def table(sub: pd.DataFrame) -> dict:
        piv = sub.pivot_table(index="Machine_Name", columns="Date", values="lbs", aggfunc="sum").reindex(columns=wdays).fillna(0.0)
        piv = piv[piv.sum(axis=1) > 0]
        order = piv.sum(axis=1).sort_values(ascending=False).index
        rows = [{"m": m, "days": [round(float(v)) for v in piv.loc[m]], "total": round(float(piv.loc[m].sum()))} for m in order]
        totals = [round(float(v)) for v in piv.sum(axis=0)] if len(piv) else [0] * 5
        return {"rows": rows, "totals": totals, "total": sum(totals)}
    week = {"all": table(df[(df["Date"] >= monday) & (df["Date"] < monday + pd.Timedelta(days=7))])}
    for sh in SHIFTS:
        week[sh] = table(df[(df["Date"] >= monday) & (df["Date"] < monday + pd.Timedelta(days=7)) & (df["Shift"] == sh)])
    # day totals for the last 4 weeks, for the 4-week average rule on the week chart
    hist = df[(df["Date"] >= monday - pd.Timedelta(weeks=4)) & (df["Date"] < monday) & (df["Date"].dt.weekday < 5)]
    day_avg = float(hist.groupby("Date")["lbs"].sum().mean()) if len(hist) else 0.0
    # weekly per machine, 4-wk average, last DEFAULT_WEEKS weeks
    w = df[df["Date"].dt.weekday < 7].copy()
    w["wk"] = w["Date"] - pd.to_timedelta(w["Date"].dt.weekday, unit="D")
    weekly = w.pivot_table(index="wk", columns="Machine_Name", values="lbs", aggfunc="sum").fillna(0.0).sort_index()
    weekly = weekly[weekly.index < monday]                                        # complete weeks only
    machines = [m for m in weekly.sum().sort_values(ascending=False).index if weekly[m].tail(DEFAULT_WEEKS).sum() > 0]
    facets = []
    for m in machines:
        s = weekly[m]
        ra = s.rolling(RUNNING_AVG_WINDOW, min_periods=1).mean()
        tail = s.tail(DEFAULT_WEEKS)
        facets.append({"m": m, "weeks": [x.strftime("%Y-%m-%d") for x in tail.index], "raw": [round(float(v)) for v in tail],
                       "avg": [round(float(v)) for v in ra.tail(DEFAULT_WEEKS)],
                       "last": round(float(ra.iloc[-1])) if len(ra) else 0,
                       "prev4": round(float(ra.iloc[-5])) if len(ra) > 4 else None})
    # End of Shift
    eos = status.get("end_of_shift") or {}
    reports = sorted(eos.get("reports", []), key=lambda r: (r["date"], r["shift"]), reverse=True)
    report_days = sorted({r["date"] for r in reports}, reverse=True)[:14]
    filed_yday = {r["shift"]: r for r in reports if r.get("date") == day.strftime("%Y-%m-%d")}
    # today's machines (for the Today view before the live feed loads)
    t_rows = df[df["Date"] == last] if last > day else d_rows
    tiles = [{"m": m, "lbs": round(float(v))} for m, v in t_rows.groupby("Machine_Name")["lbs"].sum().sort_values(ascending=False).items() if v > 0]
    return {
        "today": today.isoformat(), "day": day.strftime("%Y-%m-%d"), "day_label": day.strftime("%A, %b %-d"),
        "data_through": last.strftime("%Y-%m-%d"), "last_poll": status.get("last_poll"), "api_down_since": status.get("api_down_since"),
        "yesterday": {"lbs": round(float(d_rows["lbs"].sum())), "avg4": round(avg4),
                      "by_shift": {sh: round(float(d_rows[d_rows["Shift"] == sh]["lbs"].sum())) for sh in SHIFTS}},
        "wtd": {"lbs": round(float(wk["lbs"].sum())), "last_week": round(float(lw["lbs"].sum())), "days": (day - monday).days + 1},
        "open_jobs": status.get("open_jobs", 0), "open_lbs": status.get("open_lbs", 0),
        "week": {"monday": monday.strftime("%Y-%m-%d"), "days": [x.strftime("%a %-d") for x in wdays], "dates": [x.strftime("%Y-%m-%d") for x in wdays],
                 "day_avg": round(day_avg), "tables": week},
        "facets": facets, "reports": reports, "report_days": report_days,
        "filed": {sh: (filed_yday[sh]["by"] if sh in filed_yday else None) for sh in SHIFTS},
        "tiles": tiles, "tiles_day": (last if last > day else day).strftime("%a %b %-d"),
    }


# ---------------------------------------------------------------- svg

def svg_columns(labels: list[str], values: list[int], avg: int, width: int = 560, height: int = 150) -> str:
    """Plant pounds per day: <= 24px columns, 4px rounded caps, values on the caps, the 4-wk average as a rule."""
    top, bottom, left, right = 22, 24, 8, 8
    ph = height - top - bottom
    vmax = max(values + [avg, 1]) * 1.12
    slot = (width - left - right) / max(len(values), 1)
    bw = min(24, slot * 0.5)
    y = lambda v: top + ph - ph * v / vmax
    parts = [f'<svg class="cols" viewBox="0 0 {width} {height}" role="img" aria-label="Pounds per day this week">']
    for i, (lab, v) in enumerate(zip(labels, values)):
        cx = left + slot * i + slot / 2
        x0 = cx - bw / 2
        if v > 0:
            yt = y(v); h = y(0) - yt; r = min(4, h / 2)
            parts.append(f'<path d="M{x0:.1f},{y(0):.1f} V{yt + r:.1f} a{r},{r} 0 0 1 {r},-{r} h{bw - 2 * r:.1f} a{r},{r} 0 0 1 {r},{r} V{y(0):.1f} Z" class="bar"/>')
            parts.append(f'<text x="{cx:.1f}" y="{yt - 5:.1f}" class="val" text-anchor="middle">{compact(v)}</text>')
        else:
            parts.append(f'<text x="{cx:.1f}" y="{y(0) - 6:.1f}" class="muted" text-anchor="middle">–</text>')
        parts.append(f'<text x="{cx:.1f}" y="{height - 8}" class="tick" text-anchor="middle">{e(lab)}</text>')
    parts.append(f'<line x1="{left}" x2="{width - right}" y1="{y(0):.1f}" y2="{y(0):.1f}" class="axis"/>')
    if avg > 0:
        parts.append(f'<line x1="{left}" x2="{width - right}" y1="{y(avg):.1f}" y2="{y(avg):.1f}" class="rule"/>')
        parts.append(f'<text x="{width - right}" y="{y(avg) - 4:.1f}" class="rule-lab" text-anchor="end">4-wk avg day {compact(avg)}</text>')
    parts.append("</svg>")
    return "".join(parts)


def svg_facet(f: dict, width: int = 320, height: int = 128) -> str:
    """One machine: raw weeks as a faint step behind, the 4-wk average as the line, latest point hollow and labelled."""
    top, bottom, left, right = 10, 20, 8, 58
    pw, ph = width - left - right, height - top - bottom
    vals = f["raw"] + f["avg"]
    vmax = max(vals + [1]) * 1.08
    k = len(f["avg"])
    x = lambda i: left + (pw * i / max(k - 1, 1))
    y = lambda v: top + ph - ph * v / vmax
    grid = [vmax * 0.5, vmax * 0.9]
    ye = y(f["avg"][-1]) if k else None
    parts = [f'<svg class="facet" viewBox="0 0 {width} {height}" data-weeks="{e(json.dumps(f["weeks"]))}" data-raw="{e(json.dumps(f["raw"]))}" data-avg="{e(json.dumps(f["avg"]))}" role="img" aria-label="{e(pretty(f["m"]))}, weekly pounds, 4-week average">']
    for g in grid:
        parts.append(f'<line x1="{left}" x2="{left + pw}" y1="{y(g):.1f}" y2="{y(g):.1f}" class="grid"/>')
        if ye is None or abs(y(g) - ye) > 12:          # the end label wins the slot; the gridline stays
            parts.append(f'<text x="{left + pw + 4}" y="{y(g) + 3:.1f}" class="tick">{compact(g)}</text>')
    parts.append(f'<line x1="{left}" x2="{left + pw}" y1="{y(0):.1f}" y2="{y(0):.1f}" class="axis"/>')
    raw = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(f["raw"]))
    parts.append(f'<polyline points="{raw}" class="raw"/>')
    avg = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(f["avg"]))
    parts.append(f'<polyline points="{avg}" class="avg"/>')
    if k:
        xe = x(k - 1)
        parts.append(f'<circle cx="{xe:.1f}" cy="{ye:.1f}" r="4.5" class="end"/>')
        parts.append(f'<text x="{left + pw + 4}" y="{ye + 4:.1f}" class="endlab">{compact(f["avg"][-1])}</text>')
        first, lastw = datetime.strptime(f["weeks"][0], "%Y-%m-%d"), datetime.strptime(f["weeks"][-1], "%Y-%m-%d")
        parts.append(f'<text x="{left}" y="{height - 6}" class="tick">{first.strftime("%b %-d")}</text>')
        parts.append(f'<text x="{left + pw}" y="{height - 6}" class="tick" text-anchor="end">{lastw.strftime("%b %-d")}</text>')
    parts.append('<line class="xh" x1="0" x2="0" y1="0" y2="0" style="display:none"/><circle class="hover-dot" r="4" style="display:none"/>')
    parts.append("</svg>")
    return "".join(parts)


def svg_spark(vals: list[int], width: int = 96, height: int = 28) -> str:
    if not vals:
        return ""
    vmax = max(vals + [1]); k = len(vals)
    pts = " ".join(f"{(width - 4) * i / max(k - 1, 1) + 2:.1f},{height - 3 - (height - 6) * v / vmax:.1f}" for i, v in enumerate(vals))
    return f'<svg class="spark" viewBox="0 0 {width} {height}" aria-hidden="true"><polyline points="{pts}"/></svg>'


# ---------------------------------------------------------------- views

def stat(label: str, value: str, sub: str = "", tone: str = "", spark: str = "") -> str:
    return (f'<div class="stat"><div class="stat-label">{label}</div><div class="stat-value">{value}</div>'
            + (f'<div class="stat-sub {tone}">{sub}</div>' if sub else "") + spark + "</div>")


def delta(cur: float, base: float, what: str) -> tuple[str, str]:
    if not base:
        return f"no {what} to compare", ""
    pct = 100 * (cur - base) / base
    return f'{pct:+.0f}% vs {what} ({compact(base)})', "up" if pct >= 0 else "down"


def view_today(M: dict) -> str:
    y = M["yesterday"]
    ysub, ytone = delta(y["lbs"], y["avg4"], f'a normal {M["day_label"].split(",")[0]}')
    wsub, wtone = delta(M["wtd"]["lbs"], M["wtd"]["last_week"], "last week here")
    filed = sum(1 for v in M["filed"].values() if v)
    fsub = " · ".join(f'{sh} {e(v)}' if v else f'{sh} <span class="warn">missing</span>' for sh, v in M["filed"].items())
    tiles = "".join(f'<div class="tile" data-m="{e(t["m"])}"><span class="sw" style="background:{slot_color(t["m"])}"></span><span class="tile-name">{e(pretty(t["m"]))}</span><span class="tile-val">{n(t["lbs"])}</span></div>' for t in M["tiles"])
    return f'''
<section class="view" data-view="today">
  <div class="hero" id="hero">
    <div class="hero-label" id="heroLabel">Pounds today so far</div>
    <div class="hero-value" id="heroValue">–</div>
    <div class="hero-sub" id="heroSub">waiting for the live feed…</div>
  </div>
  <div class="stats">
    {stat(f'Yesterday · {e(M["day_label"])}', n(y["lbs"]), ysub, ytone, svg_spark([y["by_shift"][s] for s in SHIFTS]))}
    {stat(f'Week to date · {M["wtd"]["days"]} day{"s" if M["wtd"]["days"] != 1 else ""}', n(M["wtd"]["lbs"]), wsub, wtone)}
    {stat("Open jobs in cieTrade", n(M["open_jobs"]), f'{n(M["open_lbs"])} lbs not yet posted')}
    {stat(f'End of Shift filed · {e(M["day_label"].split(",")[0])}', f"{filed}<span class='of'>/3</span>", fsub, "" if filed == 3 else "warn")}
  </div>
  <div class="card" id="liveCard">
    <div class="card-head"><h2>Live from cieTrade</h2><div class="card-meta" id="liveMeta">connecting…</div></div>
    <div class="tiles" id="liveTiles">{tiles}</div>
    <div class="card-foot" id="tilesFoot">Pounds by machine on {e(M["tiles_day"])} from the last rebuild; the live feed replaces this within a minute.</div>
    <ol class="feed" id="liveFeed"></ol>
  </div>
</section>'''


def view_week(M: dict) -> str:
    W = M["week"]
    def tbl(t: dict, key: str) -> str:
        if not t["rows"]:
            return f'<div class="wk-table" data-shift="{key}" hidden><p class="muted pad">Nothing booked to this shift yet this week.</p></div>'
        rows = "".join(f'<tr><td class="mname"><span class="sw" style="background:{slot_color(r["m"])}"></span>{e(pretty(r["m"]))}</td>'
                       + "".join(f'<td class="num">{n(v) if v else "<span class=muted>–</span>"}</td>' for v in r["days"])
                       + f'<td class="num total">{n(r["total"])}</td></tr>' for r in t["rows"])
        tot = "".join(f'<td class="num">{n(v) if v else ""}</td>' for v in t["totals"])
        return (f'<div class="wk-table" data-shift="{key}"{"" if key == "all" else " hidden"}><div class="table-wrap"><table><thead><tr><th>Machine</th>'
                + "".join(f'<th class="num">{e(d)}</th>' for d in W["days"]) + '<th class="num">Week</th></tr></thead>'
                f'<tbody>{rows}</tbody><tfoot><tr><td>Plant</td>{tot}<td class="num total">{n(t["total"])}</td></tr></tfoot></table></div></div>')
    seg = "".join(f'<button class="seg-btn{" active" if k == "all" else ""}" data-shift="{k}">{lab}</button>' for k, lab in [("all", "All shifts"), ("1st", "1st"), ("2nd", "2nd"), ("3rd", "3rd")])
    return f'''
<section class="view" data-view="week" hidden>
  <div class="controls"><div class="seg" role="group" aria-label="Shift">{seg}</div><div class="controls-note">Week of {e(datetime.strptime(W["monday"], "%Y-%m-%d").strftime("%b %-d"))} · pounds by machine and day</div></div>
  <div class="card">
    <div class="card-head"><h2>The plant is at {compact(W["tables"]["all"]["total"])} lbs for the week</h2><div class="card-meta">Day totals, all shifts; the rule is the 4-week average working day</div></div>
    {svg_columns(W["days"], W["tables"]["all"]["totals"], W["day_avg"])}
  </div>
  <div class="card">
    <div class="card-head"><h2>By machine and day</h2><div class="card-meta">Guillotine's rolls count when it books no output. – means no job that day.</div></div>
    {tbl(W["tables"]["all"], "all")}{"".join(tbl(W["tables"][sh], sh) for sh in SHIFTS)}
  </div>
</section>'''


def view_shifts(M: dict) -> str:
    days = M["report_days"] or [M["day"]]
    opts = "".join(f'<option value="{d}"{" selected" if i == 0 else ""}>{datetime.strptime(d, "%Y-%m-%d").strftime("%a %b %-d")}</option>' for i, d in enumerate(days))
    by_day: dict[str, dict] = {}
    for r in M["reports"]:
        by_day.setdefault(r["date"], {})[r["shift"]] = r
    blocks = []
    for i, d in enumerate(days):
        cards = []
        for sh in SHIFTS:
            r = by_day.get(d, {}).get(sh)
            if not r:
                cards.append(f'<div class="card eos missing"><div class="card-head"><h2>{sh} shift</h2><div class="card-meta warn">No End of Shift report filed</div></div></div>')
                continue
            rows = "".join(f'<tr><td class="mname">{e(m["machine"])}</td><td class="num">{m["machine_hours"] or ""}</td><td class="num">{m["man_hours"] or ""}</td>'
                           f'<td>{e(m.get("operators", "") or "")}</td><td>{e(m.get("material", "") or "")}</td><td class="num">{m.get("downtime_min") or ""}</td>'
                           f'<td>{e(m.get("reason", "") or "")}</td><td class="c">{e(m.get("comment", "") or "")}</td></tr>' for m in r["machines"])
            mh = sum((m.get("machine_hours") or 0) for m in r["machines"]); man = sum((m.get("man_hours") or 0) for m in r["machines"])
            dt = sum((m.get("downtime_min") or 0) for m in r["machines"])
            notes = "".join(f"<li>{e(x)}</li>" for x in r.get("notes", []))
            cards.append(f'''<div class="card eos"><div class="card-head"><h2>{sh} shift</h2><div class="card-meta">filed by {e(r.get("by", ""))}{(" · " + e(str(r["filed_at"]))) if r.get("filed_at") else ""} · {len(r["machines"])} machines · {mh:g} machine h · {man:g} man h{f' · <span class="warn">{dt} min down</span>' if dt else ""}</div></div>
<div class="table-wrap"><table class="sheet"><thead><tr><th>Machine</th><th class="num">Mach h</th><th class="num">Man h</th><th>Operators</th><th>Material</th><th class="num">Down</th><th>Reason</th><th>Comments</th></tr></thead><tbody>{rows}</tbody></table></div>
{f'<div class="notes"><b>Shift notes</b><ul>{notes}</ul></div>' if notes else ""}</div>''')
        blocks.append(f'<div class="eos-day" data-day="{d}"{"" if i == 0 else " hidden"}>{"".join(cards)}</div>')
    return f'''
<section class="view" data-view="shifts" hidden>
  <div class="controls"><label class="sel-wrap">Day <select class="sel" id="eosDay">{opts}</select></label><div class="controls-note">End of Shift reports as the supervisors filed them · <a href="../shift/">open the form</a></div></div>
  {"".join(blocks)}
</section>'''


def view_machines(M: dict) -> str:
    facets = []
    for f in M["facets"]:
        sub, tone = delta(f["last"], f["prev4"] or 0, "4 weeks ago") if f["prev4"] else ("", "")
        facets.append(f'<div class="facet-card"><div class="facet-head"><span class="sw" style="background:{slot_color(f["m"])}"></span><b>{e(pretty(f["m"]))}</b><span class="facet-val">{compact(f["last"])}<small> lbs/wk</small></span></div>'
                      f'{svg_facet(f)}<div class="facet-sub {tone}">{sub}</div></div>')
    return f'''
<section class="view" data-view="machines" hidden>
  <div class="controls"><div class="controls-note">Weekly pounds per machine, last {DEFAULT_WEEKS} complete weeks. The line is the {RUNNING_AVG_WINDOW}-week average; the faint trace is each week as booked. Hover or tap a chart for values.</div></div>
  <div class="facets">{"".join(facets)}</div>
  <div class="tip" id="tip" role="status" aria-live="polite"></div>
</section>'''


def view_more(M: dict) -> str:
    return f'''
<section class="view" data-view="more" hidden>
  <div class="card"><div class="card-head"><h2>More</h2></div>
  <ul class="links">
    <li><a href="../">Current production dashboard</a><span>every chart and table, the version this beta will replace</span></li>
    <li><a href="../daily.html">Daily details</a><span>day-by-day rows, notes and validation</span></li>
    <li><a href="../shift/">End of Shift form</a><span>for supervisors, works on a phone</span></li>
  </ul>
  <div class="card-foot">Data through {e(M["data_through"])} · dashboard rebuilt {e(str(M["last_poll"] or "")[:16].replace("T", " "))} · cieTrade polled every 10 minutes · beta build.</div>
  </div>
</section>'''


# ---------------------------------------------------------------- page

CSS = r"""
:root { color-scheme: light;
  --page:#faf9f6; --card:#ffffff; --ink:#111827; --ink-2:#4b5563; --muted:#6b7280; --line:rgba(17,24,39,.10); --grid:#e7e5e0;
  --brand:#0b6e4f; --brand-ink:#0b6e4f; --brand-soft:#e7f3ee; --good:#0ca30c; --good-ink:#006300; --warn:#b45309; --bad:#d03b3b;
  --raw:#c9c6bf; --nav-w:240px; }
:root[data-theme="dark"] { color-scheme: dark; --page:#111210; --card:#1a1b18; --ink:#f3f4f1; --ink-2:#c3c2b7; --muted:#9a9890; --line:rgba(255,255,255,.10); --grid:#2c2c2a;
  --brand:#2d9a72; --brand-ink:#5cc79b; --brand-soft:#173327; --good-ink:#4fc36a; --warn:#f2b45a; --raw:#3a3a36; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) { color-scheme: dark; --page:#111210; --card:#1a1b18; --ink:#f3f4f1; --ink-2:#c3c2b7; --muted:#9a9890; --line:rgba(255,255,255,.10); --grid:#2c2c2a;
  --brand:#2d9a72; --brand-ink:#5cc79b; --brand-soft:#173327; --good-ink:#4fc36a; --warn:#f2b45a; --raw:#3a3a36; } }
* { box-sizing:border-box; }
html, body { margin:0; background:var(--page); color:var(--ink); font:15px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }
a { color:var(--brand-ink); }
.app { display:grid; grid-template-columns:var(--nav-w) minmax(0,1fr); min-height:100vh; }
.app.collapsed { --nav-w:64px; }
/* sidebar */
.side { position:sticky; top:0; height:100vh; border-right:1px solid var(--line); padding:18px 12px; display:flex; flex-direction:column; gap:4px; background:var(--card); }
.brand { display:flex; align-items:center; gap:10px; padding:6px 8px 16px; }
.brand .mark { width:28px; height:28px; border-radius:7px; background:var(--brand); flex:none; }
.brand b { font-size:14px; letter-spacing:.01em; white-space:nowrap; } .brand small { display:block; color:var(--muted); font-size:11px; white-space:nowrap; }
.nav a { display:flex; align-items:center; gap:10px; padding:9px 10px; border-radius:8px; color:var(--ink-2); text-decoration:none; font-weight:500; white-space:nowrap; }
.nav a:hover { background:var(--brand-soft); color:var(--ink); } .nav a.active { background:var(--brand-soft); color:var(--brand-ink); font-weight:600; }
.nav svg { width:18px; height:18px; flex:none; stroke:currentColor; fill:none; stroke-width:1.8; stroke-linecap:round; stroke-linejoin:round; }
.side .spacer { flex:1; }
.side-foot { font-size:12px; color:var(--muted); padding:8px 10px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.icon-btn { font:inherit; font-size:12px; color:var(--muted); background:transparent; border:1px solid var(--line); border-radius:8px; padding:6px 10px; cursor:pointer; text-align:left; white-space:nowrap; }
.icon-btn:hover { color:var(--ink); background:var(--brand-soft); }
.app.collapsed .brand b, .app.collapsed .brand small, .app.collapsed .nav a span, .app.collapsed .side-foot, .app.collapsed .icon-btn span { display:none; }
.app.collapsed .nav a { justify-content:center; padding:10px; } .app.collapsed .icon-btn { text-align:center; }
/* main */
.main { padding:22px 28px 40px; max-width:1120px; }
.topbar { display:flex; align-items:baseline; justify-content:space-between; gap:12px; margin-bottom:16px; }
.topbar h1 { margin:0; font-size:22px; font-weight:650; letter-spacing:-.01em; } .topbar .date { color:var(--muted); font-size:13px; }
.hero { padding:6px 0 14px; } .hero-label { font-size:13px; color:var(--muted); } .hero-value { font-size:44px; font-weight:700; letter-spacing:-.02em; line-height:1.1; }
.hero-sub { font-size:13px; color:var(--ink-2); margin-top:2px; }
.stats { display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:12px; margin-bottom:16px; }
.stat { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px 16px 12px; position:relative; min-height:96px; }
.stat-label { font-size:12px; color:var(--muted); margin-bottom:4px; padding-right:84px; } .stat-value { font-size:26px; font-weight:650; letter-spacing:-.01em; } .stat-value .of { font-size:16px; color:var(--muted); font-weight:500; }
.stat-sub { font-size:12px; color:var(--ink-2); margin-top:3px; } .stat-sub.up { color:var(--good-ink); } .stat-sub.down { color:var(--bad); } .stat-sub.warn, .warn { color:var(--warn); }
.spark { position:absolute; right:12px; top:14px; width:72px; height:22px; } .spark polyline { fill:none; stroke:var(--raw); stroke-width:1.5; }
.card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px 18px; margin-bottom:14px; }
.card-head { display:flex; flex-wrap:wrap; align-items:baseline; justify-content:space-between; gap:4px 14px; margin-bottom:8px; }
.card h2 { margin:0; font-size:16px; font-weight:650; } .card-meta { font-size:12px; color:var(--muted); } .card-foot { font-size:12px; color:var(--muted); margin-top:8px; }
.tiles { display:grid; grid-template-columns:repeat(auto-fill, minmax(160px,1fr)); gap:8px; }
.tile { display:grid; grid-template-columns:10px 1fr auto; gap:8px; align-items:center; padding:8px 10px; border:1px solid var(--line); border-radius:8px; font-size:13px; }
.tile-val { font-weight:650; font-variant-numeric:tabular-nums; } .tile.quiet .tile-val { color:var(--muted); } .tile .tf { grid-column:2/4; font-size:11px; color:var(--warn); }
.sw { width:10px; height:10px; border-radius:3px; display:inline-block; vertical-align:-1px; margin-right:6px; flex:none; }
.feed { list-style:none; margin:10px 0 0; padding:0; font-size:13px; } .feed li { display:grid; grid-template-columns:52px 1fr auto; gap:10px; padding:6px 0; border-top:1px solid var(--line); }
.feed .t { color:var(--muted); font-variant-numeric:tabular-nums; } .feed .v { font-weight:600; font-variant-numeric:tabular-nums; } .feed .v.neg { color:var(--bad); }
.controls { display:flex; flex-wrap:wrap; align-items:center; gap:8px 16px; margin:0 0 12px; } .controls-note { font-size:13px; color:var(--muted); }
.seg { display:inline-flex; gap:4px; } .seg-btn { font:inherit; font-size:13px; font-weight:600; color:var(--ink-2); background:var(--card); border:1px solid var(--line); border-radius:8px; padding:6px 11px; cursor:pointer; }
.seg-btn.active { background:var(--brand); border-color:var(--brand); color:#fff; }
.sel-wrap { font-size:13px; color:var(--muted); display:inline-flex; align-items:center; gap:8px; } .sel { font:inherit; font-size:14px; font-weight:600; color:var(--ink); background:var(--card); border:1px solid var(--line); border-radius:8px; padding:6px 10px; }
.table-wrap { overflow-x:auto; } table { width:100%; border-collapse:collapse; font-size:14px; }
th { font-size:11px; color:var(--muted); font-weight:600; text-align:left; text-transform:uppercase; letter-spacing:.04em; padding:6px 8px; border-bottom:1px solid var(--line); }
td { padding:8px; border-bottom:1px solid var(--line); vertical-align:top; } td.num, th.num { text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }
td.mname { white-space:nowrap; font-weight:600; } td.total { font-weight:650; } tfoot td { font-weight:650; border-top:2px solid var(--line); border-bottom:0; }
.muted { color:var(--muted); } .pad { padding:8px; } table.sheet { font-size:13px; } table.sheet td.c { color:var(--ink-2); }
.notes { font-size:13px; margin-top:8px; } .notes ul { margin:4px 0 0 18px; padding:0; }
.eos.missing { border-style:dashed; }
/* svg charts */
svg.cols { width:100%; height:auto; display:block; } svg.cols .bar { fill:var(--brand); } svg.cols .val { font-size:11px; fill:var(--ink); font-weight:600; }
svg.cols .tick, svg.facet .tick { font-size:11px; fill:var(--muted); } svg.cols .axis, svg.facet .axis { stroke:var(--grid); stroke-width:1; } svg.cols .muted { fill:var(--muted); font-size:12px; }
svg.cols .rule { stroke:var(--ink-2); stroke-width:1; } svg.cols .rule-lab { font-size:11px; fill:var(--ink-2); }
.facets { display:grid; grid-template-columns:repeat(auto-fill, minmax(300px,1fr)); gap:12px; }
.facet-card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px 14px 8px; }
.facet-head { display:flex; align-items:baseline; gap:6px; font-size:14px; margin-bottom:4px; } .facet-val { margin-left:auto; font-weight:650; font-size:15px; } .facet-val small { color:var(--muted); font-weight:400; font-size:11px; }
.facet-sub { font-size:12px; color:var(--ink-2); min-height:16px; } .facet-sub.up { color:var(--good-ink); } .facet-sub.down { color:var(--bad); }
svg.facet { width:100%; height:auto; display:block; touch-action:none; } svg.facet .grid { stroke:var(--grid); stroke-width:1; } svg.facet .raw { fill:none; stroke:var(--raw); stroke-width:1.2; stroke-linejoin:round; }
svg.facet .avg { fill:none; stroke:var(--brand); stroke-width:2; stroke-linejoin:round; stroke-linecap:round; } svg.facet .end { fill:var(--card); stroke:var(--brand); stroke-width:2; }
svg.facet .endlab { font-size:12px; font-weight:650; fill:var(--ink); } svg.facet .xh { stroke:var(--ink-2); stroke-width:1; } svg.facet .hover-dot { fill:var(--brand); stroke:var(--card); stroke-width:2; }
.tip { position:fixed; z-index:20; pointer-events:none; background:var(--card); border:1px solid var(--line); border-radius:8px; box-shadow:0 6px 20px rgba(16,24,32,.12); padding:8px 10px; font-size:12px; display:none; min-width:150px; }
.tip b { font-size:13px; } .tip .r { display:flex; justify-content:space-between; gap:12px; }
.links { list-style:none; margin:0; padding:0; } .links li { padding:10px 0; border-top:1px solid var(--line); display:grid; gap:2px; } .links li:first-child { border-top:0; } .links span { font-size:12px; color:var(--muted); }
.tabbar { display:none; }
.banner { background:#fdf2f2; border:1px solid #f1b8b8; color:#8a1c1c; border-radius:8px; padding:8px 12px; font-size:13px; margin-bottom:12px; }
@media (max-width: 900px) {
  .app { grid-template-columns:1fr; } .side { display:none; }
  .main { padding:14px 16px 84px; }
  .topbar h1 { font-size:18px; }
  .stats { grid-template-columns:repeat(2, minmax(0,1fr)); gap:8px; } .stat { padding:12px 12px 10px; min-height:0; } .stat-value { font-size:22px; } .stat-label { padding-right:0; } .spark { display:none; }
  .hero-value { font-size:38px; }
  .tabbar { display:grid; grid-template-columns:repeat(5,1fr); position:fixed; left:0; right:0; bottom:0; background:var(--card); border-top:1px solid var(--line); padding:6px 4px calc(6px + env(safe-area-inset-bottom)); z-index:10; }
  .tabbar a { display:flex; flex-direction:column; align-items:center; gap:3px; font-size:11px; color:var(--muted); text-decoration:none; padding:4px 0; }
  .tabbar a.active { color:var(--brand-ink); font-weight:600; } .tabbar svg { width:22px; height:22px; stroke:currentColor; fill:none; stroke-width:1.8; stroke-linecap:round; stroke-linejoin:round; }
  .facets { grid-template-columns:1fr; }
  table { font-size:13px; } td, th { padding:7px 6px; }
}
@media (prefers-reduced-motion: reduce) { * { transition:none !important; } }
"""

ICONS = {
    "today": '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>',
    "week": '<svg viewBox="0 0 24 24"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18M8 3v4M16 3v4"/></svg>',
    "shifts": '<svg viewBox="0 0 24 24"><path d="M6 3h9l4 4v14H6z"/><path d="M14 3v5h5M9 13h6M9 17h6"/></svg>',
    "machines": '<svg viewBox="0 0 24 24"><path d="M3 17l5-6 4 4 4-7 5 5"/><path d="M3 21h18"/></svg>',
    "more": '<svg viewBox="0 0 24 24"><circle cx="5" cy="12" r="1.5"/><circle cx="12" cy="12" r="1.5"/><circle cx="19" cy="12" r="1.5"/></svg>',
}
NAV = [("today", "Today"), ("week", "Week"), ("shifts", "Shifts"), ("machines", "Machines"), ("more", "More")]

JS = r"""
(function () {
  const $ = (s, r) => (r || document).querySelector(s), $$ = (s, r) => Array.from((r || document).querySelectorAll(s));
  const fmt = v => Math.round(v).toLocaleString("en-US");
  const parseTs = s => { const [d, t] = s.split("T"); const [Y, M, D] = d.split("-").map(Number); const [h, m, sec] = (t || "0:0:0").split(":").map(Number); return new Date(Y, M - 1, D, h, m, sec || 0); };
  const clock = d => d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
  const titles = { today: "Today", week: "Week at a glance", shifts: "End of Shift reports", machines: "Machines", more: "More" };
  // theme
  const root = document.documentElement;
  try { const t = localStorage.getItem("beta-theme"); if (t) root.setAttribute("data-theme", t); } catch (e) {}
  $$("[data-theme-toggle]").forEach(b => b.addEventListener("click", () => {
    const dark = root.getAttribute("data-theme") === "dark" || (!root.getAttribute("data-theme") && matchMedia("(prefers-color-scheme: dark)").matches);
    root.setAttribute("data-theme", dark ? "light" : "dark"); try { localStorage.setItem("beta-theme", dark ? "light" : "dark"); } catch (e) {}
  }));
  // sidebar collapse
  const app = $(".app");
  try { if (localStorage.getItem("beta-nav") === "collapsed") app.classList.add("collapsed"); } catch (e) {}
  $("#collapse").addEventListener("click", () => { app.classList.toggle("collapsed"); try { localStorage.setItem("beta-nav", app.classList.contains("collapsed") ? "collapsed" : "open"); } catch (e) {} });
  // views
  function show(name) {
    if (!titles[name]) name = "today";
    $$(".view").forEach(v => { v.hidden = v.dataset.view !== name; });
    $$("[data-nav]").forEach(a => a.classList.toggle("active", a.dataset.nav === name));
    $("#pageTitle").textContent = titles[name];
    window.scrollTo({ top: 0 });
  }
  window.addEventListener("hashchange", () => show(location.hash.slice(1)));
  show(location.hash.slice(1));
  // week: shift segments
  $$("#weekSeg .seg-btn").forEach(b => b.addEventListener("click", () => {
    $$("#weekSeg .seg-btn").forEach(x => x.classList.toggle("active", x === b));
    $$(".wk-table").forEach(t => { t.hidden = t.dataset.shift !== b.dataset.shift; });
  }));
  // shifts: day picker
  const eosDay = $("#eosDay");
  if (eosDay) eosDay.addEventListener("change", () => $$(".eos-day").forEach(d => { d.hidden = d.dataset.day !== eosDay.value; }));
  // machines: crosshair + tooltip
  const tip = $("#tip");
  $$("svg.facet").forEach(svg => {
    const weeks = JSON.parse(svg.dataset.weeks), raw = JSON.parse(svg.dataset.raw), avg = JSON.parse(svg.dataset.avg);
    const pts = $$("polyline.avg", svg)[0].getAttribute("points").split(" ").map(p => p.split(",").map(Number));
    const xh = $(".xh", svg), dot = $(".hover-dot", svg);
    const name = svg.closest(".facet-card").querySelector("b").textContent;
    function at(ev) {
      const r = svg.getBoundingClientRect(), vb = svg.viewBox.baseVal;
      const x = (ev.clientX - r.left) * vb.width / r.width;
      let i = 0, best = 1e9; pts.forEach((p, k) => { const d = Math.abs(p[0] - x); if (d < best) { best = d; i = k; } });
      xh.setAttribute("x1", pts[i][0]); xh.setAttribute("x2", pts[i][0]); xh.setAttribute("y1", 4); xh.setAttribute("y2", vb.height - 18); xh.style.display = "";
      dot.setAttribute("cx", pts[i][0]); dot.setAttribute("cy", pts[i][1]); dot.style.display = "";
      const d = new Date(weeks[i] + "T12:00:00").toLocaleDateString("en-US", { month: "short", day: "numeric" });
      tip.replaceChildren();
      const h = document.createElement("b"); h.textContent = name + " · week of " + d; tip.appendChild(h);
      [["4-wk average", avg[i]], ["That week", raw[i]]].forEach(([k, v]) => { const row = document.createElement("div"); row.className = "r"; const a = document.createElement("span"); a.textContent = k; const b = document.createElement("span"); b.textContent = fmt(v) + " lbs"; row.append(a, b); tip.appendChild(row); });
      tip.style.display = "block";
      const tw = tip.offsetWidth, th = tip.offsetHeight;
      tip.style.left = Math.min(ev.clientX + 14, innerWidth - tw - 8) + "px"; tip.style.top = Math.max(8, ev.clientY - th - 14) + "px";
    }
    function off() { xh.style.display = "none"; dot.style.display = "none"; tip.style.display = "none"; }
    svg.addEventListener("pointermove", at); svg.addEventListener("pointerdown", at); svg.addEventListener("pointerleave", off);
  });
  // live feed
  const FEED = __FEED__, colors = __COLORS__, names = __NAMES__;
  function render(L) {
    const gen = parseTs(L.generated), ageMin = Math.max(0, Math.round((Date.now() - gen) / 60000));
    $("#heroValue").textContent = fmt(L.lbs_today);
    $("#heroLabel").textContent = "Pounds today so far" + (L.current_shift ? " · " + L.current_shift + " shift on now" : "");
    let sub = "as of " + clock(gen) + " · " + L.changes_today + " changes · yesterday " + fmt(L.yesterday && L.yesterday.lbs || 0) + " lbs";
    if (!L.poll_ok) sub = "⚠ cieTrade unavailable since " + (L.api_down_since ? clock(parseTs(L.api_down_since)) : "the last poll") + "; figures through " + (L.last_ok_poll ? clock(parseTs(L.last_ok_poll)) : "the last good poll");
    else if (ageMin > 30) sub = "⚠ feed is " + ageMin + " min old; the poller may have stopped";
    $("#heroSub").textContent = sub;
    $("#liveMeta").textContent = "Machines today · " + L.open_jobs + " open jobs, " + fmt(L.open_lbs) + " lbs not yet posted";
    const tiles = $("#liveTiles"); tiles.replaceChildren();
    if (!L.machines.length) { const d = document.createElement("div"); d.className = "tile"; d.textContent = "Nothing entered yet this production day."; tiles.appendChild(d); }
    L.machines.forEach(m => {
      const t = document.createElement("div"); t.className = "tile" + (m.flag ? " quiet" : "");
      const sw = document.createElement("span"); sw.className = "sw"; sw.style.background = colors[m.m] || "#4a3aa7";
      const nm = document.createElement("span"); nm.className = "tile-name"; nm.textContent = names[m.m] || m.m;
      const v = document.createElement("span"); v.className = "tile-val"; v.textContent = fmt(m.lbs);
      t.append(sw, nm, v);
      if (m.flag) { const f = document.createElement("span"); f.className = "tf"; f.textContent = "quiet " + m.quiet_min + " min"; t.appendChild(f); }
      tiles.appendChild(t);
    });
    $("#tilesFoot").textContent = "Pounds entered per machine this production day, by shift where known. Quiet = ran this shift but nothing new for an hour.";
    const feed = $("#liveFeed"); feed.replaceChildren();
    (L.changes || []).slice(0, 12).forEach(c => {
      const li = document.createElement("li");
      const t = document.createElement("span"); t.className = "t"; t.textContent = clock(parseTs(c.t1));
      const w = document.createElement("span"); w.textContent = (names[c.m] || c.m) + (c.s ? " · " + c.s : "") + (c.units ? " · " + (c.units > 0 ? "+" : "") + c.units + " units" : "");
      const v = document.createElement("span"); v.className = "v" + (c.lbs < 0 ? " neg" : ""); v.textContent = (c.lbs > 0 ? "+" : "") + fmt(c.lbs) + " lbs";
      li.append(t, w, v); feed.appendChild(li);
    });
  }
  function refresh() {
    if (!FEED) return;
    fetch(FEED + "?t=" + Date.now(), { cache: "no-store" }).then(r => r.ok ? r.json() : Promise.reject(r.status)).then(render)
      .catch(() => { $("#heroSub").textContent = "live feed unavailable; showing the last rebuild"; $("#liveMeta").textContent = "live feed unavailable"; });
  }
  refresh(); setInterval(refresh, 60000);
})();
"""


def render(M: dict) -> str:
    nav_side = "".join(f'<a href="#{k}" data-nav="{k}">{ICONS[k]}<span>{lab}</span></a>' for k, lab in NAV)
    nav_tab = "".join(f'<a href="#{k}" data-nav="{k}">{ICONS[k]}{lab}</a>' for k, lab in NAV)
    colors = {m: slot_color(m) for m in {*MACHINE_SLOT, *[t["m"] for t in M["tiles"]], *[f["m"] for f in M["facets"]]}}
    names = {m: pretty(m) for m in colors}
    js = JS.replace("__FEED__", json.dumps(LIVE_FEED_URL)).replace("__COLORS__", json.dumps(colors)).replace("__NAMES__", json.dumps(names))
    banner = (f'<div class="banner">cieTrade API unavailable since {e(str(M["api_down_since"])[:16].replace("T", " "))}; figures are through the last good poll.</div>'
              if M.get("api_down_since") else "")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Walton production (beta)</title>
<meta name="description" content="Walton Logistics production dashboard, beta layout">
<style>{CSS}</style>
</head>
<body>
<div class="app">
  <aside class="side">
    <div class="brand"><div class="mark" aria-hidden="true"></div><div><b>Walton production</b><small>Plus Monroe · beta</small></div></div>
    <nav class="nav" aria-label="Views">{nav_side}</nav>
    <div class="spacer"></div>
    <button class="icon-btn" data-theme-toggle type="button">◐ <span>Light / dark</span></button>
    <button class="icon-btn" id="collapse" type="button" aria-label="Collapse sidebar">⇔ <span>Collapse</span></button>
    <div class="side-foot">Data through {e(M["data_through"])}</div>
  </aside>
  <main class="main">
    <div class="topbar"><h1 id="pageTitle">Today</h1><div class="date">{e(datetime.strptime(M["data_through"], "%Y-%m-%d").strftime("%a %b %-d"))} · <a href="../">current site</a></div></div>
    {banner}
    {view_today(M)}
    {view_week(M).replace('<div class="seg" role="group" aria-label="Shift">', '<div class="seg" id="weekSeg" role="group" aria-label="Shift">')}
    {view_shifts(M)}
    {view_machines(M)}
    {view_more(M)}
  </main>
</div>
<nav class="tabbar" aria-label="Views">{nav_tab}</nav>
<script>{js}</script>
</body>
</html>"""


def main(input_path: Path = DEFAULT_INPUT, output_path: Path = DEFAULT_OUTPUT, status_path: Path = STATUS_PATH) -> Path:
    df, status = load(input_path, status_path)
    page = render(model(df, status))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(page)
    print(f"Wrote beta dashboard to {output_path} ({len(page) // 1024} KB)")
    return output_path


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args()
    main(args.input, args.output)
