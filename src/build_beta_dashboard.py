"""Beta production dashboard: docs/beta/index.html.

One page, five views (Today · Week · Shifts · Machines · More), a sidebar on desktop
that becomes a bottom tab bar on a phone. The page carries the last 26 weeks of
shift-day-machine rows (pounds, machine hours, man hours, expense) as JSON and renders
every view in the browser, so the day, week, shift and metric selectors need no
rebuild. Inputs are the same as the current site: data/aggregated_daily_data.xlsx,
data/cietrade_status.json (End of Shift reports, freshness) and the live feed gist
(fetched client-side). Charts are inline SVG in the Financial Times manner: message
titles, hairline horizontal grid, direct labels, the latest point hollow.
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
from config import DATA_DIR, DEFAULT_WEEKS, LIVE_FEED_URL, PROJECT_ROOT, RUNNING_AVG_WINDOW  # noqa: E402

DEFAULT_INPUT = DATA_DIR / "aggregated_daily_data.xlsx"
STATUS_PATH = DATA_DIR / "cietrade_status.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "beta" / "index.html"
SHIFTS = ("1st", "2nd", "3rd", "unspecified")
EMBED_WEEKS = 26                                   # complete weeks carried in the page, plus the current one
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


def load(agg_path: Path = DEFAULT_INPUT, status_path: Path = STATUS_PATH) -> tuple[pd.DataFrame, dict]:
    df = pd.read_excel(agg_path)
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    out = pd.to_numeric(df["Actual_Output"], errors="coerce").fillna(0.0)
    inp = pd.to_numeric(df["Actual_Input"], errors="coerce").fillna(0.0) if "Actual_Input" in df.columns else 0.0
    guillotine = df["Machine_Name"].astype(str).str.contains("GUILLOTINE", case=False, na=False)
    df["lbs"] = out.where(~(guillotine & (out == 0)), inp)          # the support basis: Guillotine's rolls count when it books no output
    df["Shift"] = df["Shift"].astype(str)
    for c in ("Machine_Hours", "Man_Hours", "Total_Expense"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0) if c in df.columns else 0.0
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    return df, status


def dataset(df: pd.DataFrame, status: dict, today: date | None = None, weeks: int = EMBED_WEEKS) -> dict:
    """Everything the page renders from: compact rows for the last `weeks` complete weeks plus the current one."""
    today = today or date.today()
    monday = today - timedelta(days=today.weekday())
    start = pd.Timestamp(monday - timedelta(weeks=weeks))
    d = df[(df["Date"] >= start) & (df["Date"] <= pd.Timestamp(today))].copy()
    d["Shift"] = d["Shift"].where(d["Shift"].isin(SHIFTS[:3]), "unspecified")
    g = d.groupby(["Date", "Shift", "Machine_Name"], as_index=False).agg(lbs=("lbs", "sum"), mh=("Machine_Hours", "sum"), manh=("Man_Hours", "sum"), cost=("Total_Expense", "sum"))
    g = g[(g["lbs"] != 0) | (g["mh"] > 0) | (g["cost"] > 0)]
    totals = g.groupby("Machine_Name")["lbs"].sum()
    machines = sorted(totals.index, key=lambda m: (MACHINE_SLOT.get(m, 7), -totals[m]))
    dates = sorted(x.strftime("%Y-%m-%d") for x in g["Date"].unique())
    di = {s: i for i, s in enumerate(dates)}
    mi = {m: i for i, m in enumerate(machines)}
    rows = [[di[r.Date.strftime("%Y-%m-%d")], SHIFTS.index(r.Shift), mi[r.Machine_Name], round(float(r.lbs)), round(float(r.mh), 2), round(float(r.manh), 2), round(float(r.cost), 2)]
            for r in g.itertuples(index=False)]
    have = df.loc[df["Date"] <= pd.Timestamp(today), "Date"]
    data_through = (have.max() if len(have) else pd.Timestamp(today)).strftime("%Y-%m-%d")
    eos = status.get("end_of_shift") or {}
    return {
        "today": today.isoformat(), "data_through": data_through, "last_poll": status.get("last_poll"), "api_down_since": status.get("api_down_since"),
        "open_jobs": status.get("open_jobs", 0), "open_lbs": status.get("open_lbs", 0), "closures": status.get("closures", []),
        "machines": machines, "names": {m: pretty(m) for m in machines}, "colors": {m: slot_color(m) for m in machines},
        "shifts": list(SHIFTS), "dates": dates, "rows": rows,
        "reports": sorted(eos.get("reports", []), key=lambda r: (r["date"], r["shift"])),
        "avg_window": RUNNING_AVG_WINDOW, "facet_weeks": DEFAULT_WEEKS, "feed": LIVE_FEED_URL,
    }


# ---------------------------------------------------------------- page

CSS = r"""
:root { color-scheme: light;
  --page:#faf9f6; --card:#ffffff; --ink:#111827; --ink-2:#4b5563; --muted:#6b7280; --line:rgba(17,24,39,.10); --grid:#e7e5e0; --track:#f1efe9;
  --brand:#0b6e4f; --brand-ink:#0b6e4f; --brand-soft:#e7f3ee; --good:#0ca30c; --good-ink:#006300; --warn:#b45309; --bad:#d03b3b;
  --raw:#c9c6bf; --nav-w:240px; }
:root[data-theme="dark"] { color-scheme: dark; --page:#111210; --card:#1a1b18; --ink:#f3f4f1; --ink-2:#c3c2b7; --muted:#9a9890; --line:rgba(255,255,255,.10); --grid:#2c2c2a; --track:#24251f;
  --brand:#2d9a72; --brand-ink:#5cc79b; --brand-soft:#173327; --good-ink:#4fc36a; --warn:#f2b45a; --raw:#3a3a36; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) { color-scheme: dark; --page:#111210; --card:#1a1b18; --ink:#f3f4f1; --ink-2:#c3c2b7; --muted:#9a9890; --line:rgba(255,255,255,.10); --grid:#2c2c2a; --track:#24251f;
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
.topbar { display:flex; align-items:baseline; justify-content:space-between; gap:12px; margin-bottom:14px; }
.topbar h1 { margin:0; font-size:22px; font-weight:650; letter-spacing:-.01em; } .topbar .date { color:var(--muted); font-size:13px; }
.controls { display:flex; flex-wrap:wrap; align-items:center; gap:8px 14px; margin:0 0 12px; } .controls-note { font-size:13px; color:var(--muted); flex-basis:100%; }
.seg { display:inline-flex; gap:4px; flex-wrap:wrap; } .seg-btn { font:inherit; font-size:13px; font-weight:600; color:var(--ink-2); background:var(--card); border:1px solid var(--line); border-radius:8px; padding:6px 11px; cursor:pointer; }
.seg-btn.active { background:var(--brand); border-color:var(--brand); color:#fff; }
.pager { display:inline-flex; align-items:center; gap:4px; } .pager button { font:inherit; font-size:14px; color:var(--ink-2); background:var(--card); border:1px solid var(--line); border-radius:8px; width:34px; height:34px; cursor:pointer; }
.pager button:disabled { opacity:.35; cursor:default; }
.sel { font:inherit; font-size:14px; font-weight:600; color:var(--ink); background:var(--card); border:1px solid var(--line); border-radius:8px; padding:6px 10px; height:34px; max-width:60vw; }
.stats { display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:12px; margin-bottom:14px; }
.stat { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px 16px 12px; min-height:96px; }
.stat-label { font-size:12px; color:var(--muted); margin-bottom:4px; } .stat-value { font-size:26px; font-weight:650; letter-spacing:-.01em; } .stat-value .of { font-size:16px; color:var(--muted); font-weight:500; }
.stat-sub { font-size:12px; color:var(--ink-2); margin-top:3px; } .stat-sub.up, .up { color:var(--good-ink); } .stat-sub.down, .down { color:var(--bad); } .stat-sub.warn, .warn { color:var(--warn); }
.card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px 18px; margin-bottom:14px; }
.card-head { display:flex; flex-wrap:wrap; align-items:baseline; justify-content:space-between; gap:4px 14px; margin-bottom:8px; }
.card h2 { margin:0; font-size:16px; font-weight:650; } .card-meta { font-size:12px; color:var(--muted); } .card-foot { font-size:12px; color:var(--muted); margin-top:8px; }
/* headline bars (Today) */
.bars { display:grid; gap:8px; margin-top:6px; }
.bar-row { display:grid; grid-template-columns:10px 150px minmax(0,1fr) 72px; gap:10px; align-items:center; font-size:14px; }
.bar-name { font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.bar-track { position:relative; height:22px; background:var(--track); border-radius:0 4px 4px 0; }
.bar-fill { position:absolute; left:0; top:0; bottom:0; border-radius:0 4px 4px 0; min-width:2px; }
.bar-norm { position:absolute; top:-3px; bottom:-3px; width:2px; background:var(--ink-2); }
.bar-val { text-align:right; font-weight:650; font-variant-numeric:tabular-nums; } .bar-sub { grid-column:2/5; font-size:11px; color:var(--muted); margin-top:-6px; }
.bar-val small { display:block; font-size:11px; font-weight:500; }
.empty { color:var(--muted); font-size:14px; padding:8px 0; }
.sw { width:10px; height:10px; border-radius:3px; display:inline-block; vertical-align:-1px; flex:none; }
.legend { display:flex; flex-wrap:wrap; gap:4px 14px; font-size:12px; color:var(--ink-2); margin:2px 0 8px; } .legend span { display:inline-flex; align-items:center; gap:6px; }
.feed { list-style:none; margin:10px 0 0; padding:0; font-size:13px; } .feed li { display:grid; grid-template-columns:52px 1fr auto; gap:10px; padding:6px 0; border-top:1px solid var(--line); }
.feed .t { color:var(--muted); font-variant-numeric:tabular-nums; } .feed .v { font-weight:600; font-variant-numeric:tabular-nums; } .feed .v.neg { color:var(--bad); }
.table-wrap { overflow-x:auto; } table { width:100%; border-collapse:collapse; font-size:14px; }
th { font-size:11px; color:var(--muted); font-weight:600; text-align:left; text-transform:uppercase; letter-spacing:.04em; padding:6px 8px; border-bottom:1px solid var(--line); }
td { padding:8px; border-bottom:1px solid var(--line); vertical-align:top; } td.num, th.num { text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }
td.mname { white-space:nowrap; font-weight:600; } td.mname .sw { margin-right:6px; } td.total { font-weight:650; } tfoot td { font-weight:650; border-top:2px solid var(--line); border-bottom:0; }
.muted { color:var(--muted); } table.sheet { font-size:13px; } table.sheet td.c { color:var(--ink-2); }
.notes { font-size:13px; margin-top:8px; } .notes ul { margin:4px 0 0 18px; padding:0; }
.day-head { font-size:14px; font-weight:650; margin:18px 0 8px; color:var(--ink-2); } .day-head:first-child { margin-top:0; }
.eos.missing { border-style:dashed; }
/* svg charts */
svg.stack { width:100%; height:auto; display:block; } #weekChart { min-height:220px; } svg.stack .seg { stroke:none; } svg.stack .val { font-size:11px; fill:var(--ink); font-weight:600; }
svg.stack .tick, svg.facet .tick { font-size:11px; fill:var(--muted); } svg.stack .axis, svg.facet .axis { stroke:var(--grid); stroke-width:1; }
svg.stack .rule { stroke:var(--ink-2); stroke-width:1; } svg.stack .rule-lab { font-size:11px; fill:var(--ink-2); } svg.stack .muted { fill:var(--muted); font-size:12px; }
.facets { display:grid; grid-template-columns:repeat(auto-fill, minmax(300px,1fr)); gap:12px; }
.facet-card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px 14px 8px; }
.facet-head { display:flex; align-items:baseline; gap:6px; font-size:14px; margin-bottom:4px; } .facet-val { margin-left:auto; font-weight:650; font-size:15px; white-space:nowrap; } .facet-val small { color:var(--muted); font-weight:400; font-size:11px; }
.facet-sub { font-size:12px; color:var(--ink-2); min-height:16px; }
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
  .stats { grid-template-columns:repeat(2, minmax(0,1fr)); gap:8px; } .stat { padding:12px 12px 10px; min-height:0; } .stat-value { font-size:22px; }
  .bar-row { grid-template-columns:10px 96px minmax(0,1fr) 62px; gap:8px; font-size:13px; }
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
  const D = JSON.parse(document.getElementById("data").textContent);
  const $ = (s, r) => (r || document).querySelector(s), $$ = (s, r) => Array.from((r || document).querySelectorAll(s));
  const h = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const fmt = v => Math.round(v).toLocaleString("en-US");
  const compact = v => Math.abs(v) >= 1e6 ? (v / 1e6).toFixed(2) + "M" : Math.abs(v) >= 1e4 ? (v / 1e3).toFixed(1) + "K" : fmt(v);
  const money = (v, dp) => "$" + v.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp });
  const P = s => new Date(s + "T12:00:00");
  const iso = d => d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0");
  const addDays = (s, n) => { const d = P(s); d.setDate(d.getDate() + n); return iso(d); };
  const mondayOf = s => { const d = P(s); d.setDate(d.getDate() - (d.getDay() + 6) % 7); return iso(d); };
  const dow = s => (P(s).getDay() + 6) % 7;
  const fmtDay = (s, o) => P(s).toLocaleDateString("en-US", o || { weekday: "long", month: "short", day: "numeric" });
  const fmtShort = s => P(s).toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
  const parseTs = s => { const [d, t] = s.split("T"); const [Y, M, Dd] = d.split("-").map(Number); const [hh, mm, ss] = (t || "0:0:0").split(":").map(Number); return new Date(Y, M - 1, Dd, hh, mm, ss || 0); };
  const clock = d => d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
  const pct = (cur, base) => base ? Math.round((cur - base) / base * 100) : null;
  const deltaHtml = (cur, base, what) => { const p = pct(cur, base); if (p === null) return ["", ""]; return [(p >= 0 ? "+" : "") + p + "% vs " + what + " (" + compact(base) + ")", p >= 0 ? "up" : "down"]; };
  const wdName = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  const closures = new Set(D.closures || []);
  const DI = D.dates.map(s => ({ s, wk: mondayOf(s), dow: dow(s) }));
  const dateIdx = new Map(D.dates.map((s, i) => [s, i]));
  let live = null;

  // ---- aggregation over the embedded rows: [dateIdx, shiftIdx, machineIdx, lbs, mh, manh, cost]
  const shiftOk = (r, sh) => sh === "all" || D.shifts[r[1]] === sh;
  function acc() { return { lbs: 0, mh: 0, lbs_h: 0, cost: 0, mh_c: 0, cost_c: 0, lbs_c: 0, manh: 0 }; }
  function add(a, r) { a.lbs += r[3]; a.manh += r[5]; if (r[4] > 0) { a.mh += r[4]; a.lbs_h += r[3]; if (r[6] > 0) { a.mh_c += r[4]; a.cost_c += r[6]; } } if (r[6] > 0) { a.cost += r[6]; a.lbs_c += r[3]; } }
  function aggBy(pred, keyFn) { const out = new Map(); for (const r of D.rows) { if (!pred(r)) continue; const k = keyFn(r); let a = out.get(k); if (!a) { a = acc(); out.set(k, a); } add(a, r); } return out; }
  const byMachine = pred => aggBy(pred, r => D.machines[r[2]]);
  const sumLbs = pred => { let t = 0; for (const r of D.rows) if (pred(r)) t += r[3]; return t; };
  const dayLbs = (day, sh) => sumLbs(r => D.dates[r[0]] === day && shiftOk(r, sh));

  // ---- shared widgets
  function seg(el, options, active, onPick) {
    el.replaceChildren(...options.map(([k, lab]) => { const b = document.createElement("button"); b.className = "seg-btn" + (k === active ? " active" : ""); b.type = "button"; b.dataset.k = k; b.textContent = lab;
      b.addEventListener("click", () => { $$(".seg-btn", el).forEach(x => x.classList.toggle("active", x === b)); onPick(k); }); return b; }));
  }
  function pager(el, options, value, onPick) {          // options: [[value, label]] newest first
    el.replaceChildren();
    const prev = document.createElement("button"), next = document.createElement("button"), sel = document.createElement("select");
    prev.type = next.type = "button"; prev.textContent = "‹"; next.textContent = "›"; prev.setAttribute("aria-label", "Earlier"); next.setAttribute("aria-label", "Later"); sel.className = "sel";
    options.forEach(([v, lab]) => { const o = document.createElement("option"); o.value = v; o.textContent = lab; if (v === value) o.selected = true; sel.appendChild(o); });
    const i = options.findIndex(o => o[0] === value); next.disabled = i <= 0; prev.disabled = i < 0 || i >= options.length - 1;
    prev.addEventListener("click", () => onPick(options[i + 1][0])); next.addEventListener("click", () => onPick(options[i - 1][0]));
    sel.addEventListener("change", () => onPick(sel.value));
    el.append(prev, sel, next);
  }
  const tip = $("#tip");
  function showTip(ev, title, rows) {
    tip.replaceChildren(); const b = document.createElement("b"); b.textContent = title; tip.appendChild(b);
    rows.forEach(([k, v]) => { const row = document.createElement("div"); row.className = "r"; const a = document.createElement("span"); a.textContent = k; const c = document.createElement("span"); c.textContent = v; row.append(a, c); tip.appendChild(row); });
    tip.style.display = "block"; const tw = tip.offsetWidth, th = tip.offsetHeight;
    tip.style.left = Math.min(ev.clientX + 14, innerWidth - tw - 8) + "px"; tip.style.top = Math.max(8, ev.clientY - th - 14) + "px";
  }
  const hideTip = () => { tip.style.display = "none"; };
  const stat = (label, value, sub, tone) => '<div class="stat"><div class="stat-label">' + h(label) + '</div><div class="stat-value">' + value + '</div>' + (sub ? '<div class="stat-sub ' + (tone || "") + '">' + sub + '</div>' : "") + '</div>';
  const swatch = m => '<span class="sw" style="background:' + h(D.colors[m] || "#4a3aa7") + '"></span>';
  const name = m => D.names[m] || m;
  const shiftOptions = [["all", "All shifts"], ["1st", "1st"], ["2nd", "2nd"], ["3rd", "3rd"]];
  const shiftWord = sh => sh === "all" ? "all shifts" : sh + " shift";

  // ---- Today
  const T = { day: D.data_through, shift: "all", userSet: false };
  function dayOptions() {
    const ds = D.dates.slice().reverse().filter(s => s <= D.today);
    if (live && !ds.includes(live.day)) ds.unshift(live.day);
    return ds.slice(0, 40).map(s => [s, (s === (live && live.day) ? "Today · " : "") + fmtShort(s)]);
  }
  function normalFor(day, sh) {                        // same weekday, the 4 most recent occurrences before `day` with rows
    const wd = dow(day), lo = addDays(day, -35);
    const days = [...new Set(DI.filter(x => x.s < day && x.s >= lo && x.dow === wd).map(x => x.s))].sort().slice(-4);
    if (!days.length) return { n: 0, per: new Map(), total: 0 };
    const set = new Set(days), per = byMachine(r => set.has(D.dates[r[0]]) && shiftOk(r, sh));
    let total = 0; per.forEach((a, m) => { a.lbs /= days.length; total += a.lbs; });
    return { n: days.length, per, total };
  }
  function renderToday() {
    const day = T.day, sh = T.shift, isLive = !!(live && day === live.day);
    pager($("#todayPager"), dayOptions(), day, v => { T.day = v; T.userSet = true; renderToday(); });
    // per-machine pounds for the day
    let per;
    if (isLive) { per = new Map(live.machines.map(m => [m.m, { lbs: sh === "all" ? m.lbs : (m.by_shift[sh] || 0), flag: m.flag, quiet: m.quiet_min }])); }
    else { per = byMachine(r => D.dates[r[0]] === day && shiftOk(r, sh)); }
    const norm = normalFor(day, sh);
    const ms = [...per.entries()].filter(([, a]) => a.lbs > 0).sort((a, b) => b[1].lbs - a[1].lbs);
    let max = 1; ms.forEach(([m, a]) => { max = Math.max(max, a.lbs, (norm.per.get(m) || {}).lbs || 0); });
    const total = ms.reduce((t, [, a]) => t + a.lbs, 0);
    $("#todayTitle").textContent = "Pounds by machine · " + fmtDay(day) + (isLive ? " so far" : "") + (sh === "all" ? "" : " · " + sh + " shift");
    $("#todayMeta").textContent = isLive ? (live.current_shift ? live.current_shift + " shift on now · " : "") + "as of " + clock(parseTs(live.generated)) : (day === D.data_through ? "last complete production day" : "");
    const bars = $("#todayBars");
    if (!ms.length) { bars.innerHTML = '<div class="empty">' + (closures.has(day) ? "Plant closed." : "Nothing booked" + (sh === "all" ? "" : " to the " + sh + " shift") + (isLive ? " yet." : " this day.")) + '</div>'; }
    else bars.innerHTML = ms.map(([m, a]) => { const nv = (norm.per.get(m) || {}).lbs || 0; const p = pct(a.lbs, nv);
      return '<div class="bar-row">' + swatch(m) + '<span class="bar-name">' + h(name(m)) + '</span><div class="bar-track"><div class="bar-fill" style="width:' + (100 * a.lbs / max).toFixed(1) + '%;background:' + h(D.colors[m] || "#4a3aa7") + '"></div>'
        + (nv ? '<div class="bar-norm" style="left:' + (100 * nv / max).toFixed(1) + '%" title="normal ' + fmt(nv) + '"></div>' : "") + '</div>'
        + '<span class="bar-val">' + fmt(a.lbs) + (p === null ? "" : '<small class="' + (p >= 0 ? "up" : "down") + '">' + (p >= 0 ? "+" : "") + p + '%</small>') + '</span></div>'
        + (a.flag ? '<div class="bar-sub warn">quiet ' + a.quiet + ' min</div>' : ""); }).join("");
    $("#todayFoot").textContent = norm.n ? "The dark tick is a normal " + wdName[dow(day)] + " for that machine (the average of the last " + norm.n + "), and the small figure is the difference. Guillotine's rolls count when it books no output." : "No earlier " + wdName[dow(day)] + "s to compare against.";
    // stats
    const wk = mondayOf(day), lwEnd = addDays(day, -7), lw = mondayOf(lwEnd);
    const wtdData = sumLbs(r => DI[r[0]].wk === wk && D.dates[r[0]] <= day && D.dates[r[0]] !== (isLive ? day : "") && shiftOk(r, sh));
    const wtd = wtdData + (isLive ? total : 0), lastWk = sumLbs(r => DI[r[0]].wk === lw && D.dates[r[0]] <= lwEnd && shiftOk(r, sh));
    const days = dow(day) + 1;
    const [tsub, ttone] = deltaHtml(total, norm.total, "a normal " + wdName[dow(day)]);
    const [wsub, wtone] = deltaHtml(wtd, lastWk, "last week here");
    const filed = D.reports.filter(r => r.date === day);
    const by = s => (filed.find(r => r.shift === s) || {}).by;
    const nFiled = ["1st", "2nd", "3rd"].filter(s => by(s)).length;
    const fsub = ["1st", "2nd", "3rd"].map(s => by(s) ? s + " " + h(by(s)) : s + ' <span class="warn">missing</span>').join(" · ");
    const oj = live ? live.open_jobs : D.open_jobs, ol = live ? live.open_lbs : D.open_lbs;
    $("#todayStats").innerHTML = stat("Plant total · " + shiftWord(sh), fmt(total), tsub, ttone)
      + stat("Week to date · " + days + " day" + (days === 1 ? "" : "s"), fmt(wtd), wsub, wtone)
      + stat("Open jobs in cieTrade", fmt(oj), fmt(ol) + " lbs not yet posted")
      + stat("End of Shift filed · " + fmtDay(day, { weekday: "long" }), nFiled + '<span class="of">/3</span>', fsub, nFiled === 3 ? "" : "warn");
    // live changes
    const card = $("#liveCard"); card.hidden = !isLive;
    if (isLive) {
      const feed = $("#liveFeed"); feed.replaceChildren();
      (live.changes || []).slice(0, 12).forEach(c => {
        const li = document.createElement("li"); const t = document.createElement("span"); t.className = "t"; t.textContent = clock(parseTs(c.t1));
        const w = document.createElement("span"); w.textContent = name(c.m) + (c.s ? " · " + c.s : "") + (c.units ? " · " + (c.units > 0 ? "+" : "") + c.units + " units" : "");
        const v = document.createElement("span"); v.className = "v" + (c.lbs < 0 ? " neg" : ""); v.textContent = (c.lbs > 0 ? "+" : "") + fmt(c.lbs) + " lbs";
        li.append(t, w, v); feed.appendChild(li); });
      $("#liveMeta").textContent = live.changes_today + " changes today · polled " + clock(parseTs(live.last_poll));
    }
  }

  // ---- Week
  const allWeeks = [...new Set(DI.map(x => x.wk))]; const curWeek = mondayOf(D.today); if (!allWeeks.includes(curWeek)) allWeeks.push(curWeek); allWeeks.sort();
  const weekLabel = wk => "Week of " + fmtDay(wk, { month: "short", day: "numeric" }) + (wk === curWeek ? " (this week)" : "");
  const weekOptions = allWeeks.slice().reverse().map(wk => [wk, weekLabel(wk)]);
  const W = { wk: curWeek, shift: "all" };
  function roundTop(x, y, w, hh, r) { r = Math.min(r, hh / 2, w / 2); return "M" + x + "," + (y + hh) + "V" + (y + r) + "a" + r + "," + r + " 0 0 1 " + r + ",-" + r + "H" + (x + w - r) + "a" + r + "," + r + " 0 0 1 " + r + "," + r + "V" + (y + hh) + "Z"; }
  function renderWeek() {
    const wk = W.wk, sh = W.shift, end = addDays(wk, 6);
    pager($("#weekPager"), weekOptions, wk, v => { W.wk = v; renderWeek(); });
    const inWeek = r => DI[r[0]].wk === wk && shiftOk(r, sh);
    const perDay = aggBy(inWeek, r => D.dates[r[0]] + "|" + D.machines[r[2]]);
    const days = [0, 1, 2, 3, 4, 5, 6].map(i => addDays(wk, i)).filter((d, i) => i < 5 || [...perDay.keys()].some(k => k.startsWith(d)));
    const val = (d, m) => (perDay.get(d + "|" + m) || {}).lbs || 0;
    const totals = days.map(d => D.machines.reduce((t, m) => t + val(d, m), 0));
    const total = totals.reduce((a, b) => a + b, 0);
    const machines = D.machines.filter(m => days.some(d => val(d, m) > 0));
    // 4-week average working day for this shift
    const prior = new Set(allWeeks.filter(x => x < wk).slice(-4));
    const pd = aggBy(r => prior.has(DI[r[0]].wk) && DI[r[0]].dow < 5 && shiftOk(r, sh), r => D.dates[r[0]]);
    let avg = 0; if (pd.size) { let t = 0; pd.forEach(a => { t += a.lbs; }); avg = t / pd.size; }
    // comparison
    const upto = wk === curWeek ? (live ? live.day : D.data_through) : end, lastUpto = addDays(upto, -7), lw = addDays(wk, -7);
    const lastLbs = sumLbs(r => DI[r[0]].wk === lw && D.dates[r[0]] <= lastUpto && shiftOk(r, sh));
    const [csub, ctone] = deltaHtml(total, lastLbs, wk === curWeek ? "last week at this point" : "the week before");
    $("#weekTitle").textContent = (wk === curWeek ? "This week the plant is at " : "The week of " + fmtDay(wk, { month: "short", day: "numeric" }) + " came to ") + compact(total) + " lbs" + (sh === "all" ? "" : " on the " + sh + " shift");
    $("#weekMeta").innerHTML = (csub ? '<span class="' + ctone + '">' + h(csub) + '</span> · ' : "") + "the rule is the 4-week average working day";
    $("#weekLegend").innerHTML = machines.map(m => "<span>" + swatch(m) + h(name(m)) + "</span>").join("");
    // stacked columns
    const chart = $("#weekChart"), width = Math.max(300, chart.clientWidth || 560), height = 220, left = 8, right = 8, top = 26, bottom = 22, pw = width - left - right, ph = height - top - bottom, gap = 2;
    const vmax = Math.max(1, ...totals, avg) * 1.08, y = v => top + ph - ph * v / vmax, band = pw / days.length, bw = Math.min(32, Math.round(band * 0.6));
    let s = '<svg class="stack" viewBox="0 0 ' + width + " " + height + '" role="img" aria-label="Pounds by day and machine">';
    s += '<line x1="' + left + '" x2="' + (left + pw) + '" y1="' + y(0).toFixed(1) + '" y2="' + y(0).toFixed(1) + '" class="axis"/>';
    days.forEach((d, i) => {
      const cx = left + band * (i + 0.5), x = cx - bw / 2; let cum = 0;
      const stackM = machines.filter(m => val(d, m) > 0);
      stackM.forEach((m, k) => { const v = val(d, m), y1 = y(cum + v), y0 = y(cum); const hh = Math.max(0, y0 - y1 - (k < stackM.length - 1 ? gap : 0));
        const shape = k === stackM.length - 1 ? '<path d="' + roundTop(x, y1, bw, hh, 4) + '"' : '<rect x="' + x + '" y="' + y1.toFixed(1) + '" width="' + bw + '" height="' + hh.toFixed(1) + '"';
        s += shape + ' class="seg" fill="' + h(D.colors[m]) + '" data-m="' + h(m) + '" data-d="' + d + '" data-v="' + v + '" data-t="' + totals[i] + '"/>'; cum += v; });
      if (totals[i] > 0) s += '<text x="' + cx.toFixed(1) + '" y="' + (y(totals[i]) - 5).toFixed(1) + '" class="val" text-anchor="middle">' + compact(totals[i]) + '</text>';
      s += '<text x="' + cx.toFixed(1) + '" y="' + (height - 6) + '" class="tick" text-anchor="middle">' + fmtDay(d, { weekday: "short", day: "numeric" }) + '</text>';
    });
    if (avg > 0) s += '<line x1="' + left + '" x2="' + (left + pw) + '" y1="' + y(avg).toFixed(1) + '" y2="' + y(avg).toFixed(1) + '" class="rule"/><text x="' + (left + pw) + '" y="' + (y(avg) - 4).toFixed(1) + '" class="rule-lab" text-anchor="end">4-wk average day ' + compact(avg) + '</text>';
    if (!total) s += '<text x="' + (left + pw / 2) + '" y="' + (top + ph / 2) + '" class="muted" text-anchor="middle">Nothing booked' + (sh === "all" ? "" : " to the " + sh + " shift") + ' this week</text>';
    s += "</svg>";
    chart.innerHTML = s;
    $$(".seg", chart).forEach(el => { const on = ev => showTip(ev, name(el.dataset.m) + " · " + fmtShort(el.dataset.d), [["Pounds", fmt(el.dataset.v)], ["Day total", fmt(el.dataset.t)]]); el.addEventListener("pointermove", on); el.addEventListener("pointerdown", on); el.addEventListener("pointerleave", hideTip); });
    // table
    const rows = machines.map(m => ({ m, days: days.map(d => val(d, m)), total: days.reduce((t, d) => t + val(d, m), 0) })).sort((a, b) => b.total - a.total);
    $("#weekTable").innerHTML = !rows.length ? '<div class="empty">Nothing booked' + (sh === "all" ? "" : " to the " + sh + " shift") + ' this week.</div>'
      : '<div class="table-wrap"><table><thead><tr><th>Machine</th>' + days.map(d => '<th class="num">' + fmtDay(d, { weekday: "short", day: "numeric" }) + '</th>').join("") + '<th class="num">Week</th></tr></thead><tbody>'
        + rows.map(r => '<tr><td class="mname">' + swatch(r.m) + h(name(r.m)) + '</td>' + r.days.map(v => '<td class="num">' + (v ? fmt(v) : '<span class="muted">–</span>') + '</td>').join("") + '<td class="num total">' + fmt(r.total) + '</td></tr>').join("")
        + '</tbody><tfoot><tr><td>Plant</td>' + totals.map(v => '<td class="num">' + (v ? fmt(v) : "") + '</td>').join("") + '<td class="num total">' + fmt(total) + '</td></tr></tfoot></table></div>';
  }

  // ---- Shifts (End of Shift reports)
  const reportWeeks = [...new Set(D.reports.map(r => mondayOf(r.date)))]; if (!reportWeeks.includes(curWeek)) reportWeeks.push(curWeek); reportWeeks.sort();
  const S = { wk: curWeek };
  function reportCard(sh, r) {
    if (!r) return '<div class="card eos missing"><div class="card-head"><h2>' + sh + ' shift</h2><div class="card-meta warn">No End of Shift report filed</div></div></div>';
    const mh = r.machines.reduce((t, m) => t + (m.machine_hours || 0), 0), man = r.machines.reduce((t, m) => t + (m.man_hours || 0), 0), dt = r.machines.reduce((t, m) => t + (m.downtime_min || 0), 0);
    return '<div class="card eos"><div class="card-head"><h2>' + sh + ' shift</h2><div class="card-meta">filed by ' + h(r.by || "") + (r.filed_at ? " · " + h(r.filed_at) : "") + " · " + r.machines.length + " machines · " + mh + " machine h · " + man + " man h" + (dt ? ' · <span class="warn">' + dt + ' min down</span>' : "") + '</div></div>'
      + '<div class="table-wrap"><table class="sheet"><thead><tr><th>Machine</th><th class="num">Mach h</th><th class="num">Man h</th><th>Operators</th><th>Material</th><th class="num">Down</th><th>Reason</th><th>Comments</th></tr></thead><tbody>'
      + r.machines.map(m => '<tr><td class="mname">' + h(m.machine) + '</td><td class="num">' + (m.machine_hours || "") + '</td><td class="num">' + (m.man_hours || "") + '</td><td>' + h(m.operators || "") + '</td><td>' + h(m.material || "") + '</td><td class="num">' + (m.downtime_min || "") + '</td><td>' + h(m.reason || "") + '</td><td class="c">' + h(m.comment || "") + '</td></tr>').join("")
      + '</tbody></table></div>' + ((r.notes || []).length ? '<div class="notes"><b>Shift notes</b><ul>' + r.notes.map(x => "<li>" + h(x) + "</li>").join("") + '</ul></div>' : "") + '</div>';
  }
  function renderShifts() {
    const wk = S.wk;
    pager($("#shiftsPager"), reportWeeks.slice().reverse().map(w => [w, weekLabel(w)]), wk, v => { S.wk = v; renderShifts(); });
    const inWk = D.reports.filter(r => mondayOf(r.date) === wk), byDay = {};
    inWk.forEach(r => { (byDay[r.date] = byDay[r.date] || {})[r.shift] = r; });
    const last = live ? live.day : D.data_through;
    const days = [0, 1, 2, 3, 4, 5, 6].map(i => addDays(wk, i)).filter((d, i) => byDay[d] || (i < 5 && d <= last && !closures.has(d))).reverse();
    $("#shiftsBody").innerHTML = !days.length ? '<div class="empty">No production days in this week yet.</div>'
      : days.map(d => '<div class="day-head">' + fmtDay(d) + '</div>' + ["1st", "2nd", "3rd"].map(sh => reportCard(sh, (byDay[d] || {})[sh])).join("")).join("");
  }

  // ---- Machines
  const METRICS = { lbs: ["Pounds", "lbs/wk", v => compact(v)], lbs_h: ["Lbs per hour", "lbs per machine hour", v => fmt(v)], cost_h: ["Labor $ per hour", "labor $ per machine hour", v => money(v, 0)], cost_lb: ["Labor $ per lb", "labor $ per lb", v => money(v, 3)] };
  const M = { shift: "all", metric: "lbs" };
  const completeWeeks = allWeeks.filter(w => w < curWeek);
  function metricOf(a, k) { if (!a) return k === "lbs" ? 0 : null; if (k === "lbs") return a.lbs; if (k === "lbs_h") return a.mh > 0 ? a.lbs_h / a.mh : null; if (k === "cost_h") return a.mh_c > 0 ? a.cost_c / a.mh_c : null; return a.lbs_c > 0 ? a.cost / a.lbs_c : null; }
  function rollOf(list, k) {                           // ratio of sums over the window; a plain mean for pounds
    const s = acc(); let n = 0; list.forEach(a => { n++; if (!a) return; for (const f in s) s[f] += a[f]; });
    if (k === "lbs") return n ? s.lbs / n : null; return metricOf(s, k);
  }
  function facetSvg(f, fmtV) {
    const width = 320, height = 128, top = 10, bottom = 20, left = 8, right = 62, pw = width - left - right, ph = height - top - bottom;
    const vals = f.raw.concat(f.avg).filter(v => v !== null), vmax = Math.max(1, ...vals) * 1.08, k = f.avg.length;
    const x = i => left + pw * i / Math.max(k - 1, 1), y = v => top + ph - ph * v / vmax;
    const lastI = (() => { for (let i = k - 1; i >= 0; i--) if (f.avg[i] !== null) return i; return -1; })();
    const ye = lastI >= 0 ? y(f.avg[lastI]) : null;
    let s = '<svg class="facet" viewBox="0 0 ' + width + " " + height + '" role="img" aria-label="' + h(name(f.m)) + '">';
    [vmax * 0.5, vmax * 0.9].forEach(g => { s += '<line x1="' + left + '" x2="' + (left + pw) + '" y1="' + y(g).toFixed(1) + '" y2="' + y(g).toFixed(1) + '" class="grid"/>';
      if (ye === null || Math.abs(y(g) - ye) > 12) s += '<text x="' + (left + pw + 4) + '" y="' + (y(g) + 3).toFixed(1) + '" class="tick">' + fmtV(g) + '</text>'; });
    s += '<line x1="' + left + '" x2="' + (left + pw) + '" y1="' + y(0).toFixed(1) + '" y2="' + y(0).toFixed(1) + '" class="axis"/>';
    const poly = (arr, cls) => { let out = "", run = []; const flush = () => { if (run.length) out += '<polyline points="' + run.join(" ") + '" class="' + cls + '"/>'; run = []; };
      arr.forEach((v, i) => { if (v === null) flush(); else run.push(x(i).toFixed(1) + "," + y(v).toFixed(1)); }); flush(); return out; };
    s += poly(f.raw, "raw") + poly(f.avg, "avg");
    if (lastI >= 0) s += '<circle cx="' + x(lastI).toFixed(1) + '" cy="' + ye.toFixed(1) + '" r="4.5" class="end"/><text x="' + (left + pw + 4) + '" y="' + (ye + 4).toFixed(1) + '" class="endlab">' + fmtV(f.avg[lastI]) + '</text>';
    s += '<text x="' + left + '" y="' + (height - 6) + '" class="tick">' + fmtDay(f.weeks[0], { month: "short", day: "numeric" }) + '</text><text x="' + (left + pw) + '" y="' + (height - 6) + '" class="tick" text-anchor="end">' + fmtDay(f.weeks[k - 1], { month: "short", day: "numeric" }) + '</text>';
    s += '<line class="xh" style="display:none"/><circle class="hover-dot" r="4" style="display:none"/></svg>';
    return s;
  }
  function renderMachines() {
    const sh = M.shift, k = M.metric, [mlabel, unit, fmtV] = METRICS[k];
    const win = D.avg_window, shown = completeWeeks.slice(-D.facet_weeks), span = completeWeeks.slice(-(D.facet_weeks + win - 1));
    const wkSet = new Set(span), per = aggBy(r => wkSet.has(DI[r[0]].wk) && shiftOk(r, sh), r => DI[r[0]].wk + "|" + D.machines[r[2]]);
    const facets = [];
    D.machines.forEach(m => {
      const raw = shown.map(w => metricOf(per.get(w + "|" + m), k));
      if (!raw.some(v => v !== null && v !== 0)) return;
      const avg = shown.map(w => { const i = span.indexOf(w); return rollOf(span.slice(Math.max(0, i - win + 1), i + 1).map(x => per.get(x + "|" + m)), k); });
      const lastI = avg.map((v, i) => v === null ? -1 : i).filter(i => i >= 0).pop();
      const last = lastI === undefined ? null : avg[lastI], prev = lastI !== undefined && lastI - win >= 0 ? avg[lastI - win] : null;
      facets.push({ m, weeks: shown, raw, avg, last, prev });
    });
    facets.sort((a, b) => (b.last || 0) - (a.last || 0));
    $("#machNote").textContent = mlabel + " per machine by week, last " + D.facet_weeks + " complete weeks" + (sh === "all" ? "" : ", " + sh + " shift") + ". The line is the " + win + "-week average; the faint trace is each week as booked. Hover or tap a chart for values." + (k === "lbs" ? "" : " Hour and cost figures use only rows where hours were reported (the End of Shift form), so weeks without them show a gap. Labor cost is man hours at the operator rate; no overhead is added.");
    $("#facets").innerHTML = !facets.length ? '<div class="empty">No ' + mlabel.toLowerCase() + ' data for this selection.</div>' : facets.map(f => {
      const [sub, tone] = f.prev !== null && f.last !== null ? deltaHtml(f.last, f.prev, win + " weeks ago") : ["", ""];
      const subTxt = sub && k !== "lbs" ? sub.replace(/\(.*\)$/, "(" + fmtV(f.prev) + ")") : sub;
      return '<div class="facet-card"><div class="facet-head">' + swatch(f.m) + '<b>' + h(name(f.m)) + '</b><span class="facet-val">' + (f.last === null ? "–" : fmtV(f.last)) + '<small> ' + h(unit) + '</small></span></div>' + facetSvg(f, fmtV) + '<div class="facet-sub ' + tone + '">' + h(subTxt) + '</div></div>'; }).join("");
    $$("svg.facet", $("#facets")).forEach((svg, i) => {
      const f = facets[i], vb = svg.viewBox.baseVal, kk = f.avg.length, left = 8, pw = vb.width - 8 - 62, x = j => left + pw * j / Math.max(kk - 1, 1);
      const xh = $(".xh", svg), dot = $(".hover-dot", svg);
      const vals = f.raw.concat(f.avg).filter(v => v !== null), vmax = Math.max(1, ...vals) * 1.08, y = v => 10 + (vb.height - 30) - (vb.height - 30) * v / vmax;
      function at(ev) { const r = svg.getBoundingClientRect(), px = (ev.clientX - r.left) * vb.width / r.width; let j = Math.round((px - left) / pw * Math.max(kk - 1, 1)); j = Math.max(0, Math.min(kk - 1, j));
        xh.setAttribute("x1", x(j)); xh.setAttribute("x2", x(j)); xh.setAttribute("y1", 4); xh.setAttribute("y2", vb.height - 18); xh.style.display = "";
        if (f.avg[j] !== null) { dot.setAttribute("cx", x(j)); dot.setAttribute("cy", y(f.avg[j])); dot.style.display = ""; } else dot.style.display = "none";
        showTip(ev, name(f.m) + " · week of " + fmtDay(f.weeks[j], { month: "short", day: "numeric" }), [[win + "-wk average", f.avg[j] === null ? "–" : fmtV(f.avg[j]) + " " + unit], ["That week", f.raw[j] === null ? "no hours reported" : fmtV(f.raw[j]) + " " + unit]]); }
      const off = () => { xh.style.display = "none"; dot.style.display = "none"; hideTip(); };
      svg.addEventListener("pointermove", at); svg.addEventListener("pointerdown", at); svg.addEventListener("pointerleave", off);
    });
  }

  // ---- chrome: theme, sidebar, routing
  const root = document.documentElement;
  try { const t = localStorage.getItem("beta-theme"); if (t) root.setAttribute("data-theme", t); } catch (e) {}
  $$("[data-theme-toggle]").forEach(b => b.addEventListener("click", () => {
    const dark = root.getAttribute("data-theme") === "dark" || (!root.getAttribute("data-theme") && matchMedia("(prefers-color-scheme: dark)").matches);
    root.setAttribute("data-theme", dark ? "light" : "dark"); try { localStorage.setItem("beta-theme", dark ? "light" : "dark"); } catch (e) {} }));
  const app = $(".app");
  try { if (localStorage.getItem("beta-nav") === "collapsed") app.classList.add("collapsed"); } catch (e) {}
  $("#collapse").addEventListener("click", () => { app.classList.toggle("collapsed"); try { localStorage.setItem("beta-nav", app.classList.contains("collapsed") ? "collapsed" : "open"); } catch (e) {} });
  const titles = { today: "Today", week: "Week at a glance", shifts: "End of Shift reports", machines: "Machines", more: "More" };
  function show(n) { if (!titles[n]) n = "today"; $$(".view").forEach(v => { v.hidden = v.dataset.view !== n; }); $$("[data-nav]").forEach(a => a.classList.toggle("active", a.dataset.nav === n)); $("#pageTitle").textContent = titles[n]; hideTip(); window.scrollTo({ top: 0 }); if (n === "week" && typeof renderWeek === "function" && $("#weekPager").children.length) renderWeek(); }
  window.addEventListener("hashchange", () => show(location.hash.slice(1)));
  show(location.hash.slice(1));
  seg($("#todaySeg"), shiftOptions, T.shift, k => { T.shift = k; renderToday(); });
  seg($("#weekSeg"), shiftOptions, W.shift, k => { W.shift = k; renderWeek(); });
  seg($("#machSeg"), shiftOptions, M.shift, k => { M.shift = k; renderMachines(); });
  seg($("#metricSeg"), Object.entries(METRICS).map(([k, v]) => [k, v[0]]), M.metric, k => { M.metric = k; renderMachines(); });
  renderToday(); renderWeek(); renderShifts(); renderMachines();
  let rt; window.addEventListener("resize", () => { clearTimeout(rt); rt = setTimeout(renderWeek, 150); });

  // ---- live feed
  function refresh() {
    if (!D.feed) return;
    fetch(D.feed + "?t=" + Date.now(), { cache: "no-store" }).then(r => r.ok ? r.json() : Promise.reject(r.status)).then(L => {
      const first = !live; live = L; $("#feedState").textContent = "";
      if (first && !T.userSet) T.day = L.day;
      renderToday(); if (first) { renderWeek(); renderShifts(); }
    }).catch(() => { $("#feedState").textContent = "live feed unavailable; showing the last rebuild"; });
  }
  refresh(); setInterval(refresh, 60000);
})();
"""


def render(D: dict) -> str:
    nav_side = "".join(f'<a href="#{k}" data-nav="{k}">{ICONS[k]}<span>{lab}</span></a>' for k, lab in NAV)
    nav_tab = "".join(f'<a href="#{k}" data-nav="{k}">{ICONS[k]}{lab}</a>' for k, lab in NAV)
    banner = (f'<div class="banner">cieTrade API unavailable since {e(str(D["api_down_since"])[:16].replace("T", " "))}; figures are through the last good poll.</div>'
              if D.get("api_down_since") else "")
    data = json.dumps(D, separators=(",", ":")).replace("</", "<\\/")
    through = datetime.strptime(D["data_through"], "%Y-%m-%d").strftime("%a %b %-d")
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
    <div class="side-foot">Data through {e(D["data_through"])}</div>
  </aside>
  <main class="main">
    <div class="topbar"><h1 id="pageTitle">Today</h1><div class="date">{e(through)} · <a href="../">current site</a></div></div>
    {banner}
    <section class="view" data-view="today">
      <div class="controls"><div class="pager" id="todayPager"></div><div class="seg" id="todaySeg" role="group" aria-label="Shift"></div><div class="controls-note" id="feedState"></div></div>
      <div class="card">
        <div class="card-head"><h2 id="todayTitle">Pounds by machine</h2><div class="card-meta" id="todayMeta"></div></div>
        <div class="bars" id="todayBars"></div>
        <div class="card-foot" id="todayFoot"></div>
      </div>
      <div class="stats" id="todayStats"></div>
      <div class="card" id="liveCard" hidden>
        <div class="card-head"><h2>Latest changes in cieTrade</h2><div class="card-meta" id="liveMeta"></div></div>
        <ol class="feed" id="liveFeed"></ol>
      </div>
    </section>
    <section class="view" data-view="week" hidden>
      <div class="controls"><div class="pager" id="weekPager"></div><div class="seg" id="weekSeg" role="group" aria-label="Shift"></div></div>
      <div class="card">
        <div class="card-head"><h2 id="weekTitle"></h2><div class="card-meta" id="weekMeta"></div></div>
        <div class="legend" id="weekLegend"></div>
        <div id="weekChart"></div>
      </div>
      <div class="card">
        <div class="card-head"><h2>By machine and day</h2><div class="card-meta">Guillotine's rolls count when it books no output. – means no job that day.</div></div>
        <div id="weekTable"></div>
      </div>
    </section>
    <section class="view" data-view="shifts" hidden>
      <div class="controls"><div class="pager" id="shiftsPager"></div><div class="controls-note">End of Shift reports as the supervisors filed them, newest day first · <a href="../shift/">open the form</a></div></div>
      <div id="shiftsBody"></div>
    </section>
    <section class="view" data-view="machines" hidden>
      <div class="controls"><div class="seg" id="machSeg" role="group" aria-label="Shift"></div><div class="seg" id="metricSeg" role="group" aria-label="Metric"></div><div class="controls-note" id="machNote"></div></div>
      <div class="facets" id="facets"></div>
    </section>
    <section class="view" data-view="more" hidden>
      <div class="card"><div class="card-head"><h2>More</h2></div>
      <ul class="links">
        <li><a href="../">Current production dashboard</a><span>every chart and table, the version this beta will replace</span></li>
        <li><a href="../daily.html">Daily details</a><span>day-by-day rows, notes and validation</span></li>
        <li><a href="../shift/">End of Shift form</a><span>for supervisors, works on a phone</span></li>
      </ul>
      <div class="card-foot">Data through {e(D["data_through"])} · dashboard rebuilt {e(str(D["last_poll"] or "")[:16].replace("T", " "))} · cieTrade polled every 10 minutes · the page carries the last {EMBED_WEEKS} weeks · beta build.</div>
      </div>
    </section>
  </main>
</div>
<nav class="tabbar" aria-label="Views">{nav_tab}</nav>
<div class="tip" id="tip" role="status" aria-live="polite"></div>
<script id="data" type="application/json">{data}</script>
<script>{JS}</script>
</body>
</html>"""


def main(input_path: Path = DEFAULT_INPUT, output_path: Path = DEFAULT_OUTPUT, status_path: Path = STATUS_PATH) -> Path:
    df, status = load(input_path, status_path)
    page = render(dataset(df, status))
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
