"""Week-at-a-glance table and daily production chart for docs/index.html.

Folded in from the cieTrade pilot page. Both render client-side from one compact
payload (one entry per date, shift and machine) so the Guillotine-support toggle
and the Machine menu apply without a second copy of the markup. The status
sidecar written by cietrade_daily marks shift-days that are still awaiting a poll
or a posting, and plant closures.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import DATA_DIR

STATUS_PATH = DATA_DIR / "cietrade_status.json"
BASIS_CODE = {"exact": "e", "averaged": "a", "partial": "p"}


def load_status(path: Path = STATUS_PATH) -> dict | None:
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None


def build_daily_payload(df: pd.DataFrame, status: dict | None = None) -> dict:
    """Compact rows: [date, shift, machine index, output, output incl. Guillotine support, basis]."""
    d = df.copy()
    support = d["Actual_Output"].astype(float)
    mask = d["Machine_Name"].str.contains("GUILLOTINE", case=False, na=False) & (support == 0) & (d["Actual_Input"] > 0)
    support = support.where(~mask, d["Actual_Input"].astype(float))
    d["u"] = support
    d["o"] = d["Actual_Output"].astype(float)
    basis = d["Basis"].map(BASIS_CODE) if "Basis" in d.columns else pd.Series("e", index=d.index)
    d["b"] = basis.fillna("e")
    d["Date"] = d["Date"].astype(str).str[:10]
    rank = {"e": 0, "a": 1, "p": 2}
    g = (d.groupby(["Date", "Shift", "Machine_Name"])
          .agg(o=("o", "sum"), u=("u", "sum"), b=("b", lambda s: max(s, key=lambda x: rank.get(x, 0))))
          .reset_index())
    order = g.groupby("Machine_Name")["u"].sum().sort_values(ascending=False)
    machines = list(order.index)
    idx = {m: i for i, m in enumerate(machines)}
    rows = [[r.Date, r.Shift, idx[r.Machine_Name], int(round(r.o)), int(round(r.u)), r.b]
            for r in g.sort_values(["Date", "Shift", "Machine_Name"]).itertuples()]
    return {"machines": machines, "rows": rows, "status": status or {}}


def status_line_html(status: dict | None, total_weeks: int, last_date: str) -> str:
    """Header line: data freshness, poller state, open jobs."""
    def when(s: str) -> str:
        try:
            t = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            return t.strftime("%a %b %-d, %-I:%M %p")
        except ValueError:
            return s
    def day(s: str) -> str:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%a %b %-d")
        except ValueError:
            return s
    parts = [f"Data through <b>{day(last_date)}</b>", f"{total_weeks} weeks of data"]
    if status and status.get("last_poll"):
        parts.append(f"fed by cieTrade every 10 min (dashboard rebuilt {when(status['last_poll'])}); the Live card below updates on its own")
    return " · ".join(parts)


DAILY_CSS = r"""
    #weekCard .filters, #dailyCard .filters { display:flex; flex-wrap:wrap; gap:10px 18px; align-items:center; }
    #weekCard .filters label, #dailyCard .filters label { font-size:12px; color:var(--muted); font-weight:500; display:inline-flex; align-items:center; gap:8px; margin:0; }
    .sel { font:inherit; font-size:13px; font-weight:600; color:var(--text); background:var(--card); border:1px solid var(--border); border-radius:8px; padding:7px 10px; min-width:150px; }
    .seg { display:inline-flex; gap:4px; align-items:center; }
    .seg-label { font-size:12px; color:var(--muted); margin-right:4px; }
    .seg-btn { font:inherit; font-size:12px; font-weight:600; color:var(--text); background:transparent; border:1px solid var(--border); border-radius:8px; padding:6px 10px; cursor:pointer; }
    .seg-btn:hover { background:var(--brand-soft); }
    .seg-btn.active { background:var(--brand); color:var(--card); border-color:var(--brand); }
    .seg-btn:focus-visible, .sel:focus-visible { outline:2px solid var(--brand); outline-offset:2px; }
    #dailyCard .controls-row { display:flex; flex-wrap:wrap; gap:8px 24px; align-items:center; margin:6px 0 12px; }
    .legend { display:flex; flex-wrap:wrap; gap:6px 18px; margin:6px 0; font-size:12px; color:var(--muted); }
    .legend .k, .basis-key .k { display:inline-flex; align-items:center; gap:7px; }
    .sw { width:12px; height:12px; border-radius:3px; display:inline-block; }
    .lkey { width:14px; height:3px; border-radius:2px; display:inline-block; }
    .basis-key { display:flex; flex-wrap:wrap; gap:6px 18px; font-size:12px; color:var(--muted); margin-top:10px; }
    .basis-key .glyph { font-weight:700; font-size:12px; width:14px; text-align:center; }
    .chart-row { display:grid; grid-template-columns:minmax(0,1fr) 230px; gap:20px; align-items:start; }
    .chart-host { min-width:0; }
    .chart-host svg { display:block; width:100%; height:auto; overflow:visible; }
    .totals { display:grid; gap:8px; font-size:13px; padding-top:34px; }
    .totals .row { display:grid; grid-template-columns:12px 1fr auto; gap:8px; align-items:center; }
    .totals .row .n { color:var(--muted); font-size:12px; }
    .totals .row .v { font-variant-numeric:tabular-nums; font-weight:600; text-align:right; }
    .totals .row .v small { display:block; font-weight:400; color:var(--muted); font-size:11px; }
    .totals .title { font-size:12px; color:var(--muted); margin-bottom:2px; }
    #weekCard table, #dailyCard table { width:100%; border-collapse:collapse; font-size:13px; }
    #weekCard th, #weekCard td, #dailyCard th, #dailyCard td { text-align:left; padding:7px 8px; border-bottom:1px solid var(--border); vertical-align:top; background:transparent; color:inherit; }
    #weekCard th, #dailyCard th { font-size:12px; color:var(--muted); font-weight:600; }
    #weekCard td.num, #weekCard th.num, #dailyCard td.num, #dailyCard th.num { text-align:right; font-variant-numeric:tabular-nums; }
    .pill { display:inline-block; font-size:11px; font-weight:600; padding:2px 8px; border-radius:999px; white-space:nowrap; }
    .pill::before { content:""; display:inline-block; width:7px; height:7px; border-radius:50%; margin-right:6px; vertical-align:1px; }
    .pill.exact { background:color-mix(in srgb, var(--good) 14%, transparent); } .pill.exact::before { background:var(--good); }
    .pill.avg { background:color-mix(in srgb, var(--warn) 22%, transparent); } .pill.avg::before { background:var(--warn); }
    .pill.live { background:color-mix(in srgb, var(--brand) 16%, transparent); } .pill.live::before { background:var(--brand); }
    .pill.await { background:color-mix(in srgb, var(--await) 45%, transparent); } .pill.await::before { background:var(--await-ink); }
    .pill.idle { background:color-mix(in srgb, var(--idle) 70%, transparent); color:var(--muted); } .pill.idle::before { background:var(--muted); }
    .wk td.avg::before { content:"\2248 "; color:var(--muted); }
    .wk td.live::before { content:"\25B8 "; color:var(--brand); }
    .wk td.await::before { content:"\2026 "; color:var(--muted); }
    .wk td.await, .wk td.idle { color:var(--muted); }
    .wk td.closed { color:var(--muted); font-style:italic; }
    .wk tr.total td { font-weight:600; border-top:2px solid var(--border); }
    .wk td.total { font-weight:600; border-left:1px solid var(--border); }
    .wk tr.shift-row td { color:var(--muted); font-size:12px; }
    .wk .wk-name .sw { margin-right:8px; vertical-align:-1px; }
    .tip { position:absolute; z-index:10; pointer-events:none; background:var(--card); color:var(--text); border:1px solid var(--border); border-radius:10px; box-shadow:var(--shadow-card); padding:10px 12px; font-size:12px; min-width:190px; display:none; }
    .tip .h { font-weight:600; margin-bottom:2px; }
    .tip .b { color:var(--muted); margin-bottom:6px; }
    .tip .r { display:grid; grid-template-columns:auto 1fr; gap:10px; align-items:center; padding:2px 0; }
    .tip .r .v { font-weight:700; font-variant-numeric:tabular-nums; text-align:right; min-width:56px; }
    .tip .r .n { display:inline-flex; align-items:center; gap:6px; color:var(--muted); }
    .tip .r .n i { width:10px; height:3px; border-radius:2px; display:inline-block; }
    .tip .t { border-top:1px solid var(--border); margin-top:4px; padding-top:4px; }
    .hit { fill:transparent; cursor:pointer; }
    .day.hover rect, .day.hover path { filter:brightness(1.12); }
    .day .hm, .day .xh { opacity:0; }
    .day.hover .hm, .day.hover .xh { opacity:1; }
    .await-note { font-size:12px; color:var(--muted); margin-top:8px; }
    @media (max-width: 900px) { .chart-row { grid-template-columns:minmax(0,1fr); } .totals { padding-top:0; } }
"""

DAILY_HTML = r"""
    <section class="card wk" id="weekCard">
      <div class="card-head">
        <div>
          <h2>Week at a glance</h2>
          <p class="lede">Pounds per machine per day. ≈ averaged over a multi-day job · ▸ in progress at the last poll · … awaiting a poll or a posting · — no job that day · closed = plant closed. These two menus are independent of the controls above.</p>
        </div>
        <div class="filters">
          <label>Week <select class="sel" id="wkWeek" aria-label="Week"></select></label>
          <label>Shift <select class="sel" id="wkShift" aria-label="Shift for the week table"></select></label>
        </div>
      </div>
      <div class="table-wrap" id="weekTable"></div>
    </section>

    <section class="card" id="dailyCard">
      <div class="card-head">
        <div>
          <h2>Daily production by machine</h2>
          <p class="lede">Pounds of output per working day. The Machine menu above scopes this chart; smoothing is a centered moving average over working days.</p>
        </div>
        <button class="seg-btn" id="dailyTableBtn" aria-expanded="false" aria-controls="dailyTable">Show as table</button>
      </div>
      <div class="controls-row">
        <div class="seg" role="group" aria-label="Chart view"><span class="seg-label">View</span>
          <button class="seg-btn active" data-view="lines" aria-pressed="true">Lines by machine</button>
          <button class="seg-btn" data-view="stacked" aria-pressed="false">Stacked total</button></div>
        <div class="seg" role="group" aria-label="Smoothing"><span class="seg-label">Smoothing</span>
          <button class="seg-btn" data-smooth="1" aria-pressed="false">Raw</button>
          <button class="seg-btn active" data-smooth="3" aria-pressed="true">3-day</button>
          <button class="seg-btn" data-smooth="5" aria-pressed="false">5-day</button></div>
        <div class="seg" role="group" aria-label="Shift"><span class="seg-label">Shift</span>
          <button class="seg-btn active" data-shift="all" aria-pressed="true">All</button>
          <button class="seg-btn" data-shift="1st" aria-pressed="false">1st</button>
          <button class="seg-btn" data-shift="2nd" aria-pressed="false">2nd</button>
          <button class="seg-btn" data-shift="3rd" aria-pressed="false">3rd</button></div>
        <div class="seg" role="group" aria-label="Range"><span class="seg-label">Range</span>
          <button class="seg-btn" data-range="4" aria-pressed="false">4 wk</button>
          <button class="seg-btn" data-range="8" aria-pressed="false">8 wk</button>
          <button class="seg-btn active" data-range="13" aria-pressed="true">13 wk</button>
          <button class="seg-btn" data-range="26" aria-pressed="false">26 wk</button>
          <button class="seg-btn" data-range="all" aria-pressed="false">All</button></div>
      </div>
      <div class="legend" id="dailyLegend"></div>
      <div class="chart-row">
        <div class="chart-host" id="dailyChart"></div>
        <aside class="totals" id="dailyTotals"></aside>
      </div>
      <div class="basis-key">
        <span class="k"><span class="sw" style="background:var(--good)"></span><span class="glyph">✓</span>Exact — one job window</span>
        <span class="k"><span class="sw" style="background:var(--warn)"></span><span class="glyph">≈</span>Averaged over a multi-day job</span>
        <span class="k"><span class="sw" style="background:var(--brand)"></span><span class="glyph">▸</span>In progress at the last poll</span>
        <span class="k"><span class="sw" style="background:var(--await)"></span><span class="glyph">…</span>Awaiting a poll or a posting</span>
        <span class="k"><span class="sw" style="background:var(--idle)"></span><span class="glyph">○</span>No job open</span>
      </div>
      <div class="table-wrap" id="dailyTable" hidden></div>
    </section>
    <div class="tip" id="dtip" role="status" aria-live="polite"></div>
"""

DAILY_JS = r"""
(function () {
  const PAYLOAD = __DAILY_PAYLOAD__;
  const STATUS = PAYLOAD.status || {};
  const SERIES = ["var(--s1)","var(--s2)","var(--s3)","var(--s4)","var(--s5)","var(--s6)","var(--s7)","var(--s8)","var(--s9)","var(--s10)"];
  const label = m => m.split(" ").map(w => /^[A-Z]{2,}$/.test(w) ? w[0] + w.slice(1).toLowerCase() : w).join(" ").replace(/\((.*?)\)/, (a, b) => "(" + b.toLowerCase() + ")");
  const MACHINES = PAYLOAD.machines.map((m, i) => ({ key: m, label: label(m), color: SERIES[i % SERIES.length] }));
  const machineByKey = key => MACHINES.find(m => m.key === key);
  window.waltonMachineColors = Object.fromEntries(MACHINES.map(m => [m.key, m.color]));
  const MODE = { e: "exact", a: "spread", p: "partial" };
  const ROWS = PAYLOAD.rows.map(r => ({ d: r[0], s: r[1], m: PAYLOAD.machines[r[2]], o: r[3], u: r[4], mode: MODE[r[5]] || "exact" }));
  const AWAIT = new Set((STATUS.awaiting || []).map(a => a.join("|")));
  const CLOSED = new Set(STATUS.closures || []);
  const DATA_THROUGH = STATUS.data_through || ROWS.reduce((a, r) => r.d > a ? r.d : a, "");
  const SHIFTS = ["1st", "2nd", "3rd"];
  const NS = "http://www.w3.org/2000/svg";
  const fmt = n => Math.round(n).toLocaleString("en-US");
  const DOW = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"], MON = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const parse = s => { const [y,m,d] = s.split("-").map(Number); return new Date(y, m-1, d); };
  const iso = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  const short = s => { const d = parse(s); return `${MON[d.getMonth()]} ${d.getDate()}`; };
  const long = s => { const d = parse(s); return `${DOW[d.getDay()]} ${MON[d.getMonth()]} ${d.getDate()}`; };
  const weekOf = s => { const d = parse(s); d.setDate(d.getDate() - ((d.getDay() + 6) % 7)); return iso(d); };
  const val = r => (typeof includeSupport !== "undefined" && includeSupport) ? r.u : r.o;
  const selectedMachine = () => { const el = document.getElementById("machineSelect"); return el && el.value !== "All Machines" ? el.value : "all"; };
  // every working day between the first row and the data horizon (weekends only when they carry rows)
  const rowDates = new Set(ROWS.map(r => r.d));
  (STATUS.awaiting || []).forEach(a => rowDates.add(a[0]));
  const dates = [...rowDates].sort();
  const basisOf = modes => modes.includes("not yet posted") ? "await" : modes.includes("partial") ? "live" : modes.includes("spread") ? "avg" : modes.includes("exact") ? "exact" : "idle";
  const basisText = { exact: "Exact — one job window", avg: "Averaged over a multi-day job", live: "In progress at the last poll", await: "Awaiting a poll or a posting", idle: "No job open" };
  const basisLabel = { exact: "exact", avg: "averaged", live: "in progress", await: "awaiting", idle: "no job" };
  const cellMode = (d, s, m) => AWAIT.has([d, s, m].join("|")) ? "not yet posted" : null;

  const state = { view: "lines", smooth: 3, shift: "all", range: 13 };
  let D = null;

  function compute() {
    const machines = selectedMachine() === "all" ? MACHINES : MACHINES.filter(m => m.key === selectedMachine());
    const shifts = state.shift === "all" ? SHIFTS : [state.shift];
    const mset = new Set(machines.map(m => m.key)), sset = new Set(shifts);
    const lastWeek = weekOf(dates[dates.length - 1]);
    let cutoff = "0000-00-00";
    if (state.range !== "all") { const d = parse(lastWeek); d.setDate(d.getDate() - 7 * (state.range - 1)); cutoff = iso(d); }
    const inRange = dates.filter(d => weekOf(d) >= cutoff);
    const rows = ROWS.filter(r => mset.has(r.m) && sset.has(r.s) && inRange.includes(r.d));
    const days = inRange.map(d => {
      const cells = rows.filter(r => r.d === d);
      const byM = {}, byS = {}, modes = [];
      machines.forEach(m => { byM[m.key] = cells.filter(c => c.m === m.key).reduce((a, c) => a + val(c), 0); });
      shifts.forEach(s => { byS[s] = cells.filter(c => c.s === s).reduce((a, c) => a + val(c), 0);
        machines.forEach(m => { const aw = cellMode(d, s, m.key); if (aw) modes.push(aw); }); });
      cells.forEach(c => modes.push(c.mode));
      const total = Object.values(byM).reduce((a, b) => a + b, 0);
      return { d, byM, byS, total, basis: basisOf(modes) };
    }).filter(x => x.total > 0 || x.basis === "await" || x.basis === "live");
    return { machines, shifts, days };
  }

  // ---- tooltip ----
  const tip = document.getElementById("dtip");
  function showTip(node, x, y) {
    tip.replaceChildren(node); tip.style.display = "block";
    const pad = 14, w = tip.offsetWidth, h = tip.offsetHeight;
    let left = x + pad, top = y + pad;
    if (left + w > window.scrollX + document.documentElement.clientWidth - 8) left = x - w - pad;
    if (top + h > window.scrollY + document.documentElement.clientHeight - 8) top = y - h - pad;
    tip.style.left = left + "px"; tip.style.top = top + "px";
  }
  const hideTip = () => { tip.style.display = "none"; };
  function rowEl(value, lbl, color) {
    const r = document.createElement("div"); r.className = "r";
    const v = document.createElement("span"); v.className = "v"; v.textContent = value;
    const n = document.createElement("span"); n.className = "n";
    if (color) { const i = document.createElement("i"); i.style.background = color; n.appendChild(i); }
    n.appendChild(document.createTextNode(lbl)); r.append(v, n); return r;
  }
  function headEl(title, sub) {
    const f = document.createDocumentFragment();
    const h = document.createElement("div"); h.className = "h"; h.textContent = title; f.appendChild(h);
    const b = document.createElement("div"); b.className = "b"; b.textContent = sub; f.appendChild(b);
    return f;
  }
  function dayTip(x) {
    const f = headEl(long(x.d), basisText[x.basis]);
    [...D.machines].reverse().forEach(m => { if (x.byM[m.key] > 0) f.appendChild(rowEl(fmt(x.byM[m.key]), m.label, m.color)); });
    const t = rowEl(fmt(x.total), "total lbs"); t.classList.add("t"); f.appendChild(t);
    return f;
  }
  function lineTip(x, i, smByM, smTotal) {
    const f = headEl(long(x.d) + (state.smooth > 1 ? ` · ${state.smooth}-day average` : ""), basisText[x.basis]);
    D.machines.map(m => ({ m, v: smByM[m.key][i] })).filter(o => o.v > 0).sort((a, c) => c.v - a.v).forEach(({ m, v }) => f.appendChild(rowEl(fmt(v), m.label, m.color)));
    if (D.machines.length > 1) { const t = rowEl(fmt(smTotal[i]), state.smooth > 1 ? `total, averaged (day: ${fmt(x.total)})` : "total"); t.classList.add("t"); f.appendChild(t); }
    else if (state.smooth > 1) { const t = rowEl(fmt(x.total), "that day, unsmoothed"); t.classList.add("t"); f.appendChild(t); }
    return f;
  }
  function bindHit(hit, group, build) {
    const on = e => { group && group.classList.add("hover"); const p = e && e.clientX != null ? e : null;
      const r = hit.getBoundingClientRect();
      showTip(build(), (p ? p.clientX : r.left + r.width / 2) + window.scrollX, (p ? p.clientY : r.top) + window.scrollY); };
    const off = () => { group && group.classList.remove("hover"); hideTip(); };
    hit.addEventListener("pointerenter", on); hit.addEventListener("pointermove", on); hit.addEventListener("pointerleave", off);
    hit.addEventListener("focus", () => on(null)); hit.addEventListener("blur", off);
  }

  // ---- svg helpers ----
  const el = (tag, attrs) => { const n = document.createElementNS(NS, tag); for (const k in attrs) n.setAttribute(k, attrs[k]); return n; };
  const niceMax = v => { v = Math.max(v, 1); const steps = [500, 1000, 2000, 2500, 5000, 10000, 20000, 25000, 50000, 100000];
    for (const s of steps) { const m = Math.ceil(v / s) * s; if (m / s <= 5) return { max: m, step: s }; } return { max: Math.ceil(v / 100000) * 100000, step: 100000 }; };
  const topPath = (x, y, w, h, r) => { r = Math.min(r, w / 2, h); return `M${x},${y + h} V${y + r} Q${x},${y} ${x + r},${y} H${x + w - r} Q${x + w},${y} ${x + w},${y + r} V${y + h} Z`; };
  const stripFill = { exact: "var(--good)", avg: "var(--warn)", live: "var(--brand)", await: "var(--await)", idle: "var(--idle)" };
  function smoothed(series, k) {
    if (k <= 1) return series.slice();
    const half = Math.floor(k / 2);
    return series.map((_, i) => { let sum = 0, n = 0;
      for (let j = i - half; j <= i + half; j++) if (j >= 0 && j < series.length) { sum += series[j]; n++; }
      return sum / n; });
  }

  function drawLegend() {
    const legend = document.getElementById("dailyLegend"); legend.replaceChildren();
    const lines = state.view === "lines";
    D.machines.filter(m => D.days.some(x => x.byM[m.key] > 0)).forEach(m => { const k = document.createElement("span"); k.className = "k";
      const sw = document.createElement("span"); sw.className = lines ? "lkey" : "sw"; sw.style.background = m.color;
      k.append(sw, document.createTextNode(m.label)); legend.appendChild(k); });
    if (!lines) { const k = document.createElement("span"); k.className = "k";
      const sw = document.createElement("span"); sw.className = "lkey"; sw.style.background = "var(--text)";
      k.append(sw, document.createTextNode(state.smooth > 1 ? `Total, ${state.smooth}-day average` : "Total")); legend.appendChild(k); }
  }
  function drawTotals() {
    const { days, machines } = D;
    const totals = document.getElementById("dailyTotals"); totals.replaceChildren();
    if (!days.length) return;
    const tt = document.createElement("div"); tt.className = "title";
    tt.textContent = `Output ${short(days[0].d)} – ${short(days[days.length - 1].d)}` + (state.shift === "all" ? "" : `, ${state.shift} shift`);
    totals.appendChild(tt);
    const grand = days.reduce((a, x) => a + x.total, 0);
    machines.map(m => ({ m, v: days.reduce((a, x) => a + x.byM[m.key], 0), ran: days.filter(x => x.byM[m.key] > 0).length }))
      .filter(o => o.v > 0).sort((a, b) => b.v - a.v).forEach(({ m, v, ran }) => {
        const r = document.createElement("div"); r.className = "row";
        const sw = document.createElement("span"); sw.className = "sw"; sw.style.background = m.color;
        const n = document.createElement("span"); n.className = "n"; n.textContent = m.label;
        const v2 = document.createElement("span"); v2.className = "v"; v2.textContent = fmt(v);
        const sh = document.createElement("small");
        sh.textContent = machines.length > 1 ? (grand ? (100 * v / grand).toFixed(0) + "% of selection" : "—") : `output on ${ran} of ${days.length} days`;
        v2.appendChild(sh); r.append(sw, n, v2); totals.appendChild(r);
      });
  }

  function drawChart() {
    drawLegend();
    const { days } = D;
    const machines = D.machines.filter(m => days.some(x => x.byM[m.key] > 0));
    const host = document.getElementById("dailyChart"); host.replaceChildren();
    if (!days.length) { const p = document.createElement("p"); p.className = "muted"; p.textContent = "No production recorded for this selection."; host.appendChild(p); return; }
    const lines = state.view === "lines";
    const W = Math.max(520, host.clientWidth), L = 62, R = 16, T = 18, plotH = 300, stripY = 12, B = 70, H = T + plotH + B;
    const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: W, height: H, role: "img", "aria-label": lines ? "Daily production, one line per machine" : "Daily production by machine, stacked columns with smoothed total" });
    const plotW = W - L - R, slot = plotW / days.length, barW = Math.min(24, slot * 0.62), cx = i => L + i * slot + slot / 2;
    const rawTotals = days.map(x => x.total), smTotal = smoothed(rawTotals, state.smooth);
    const rawByM = {}, smByM = {};
    machines.forEach(m => { rawByM[m.key] = days.map(x => x.byM[m.key]); smByM[m.key] = smoothed(rawByM[m.key], state.smooth); });
    const { max, step } = niceMax(lines ? Math.max(1, ...machines.map(m => Math.max(...rawByM[m.key]))) : Math.max(...rawTotals));
    const y = v => T + plotH - (v / max) * plotH, base = T + plotH; let wk = 0;
    for (let v = 0; v <= max; v += step) {
      svg.appendChild(el("line", { x1: L, x2: W - R, y1: y(v), y2: y(v), stroke: "var(--grid)", "stroke-width": 1 }));
      const t = el("text", { x: L - 8, y: y(v) + 4, "text-anchor": "end", "font-size": 11, fill: "var(--muted)" }); t.textContent = fmt(v); svg.appendChild(t);
    }
    const yl = el("text", { x: L - 8, y: T - 13, "text-anchor": "end", "font-size": 11, fill: "var(--muted)" });
    yl.textContent = lines && state.smooth > 1 ? `lbs, ${state.smooth}-day avg` : "lbs"; svg.appendChild(yl);
    days.forEach((x, i) => {
      const x0 = L + i * slot, bx = x0 + (slot - barW) / 2;
      if (i > 0 && (parse(x.d) - parse(days[i - 1].d)) > 86400000 * 1.5)
        svg.appendChild(el("line", { x1: x0, x2: x0, y1: T, y2: base + stripY + 10, stroke: "var(--border)", "stroke-width": 1 }));
      svg.appendChild(el("rect", { x: bx, y: base + stripY, width: barW, height: 7, rx: 2, style: `fill:${stripFill[x.basis]}` }));
      if (slot >= 18 || (parse(x.d).getDay() === 1 && slot * 5 >= 22)) { const dl = el("text", { x: x0 + slot / 2, y: base + stripY + 26, "text-anchor": "middle", "font-size": 11, fill: "var(--muted)" }); dl.textContent = parse(x.d).getDate(); svg.appendChild(dl); }
      if (i === 0 || weekOf(x.d) !== weekOf(days[i - 1].d)) { wk++; const wide = slot * 5 >= 78; const every = Math.max(1, Math.ceil(46 / (slot * 5)));
        if ((wk - 1) % every === 0) { const wl = el("text", { x: x0 + 2, y: base + stripY + 46, "font-size": 11, fill: "var(--text)", "font-weight": 600 }); wl.textContent = (wide ? "Wk of " : "") + short(weekOf(x.d)); svg.appendChild(wl); } }
    });
    const linePath = vals => vals.map((v, i) => `${i ? "L" : "M"}${cx(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
    const dayGroups = [];
    if (lines) {
      machines.forEach(m => svg.appendChild(el("path", { d: linePath(smByM[m.key]), fill: "none", stroke: m.color, "stroke-width": 2, "stroke-linejoin": "round", "stroke-linecap": "round" })));
      let i = 0; while (i < days.length) { if (days[i].basis !== "await") { i++; continue; } let j = i; while (j + 1 < days.length && days[j + 1].basis === "await") j++;
        svg.appendChild(el("rect", { x: L + i * slot, y: T, width: (j - i + 1) * slot, height: plotH, fill: "var(--card)", opacity: 0.55, "pointer-events": "none" })); i = j + 1; }
      machines.forEach(m => { const n = days.length - 1; svg.appendChild(el("circle", { cx: cx(n), cy: y(smByM[m.key][n]), r: 4, fill: m.color, stroke: "var(--card)", "stroke-width": 2 })); });
      days.forEach((x, i) => { const g = el("g", { class: "day" });
        g.appendChild(el("line", { class: "xh", x1: cx(i), x2: cx(i), y1: T, y2: base, stroke: "var(--muted)", "stroke-width": 1 }));
        machines.forEach(m => g.appendChild(el("circle", { class: "hm", cx: cx(i), cy: y(smByM[m.key][i]), r: 4, fill: m.color, stroke: "var(--card)", "stroke-width": 2 })));
        svg.appendChild(g); dayGroups.push(g); });
    } else {
      days.forEach((x, i) => {
        const bx = L + i * slot + (slot - barW) / 2;
        const g = el("g", { class: "day" }); if (x.basis === "await") g.setAttribute("opacity", "0.45");
        let cum = 0; const segs = machines.map(m => ({ m, v: x.byM[m.key] })).filter(sg => sg.v > 0);
        segs.forEach((sg, k) => { const yTop = y(cum + sg.v), yBot = y(cum), last = k === segs.length - 1, h = yBot - yTop;
          if (h >= 1) { if (last) g.appendChild(el("path", { d: topPath(bx, yTop, barW, h, 4), style: `fill:${sg.m.color}` }));
            else g.appendChild(el("rect", { x: bx, y: yTop, width: barW, height: Math.max(0, h - 2), style: `fill:${sg.m.color}` })); }
          cum += sg.v; });
        svg.appendChild(g); dayGroups.push(g); });
      svg.appendChild(el("path", { d: linePath(smTotal), fill: "none", stroke: "var(--text)", "stroke-width": 2, opacity: 0.85, "stroke-linejoin": "round", "stroke-linecap": "round", "pointer-events": "none" }));
    }
    days.forEach((x, i) => {
      const hit = el("rect", { class: "hit", x: L + i * slot, y: T, width: slot, height: plotH + stripY + 12, tabindex: 0, role: "img", "aria-label": `${long(x.d)}: ${fmt(x.total)} lbs, ${basisText[x.basis]}` });
      bindHit(hit, dayGroups[i], () => lines ? lineTip(x, i, smByM, smTotal) : dayTip(x)); svg.appendChild(hit);
    });
    svg.appendChild(el("line", { x1: L, x2: W - R, y1: base, y2: base, stroke: "var(--border)", "stroke-width": 1 }));
    host.appendChild(svg);
  }

  // ---- tables ----
  const pill = (basis, txt) => { const p = document.createElement("span"); p.className = "pill " + basis; p.textContent = txt; return p; };
  function table(headers) {
    const tbl = document.createElement("table"); const thead = document.createElement("thead"); const tr = document.createElement("tr");
    headers.forEach(([t, c]) => { const th = document.createElement("th"); th.textContent = t; if (c) th.className = c; tr.appendChild(th); });
    thead.appendChild(tr); tbl.appendChild(thead); const tb = document.createElement("tbody"); tbl.appendChild(tb); return { tbl, tb };
  }
  const td = (text, cls) => { const c = document.createElement("td"); if (cls) c.className = cls; if (text != null) c.textContent = text; return c; };
  function drawDailyTable() {
    const { days, shifts } = D;
    const machines = D.machines.filter(m => days.some(x => x.byM[m.key] > 0));
    const host = document.getElementById("dailyTable"); host.replaceChildren();
    const { tbl, tb } = table([["Date", ""], ...machines.map(m => [m.label, "num"]), ["Total", "num"], ...shifts.map(s => [s + " shift", "num"]), ["Basis", ""]]);
    days.forEach(x => { const r = document.createElement("tr");
      r.appendChild(td(long(x.d)));
      machines.forEach(m => r.appendChild(td(x.byM[m.key] > 0 ? fmt(x.byM[m.key]) : "—", "num")));
      const t = td(fmt(x.total), "num"); t.style.fontWeight = "600"; r.appendChild(t);
      shifts.forEach(s => r.appendChild(td(x.byS[s] > 0 ? fmt(x.byS[s]) : "—", "num")));
      const b = td(null); b.appendChild(pill(x.basis, basisLabel[x.basis])); r.appendChild(b);
      tb.appendChild(r); });
    host.appendChild(tbl);
  }

  // ---- week at a glance (own controls) ----
  const wkState = { week: null, shift: "1st" };
  const allWeeks = [...new Set(dates.map(weekOf))].sort();
  const wkWeekSel = document.getElementById("wkWeek"), wkShiftSel = document.getElementById("wkShift");
  const opt = (v, t) => { const o = document.createElement("option"); o.value = v; o.textContent = t; return o; };
  const weekLabel = w => { const d = parse(w); const e = new Date(d); e.setDate(d.getDate() + 4); return `${short(w)} – ${MON[e.getMonth()]} ${e.getDate()}`; };
  [...allWeeks].reverse().forEach(w => wkWeekSel.appendChild(opt(w, weekLabel(w))));
  wkShiftSel.appendChild(opt("all", "All shifts")); SHIFTS.forEach(s => wkShiftSel.appendChild(opt(s, s + " shift")));
  wkState.week = allWeeks[allWeeks.length - 1]; wkWeekSel.value = wkState.week; wkShiftSel.value = wkState.shift;
  wkWeekSel.addEventListener("change", () => { wkState.week = wkWeekSel.value; drawWeekTable(); });
  wkShiftSel.addEventListener("change", () => { wkState.shift = wkShiftSel.value; drawWeekTable(); });

  function drawWeekTable() {
    const host = document.getElementById("weekTable"); host.replaceChildren();
    const monday = parse(wkState.week);
    const weekRows = ROWS.filter(r => weekOf(r.d) === wkState.week);
    const hasSat = weekRows.some(r => parse(r.d).getDay() === 6);
    const cols = (hasSat ? [0, 1, 2, 3, 4, 5] : [0, 1, 2, 3, 4]).map(k => { const d = new Date(monday); d.setDate(monday.getDate() + k); return iso(d); });
    const shifts = wkState.shift === "all" ? SHIFTS : [wkState.shift];
    const rows = weekRows.filter(r => shifts.includes(r.s));
    const active = MACHINES.filter(m => rows.some(r => r.m === m.key && val(r) > 0) || cols.some(c => shifts.some(s => cellMode(c, s, m.key))));
    const { tbl, tb } = table([["Machine", ""], ...cols.map(c => [`${DOW[parse(c).getDay()]} ${parse(c).getDate()}`, "num"]), ["Week total", "num total"]]);
    const dayTotals = Object.fromEntries(cols.map(c => [c, 0])); let grand = 0;
    if (!active.length) { const p = document.createElement("p"); p.className = "muted"; p.textContent = "No production recorded for this week and shift."; host.appendChild(p); return; }
    active.forEach(m => {
      const r = document.createElement("tr");
      const name = td(m.label, "wk-name"); const sw = document.createElement("span"); sw.className = "sw"; sw.style.background = m.color; name.prepend(sw); r.appendChild(name);
      let mt = 0;
      cols.forEach(c => {
        const cells = rows.filter(x => x.m === m.key && x.d === c);
        const lbs = cells.reduce((a, x) => a + val(x), 0);
        const modes = cells.map(x => x.mode); shifts.forEach(s => { const aw = cellMode(c, s, m.key); if (aw) modes.push(aw); });
        const basis = modes.length ? basisOf(modes) : "idle";
        const future = c > DATA_THROUGH, closed = CLOSED.has(c);
        const text = future ? "" : closed ? "closed" : basis === "idle" ? "—" : (basis === "await" && lbs === 0) ? "" : fmt(lbs);
        const cell = td(text, "num " + (future ? "future" : closed ? "closed" : basis));
        cell.title = future ? `${long(c)} · not yet reached` : closed ? `${long(c)} · plant closed` : `${m.label} · ${long(c)} · ${basisText[basis]}`;
        r.appendChild(cell); mt += lbs; dayTotals[c] += lbs;
      });
      r.appendChild(td(fmt(mt), "num total")); grand += mt; tb.appendChild(r);
    });
    if (wkState.shift === "all") SHIFTS.forEach(s => {
      const r = document.createElement("tr"); r.className = "shift-row"; r.appendChild(td(`${s} shift`));
      let st = 0; cols.forEach(c => { const v = rows.filter(x => x.s === s && x.d === c).reduce((a, x) => a + val(x), 0); st += v;
        r.appendChild(td(c > DATA_THROUGH || CLOSED.has(c) ? "" : (v > 0 ? fmt(v) : "—"), "num")); });
      r.appendChild(td(fmt(st), "num total")); tb.appendChild(r); });
    const tr = document.createElement("tr"); tr.className = "total"; tr.appendChild(td("Total"));
    cols.forEach(c => tr.appendChild(td(c > DATA_THROUGH || CLOSED.has(c) ? "" : fmt(dayTotals[c]), "num")));
    tr.appendChild(td(fmt(grand), "num total")); tb.appendChild(tr);
    host.appendChild(tbl);
    const awaiting = cols.filter(c => c <= DATA_THROUGH && !CLOSED.has(c)).reduce((n, c) => n + active.reduce((k, m) => k + shifts.filter(s => cellMode(c, s, m.key)).length, 0), 0);
    if (awaiting) { const n = document.createElement("div"); n.className = "await-note"; n.textContent = `${awaiting} machine-shift day(s) still awaiting a poll or a posting; those figures will fill in.`; host.appendChild(n); }
  }

  // ---- wiring ----
  const btn = document.getElementById("dailyTableBtn"), dt = document.getElementById("dailyTable");
  btn.addEventListener("click", () => { const open = dt.hidden; dt.hidden = !open; btn.setAttribute("aria-expanded", String(open)); btn.textContent = open ? "Hide table" : "Show as table"; });
  const wire = (attr, key, cast) => { const btns = [...document.querySelectorAll(`#dailyCard [${attr}]`)];
    btns.forEach(b => b.addEventListener("click", () => { state[key] = cast(b.getAttribute(attr));
      btns.forEach(o => { o.classList.toggle("active", o === b); o.setAttribute("aria-pressed", String(o === b)); }); rerender(); })); };
  wire("data-view", "view", v => v); wire("data-smooth", "smooth", Number); wire("data-shift", "shift", v => v); wire("data-range", "range", v => v === "all" ? "all" : Number(v));
  function rerender() { D = compute(); drawTotals(); drawChart(); drawDailyTable(); }
  window.dailyRerender = () => { rerender(); drawWeekTable(); };
  const ms = document.getElementById("machineSelect"); if (ms) ms.addEventListener("change", rerender);
  const sb = document.getElementById("supportBtn"); if (sb) sb.addEventListener("click", () => setTimeout(window.dailyRerender, 0));
  rerender(); drawWeekTable();
  let raf = null; window.addEventListener("resize", () => { cancelAnimationFrame(raf); raf = requestAnimationFrame(drawChart); });
})();
"""


def daily_script(payload: dict) -> str:
    return "  <script>" + DAILY_JS.replace("__DAILY_PAYLOAD__", json.dumps(payload, separators=(",", ":"))) + "  </script>"
