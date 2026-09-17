"""Live card for docs/index.html: today so far, the day as a cumulative curve, and the
latest changes. Renders from live.json (embedded at build time, then refreshed from the
gist the poller republishes every 10 minutes)."""
from __future__ import annotations

import json
from pathlib import Path

from config import LIVE_FEED_URL, LIVE_JSON_PATH


def load_live(path: Path = LIVE_JSON_PATH) -> dict | None:
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None


LIVE_CSS = r"""
    #liveCard .live-dot { display:inline-block; width:9px; height:9px; border-radius:50%; background:var(--good); margin-left:8px; vertical-align:middle; }
    #liveCard .live-dot.stale { background:var(--warn); }
    #liveCard .live-meta { font-size:12px; color:var(--muted); text-align:right; }
    #liveCard .live-meta b { color:var(--text); font-weight:600; }
    #liveCard .live-warn { color:var(--warn); font-weight:600; }
    .tiles { display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:12px; margin:10px 0 16px; }
    .tile { border:1px solid var(--border); border-radius:12px; padding:12px 14px; background:var(--card); }
    .tile .tn { display:flex; align-items:center; gap:8px; font-size:12px; color:var(--muted); }
    .tile .tv { font-size:24px; font-weight:600; line-height:1.15; margin-top:4px; font-variant-numeric:tabular-nums; }
    .tile .tv small { font-size:12px; font-weight:500; color:var(--muted); margin-left:4px; }
    .tile .ts { font-size:12px; color:var(--muted); margin-top:4px; }
    .tile .tf { font-size:12px; color:var(--warn); font-weight:600; margin-top:4px; }
    .tile.empty { grid-column:1 / -1; color:var(--muted); font-size:13px; }
    .live-chart svg { display:block; width:100%; height:auto; overflow:visible; }
    .live-legend { display:flex; flex-wrap:wrap; gap:6px 18px; margin:8px 0 4px; font-size:12px; color:var(--muted); }
    .live-legend .k { display:inline-flex; align-items:center; gap:7px; }
    .feed summary { cursor:pointer; font-size:13px; font-weight:600; margin-top:12px; }
    .feed .corr { color:#b91c1c; }
    html[data-theme="dark"] .feed .corr { color:#f08a8a; }
    .feed td.when { white-space:nowrap; color:var(--muted); }
    .feed .sw { margin-right:8px; vertical-align:-1px; }
"""

LIVE_HTML = r"""
    <section class="card live" id="liveCard">
      <div class="card-head">
        <div>
          <h2>Live<span class="live-dot" id="liveDot"></span></h2>
          <p class="lede" id="liveLede">Pounds entered into cieTrade so far this production day (6 AM to 6 AM), from a poll every 10 minutes. Guillotine counts rolls cut.</p>
        </div>
        <div class="live-meta" id="liveMeta"></div>
      </div>
      <div class="tiles" id="liveTiles"></div>
      <div class="live-chart" id="liveChart"></div>
      <div class="live-legend" id="liveLegend"></div>
      <details class="feed" open>
        <summary id="liveFeedTitle">Latest changes</summary>
        <div class="table-wrap" id="liveFeed"></div>
      </details>
    </section>
"""

LIVE_JS = r"""
(function () {
  const FEED_URL = "__LIVE_FEED_URL__";
  let LIVE = __LIVE_INITIAL__;
  const NS = "http://www.w3.org/2000/svg";
  const fmt = n => Math.round(n).toLocaleString("en-US");
  const DOW = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"], MON = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const parseTs = s => new Date(s);
  const clock = d => d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
  const hourLabel = h => { h = ((h % 24) + 24) % 24; return `${h % 12 || 12}${h < 12 ? "a" : "p"}`; };
  const ago = min => min < 1 ? "just now" : min < 60 ? `${min} min ago` : `${Math.floor(min / 60)} h ${min % 60} min ago`;
  const colors = () => (window.waltonMachineColors || {});
  const colorOf = m => colors()[m] || "var(--s10)";
  const label = m => m.split(" ").map(w => /^[A-Z]{2,}$/.test(w) ? w[0] + w.slice(1).toLowerCase() : w).join(" ").replace(/\((.*?)\)/, (a, b) => "(" + b.toLowerCase() + ")");
  const el = (tag, attrs) => { const n = document.createElementNS(NS, tag); for (const k in attrs) n.setAttribute(k, attrs[k]); return n; };
  const niceMax = v => { v = Math.max(v, 1); const steps = [500, 1000, 2000, 2500, 5000, 10000, 20000, 25000, 50000, 100000, 200000];
    for (const s of steps) { const m = Math.ceil(v / s) * s; if (m / s <= 5) return { max: m, step: s }; } return { max: Math.ceil(v / 200000) * 200000, step: 200000 }; };
  const td = (text, cls) => { const c = document.createElement("td"); if (cls) c.className = cls; if (text != null) c.textContent = text; return c; };

  function render(L) {
    const now = new Date(), gen = parseTs(L.generated), ageMin = Math.max(0, Math.round((now - gen) / 60000));
    const stale = ageMin > 30;
    document.getElementById("liveDot").classList.toggle("stale", stale || !L.poll_ok);
    const meta = document.getElementById("liveMeta"); meta.replaceChildren();
    const line1 = document.createElement("div");
    line1.append("As of ", Object.assign(document.createElement("b"), { textContent: clock(gen) }),
      L.current_shift ? ` · ${L.current_shift} shift on now` : " · no shift scheduled", ` · ${L.polls_today} polls today`);
    meta.appendChild(line1);
    const line2 = document.createElement("div");
    line2.textContent = `${fmt(L.lbs_today)} lbs entered today · ${L.open_jobs} open jobs, ${fmt(L.open_lbs)} lbs not yet posted`;
    meta.appendChild(line2);
    if (stale || !L.poll_ok) { const w = document.createElement("div"); w.className = "live-warn";
      w.textContent = !L.poll_ok ? "⚠ the last poll failed — figures may be behind" : `⚠ feed is ${ago(ageMin)} — the poller may have stopped`; meta.appendChild(w); }

    // tiles
    const tiles = document.getElementById("liveTiles"); tiles.replaceChildren();
    if (!L.machines.length) { const t = document.createElement("div"); t.className = "tile empty"; t.textContent = "Nothing entered yet this production day."; tiles.appendChild(t); }
    L.machines.forEach(m => {
      const t = document.createElement("div"); t.className = "tile";
      const n = document.createElement("div"); n.className = "tn";
      const sw = document.createElement("span"); sw.className = "sw"; sw.style.background = colorOf(m.m); n.append(sw, label(m.m));
      const v = document.createElement("div"); v.className = "tv"; v.textContent = fmt(m.lbs);
      const u = document.createElement("small"); u.textContent = "lbs"; v.appendChild(u);
      const s = document.createElement("div"); s.className = "ts";
      s.textContent = Object.keys(m.by_shift).map(k => `${k} ${fmt(m.by_shift[k])}`).join(" · ") + ` · last ${clock(parseTs(m.last_change))}`;
      t.append(n, v, s);
      if (m.flag) { const f = document.createElement("div"); f.className = "tf"; f.textContent = `quiet ${m.quiet_min} min`; t.appendChild(f); }
      tiles.appendChild(t);
    });

    // the day as a curve
    const host = document.getElementById("liveChart"); host.replaceChildren();
    const W = Math.max(520, host.clientWidth), Lm = 62, R = 16, T = 22, plotH = 240, B = 34, H = T + plotH + B, plotW = W - Lm - R;
    const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: W, height: H, role: "img", "aria-label": "Cumulative pounds through the production day, by machine" });
    const x = min => Lm + (min / 1440) * plotW;
    const machines = Object.keys(L.series);
    const plant = (() => { const pts = [], ev = []; machines.forEach(m => L.series[m].forEach(p => ev.push(p)));
      const byMin = new Map(); machines.forEach(m => { let last = 0; L.series[m].forEach(p => { last = p[1]; byMin.set(p[0], (byMin.get(p[0]) || 0)); }); });
      let cum = 0; const mins = [...new Set(machines.flatMap(m => L.series[m].map(p => p[0])))].sort((a, b) => a - b);
      mins.forEach(mn => { cum = machines.reduce((a, m) => { const s = L.series[m]; let v = 0; for (const p of s) { if (p[0] <= mn) v = p[1]; } return a + v; }, 0); pts.push([mn, cum]); });
      return pts; })();
    const yToday = plant.length ? Math.max(...plant.map(p => p[1])) : 0;
    const yYest = L.yesterday.total.length ? Math.max(...L.yesterday.total.map(p => p[1])) : 0;
    const { max, step } = niceMax(Math.max(yToday, yYest, 1000));
    const y = v => T + plotH - (v / max) * plotH, base = T + plotH;
    const sh = L.shift_hours, start = sh["1st"][0];
    [["1st", sh["1st"]], ["2nd", sh["2nd"]], ["3rd", sh["3rd"]]].forEach(([name, [a, b]], i) => {
      const x0 = x((a - start) * 60), x1 = x((b - start) * 60);
      svg.appendChild(el("rect", { x: x0, y: T, width: x1 - x0, height: plotH, fill: i % 2 ? "var(--brand-soft)" : "transparent", opacity: 0.6 }));
      const t = el("text", { x: x0 + 6, y: T + 13, "font-size": 11, fill: "var(--muted)", "font-weight": 600 }); t.textContent = `${name} shift`; svg.appendChild(t);
    });
    for (let v = 0; v <= max; v += step) {
      svg.appendChild(el("line", { x1: Lm, x2: W - R, y1: y(v), y2: y(v), stroke: "var(--grid)", "stroke-width": 1 }));
      const t = el("text", { x: Lm - 8, y: y(v) + 4, "text-anchor": "end", "font-size": 11, fill: "var(--muted)" }); t.textContent = fmt(v); svg.appendChild(t);
    }
    for (let h = 0; h <= 24; h += 2) { const t = el("text", { x: x(h * 60), y: base + 18, "text-anchor": "middle", "font-size": 11, fill: "var(--muted)" }); t.textContent = hourLabel(start + h); svg.appendChild(t); }
    const path = pts => pts.map((p, i) => `${i ? "L" : "M"}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`).join(" ");
    if (L.yesterday.total.length > 1) svg.appendChild(el("path", { d: path(L.yesterday.total), fill: "none", stroke: "var(--muted)", "stroke-width": 1.5, "stroke-dasharray": "4 4", opacity: 0.7 }));
    machines.forEach(m => {
      svg.appendChild(el("path", { d: path(L.series[m]), fill: "none", stroke: colorOf(m), "stroke-width": 2, "stroke-linejoin": "round" }));
      L.series[m].forEach((p, i, arr) => { if (i > 0 && arr[i - 1][0] === p[0] && p[1] !== arr[i - 1][1]) svg.appendChild(el("circle", { cx: x(p[0]), cy: y(p[1]), r: 3, fill: colorOf(m), stroke: "var(--card)", "stroke-width": 1.5 })); });
    });
    if (plant.length > 1) svg.appendChild(el("path", { d: path(plant), fill: "none", stroke: "var(--text)", "stroke-width": 2.5, opacity: 0.85 }));
    const dayStart = parseTs(L.day_start), nowMin = Math.round((now - dayStart) / 60000);
    if (nowMin >= 0 && nowMin <= 1440) { svg.appendChild(el("line", { x1: x(nowMin), x2: x(nowMin), y1: T, y2: base, stroke: "var(--brand)", "stroke-width": 1.5, "stroke-dasharray": "3 3" }));
      const t = el("text", { x: x(nowMin) + 4, y: base - 6, "font-size": 11, fill: "var(--brand)", "font-weight": 600 }); t.textContent = "now"; svg.appendChild(t); }
    svg.appendChild(el("line", { x1: Lm, x2: W - R, y1: base, y2: base, stroke: "var(--border)", "stroke-width": 1 }));
    host.appendChild(svg);
    const legend = document.getElementById("liveLegend"); legend.replaceChildren();
    const key = (color, text, dashed) => { const k = document.createElement("span"); k.className = "k"; const sw = document.createElement("span"); sw.className = "lkey"; sw.style.background = color; if (dashed) sw.style.opacity = "0.6"; k.append(sw, text); legend.appendChild(k); };
    machines.forEach(m => key(colorOf(m), label(m)));
    if (plant.length > 1) key("var(--text)", "Plant total");
    if (L.yesterday.total.length > 1) key("var(--muted)", `Yesterday's plant total (${fmt(L.yesterday.lbs)} lbs)`, true);

    // feed
    const feed = document.getElementById("liveFeed"); feed.replaceChildren();
    document.getElementById("liveFeedTitle").textContent = `Latest changes (${L.changes.length})`;
    if (!L.changes.length) { const p = document.createElement("p"); p.className = "muted"; p.textContent = "No changes logged yet."; feed.appendChild(p); return; }
    const tbl = document.createElement("table"); const thead = document.createElement("thead"); const hr = document.createElement("tr");
    [["When", ""], ["Machine", ""], ["Shift", ""], ["Entered", "num"], ["Job", ""]].forEach(([t, c]) => { const th = document.createElement("th"); th.textContent = t; if (c) th.className = c; hr.appendChild(th); });
    thead.appendChild(hr); tbl.appendChild(thead); const tb = document.createElement("tbody");
    L.changes.forEach(c => {
      const r = document.createElement("tr");
      const t0 = parseTs(c.t0), t1 = parseTs(c.t1), sameDay = t1.toDateString() === now.toDateString();
      r.appendChild(td(`${sameDay ? "" : DOW[t1.getDay()] + " " + MON[t1.getMonth()] + " " + t1.getDate() + ", "}${clock(t0)}–${clock(t1)}`, "when"));
      const m = td(null); const sw = document.createElement("span"); sw.className = "sw"; sw.style.background = colorOf(c.m); m.append(sw, label(c.m)); r.appendChild(m);
      r.appendChild(td(c.s || ""));
      const v = td(`${c.lbs > 0 ? "+" : ""}${fmt(c.lbs)} lbs` + (c.units ? ` · ${c.units > 0 ? "+" : ""}${c.units} bale${Math.abs(c.units) === 1 ? "" : "s"}` : "") + (c.correction ? " (removed)" : ""), "num" + (c.correction ? " corr" : ""));
      r.appendChild(v); r.appendChild(td(String(c.job)));
      tb.appendChild(r);
    });
    tbl.appendChild(tb); feed.appendChild(tbl);
  }

  function refresh() {
    if (!FEED_URL) return;
    fetch(FEED_URL + "?t=" + Date.now(), { cache: "no-store" }).then(r => r.ok ? r.json() : Promise.reject(r.status))
      .then(j => { LIVE = j; render(j); }).catch(() => { if (LIVE) render(LIVE); });
  }
  if (LIVE) render(LIVE); else document.getElementById("liveCard").hidden = !FEED_URL;
  refresh();
  setInterval(refresh, 5 * 60 * 1000);
  let raf = null; window.addEventListener("resize", () => { cancelAnimationFrame(raf); raf = requestAnimationFrame(() => LIVE && render(LIVE)); });
})();
"""


def live_script(initial: dict | None, feed_url: str = LIVE_FEED_URL) -> str:
    js = LIVE_JS.replace("__LIVE_FEED_URL__", feed_url).replace("__LIVE_INITIAL__", json.dumps(initial, separators=(",", ":")) if initial else "null")
    return "  <script>" + js + "  </script>"
