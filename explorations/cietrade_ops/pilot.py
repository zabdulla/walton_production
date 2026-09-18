"""Render the cieTrade operations pilot page from the agents' analyses.

    python3 explorations/cietrade_ops/pilot.py      # -> out/cietrade_ops_pilot.html (open in a browser)

Inputs (all under out/, gitignored): discovery.json from explore.py and
analysis/{inbound,orders,onhand,loads}.json written by the analysis passes.
Aggregates only; no lot- or order-level rows reach the page. No external
libraries: bars are plain HTML, every chart has a table view, light and dark.
"""
from __future__ import annotations

import html
import json
import re
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
AN = OUT / "analysis"
esc = html.escape
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]   # validated categorical order
SERIES_DARK = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
AGES = ["0-14", "15-30", "31-60", "61-120", "120+"]


def load(name: str) -> dict | None:
    p = AN / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None


def fmt(n, unit="") -> str:
    if n is None:
        return "–"
    n = float(n)
    s = f"{n/1e6:.2f}M" if abs(n) >= 1e6 else (f"{n/1e3:.0f}k" if abs(n) >= 1e4 else f"{n:,.0f}")
    return s + unit


def md_section(md_path: Path, heading_prefix: str) -> str:
    """Return the text under the first H2 whose title starts with heading_prefix."""
    if not md_path.exists():
        return ""
    txt = md_path.read_text()
    parts = re.split(r"^## ", txt, flags=re.M)
    for part in parts:
        if part.lower().startswith(heading_prefix.lower()):
            body = part.split("\n", 1)[1] if "\n" in part else ""
            return body.strip()
    return ""


# ---------- chart primitives (HTML bars; every chart also gets a table) ----------

def short(lbl: str) -> str:
    """Axis label: 2026-W13 -> W13, 2026-08 -> Aug 26, 2026-09-14 -> 9/14."""
    m = re.match(r"^(\d{4})-W(\d{2})$", lbl)
    if m:
        return f"W{m.group(2)}"
    m = re.match(r"^(\d{4})-(\d{2})$", lbl)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), 1).strftime("%b %y")
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", lbl)
    if m:
        return f"{int(m.group(2))}/{int(m.group(3))}"
    return lbl


def bars(rows: list[tuple[str, float]], unit: str = "", color: int = 0, height: int = 160, note: str = "") -> str:
    """Vertical bars over an ordered category (weeks, days). rows = [(label, value)]."""
    if not rows:
        return '<p class="muted">no data</p>'
    mx = max(v for _, v in rows) or 1
    n = len(rows)
    step = max(1, -(-n // 5))
    cols = []
    for i, (lbl, v) in enumerate(rows):
        h = max(2, round(100 * v / mx))
        show = i % step == 0 or i == n - 1
        cols.append(f'<div class="col" title="{esc(lbl)}: {fmt(v, unit)}"><div class="v" style="--h:{h}%;--c:{color}"></div>'
                    f'<span class="x">{esc(short(lbl)) if show else ""}</span></div>')
    table = "".join(f"<tr><td>{esc(l)}</td><td class='num'>{float(v):,.0f}</td></tr>" for l, v in rows)
    return (f'<div class="chart" style="--ch:{height}px">{"".join(cols)}</div>'
            + (f'<p class="muted small">{esc(note)}</p>' if note else "")
            + f'<details><summary>table</summary><table class="t"><tr><th>period</th><th class="num">value</th></tr>{table}</table></details>')


def stacked(rows: list[tuple[str, dict]], keys: list[str], unit: str = "", height: int = 180, note: str = "") -> str:
    """Stacked vertical bars. rows = [(label, {key: value})], keys in fixed color order."""
    if not rows:
        return '<p class="muted">no data</p>'
    tot = [sum(d.get(k, 0) for k in keys) for _, d in rows]
    mx = max(tot) or 1
    n = len(rows); step = max(1, -(-n // 5))
    cols = []
    for i, ((lbl, d), t) in enumerate(zip(rows, tot)):
        segs = "".join(f'<div class="seg" style="--h:{100 * d.get(k, 0) / mx:.2f}%;--c:{j}" title="{esc(lbl)} · {esc(k)}: {fmt(d.get(k, 0), unit)}"></div>'
                       for j, k in enumerate(keys) if d.get(k, 0))
        show = i % step == 0 or i == n - 1
        cols.append(f'<div class="col"><div class="stack">{segs}</div><span class="x">{esc(short(lbl)) if show else ""}</span></div>')
    legend = "".join(f'<span class="lg"><i style="--c:{j}"></i>{esc(k)}</span>' for j, k in enumerate(keys))
    head = "".join(f"<th class='num'>{esc(k)}</th>" for k in keys)
    body = "".join(f"<tr><td>{esc(l)}</td>" + "".join(f"<td class='num'>{float(d.get(k, 0)):,.0f}</td>" for k in keys) + f"<td class='num'>{t:,.0f}</td></tr>" for (l, d), t in zip(rows, tot))
    return (f'<div class="legend">{legend}</div><div class="chart" style="--ch:{height}px">{"".join(cols)}</div>'
            + (f'<p class="muted small">{esc(note)}</p>' if note else "")
            + f'<details><summary>table</summary><table class="t"><tr><th>period</th>{head}<th class="num">total</th></tr>{body}</table></details>')


def hbars(rows: list[tuple[str, float]], unit: str = "", color: int = 0, note: str = "") -> str:
    """Horizontal bars for a ranked category list, value labelled."""
    if not rows:
        return '<p class="muted">no data</p>'
    mx = max(v for _, v in rows) or 1
    out = "".join(f'<div class="hrow" title="{esc(l)}: {fmt(v, unit)}"><span class="hl">{esc(l)}</span><div class="hb"><i style="--w:{100 * v / mx:.1f}%;--c:{color}"></i></div><span class="hv">{fmt(v, unit)}</span></div>' for l, v in rows)
    return f'<div class="hbars">{out}</div>' + (f'<p class="muted small">{esc(note)}</p>' if note else "")


def hstacked(rows: list[tuple[str, dict]], keys: list[str], unit: str = "", note: str = "") -> str:
    if not rows:
        return '<p class="muted">no data</p>'
    tot = [sum(d.get(k, 0) for k in keys) for _, d in rows]
    mx = max(tot) or 1
    out = []
    for (l, d), t in zip(rows, tot):
        segs = "".join(f'<i style="--w:{100 * d.get(k, 0) / mx:.2f}%;--c:{j}" title="{esc(l)} · {esc(k)}: {fmt(d.get(k, 0), unit)}"></i>' for j, k in enumerate(keys) if d.get(k, 0))
        out.append(f'<div class="hrow"><span class="hl">{esc(l)}</span><div class="hb stackh">{segs}</div><span class="hv">{fmt(t, unit)}</span></div>')
    legend = "".join(f'<span class="lg"><i style="--c:{j}"></i>{esc(k)}</span>' for j, k in enumerate(keys))
    head = "".join(f"<th class='num'>{esc(k)}</th>" for k in keys)
    body = "".join(f"<tr><td>{esc(l)}</td>" + "".join(f"<td class='num'>{float(d.get(k, 0)):,.0f}</td>" for k in keys) + f"<td class='num'>{t:,.0f}</td></tr>" for (l, d), t in zip(rows, tot))
    return (f'<div class="legend">{legend}</div><div class="hbars">{"".join(out)}</div>' + (f'<p class="muted small">{esc(note)}</p>' if note else "")
            + f'<details><summary>table</summary><table class="t"><tr><th></th>{head}<th class="num">total</th></tr>{body}</table></details>')


def tiles(items: list[tuple[str, str, str]]) -> str:
    return '<div class="tiles">' + "".join(f'<div class="tile"><div class="tv">{esc(v)}</div><div class="tl">{esc(l)}</div><div class="ts">{esc(s)}</div></div>' for l, v, s in items) + "</div>"


def card(title: str, body: str, sub: str = "", anchor: str = "") -> str:
    return f'<section class="card" id="{esc(anchor)}"><h2>{esc(title)}</h2>' + (f'<p class="sub">{esc(sub)}</p>' if sub else "") + body + "</section>"


def panel(title: str, body: str, sub: str = "") -> str:
    return f'<div class="panel"><h3>{esc(title)}</h3>' + (f'<p class="muted small">{esc(sub)}</p>' if sub else "") + body + "</div>"


# ---------- sections ----------

def sec_findings(inb, ords, onh, loads) -> str:
    items = []
    if onh:
        pm = onh["monroe_totals"]["Plus Monroe Warehouse"]
        items.append(f"cieTrade says Plus Monroe holds {fmt(pm['PR']['lbs'])} lbs of feedstock and {fmt(pm['CJ']['lbs'])} lbs of finished goods, but posting a converting job does not relieve the lots it consumed: only a fraction of what production used ever left the books. Until receiving and production relieve lots, on-hand figures from the API are a paper number.")
    if inb:
        lp = inb["monroe_load_profile"]
        items.append(f"Monroe receiving is steady and weekday-only: {lp['loads']:,} loads in 400 days, median load {lp['net_per_load_med']:,} lbs, no weekend receipts. The (CONTAINER) pseudo-warehouse, not a physical site, is the largest 'warehouse' by loads.")
    if ords:
        items.append(f"Purchase orders link cleanly to receipts by order number: {ords['receipt_timing']['pos_with_receipt']:,} of {ords['_meta']['po_rows']:,} POs in 180 days have receipts, median {ords['receipt_timing']['first_receipt_lag_days']['median']:.0f} days from order to first load. {ords['open_po_over_14d_no_receipt_total']} open POs are over two weeks old with nothing received, which is the inbound due board.")
        items.append("Order quantities are almost never entered on PO headers (3% non-zero), so 'expected lbs' must come from order detail lines or supplier history, not the header.")
    if loads:
        hi = loads["highlights"]
        items.append(f"Monroe's export throughput is booked as purchases: {hi['containers']} containers ({fmt(hi['monroe_container_out_lbs_30d'])} lbs) in 30 days appear as receipts from supplier 'Walton logistics' into the (CONTAINER) warehouse, against {fmt(hi['monroe_in_lbs_30d'])} lbs received. Domestic outbound truckloads are not identifiable at all.")
        items.append(f"Posting is one-sided: receipts are {hi['receipt_ws_posted_share'] * 100:.0f}% posted, while {hi['inv_sale_ws_unposted_share'] * 100:.0f}% of inventory-sale worksheets sit in WORK, and one worksheet carries 44M lbs from a unit-of-measure slip. Weight totals from sales worksheets need that cleanup first.")
    return card("What the API can and cannot tell us", "<ul class='find'>" + "".join(f"<li>{esc(t)}</li>" for t in items) + "</ul>",
                "Six endpoints beyond converting jobs answer with usable data. The findings below are from the last 30 to 400 days, pulled 2026-09-18.", "findings")


def sec_discovery(disc) -> str:
    rows = []
    for e, r in sorted(disc["endpoints"].items(), key=lambda kv: (kv[1]["kind"] != "ok", -kv[1]["rows"])):
        if r["kind"] != "ok" and r.get("http") == 404:
            continue
        cls = "ok" if r["kind"] == "ok" else "bad"
        detail = f"{len(r['columns'])} columns" if r["kind"] == "ok" else (r.get("error") or "")
        win = ", ".join(f"{k}={v}" for k, v in r.get("params", {}).items())
        rows.append(f'<tr><td>{esc(e)}</td><td class="{cls}">{esc(r["kind"])}</td><td class="num">{r["rows"]:,}</td><td class="small muted">{esc(win)}</td><td class="small">{esc(detail[:120])}</td></tr>')
    return card("Endpoints that answer", f'<table class="t"><tr><th>endpoint</th><th>result</th><th class="num">rows</th><th>query</th><th></th></tr>{"".join(rows)}</table>',
                "Probed with the poller's credentials. Full parameter and field reference in API_REFERENCE.md.", "endpoints")


def sec_inbound(inb) -> str:
    if not inb:
        return ""
    wk_sup: dict[str, dict] = {}
    for r in inb["monroe_weekly_by_supplier"]:
        wk_sup.setdefault(r["wk"], {})[r["sup"]] = r["lbs"]
    top5 = [s["sup"] for s in inb["monroe_top_suppliers_26w"][:5]]
    keys = top5 + ["Other"]
    rows = []
    for wk in sorted(wk_sup):
        d = {k: 0 for k in keys}
        for s, v in wk_sup[wk].items():
            d[s if s in top5 else "Other"] += v
        rows.append((wk, d))
    wk_loads = {}
    for r in inb["weekly_by_warehouse"]:
        if r["wh"] == "Plus Monroe Warehouse":
            wk_loads[r["wk"]] = r["loads"]
    last14 = {}
    for r in inb["last_14_days_by_day_warehouse"]:
        if r["wh"] == "Plus Monroe Warehouse":
            last14[r["d"]] = last14.get(r["d"], 0) + r["lbs"]
    tare = [(t["grade"], t["med"] * 100) for t in sorted(inb["monroe_tare_pct_by_grade"], key=lambda t: -t["lbs"])[:10]]
    lp = inb["monroe_load_profile"]
    body = tiles([("loads, 400 days", f"{lp['loads']:,}", "Plus Monroe Warehouse"), ("net lbs received", fmt(lp['lbs']), "LBS lots only"),
                  ("median load", f"{lp['net_per_load_med']:,} lbs", f"{lp['lots_per_load_med']:.0f} lots per load"),
                  ("weekend loads", "0", "all receiving Mon–Fri"),
                  ("tare overall", f"{100 * inb['monroe_tare_pct_overall']['tare_lbs'] / inb['monroe_tare_pct_overall']['gross_lbs']:.1f}%", f"{inb['monroe_tare_pct_overall']['zero_tare_share'] * 100:.0f}% of lots record no tare")])
    body += '<div class="grid2">'
    body += panel("Net lbs received per week, by supplier", stacked(rows, keys, " lbs", note="Top five suppliers by 26-week volume; last week is partial."), "Plus Monroe Warehouse, 26 ISO weeks")
    body += panel("Loads per week", bars(sorted(wk_loads.items()), " loads", 0, 120), "distinct receiving worksheets")
    body += panel("Last 14 days, lbs per day", bars(sorted(last14.items()), " lbs", 2, 120), "what landed at Monroe each day")
    body += panel("Top grades received, 26 weeks", hbars([(g["grade"], g["lbs"]) for g in inb["monroe_top_grades_26w"][:10]], " lbs", 0), "feedstock mix")
    body += panel("Median tare % by grade", hbars(tare, "%", 3, note="tare ÷ gross on lots with gross > 0; loose grades run 25–30%"), "ten largest grades by lbs")
    body += panel("Monthly lbs", bars([(m["m"], m["lbs"]) for m in inb["monroe_monthly"]], " lbs", 0, 120), "13 months, current month to date")
    body += "</div>"
    return card("Inbound receiving at Monroe", body, "Source: ListInventory purchase-receipt lots (worksheet PR-*), received_date window of 400 days.", "inbound")


def sec_orders(ords) -> str:
    if not ords:
        return ""
    depts = sorted(ords["open_po_count_by_department"], key=lambda d: -ords["open_po_count_by_department"][d])
    age_rows = [(d, ords["open_po_age_buckets_by_department"][d]) for d in depts]
    buckets = ["0-7", "8-14", "15-30", "31-60", "60+"]
    nore = sorted(ords["open_po_over_14d_no_receipt_by_department"].items(), key=lambda kv: -kv[1])
    rc = ords["po_receipt_class_by_department"]
    rc_rows = [(d, rc[d]) for d in sorted(rc, key=lambda d: -sum(rc[d].values()))]
    wk = ords["pos_per_week_by_department_26w"]
    weeks = wk["week_start_monday"]
    bd = wk["by_department"] if "by_department" in wk else {}
    wdep = ["Walton Domestic", "Walton Logistics"]
    wk_rows = [(w, {d: (bd.get(d) or [0] * len(weeks))[i] for d in wdep}) for i, w in enumerate(weeks)] if bd else []
    rt = ords["receipt_timing"]
    body = tiles([("open POs", f"{sum(ords['open_po_count_by_department'].values()):,}", "all departments"),
                  ("open > 14 d, nothing received", f"{ords['open_po_over_14d_no_receipt_total']}", "the inbound due board"),
                  ("order → first load", f"{rt['first_receipt_lag_days']['median']:.0f} d median", f"p25 {rt['first_receipt_lag_days']['p25']:.0f} · p75 {rt['first_receipt_lag_days']['p75']:.0f} d"),
                  ("POs with a receipt", f"{rt['pos_with_receipt']:,} of {ords['_meta']['po_rows']:,}", f"{fmt(rt['total_receipt_lbs_matched'])} lbs matched"),
                  ("closed with no receipt", f"{rt['closed_pos_no_receipt']}", "direct / FAS containers never hit inventory")])
    body += '<div class="grid2">'
    body += panel("Open POs by age, per department", hstacked(age_rows, buckets, " POs"), "days since order date, as of 2026-09-18")
    body += panel("Open > 14 days with nothing received", hbars(nore, " POs", 7), "by department")
    body += panel("Receipt status of all POs, 180 days", hstacked(rc_rows, ["received", "partly", "nothing"], " POs", note="received = closed with receipts; partly = open with receipts; nothing = no receipt yet"))
    if wk_rows:
        body += panel("Walton POs raised per week", stacked(wk_rows, wdep, " POs", 140), "26 weeks")
    for d in wdep:
        w = ords["walton_departments"].get(d)
        if w:
            sup = sorted(w["top_suppliers"].items(), key=lambda kv: -kv[1])[:8]
            prod = sorted(w["top_products"].items(), key=lambda kv: -kv[1])[:8]
            body += panel(f"{d}: suppliers", hbars(sup, " POs", 0), f"{w['pos']} POs · {w['status'].get('OPEN', 0)} open")
            body += panel(f"{d}: products on the PO", hbars(prod, " POs", 2), "header product field; grade detail lives on order lines")
    ports = sorted(ords["so_destination_port"].items(), key=lambda kv: -kv[1])[:8]
    via = sorted(ords["so_ship_via"].items(), key=lambda kv: -kv[1])[:8]
    body += panel("Sales orders: destination ports", hbars(ports, " SOs", 4), f"{sum(ords['so_status'].values())} SOs in 30 days, {ords['so_status'].get('OPEN', 0)} open")
    body += panel("Sales orders: ship via", hbars(via, " SOs", 5), f"{ords['so_ref_number_matches_po_order_number']['count']} of {ords['so_ref_number_matches_po_order_number']['of']} SOs reference a PO number (back-to-back trades)")
    body += "</div>"
    return card("Purchase and sales orders", body, "Source: ListOrders headers, 180 days of POs and 30 days of SOs, joined to receipt lots by order number.", "orders")


def sec_onhand(onh) -> str:
    if not onh:
        return ""
    feed = onh["feedstock_on_hand_by_grade"][:12]
    fg = onh["finished_goods_on_hand_by_grade"]
    dos = onh["days_of_supply"]
    tot = dos["total_weekly_input_all_machines"]
    warn = ('<div class="warn">Read with care: converting jobs consumed about 10.7M lbs this year, but the API shows only 5.8M lbs relieved from feedstock lots and 1.4M lbs from finished-goods lots over 400 days. '
            'On-hand and days-of-supply figures are therefore upper bounds until jobs and shipments relieve lots in cieTrade.</div>')
    body = warn + tiles([("feedstock on hand", fmt(onh["monroe_totals"]["Plus Monroe Warehouse"]["PR"]["lbs"]) + " lbs", "Plus Monroe, per cieTrade"),
                         ("finished goods on hand", fmt(onh["monroe_totals"]["Plus Monroe Warehouse"]["CJ"]["lbs"] + onh["monroe_totals"]["Monroe Processing Warehouse"]["CJ"]["lbs"]) + " lbs", "both Monroe sites, per cieTrade"),
                         ("machines consume", fmt(sum(tot.values()) / len(tot)) + " lbs/wk", "8-week average, posted jobs"),
                         ("feedstock older than 120 d", fmt(onh["age_distribution_monroe"]["PR"]["120+"]["lbs"]) + " lbs", f"{onh['age_distribution_monroe']['PR']['120+']['lots']:,} lots, per cieTrade")])
    body += '<div class="grid2">'
    body += panel("Feedstock on hand by grade and age", hstacked([(g["grade"], g["age_lbs"]) for g in feed], AGES, " lbs"), "twelve largest grades; age in days since receipt")
    fgrows = [(f"{g['grade']} · {wh.split()[0]}", g["age_lbs"]) for wh, lst in fg.items() for g in lst[:6]]
    fgrows.sort(key=lambda r: -sum(r[1].values()))
    body += panel("Finished goods on hand by grade and age", hstacked(fgrows[:12], AGES, " lbs"), "converting-job output lots")
    body += panel("Weekly feedstock consumed, all machines", bars(sorted(tot.items()), " lbs", 2, 120), "posted converting jobs, input lbs")
    def dos_row(g: dict) -> str:
        grades = ", ".join(g["feedstock_grades"][:4]) + (" …" if len(g["feedstock_grades"]) > 4 else "")
        days = "" if g.get("days_of_supply") is None else f"{g['days_of_supply']:,.0f}"
        return (f"<tr><td>{esc(g['machine_group'])}</td><td class='small'>{esc(grades)}</td><td class='num'>{fmt(g.get('on_hand_lbs'))}</td>"
                f"<td class='num'>{fmt(g.get('avg_weekly_consumption_lbs'))}</td><td class='num'>{days}</td><td class='small'>{esc(str(g.get('confidence', '')))}</td></tr>")
    trs = "".join(dos_row(g) for g in dos["groups"])
    body += "</div>"
    body += panel("Days of supply by machine group", f'<table class="t"><tr><th>machine group</th><th>feedstock grades</th><th class="num">on hand lbs</th><th class="num">lbs / week</th><th class="num">days</th><th>confidence</th></tr>{trs}</table>',
                  "on hand ÷ daily consumption; grade-to-machine mapping inferred from lot depletion and job outputs")
    return card("On hand and days of supply", body, "Source: ListInventory lots with quantity_on_hand > 0 at the Monroe sites, and posted converting jobs from data/cietrade.", "onhand")


def sec_loads(loads) -> str:
    if not loads:
        return card("Loads and shipments", '<p class="muted">analysis/loads.json not present yet</p>', anchor="loads")
    hi = loads["highlights"]
    mi, mo = loads["monroe_in"], loads["monroe_out"]["container_stuffing"]
    weeks = sorted(set(mi["by_week"]) | set(mo["by_week"]))
    wk_in = [(w, mi["by_week"].get(w, {}).get("lbs", 0)) for w in weeks]
    wk_out = [(w, mo["by_week"].get(w, {}).get("lbs", 0)) for w in weeks]
    wal = loads["walton"]["by_dept_week"]
    wdep = ["Walton Domestic", "Walton Logistics"]
    wweeks = sorted({k.split("|")[1] for k in wal})
    wrows = [(w, {d: wal.get(f"{d}|{w}", {}).get("lbs", 0) for d in wdep}) for w in wweeks]
    lt = loads["lead_times"]
    po_lt = sorted(((d, v["med"]) for d, v in lt["po_to_receipt"].items() if v["n"] >= 5), key=lambda kv: kv[1])
    so_lt = sorted(((d, v["med"]) for d, v in lt["so_to_ship"].items() if v["n"] >= 5), key=lambda kv: kv[1])
    st = loads["ws_status"]
    body = tiles([("received at Monroe, 30 d", fmt(hi["monroe_in_lbs_30d"]) + " lbs", f"{mi['total']['ws']} loads, {100 * mi['total']['posted_lbs'] / mi['total']['lbs']:.0f}% posted"),
                  ("stuffed into containers", fmt(hi["monroe_container_out_lbs_30d"]) + " lbs", f"{hi['containers']} containers, {hi['avg_container_lbs']:,} lbs each"),
                  ("peak week", hi["peak_week"], f"in {fmt(hi['peak_week_in_lbs'])} · out {fmt(hi['peak_week_out_lbs'])} lbs"),
                  ("worksheets unposted", f"{st['WORK']['n']} of {sum(v['n'] for v in st.values())}", f"{hi['inv_sale_ws_unposted_share'] * 100:.0f}% of inventory sales still in WORK"),
                  ("open POs with a load in 30 d", f"{loads['open_po_hdr_vs_receipts']['with_receipt_30d']} of {loads['open_po_hdr_vs_receipts']['open_pos']}", f"median open age {loads['open_po_hdr_vs_receipts']['open_po_age_days_median']} d")])
    body += '<div class="grid2">'
    body += panel("Lbs received at Monroe per week", bars(wk_in, " lbs", 0, 130), "trading lines shipped to Plus Monroe Warehouse; a single 1.0M lb bulk adjustment line excluded")
    body += panel("Lbs stuffed into export containers per week", bars(wk_out, " lbs", 1, 130), "receipts from supplier 'Walton logistics' into (CONTAINER): how outbound is booked")
    body += panel("Walton departments, lbs per week", stacked(wrows, wdep, " lbs", 130), "all worksheets in the two Walton departments")
    body += panel("PO order date to receipt, median days", hbars(po_lt, " d", 2), "departments with 5+ loads in 30 days")
    body += panel("SO order date to shipment, median days", hbars(so_lt, " d", 4), "departments with 5+ shipments in 30 days")
    body += "</div>"
    return card("Loads and shipments", body, "Source: ListWorksheets, TradingInquiry and order lines, last 30 days. A worksheet is one load; PostDate present means posted (invoiced).", "loads")


def sec_notes() -> str:
    parts = []
    for name, title in (("inbound", "Inbound"), ("orders", "Orders"), ("onhand", "On hand"), ("loads", "Loads")):
        p = AN / f"{name}.md"
        if not p.exists():
            continue
        views = md_section(p, "Five") or md_section(p, "Dashboard")
        caveats = md_section(p, "Data") or md_section(p, "Caveats")
        surprises = md_section(p, "Three") or md_section(p, "Surprises")
        parts.append(f"<details class='notes'><summary>{esc(title)}: proposed views, caveats, surprises</summary>"
                     + "".join(f"<h4>{esc(h)}</h4><pre>{esc(t)}</pre>" for h, t in (("Views", views), ("Data-quality caveats", caveats), ("Surprises", surprises)) if t) + "</details>")
    return card("Analyst notes", "".join(parts), "Verbatim from the analysis passes; the raw aggregates are in out/analysis/.", "notes")


CSS = """
:root{--page:#f9f9f7;--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;--warnbg:#fff4e0;--warnink:#6b4a00;
 --s0:#2a78d6;--s1:#eb6834;--s2:#1baf7a;--s3:#eda100;--s4:#e87ba4;--s5:#008300;--s6:#4a3aa7;--s7:#e34948}
@media(prefers-color-scheme:dark){:root{--page:#0d0d0d;--surface:#1a1a19;--ink:#fff;--ink2:#c3c2b7;--muted:#898781;--grid:#2c2c2a;--axis:#383835;--warnbg:#33260a;--warnink:#fab219;
 --s0:#3987e5;--s1:#d95926;--s2:#199e70;--s3:#c98500;--s4:#d55181;--s5:#008300;--s6:#9085e9;--s7:#e66767}}
*{box-sizing:border-box}body{margin:0;background:var(--page);color:var(--ink);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding:16px}
main{max-width:1240px;margin:0 auto}h1{font-size:24px;margin:0 0 4px}h2{font-size:18px;margin:0 0 4px}h3{font-size:13px;margin:0 0 6px;color:var(--ink2)}h4{font-size:12px;margin:12px 0 4px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.sub{color:var(--ink2);margin:0 0 12px;font-size:13px}.muted{color:var(--muted)}.small{font-size:12px}.num{text-align:right;font-variant-numeric:tabular-nums}
.card{background:var(--surface);border:1px solid var(--grid);border-radius:12px;padding:16px 18px;margin:14px 0}.panel{background:var(--surface);border:1px solid var(--grid);border-radius:10px;padding:12px 14px;min-width:0}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:12px}
nav{display:flex;gap:14px;flex-wrap:wrap;margin:8px 0 0}nav a{color:var(--s0);text-decoration:none;font-size:13px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin:8px 0 14px}.tile{border:1px solid var(--grid);border-radius:10px;padding:10px 12px}.tv{font-size:22px;font-weight:600;font-variant-numeric:tabular-nums}.tl{font-size:12px;color:var(--ink2)}.ts{font-size:11px;color:var(--muted)}
.chart{display:flex;align-items:flex-end;gap:2px;height:var(--ch,160px);border-bottom:1px solid var(--axis);padding-top:6px;margin-bottom:18px}.col{flex:1;display:flex;flex-direction:column;align-items:stretch;justify-content:flex-end;height:100%;min-width:0;position:relative}
.col .v{height:var(--h);background:var(--s0);border-radius:4px 4px 0 0;min-height:2px}.col .v[style*="--c:2"]{background:var(--s2)}.col .v[style*="--c:3"]{background:var(--s3)}
.col .stack{display:flex;flex-direction:column-reverse;height:100%;gap:2px;justify-content:flex-start}.seg{height:var(--h);border-radius:2px}
.col .x{position:absolute;top:100%;font-size:10px;color:var(--muted);white-space:nowrap;margin-top:3px}
[style*="--c:0"].seg,.lg i[style*="--c:0"],.hb i[style*="--c:0"]{background:var(--s0)}[style*="--c:1"].seg,.lg i[style*="--c:1"],.hb i[style*="--c:1"]{background:var(--s1)}[style*="--c:2"].seg,.lg i[style*="--c:2"],.hb i[style*="--c:2"]{background:var(--s2)}
[style*="--c:3"].seg,.lg i[style*="--c:3"],.hb i[style*="--c:3"]{background:var(--s3)}[style*="--c:4"].seg,.lg i[style*="--c:4"],.hb i[style*="--c:4"]{background:var(--s4)}[style*="--c:5"].seg,.lg i[style*="--c:5"],.hb i[style*="--c:5"]{background:var(--s5)}
[style*="--c:6"].seg,.lg i[style*="--c:6"],.hb i[style*="--c:6"]{background:var(--s6)}[style*="--c:7"].seg,.lg i[style*="--c:7"],.hb i[style*="--c:7"]{background:var(--s7)}
.legend{display:flex;gap:12px;flex-wrap:wrap;margin:0 0 6px}.lg{font-size:12px;color:var(--ink2);display:inline-flex;align-items:center;gap:5px}.lg i{width:10px;height:10px;border-radius:3px;display:inline-block}
.hbars{display:flex;flex-direction:column;gap:4px;margin-top:14px}.hrow{display:grid;grid-template-columns:minmax(120px,32%) 1fr 70px;align-items:center;gap:8px;font-size:12px}.hl{color:var(--ink2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.hb{height:12px;display:flex;gap:2px}.hb i{display:block;height:100%;width:var(--w);border-radius:0 4px 4px 0;min-width:2px}.hb.stackh i{border-radius:2px}.hv{text-align:right;font-variant-numeric:tabular-nums;color:var(--ink2)}
table.t{border-collapse:collapse;width:100%;font-size:12px;margin-top:6px}table.t th,table.t td{padding:4px 6px;border-bottom:1px solid var(--grid);text-align:left;vertical-align:top}table.t th{color:var(--muted);font-weight:500}
details{margin-top:8px;font-size:12px}summary{color:var(--muted);cursor:pointer}.ok{color:var(--s5);font-weight:600}.bad{color:var(--s7)}
.warn{background:var(--warnbg);color:var(--warnink);border-radius:8px;padding:10px 12px;font-size:13px;margin:6px 0 12px}
ul.find{margin:8px 0 0 18px;padding:0}ul.find li{margin:6px 0}
.notes pre{white-space:pre-wrap;font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace;color:var(--ink2);background:var(--page);padding:10px;border-radius:8px}
"""


def main() -> None:
    disc = json.loads((OUT / "discovery.json").read_text()) if (OUT / "discovery.json").exists() else {"endpoints": {}}
    inb, ords, onh, loads = load("inbound"), load("orders"), load("onhand"), load("loads")
    parts = [f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>cieTrade operations pilot</title><style>{CSS}</style></head><body><main>',
             f'<h1>cieTrade operations pilot</h1><p class="sub">Inbound, orders and inventory for the Monroe sites and Walton Logistics, straight from the cieTrade API. Built {datetime.now():%b %-d, %Y %-I:%M %p} · local only, nothing here is published.</p>',
             '<nav><a href="#findings">Findings</a><a href="#inbound">Inbound</a><a href="#orders">Orders</a><a href="#onhand">On hand</a><a href="#loads">Loads</a><a href="#endpoints">Endpoints</a><a href="#notes">Notes</a></nav>',
             sec_findings(inb, ords, onh, loads), sec_inbound(inb), sec_orders(ords), sec_onhand(onh), sec_loads(loads), sec_discovery(disc), sec_notes(),
             "</main></body></html>"]
    OUT.mkdir(exist_ok=True)
    out = OUT / "cietrade_ops_pilot.html"
    out.write_text("".join(parts))
    print(f"wrote {out} ({out.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
