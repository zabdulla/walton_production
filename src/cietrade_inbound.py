"""Inbound dashboard from cieTrade purchase-receipt worksheets.

In cieTrade a purchase order is a promise; the PR worksheet is the receipt, with
the receiver's gross / tare / net per grade line. TradingInquiry returns those
lines (one per grade per worksheet) with the PO number and PO date, supplier,
receiving warehouse, product, units, price and posting status. This module:

  1. fetches TradingInquiry (Status=ALL, DateType=SHIP, last N days) and keeps
     the PR-* lines, plus every OPEN purchase order (ListOrders, Source=PO);
  2. normalises them (KG -> lbs, EA/ST flagged, tare %, PO -> receipt lead days);
  3. writes a self-contained page with client-side filters (warehouse,
     department, window): weekly lbs by supplier, loads per day, grade mix,
     supplier table, the inbound due board (open POs with nothing received),
     the receipts log and data flags.

    python3 src/cietrade_inbound.py                    # -> reports/inbound.html (local only)
    python3 src/cietrade_inbound.py --days 60 --out /path/page.html
    python3 src/cietrade_inbound.py --from-samples explorations/cietrade_ops/out/samples   # offline, from probe samples

Nothing is written to cieTrade. reports/ is gitignored: supplier names and volumes
stay off the public site unless the page is deliberately copied into docs/.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cietrade_api as api  # noqa: E402
from config import PROJECT_ROOT  # noqa: E402

DEFAULT_OUT = PROJECT_ROOT / "reports" / "inbound.html"
KG_TO_LBS = 2.20462
WEIGHT_UOMS = {"LBS", "KG"}
DEFAULT_WAREHOUSE = "Plus Monroe Warehouse"


# ---------------------------------------------------------------- fetch

def _get(creds: dict, endpoint: str, **params: Any) -> list[dict]:
    q = {"UserID": creds["user_id"]}
    q.update({k: str(v) for k, v in params.items() if v not in (None, "")})
    url = f"{creds['base_url'].rstrip('/')}/{endpoint}?{urllib.parse.urlencode(q)}"
    body = api._request(url, {"Authorization": f"Bearer {creds['api_key']}", "Accept": "application/json"})
    data = json.loads(body)
    if isinstance(data, dict):
        data = [data]
    if data and isinstance(data[0], dict) and ("ERROR" in data[0] or "INPUT ERROR" in data[0]):
        raise api.CieTradeError(str(data[0].get("ERROR") or data[0].get("INPUT ERROR")))
    return data


def fetch(days: int = 91, creds: dict | None = None) -> tuple[list[dict], list[dict]]:
    """(trading lines for PR worksheets received in the window, OPEN purchase orders of the last year)."""
    creds = creds or api.load_credentials()
    today = date.today()
    lines = _get(creds, "TradingInquiry", Status="ALL", DateType="SHIP",
                 DateFrom=(today - timedelta(days=days)).strftime("%m/%d/%Y"), DateTo=today.strftime("%m/%d/%Y"))
    pos = _get(creds, "ListOrders", Source="PO", Status="OPEN",
               DateFrom=(today - timedelta(days=365)).strftime("%m/%d/%Y"), DateTo=today.strftime("%m/%d/%Y"))
    return [r for r in lines if str(r.get("WorksheetNo", "")).startswith("PR-")], pos


def load_samples(folder: Path) -> tuple[list[dict], list[dict]]:
    """Offline input from the exploration probe's sample files."""
    ti = next(iter(sorted(folder.glob("TradingInquiry*.json"))), None)
    po = next(iter(sorted(folder.glob("ListOrders*Source-PO*Status-OPEN*.json"))), None) or next(iter(sorted(folder.glob("ListOrders*PO*.json"))), None)
    if not ti or not po:
        raise FileNotFoundError(f"need TradingInquiry*.json and ListOrders*PO*.json in {folder}")
    lines = [r for r in json.loads(ti.read_text()) if str(r.get("WorksheetNo", "")).startswith("PR-")]
    pos = [p for p in json.loads(po.read_text()) if p.get("status") == "OPEN"]
    return lines, pos


# ---------------------------------------------------------------- model

def _f(v: Any) -> float:
    try:
        return float(str(v).replace(",", "").strip()) if v not in (None, "") else 0.0
    except ValueError:
        return 0.0


def _d(v: Any) -> str:
    s = str(v or "")[:10]
    return s if len(s) == 10 and s[4] == "-" else ""


EXCLUDE_GRADES = ("Processing Input",)   # bulk processing adjustments booked as PR lines, not loads


def normalize_lines(lines: list[dict]) -> pd.DataFrame:
    """One row per receipt grade line, weights in lbs, with flags. Bulk adjustment lines are dropped."""
    rows = []
    for r in lines:
        if str(r.get("ProductName") or "").strip().startswith(EXCLUDE_GRADES):
            continue
        uom = str(r.get("PWeightUOM") or "").strip().upper()
        factor = KG_TO_LBS if uom == "KG" else 1.0
        weighed = uom in WEIGHT_UOMS
        gross, tare, net = (_f(r.get("GrossWt")) * factor, _f(r.get("TareWt")) * factor, _f(r.get("NetWt")) * factor) if weighed else (0.0, 0.0, 0.0)
        if weighed and net == 0:
            net = _f(r.get("PWeight")) * factor
        rec_date, po_date = _d(r.get("ShippingDt")), _d(r.get("PoOrderDt"))
        lead = (datetime.strptime(rec_date, "%Y-%m-%d") - datetime.strptime(po_date, "%Y-%m-%d")).days if rec_date and po_date else None
        flags = []
        if not weighed:
            flags.append(f"uom {uom or '?'}")
        if weighed and gross <= 0:
            flags.append("no gross")
        if weighed and gross > 0 and net > gross + 1:
            flags.append("net > gross")
        if weighed and gross > 0 and tare == 0:
            flags.append("no tare")
        if not r.get("PO"):
            flags.append("no PO")
        if lead is not None and lead < 0:
            flags.append("received before PO")
        rows.append(dict(ws=str(r.get("WorksheetNo")), date=rec_date, dept=str(r.get("Department") or ""), supplier=str(r.get("Supplier") or "").strip(),
                         warehouse=str(r.get("ShipTo") or "").strip(), grade=str(r.get("ProductName") or "").strip(), gross=round(gross), tare=round(tare), net=round(net),
                         units=int(_f(r.get("Units"))), unit_type=str(r.get("UnitType") or ""), uom=uom, po=str(r.get("PO") or ""), po_date=po_date, lead=lead,
                         posted=str(r.get("Status") or "").upper() == "INVOICED", price=_f(r.get("PPrice")), amount=_f(r.get("PAmount")),
                         freight=_f(r.get("FreightExp")), ship_via=str(r.get("POShipVia") or ""), buyer=str(r.get("Buy-Rep") or ""), equip=str(r.get("EquipNo") or ""),
                         flags=flags))
    df = pd.DataFrame(rows)
    return df.sort_values(["date", "ws"]).reset_index(drop=True) if len(df) else df


def loads_from_lines(df: pd.DataFrame) -> pd.DataFrame:
    """One row per PR worksheet."""
    if df.empty:
        return df
    g = df.groupby("ws", sort=False)
    out = g.agg(date=("date", "max"), dept=("dept", "first"), supplier=("supplier", "first"), warehouse=("warehouse", "first"),
                gross=("gross", "sum"), tare=("tare", "sum"), net=("net", "sum"), units=("units", "sum"), lines=("grade", "size"),
                po=("po", "first"), po_date=("po_date", "first"), lead=("lead", "first"), posted=("posted", "all"),
                amount=("amount", "sum"), freight=("freight", "sum"), ship_via=("ship_via", "first"), buyer=("buyer", "first")).reset_index()
    out["grades"] = g["grade"].apply(lambda s: ", ".join(dict.fromkeys(x for x in s if x))).values
    out["flags"] = g["flags"].apply(lambda s: sorted({f for fl in s for f in fl})).values
    out["tare_pct"] = out.apply(lambda r: round(100 * r.tare / r.gross, 1) if r.gross > 0 else None, axis=1)
    return out.sort_values("date", ascending=False).reset_index(drop=True)


def due_board(pos: list[dict], loads: pd.DataFrame, as_of: date) -> list[dict]:
    """OPEN purchase orders with no receipt in the window, oldest first."""
    seen = set(loads["po"].astype(str)) if len(loads) else set()
    last_receipt = loads.groupby("po")["date"].max().to_dict() if len(loads) else {}
    out = []
    for p in pos:
        po = str(p.get("order_number") or "")
        od = _d(p.get("order_date"))
        age = (as_of - datetime.strptime(od, "%Y-%m-%d").date()).days if od else None
        out.append(dict(po=po, supplier=str(p.get("account_name") or "").strip(), dept=str(p.get("department") or ""), product=str(p.get("product") or "").strip(),
                        order_date=od, age=age, ship_via=str(p.get("ship_via") or ""), warehouse=str(p.get("UDF1") or "").strip(), freight=str(p.get("UDF2") or "").strip(),
                        received=po in seen, last_receipt=last_receipt.get(po, ""), delivery=_d(p.get("delivery_date"))))
    return sorted(out, key=lambda r: (r["received"], -(r["age"] or 0)))


def build_payload(lines: list[dict], pos: list[dict], as_of: date | None = None, days: int = 91) -> dict:
    as_of = as_of or date.today()
    df = normalize_lines(lines)
    loads = loads_from_lines(df)
    warehouses = sorted(loads["warehouse"].unique().tolist()) if len(loads) else []
    depts = sorted(loads["dept"].unique().tolist()) if len(loads) else []
    excluded = sum(1 for r in lines if str(r.get("ProductName") or "").strip().startswith(EXCLUDE_GRADES))
    return {
        "as_of": as_of.isoformat(), "days": days, "excluded_adjustment_lines": excluded, "window_from": (as_of - timedelta(days=days)).isoformat(),
        "default_warehouse": DEFAULT_WAREHOUSE if DEFAULT_WAREHOUSE in warehouses else (warehouses[0] if warehouses else ""),
        "warehouses": warehouses, "departments": depts,
        "lines": df.to_dict("records"),
        "loads": loads.to_dict("records"),
        "due": due_board(pos, loads, as_of),
    }


# ---------------------------------------------------------------- page

CSS = """
:root{--page:#f9f9f7;--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;--warnbg:#fff4e0;--warnink:#6b4a00;--bad:#d03b3b;--good:#0ca30c;
 --s0:#2a78d6;--s1:#eb6834;--s2:#1baf7a;--s3:#eda100;--s4:#e87ba4;--s5:#008300;--s6:#4a3aa7;--s7:#e34948;--s8:#898781}
@media(prefers-color-scheme:dark){:root{--page:#0d0d0d;--surface:#1a1a19;--ink:#fff;--ink2:#c3c2b7;--muted:#898781;--grid:#2c2c2a;--axis:#383835;--warnbg:#33260a;--warnink:#fab219;
 --s0:#3987e5;--s1:#d95926;--s2:#199e70;--s3:#c98500;--s4:#d55181;--s5:#008300;--s6:#9085e9;--s7:#e66767;--s8:#898781}}
*{box-sizing:border-box}body{margin:0;background:var(--page);color:var(--ink);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;padding:16px}
main{max-width:1280px;margin:0 auto}h1{font-size:24px;margin:0 0 2px}h2{font-size:17px;margin:0 0 10px}h3{font-size:13px;margin:0 0 6px;color:var(--ink2)}
.sub{color:var(--ink2);margin:0 0 10px;font-size:13px}.muted{color:var(--muted)}.small{font-size:12px}.num{text-align:right;font-variant-numeric:tabular-nums}
.filters{display:flex;gap:14px;flex-wrap:wrap;align-items:center;margin:10px 0 14px}.filters label{font-size:12px;color:var(--muted);display:inline-flex;gap:6px;align-items:center}
select{font:inherit;font-size:13px;padding:6px 8px;border:1px solid var(--grid);border-radius:8px;background:var(--surface);color:var(--ink)}
.card{background:var(--surface);border:1px solid var(--grid);border-radius:12px;padding:14px 16px;margin:12px 0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:12px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin:0 0 12px}.tile{background:var(--surface);border:1px solid var(--grid);border-radius:10px;padding:10px 12px}.tv{font-size:22px;font-weight:600;font-variant-numeric:tabular-nums}.tl{font-size:12px;color:var(--ink2)}.ts{font-size:11px;color:var(--muted)}
.chart{display:flex;align-items:flex-end;gap:2px;height:150px;border-bottom:1px solid var(--axis);margin-bottom:20px;padding-top:4px}.col{flex:1;position:relative;height:100%;display:flex;flex-direction:column-reverse;gap:2px;min-width:0}
.col i{display:block;border-radius:2px;min-height:1px}.col .x{position:absolute;top:100%;margin-top:3px;font-size:10px;color:var(--muted);white-space:nowrap}
.legend{display:flex;gap:12px;flex-wrap:wrap;margin:0 0 6px}.lg{font-size:12px;color:var(--ink2);display:inline-flex;align-items:center;gap:5px}.lg i{width:10px;height:10px;border-radius:3px;display:inline-block}
.hbars{display:flex;flex-direction:column;gap:4px}.hrow{display:grid;grid-template-columns:minmax(120px,34%) 1fr 74px;align-items:center;gap:8px;font-size:12px}.hl{color:var(--ink2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.hb{height:12px}.hb i{display:block;height:100%;border-radius:0 4px 4px 0;min-width:2px}.hv{text-align:right;font-variant-numeric:tabular-nums;color:var(--ink2)}
table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:5px 6px;border-bottom:1px solid var(--grid);text-align:left;vertical-align:top}th{color:var(--muted);font-weight:500;white-space:nowrap}.scroll{overflow:auto;max-height:520px}
.tag{display:inline-block;font-size:11px;padding:1px 6px;border-radius:6px;background:var(--warnbg);color:var(--warnink);margin:0 2px 2px 0}.no{color:var(--bad)}.yes{color:var(--good)}
.warn{background:var(--warnbg);color:var(--warnink);border-radius:8px;padding:8px 12px;font-size:13px;margin:0 0 12px}
"""

JS = r"""
const P = window.__P; const $ = id => document.getElementById(id);
const C = ['s0','s1','s2','s3','s4','s5','s6','s7','s8'];
const fmt = (n, u='') => n==null ? '–' : (Math.abs(n)>=1e6 ? (n/1e6).toFixed(2)+'M' : Math.abs(n)>=1e4 ? Math.round(n/1e3)+'k' : Math.round(n).toLocaleString()) + u;
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const iso = d => d.toISOString().slice(0,10);
const monday = s => { const d = new Date(s+'T12:00:00'); d.setDate(d.getDate() - ((d.getDay()+6)%7)); return iso(d); };
function state(){ return { wh: $('wh').value, dept: $('dept').value, weeks: +$('win').value, age: +$('age').value }; }
function filt(rows, s){ const from = iso(new Date(new Date(P.as_of+'T12:00:00') - (s.weeks*7-1)*864e5)); return rows.filter(r => (!s.wh || r.warehouse===s.wh) && (!s.dept || r.dept===s.dept) && r.date >= from); }
function stacked(el, rows, keys, unit){
  const tot = rows.map(([,d]) => keys.reduce((a,k)=>a+(d[k]||0),0)), mx = Math.max(1,...tot), step = Math.max(1, Math.ceil(rows.length/6));
  el.innerHTML = '<div class="legend">'+keys.map((k,j)=>`<span class="lg"><i style="background:var(--${C[j]})"></i>${esc(k)}</span>`).join('')+'</div><div class="chart">'+
    rows.map(([l,d],i)=>'<div class="col">'+keys.map((k,j)=>d[k]?`<i style="height:${100*d[k]/mx}%;background:var(--${C[j]})" title="${esc(l)} · ${esc(k)}: ${fmt(d[k],unit)}"></i>`:'').join('')+`<span class="x">${(i%step==0||i==rows.length-1)?esc(l.slice(5)):''}</span></div>`).join('')+'</div>';
}
function bars(el, rows, unit, color){ const mx = Math.max(1,...rows.map(r=>r[1])), step = Math.max(1, Math.ceil(rows.length/7));
  el.innerHTML = '<div class="chart">'+rows.map(([l,v],i)=>`<div class="col"><i style="height:${100*v/mx}%;background:var(--${color})" title="${esc(l)}: ${fmt(v,unit)}"></i><span class="x">${(i%step==0||i==rows.length-1)?esc(l.slice(5)):''}</span></div>`).join('')+'</div>'; }
function hbars(el, rows, unit, color){ const mx = Math.max(1,...rows.map(r=>r[1]));
  el.innerHTML = '<div class="hbars">'+rows.map(([l,v])=>`<div class="hrow" title="${esc(l)}: ${fmt(v,unit)}"><span class="hl">${esc(l)}</span><div class="hb"><i style="width:${100*v/mx}%;background:var(--${color})"></i></div><span class="hv">${fmt(v,unit)}</span></div>`).join('')+'</div>'; }
const med = a => { if(!a.length) return null; const s=[...a].sort((x,y)=>x-y), m=s.length>>1; return s.length%2? s[m] : (s[m-1]+s[m])/2; };
function render(){
  const s = state(), L = filt(P.loads, s), N = filt(P.lines, s);
  const net = L.reduce((a,r)=>a+r.net,0), unposted = L.filter(r=>!r.posted).length;
  const thisWeek = monday(P.as_of), lastWeek = iso(new Date(new Date(thisWeek+'T12:00:00')-7*864e5));
  const wk = w => L.filter(r=>monday(r.date)===w);
  const dueAll = P.due.filter(d => !d.received && (!s.dept || d.dept===s.dept) && (!s.wh || !d.warehouse || d.warehouse===s.wh));
  const due = dueAll.filter(d => (d.age||0) <= s.age), stale = dueAll.filter(d => (d.age||0) > 90).length;
  $('tiles').innerHTML = [[`${L.length}`,'loads received',`${s.weeks} weeks · ${fmt(net,' lbs')}`],[`${wk(thisWeek).length}`,'this week',`${fmt(wk(thisWeek).reduce((a,r)=>a+r.net,0),' lbs')} · last week ${wk(lastWeek).length} / ${fmt(wk(lastWeek).reduce((a,r)=>a+r.net,0),' lbs')}`],
    [fmt(L.length?net/L.length:0,' lbs'),'average load','net, weighed loads'],[`${unposted}`,'unposted worksheets','still in WORK'],[`${due.length}`,'open POs, nothing received',`${due.filter(d=>(d.age||0)>14).length} older than 14 days · ${stale} stale (> 90 d)`]]
    .map(([v,l,x])=>`<div class="tile"><div class="tv">${v}</div><div class="tl">${l}</div><div class="ts">${x}</div></div>`).join('');
  // weekly by supplier
  const supTot = {}; N.forEach(r=>supTot[r.supplier]=(supTot[r.supplier]||0)+r.net);
  const top = Object.entries(supTot).sort((a,b)=>b[1]-a[1]).slice(0,5).map(e=>e[0]); const keys = [...top, 'Other'];
  const byW = {}; N.forEach(r=>{ const w=monday(r.date); byW[w]=byW[w]||{}; const k = top.includes(r.supplier)?r.supplier:'Other'; byW[w][k]=(byW[w][k]||0)+r.net; });
  stacked($('weekly'), Object.keys(byW).sort().map(w=>[w,byW[w]]), keys, ' lbs');
  // loads per day, 14 days
  const days = [...Array(14)].map((_,i)=>iso(new Date(new Date(P.as_of+'T12:00:00')-(13-i)*864e5)));
  const byD = {}; P.loads.filter(r=>(!s.wh||r.warehouse===s.wh)&&(!s.dept||r.dept===s.dept)).forEach(r=>byD[r.date]=(byD[r.date]||0)+1);
  bars($('daily'), days.map(d=>[d, byD[d]||0]), ' loads', 's2');
  // grade mix, tare
  const gr = {}; N.forEach(r=>gr[r.grade]=(gr[r.grade]||0)+r.net); hbars($('grades'), Object.entries(gr).sort((a,b)=>b[1]-a[1]).slice(0,10), ' lbs', 's0');
  // suppliers table
  const S = {}; L.forEach(r=>{ const o=S[r.supplier]=S[r.supplier]||{loads:0,net:0,gross:0,tare:0,lead:[],unposted:0}; o.loads++; o.net+=r.net; o.gross+=r.gross; o.tare+=r.tare; if(r.lead!=null&&r.lead>=0) o.lead.push(r.lead); if(!r.posted) o.unposted++; });
  $('suppliers').innerHTML = '<tr><th>supplier</th><th class="num">loads</th><th class="num">net lbs</th><th class="num">avg load</th><th class="num">tare %</th><th class="num">PO → receipt, median d</th><th class="num">unposted</th></tr>'+
    Object.entries(S).sort((a,b)=>b[1].net-a[1].net).map(([k,o])=>`<tr><td>${esc(k)}</td><td class="num">${o.loads}</td><td class="num">${fmt(o.net)}</td><td class="num">${fmt(o.net/o.loads)}</td><td class="num">${o.gross?(100*o.tare/o.gross).toFixed(1):'–'}</td><td class="num">${med(o.lead)??'–'}</td><td class="num">${o.unposted||''}</td></tr>`).join('');
  // due board
  $('due').innerHTML = '<tr><th>PO</th><th>supplier</th><th>department</th><th>product</th><th>ordered</th><th class="num">age d</th><th>ship via</th><th>to</th><th>freight</th></tr>'+
    due.slice(0,150).map(d=>`<tr><td>${esc(d.po)}</td><td>${esc(d.supplier)}</td><td>${esc(d.dept)}</td><td>${esc(d.product)}</td><td>${esc(d.order_date)}</td><td class="num ${(d.age||0)>14?'no':''}">${d.age??''}</td><td>${esc(d.ship_via)}</td><td>${esc(d.warehouse)}</td><td>${esc(d.freight)}</td></tr>`).join('');
  $('duecount').textContent = `${due.length} open purchase orders with no receipt in the last ${P.days} days` + (stale ? ` · ${stale} open POs are older than 90 days and probably need closing` : '') + (due.length>150?' (first 150 shown)':'');
  // receipts log
  $('log').innerHTML = '<tr><th>received</th><th>worksheet</th><th>supplier</th><th>warehouse</th><th>grades</th><th class="num">gross</th><th class="num">tare</th><th class="num">net lbs</th><th class="num">units</th><th>PO</th><th class="num">lead d</th><th>posted</th><th>buyer</th><th>flags</th></tr>'+
    L.slice(0,200).map(r=>`<tr><td>${esc(r.date)}</td><td>${esc(r.ws)}</td><td>${esc(r.supplier)}</td><td>${esc(r.warehouse)}</td><td>${esc(r.grades)}</td><td class="num">${fmt(r.gross)}</td><td class="num">${fmt(r.tare)}</td><td class="num">${fmt(r.net)}</td><td class="num">${r.units||''}</td><td>${esc(r.po)}</td><td class="num">${r.lead??''}</td><td class="${r.posted?'yes':'no'}">${r.posted?'yes':'no'}</td><td>${esc(r.buyer)}</td><td>${(r.flags||[]).map(f=>`<span class="tag">${esc(f)}</span>`).join('')}</td></tr>`).join('');
  $('logcount').textContent = `${L.length} loads` + (L.length>200?' (first 200 shown)':'');
  const fl = {}; L.forEach(r=>(r.flags||[]).forEach(f=>fl[f]=(fl[f]||0)+1));
  $('flags').innerHTML = Object.entries(fl).sort((a,b)=>b[1]-a[1]).map(([f,n])=>`<span class="tag">${esc(f)}: ${n} loads</span>`).join(' ') + (P.excluded_adjustment_lines ? ` <span class="muted">· ${P.excluded_adjustment_lines} bulk processing-input line(s) excluded</span>` : '') || '<span class="muted">none</span>';
}
for (const id of ['wh','dept','win','age']) $(id).addEventListener('change', render);
$('wh').value = P.default_warehouse; render();
"""


def render_html(payload: dict) -> str:
    opt = lambda vals, all_label: f'<option value="">{all_label}</option>' + "".join(f'<option value="{v}">{v}</option>' for v in vals)
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Inbound</title><style>{CSS}</style></head><body><main>
<h1>Inbound</h1><p class="sub">Purchase receipts from cieTrade PR worksheets with the receiver's weights, and the open purchase orders still expected. As of {payload['as_of']}, receipts since {payload['window_from']}.</p>
<div class="filters"><label>warehouse <select id="wh">{opt(payload['warehouses'], 'all warehouses')}</select></label><label>department <select id="dept">{opt(payload['departments'], 'all departments')}</select></label>
<label>window <select id="win"><option value="2">2 weeks</option><option value="4">4 weeks</option><option value="8">8 weeks</option><option value="13" selected>13 weeks</option></select></label>
<label>open POs <select id="age"><option value="30">ordered in the last 30 days</option><option value="90" selected>ordered in the last 90 days</option><option value="9999">all, including stale</option></select></label></div>
<div class="tiles" id="tiles"></div>
<div class="grid"><section class="card"><h3>Net lbs received per week, by supplier</h3><div id="weekly"></div></section>
<section class="card"><h3>Loads per day, last 14 days</h3><div id="daily"></div></section>
<section class="card"><h3>Grade mix, net lbs</h3><div id="grades"></div></section></div>
<section class="card"><h2>Suppliers</h2><div class="scroll"><table id="suppliers"></table></div></section>
<section class="card"><h2>Inbound due board</h2><p class="sub" id="duecount"></p><div class="scroll"><table id="due"></table></div></section>
<section class="card"><h2>Receipts log</h2><p class="sub" id="logcount"></p><p class="small" id="flags"></p><div class="scroll"><table id="log"></table></div></section>
<p class="muted small">Source: cieTrade TradingInquiry (PR worksheets, ship date window) and ListOrders (open POs, order dates within a year). KG converted to lbs; EA/ST lines carry no weight and are flagged. Posted means the worksheet is invoiced.</p>
</main><script>window.__P = {json.dumps(payload, separators=(',', ':'))};</script><script>{JS}</script></body></html>"""


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--days", type=int, default=91, help="receipt window in days (default 91)")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--json", type=Path, help="also write the payload as JSON")
    ap.add_argument("--from-samples", type=Path, help="offline: read TradingInquiry / ListOrders sample files from this folder")
    args = ap.parse_args(argv)
    lines, pos = load_samples(args.from_samples) if args.from_samples else fetch(args.days)
    payload = build_payload(lines, pos, days=args.days)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render_html(payload))
    if args.json:
        args.json.write_text(json.dumps(payload, indent=1))
    print(f"{len(payload['loads'])} loads / {len(payload['lines'])} lines, {sum(1 for d in payload['due'] if not d['received'])} open POs awaiting -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
