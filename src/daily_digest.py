"""Daily production email: yesterday, in three parts.

1. Pounds by machine, the dashboard's Today bars drawn inside the message: one
   bar per machine in the reading order, 1st / 2nd / 3rd shift shades, a tick at
   the machine's normal same weekday (last four) and the difference beside the
   value. The bars are table cells with background colours, the shades are
   pre-mixed solid colours and the tick is a 2px cell, so Gmail, Outlook, Apple
   Mail and the phones all draw them.
2. Week at a glance, one card per shift (1st, 2nd, 3rd): pounds per machine per
   day for the current week through yesterday, week-to-date, plant total.
   Weekdays only: Monday's email covers Friday and the complete previous week.
3. Yesterday's three End of Shift reports, each laid out like the dashboard's
   submitted forms (machine, machine h, man h, operators, material, downtime,
   reason, comments, shift notes). A shift nobody filed says so.

Everything else, charts included, lives on the dashboard, and the email says so
with a link at the top and the bottom. No images: nothing to load or block.

    python3 src/daily_digest.py                       # preview: reports/digest/<date>.html for yesterday (gitignored)
    python3 src/daily_digest.py --date 2026-09-21     # a specific production day
    python3 src/daily_digest.py --send --to you@x.com # build and send through the Gmail API
    python3 src/daily_digest.py --send-if-due         # cloud: send once a day after DIGEST_SEND_HOUR local (recipients DIGEST_TO, blind copies DIGEST_BCC)
    python3 src/daily_digest.py --authorize           # one-time consent for the gmail.send scope (browser)

Inputs: data/aggregated_daily_data.xlsx (pounds per shift-day-machine) and
data/cietrade_status.json (End of Shift reports).
"""
from __future__ import annotations

import argparse
import base64
import html as _h
import json
import os
import sys
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_beta_dashboard import pretty, slot_color  # noqa: E402
from config import DATA_DIR, PROJECT_ROOT, WALTON_CONFIG_DIR, machine_rank, mail_provider  # noqa: E402

AGG_PATH = DATA_DIR / "aggregated_daily_data.xlsx"
STATUS_PATH = DATA_DIR / "cietrade_status.json"
OUT_DIR = PROJECT_ROOT / "reports" / "digest"
STATE_PATH = DATA_DIR / "digest_state.json"
DASHBOARD_URL = "https://zabdulla.github.io/walton_production/"
CLASSIC_URL = DASHBOARD_URL + "classic/"    # the previous site: every chart and table
SEND_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
SEND_TOKEN_PATH = WALTON_CONFIG_DIR / "gmail_send_token.json"
CREDENTIALS_PATH = WALTON_CONFIG_DIR / "gmail_credentials.json"
SHIFTS = ("1st", "2nd", "3rd")
SHADE = {"1st": 1.0, "2nd": 0.72, "3rd": 0.46}        # the dashboard's shift shades, mixed with the surface below
TRACK = "#f1efe9"                                      # the empty part of a bar
TICK = "#4b5563"                                       # the normal-weekday marker
NORMAL_DAYS = 4                                        # same weekdays averaged for "normal"


# ---------------------------------------------------------------- data

def load_inputs(agg_path: Path = AGG_PATH, status_path: Path = STATUS_PATH) -> tuple[pd.DataFrame, dict]:
    df = pd.read_excel(agg_path)
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    return df, status


def _lbs(df: pd.DataFrame) -> pd.Series:
    """Output pounds, counting Guillotine's rolls in when it books no output (the dashboard's support basis)."""
    out = pd.to_numeric(df["Actual_Output"], errors="coerce").fillna(0.0)
    inp = pd.to_numeric(df.get("Actual_Input"), errors="coerce").fillna(0.0) if "Actual_Input" in df.columns else 0.0
    guillotine = df["Machine_Name"].astype(str).str.contains("GUILLOTINE", case=False, na=False)
    return out.where(~(guillotine & (out == 0)), inp)


def pick_day(df: pd.DataFrame, for_date: date | None = None) -> date:
    """Yesterday, or the latest production day on or before it that has rows."""
    target = pd.Timestamp(for_date or (date.today() - timedelta(days=1)))
    have = df.loc[df["Date"] <= target, "Date"]
    return (have.max() if len(have) else target).date()


def build_digest(df: pd.DataFrame, status: dict, for_date: date) -> dict:
    d = df.copy()
    d["lbs"] = _lbs(d)
    d["Shift"] = d["Shift"].astype(str)
    day = pd.Timestamp(for_date)
    monday = day - pd.Timedelta(days=day.weekday())
    wdays = [monday + pd.Timedelta(days=i) for i in range((day - monday).days + 1)]
    wk = d[(d["Date"] >= monday) & (d["Date"] <= day)]
    # week at a glance, one block per shift
    week_by_shift = []
    for sh in SHIFTS:
        s = wk[wk["Shift"] == sh]
        piv = s.pivot_table(index="Machine_Name", columns="Date", values="lbs", aggfunc="sum").reindex(columns=wdays).fillna(0.0)
        piv = piv[piv.sum(axis=1) > 0]
        rows = [{"machine": m, "days": [round(float(v)) for v in piv.loc[m]], "wtd": round(float(piv.loc[m].sum()))}
                for m in sorted(piv.index, key=machine_rank)]                      # the dashboard's reading order, not rank
        totals = [round(float(v)) for v in piv.sum(axis=0)] if len(piv) else [0 for _ in wdays]
        week_by_shift.append({"shift": sh, "rows": rows, "totals": totals, "wtd": sum(totals), "day": totals[-1] if totals else 0})
    plant_days = [round(float(wk[wk["Date"] == x]["lbs"].sum())) for x in wdays]
    bars = bars_for_day(df, for_date)
    # yesterday's End of Shift reports, as filed
    eos = status.get("end_of_shift") or {}
    filed = {r["shift"]: r for r in eos.get("reports", []) if r.get("date") == for_date.isoformat()}
    reports = []
    for sh in SHIFTS:
        r = filed.get(sh)
        if not r:
            reports.append({"shift": sh, "filed": False}); continue
        machines = [{"machine": m.get("machine", ""), "machine_hours": m.get("machine_hours"), "man_hours": m.get("man_hours"),
                     "operators": m.get("operators", "") or "", "material": m.get("material", "") or "",
                     "downtime_min": m.get("downtime_min") or 0, "reason": m.get("reason", "") or "", "comment": m.get("comment", "") or ""}
                    for m in sorted(r.get("machines", []), key=lambda m: machine_rank(m.get("machine", "")))]
        reports.append({"shift": sh, "filed": True, "by": r.get("by", ""), "filed_at": r.get("filed_at", ""), "machines": machines,
                        "notes": list(r.get("notes", [])),
                        "machine_h": round(sum((m["machine_hours"] or 0) for m in machines), 2),
                        "man_h": round(sum((m["man_hours"] or 0) for m in machines), 2),
                        "downtime_min": int(sum(m["downtime_min"] for m in machines))})
    return {"date": for_date.isoformat(), "day_label": day.strftime("%A, %B %-d"), "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "day_total": plant_days[-1] if plant_days else 0,
            "week": {"monday": monday.strftime("%Y-%m-%d"), "days": [x.strftime("%a %-d") for x in wdays], "plant_days": plant_days,
                     "plant_wtd": sum(plant_days), "by_shift": week_by_shift},
            "reports": reports,
            "flags": [f for f in status.get("shift_flags", []) if monday.strftime("%Y-%m-%d") <= f.get("date", "") <= for_date.isoformat()],
            "bars": bars, "status": {"last_poll": status.get("last_poll")}, "url": DASHBOARD_URL, "classic_url": CLASSIC_URL}


# ---------------------------------------------------------------- email (inline styles only: mail clients strip stylesheets)

F = "font-family:Arial,Helvetica,sans-serif;"
_TH = f'style="{F}font-size:11px;color:#6b7280;padding:6px 8px;border-bottom:2px solid #d9dde2;text-align:left;text-transform:uppercase;white-space:nowrap"'
_THN = _TH.replace("text-align:left", "text-align:right")
_TD = f'style="{F}font-size:13px;color:#1a1d21;padding:7px 8px;border-bottom:1px solid #e6e8eb;vertical-align:top"'
_TDN = f'style="{F}font-size:14px;color:#1a1d21;padding:7px 8px;border-bottom:1px solid #e6e8eb;vertical-align:top;text-align:right;white-space:nowrap"'
_TDM = f'style="{F}font-size:13px;color:#1a1d21;padding:7px 8px;border-bottom:1px solid #e6e8eb;vertical-align:top;white-space:nowrap;font-weight:bold"'
_SMALL = f'style="{F}font-size:12px;color:#6b7280"'


def _n(v) -> str:
    return "" if v in (None, "", 0, 0.0) else f"{int(round(float(v))):,}"


def _hrs(v) -> str:
    return "" if v in (None, "", 0, 0.0) else f"{float(v):g}"


def _card(title: str, meta: str, body: str, head_bg: str = "#e7f3ee") -> str:
    """A bordered card with a header band, the way the dashboard frames a section."""
    return (f'<table width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #d9dde2;border-radius:10px;margin:0 0 14px;border-collapse:separate">'
            f'<tr><td style="{F}background:{head_bg};padding:10px 14px;border-radius:10px 10px 0 0;border-bottom:1px solid #d9dde2">'
            f'<span style="font-size:15px;font-weight:bold;color:#1a1d21">{title}</span>'
            + (f' <span style="font-size:12px;color:#4b5563">&nbsp;·&nbsp; {meta}</span>' if meta else "")
            + f'</td></tr><tr><td style="padding:4px 8px 8px">{body}</td></tr></table>')


def _week_card(block: dict, wk: dict, e) -> str:
    ncols = len(wk["days"])
    if not block["rows"]:
        body = f'<div {_SMALL} style="padding:8px 6px">No production booked to this shift this week.</div>'
        return _card(f'{e(block["shift"])} shift', "", body)
    rows = "".join(f'<tr><td {_TDM}>{e(r["machine"])}</td>' + "".join(f'<td {_TDN}>{_n(v)}</td>' for v in r["days"])
                   + f'<td {_TDN}><b>{_n(r["wtd"])}</b></td></tr>' for r in block["rows"])
    rows += (f'<tr><td {_TDM.replace("border-bottom:1px solid #e6e8eb", "border-top:2px solid #d9dde2;border-bottom:0")}>Shift total</td>'
             + "".join(f'<td {_TDN.replace("border-bottom:1px solid #e6e8eb", "border-top:2px solid #d9dde2;border-bottom:0")}><b>{_n(v)}</b></td>' for v in block["totals"])
             + f'<td {_TDN.replace("border-bottom:1px solid #e6e8eb", "border-top:2px solid #d9dde2;border-bottom:0")}><b>{_n(block["wtd"])}</b></td></tr>')
    head = f'<tr><th {_TH}>machine</th>' + "".join(f'<th {_THN}>{e(x)}</th>' for x in wk["days"]) + f'<th {_THN}>week to date</th></tr>'
    table = f'<table width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse">{head}{rows}</table>'
    meta = f'{_n(block["day"]) or "0"} lbs {e(wk["days"][-1])} &nbsp;·&nbsp; {_n(block["wtd"]) or "0"} lbs week to date'
    return _card(f'{e(block["shift"])} shift', meta, table)


def _report_card(r: dict, e) -> str:
    if not r["filed"]:
        return _card(f'{e(r["shift"])} shift', "", f'<div style="{F}font-size:13px;color:#c0392b;padding:8px 6px">No End of Shift report was filed for this shift.</div>', head_bg="#fdf2f2")
    cols = '<col width="116"><col width="50"><col width="46"><col width="94"><col width="86"><col width="40"><col width="74"><col width="126">'
    head = (f'<tr><th {_TH}>machine</th><th {_THN}>mach h</th><th {_THN}>man h</th><th {_TH}>operators</th><th {_TH}>material</th>'
            f'<th {_THN}>down</th><th {_TH}>reason</th><th {_TH}>comments</th></tr>')
    tdw = _TD.replace('vertical-align:top"', 'vertical-align:top;font-size:12px"')
    tdm = _TD.replace('vertical-align:top"', 'vertical-align:top;font-weight:bold;font-size:12px"')   # may wrap: long machine names
    rows = "".join(f'<tr><td {tdm}>{e(m["machine"])}</td><td {_TDN}>{_hrs(m["machine_hours"])}</td><td {_TDN}>{_hrs(m["man_hours"])}</td>'
                   f'<td {tdw}>{e(m["operators"])}</td><td {tdw}>{e(m["material"])}</td>'
                   f'<td {_TDN}>{(str(m["downtime_min"]) if m["downtime_min"] else "")}</td><td {tdw}>{e(m["reason"])}</td><td {tdw}>{e(m["comment"])}</td></tr>'
                   for m in r["machines"])
    table = f'<table width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;table-layout:fixed">{cols}{head}{rows}</table>'
    notes = "".join(f'<li style="margin:2px 0">{e(n)}</li>' for n in r["notes"])
    if notes:
        table += f'<div style="{F}font-size:13px;color:#1a1d21;padding:8px 6px 2px"><b>Shift notes</b><ul style="margin:4px 0 0 18px;padding:0">{notes}</ul></div>'
    down = f' &nbsp;·&nbsp; <span style="color:#c0392b"><b>{r["downtime_min"]} min down</b></span>' if r["downtime_min"] else ""
    when = f' at {e(str(r["filed_at"]))}' if r.get("filed_at") else ""
    meta = f'filed by {e(r["by"])}{when} &nbsp;·&nbsp; {len(r["machines"])} machines &nbsp;·&nbsp; {r["machine_h"]:g} machine h &nbsp;·&nbsp; {r["man_h"]:g} man h{down}'
    return _card(f'{e(r["shift"])} shift', meta, table)


def _flag_line(f: dict, e) -> str:
    day = datetime.strptime(f["date"], "%Y-%m-%d").strftime("%a %b %-d")
    hm = lambda t: datetime.strptime(t, "%Y-%m-%dT%H:%M").strftime("%-I:%M %p")
    when = hm(f["last"]) if f.get("first") == f.get("last") or not f.get("first") else f"{hm(f['first'])}–{hm(f['last'])}"
    m = e(f.get("machine", "").title())
    if f.get("kind") == "reassigned":
        return (f'{e(day)} · <b>{m}</b>: {f["lbs"]:,} lbs entered on the {e(f["from_shift"])}-shift job at {e(when)}, '
                f'during {e(f["shift"])} shift hours; counted as {e(f["shift"])}.')
    return f'{e(day)} · <b>{m}</b>: {f["lbs"]:,} lbs of {e(f.get("kind", "weekend"))} work on the {e(f["shift"])} shift, kept on its own day.'


# ---------------------------------------------------------------- pounds by machine, drawn from table cells

def mix(hex_color: str, alpha: float, over: str = "#ffffff") -> str:
    """The colour an alpha-blended fill would show over a white surface, as a solid hex."""
    c = [int(hex_color[i:i + 2], 16) for i in (1, 3, 5)]
    o = [int(over[i:i + 2], 16) for i in (1, 3, 5)]
    return "#" + "".join(f"{round(a * alpha + b * (1 - alpha)):02x}" for a, b in zip(c, o))


def bars_for_day(df: pd.DataFrame, for_date: date) -> dict:
    """Pounds per machine and shift for the day, plus each machine's normal same weekday (the dashboard's basis)."""
    d = df.copy()
    d["lbs"] = _lbs(d)
    d["Shift"] = d["Shift"].astype(str)
    day = pd.Timestamp(for_date)
    today = d[d["Date"] == day]
    per = today.groupby("Machine_Name")["lbs"].sum()
    by_shift = today.groupby(["Machine_Name", "Shift"])["lbs"].sum()
    earlier = d[(d["Date"] < day) & (d["Date"].dt.weekday == day.weekday()) & (d["lbs"] > 0)]
    days = sorted(earlier["Date"].unique())[-NORMAL_DAYS:]
    base = earlier[earlier["Date"].isin(days)]
    normal = (base.groupby("Machine_Name")["lbs"].sum() / len(days)) if len(days) else pd.Series(dtype=float)
    machines = sorted([m for m, v in per.items() if v > 0], key=machine_rank)
    rows = []
    for m in machines:
        shifts = {sh: float(by_shift.get((m, sh), 0.0)) for sh in SHIFTS}
        nv = float(normal.get(m, 0.0))
        lbs = float(per[m])
        rows.append({"machine": m, "name": pretty(m), "color": slot_color(m), "lbs": round(lbs), "by_shift": {k: round(v) for k, v in shifts.items()},
                     "normal": round(nv), "pct": (round(100 * (lbs - nv) / nv) if nv > 0 else None)})
    scale = max([1.0] + [r["lbs"] for r in rows] + [r["normal"] for r in rows])
    return {"date": for_date.isoformat(), "weekday": day.strftime("%A"), "rows": rows, "scale": scale, "normal_days": len(days),
            "total": round(float(per.sum())), "normal_total": round(float(normal.sum())) if len(days) else 0}


def _track(row: dict, scale: float) -> str:
    """One bar as a single-row table: shift cells in three shades, the normal tick as a 2px cell, the rest as track."""
    width = 100.0
    # fills: each shift's share of the bar, left to right; the remainder is empty track
    fills, left = [], 0.0
    for sh in SHIFTS:
        w = 100.0 * row["by_shift"][sh] / scale
        if w > 0:
            fills.append((left, left + w, mix(row["color"], SHADE[sh]), sh)); left += w
    other = 100.0 * row["lbs"] / scale - left
    if other > 0.05:
        fills.append((left, left + other, mix(row["color"], 0.28), "")); left += other
    fills.append((left, width, TRACK, ""))
    # cut every fill at the tick so the tick can sit inside it as its own cell
    tick = 100.0 * row["normal"] / scale if row["normal"] else None
    cells = []
    for a, b, color, label in fills:
        if tick is not None and a <= tick < b:
            if tick - a > 0.05:
                cells.append((tick - a, color, label))
            cells.append((None, TICK, ""))                        # the tick: 2px, not a percentage
            if b - tick > 0.05:
                cells.append((b - tick, color, ""))
        else:
            cells.append((b - a, color, label))
    out = []
    for w, color, label in cells:
        if w is None:
            out.append(f'<td width="2" bgcolor="{TICK}" style="width:2px;background:{TICK};font-size:1px;line-height:18px;height:18px">&nbsp;</td>')
            continue
        text = (f'<span style="{F}font-size:10px;font-weight:bold;color:#ffffff;letter-spacing:.04em">{label}</span>' if label and w >= 9 else "&nbsp;")
        out.append(f'<td width="{w:.1f}%" bgcolor="{color}" style="width:{w:.1f}%;background:{color};font-size:1px;line-height:18px;height:18px;padding:0 0 0 {4 if label and w >= 9 else 0}px;white-space:nowrap;overflow:hidden">{text}</td>')
    return f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;table-layout:fixed;height:18px"><tr>{"".join(out)}</tr></table>'


def render_bars_card(b: dict, day_label: str) -> str:
    e = _h.escape
    if not b["rows"]:
        body = f'<div style="{F}font-size:13px;color:#6b7280;padding:4px 0">Nothing booked this day.</div>'
    else:
        lines = []
        for r in b["rows"]:
            pct = r["pct"]
            delta = "" if pct is None else f'<br><span style="font-size:11px;font-weight:normal;color:{"#006300" if pct >= 0 else "#d03b3b"}">{"+" if pct >= 0 else ""}{pct}%</span>'
            lines.append(
                f'<tr>'
                f'<td width="10" style="width:10px;padding:6px 0"><div style="width:10px;height:10px;border-radius:3px;background:{r["color"]};font-size:1px;line-height:10px">&nbsp;</div></td>'
                f'<td width="118" style="{F}font-size:13px;font-weight:bold;color:#1a1d21;padding:6px 8px;white-space:nowrap">{e(r["name"])}</td>'
                f'<td style="padding:6px 0">{_track(r, b["scale"])}</td>'
                f'<td width="64" align="right" style="{F}font-size:13px;font-weight:bold;color:#1a1d21;padding:6px 0 6px 10px;text-align:right;white-space:nowrap;line-height:1.2">{r["lbs"]:,}{delta}</td>'
                f'</tr>')
        sq = "".join(f'<span style="display:inline-block;width:9px;height:9px;background:{mix("#4b5563", a)};vertical-align:middle"></span>' for a in SHADE.values())
        key = (f'<div style="{F}font-size:11px;color:#4b5563;padding:6px 0 0 18px;line-height:1.6">'
               f'<span style="display:inline-block;width:2px;height:11px;background:{TICK};vertical-align:middle;margin-right:5px"></span>'
               + (f'normal {e(b["weekday"])} for the machine (last {b["normal_days"]}), with the difference beside the value' if b["normal_days"] else f'no earlier {e(b["weekday"])}s to compare against')
               + f' &nbsp;&nbsp; {sq} <span style="margin-left:4px">1st · 2nd · 3rd</span></div>')
        body = f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse">{"".join(lines)}</table>{key}'
    meta = f'{b["total"]:,} lbs' + (f' · a normal {e(b["weekday"])} is {b["normal_total"]:,}' if b["normal_total"] else "")
    return _card(f'Pounds by machine · {e(day_label)}', meta, body)


def render_email(dg: dict) -> str:
    e = _h.escape
    wk = dg["week"]
    bars_card = render_bars_card(dg["bars"], dg["day_label"]) if dg.get("bars") else ""
    h2 = f'style="{F}font-size:17px;font-weight:bold;color:#1a1d21;margin:0;padding:22px 0 10px"'
    classic = dg.get("classic_url") or dg["url"] + "classic/"
    link = (f'<div style="{F}font-size:13px;color:#1a1d21;background:#e7f3ee;border:1px solid #cfe5db;border-radius:8px;padding:10px 14px;line-height:1.5">'
            f'<b>Production dashboard</b>: <a href="{dg["url"]}" style="color:#0b6e4f;font-weight:bold">{dg["url"].replace("https://", "")}</a>'
            f'<span style="color:#4b5563"> · today by machine, live feed, the week, shift reports, machine trends; built for your phone</span><br>'
            f'<b>Classic dashboard</b>: <a href="{classic}" style="color:#0b6e4f;font-weight:bold">{classic.replace("https://", "")}</a>'
            f'<span style="color:#4b5563"> · the previous site, every chart and table</span></div>')
    week_cards = "".join(_week_card(b, wk, e) for b in wk["by_shift"])
    plant = (f'<div style="{F}font-size:13px;color:#1a1d21;padding:2px 6px 6px"><b>Plant</b>: '
             + " &nbsp;·&nbsp; ".join(f'{e(dd)} {_n(v) or "0"}' for dd, v in zip(wk["days"], wk["plant_days"]))
             + f' &nbsp;·&nbsp; <b>week to date {_n(wk["plant_wtd"]) or "0"} lbs</b></div>')
    report_cards = "".join(_report_card(r, e) for r in dg["reports"])
    flags = ""
    if dg.get("flags"):
        items = "".join(f'<li style="padding:3px 0">{_flag_line(f, e)}</li>' for f in dg["flags"])
        flags = (f'<div style="{F}font-size:13px;color:#1a1d21;background:#fff7e6;border:1px solid #f1d9a6;border-radius:8px;padding:8px 14px;margin:6px 0 4px">'
                 f'<b>Shift notes from the cieTrade clock</b> (already reflected above)<ul style="margin:4px 0 0 18px;padding:0">{items}</ul></div>')
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Walton production · {e(dg["day_label"])}</title></head>
<body style="margin:0;padding:0;background:#f3f4f6">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f4f6"><tr><td align="center" style="padding:16px 8px">
<table role="presentation" width="680" cellspacing="0" cellpadding="0" style="max-width:680px;width:100%;background:#ffffff;border:1px solid #e6e8eb;border-radius:8px">
<tr><td style="padding:22px 24px 6px">
  <div {_SMALL}>Walton production · daily email</div>
  <div style="{F}font-size:24px;font-weight:bold;color:#1a1d21;padding:2px 0 4px">{e(dg["day_label"])}</div>
  <div style="{F}font-size:15px;color:#1a1d21;padding-bottom:12px"><b>{_n(dg["day_total"]) or "0"} lbs</b> produced across three shifts</div>
  {bars_card}{link}
</td></tr>
<tr><td style="padding:0 24px">
  <div {h2}>Week at a glance <span style="font-weight:normal;color:#6b7280;font-size:13px">week of {e(datetime.strptime(wk["monday"], "%Y-%m-%d").strftime("%b %-d"))}{", complete week" if datetime.strptime(dg["date"], "%Y-%m-%d").weekday() >= 4 else ""}, pounds by machine and day</span></div>
  {week_cards}{plant}{flags}
</td></tr>
<tr><td style="padding:0 24px">
  <div {h2}>End of Shift reports <span style="font-weight:normal;color:#6b7280;font-size:13px">{e(dg["day_label"])}, as filed by the supervisors</span></div>
  {report_cards}
</td></tr>
<tr><td style="padding:14px 24px 20px;border-top:1px solid #e6e8eb">
  {link}
  <div {_SMALL} style="padding-top:8px">Built {e(dg["generated"])} from cieTrade polls through {e(str(dg["status"]["last_poll"] or "")[:16].replace("T", " "))}. Pounds from cieTrade converting jobs; hours, crew, material, downtime and comments from the End of Shift forms.</div>
</td></tr>
</table></td></tr></table></body></html>"""


# ---------------------------------------------------------------- send (Gmail API, send scope only)

def gmail_send_service():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    raw = os.environ.get("GMAIL_SEND_TOKEN_JSON")
    creds = Credentials.from_authorized_user_info(json.loads(raw), SEND_SCOPES) if raw else (
        Credentials.from_authorized_user_file(str(SEND_TOKEN_PATH), SEND_SCOPES) if SEND_TOKEN_PATH.exists() else None)
    if creds is None:
        raise FileNotFoundError(f"no send token: run `python3 src/daily_digest.py --authorize` once (writes {SEND_TOKEN_PATH})")
    if not creds.valid and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def authorize() -> Path:
    from google_auth_oauthlib.flow import InstalledAppFlow
    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SEND_SCOPES)
    creds = flow.run_local_server(port=0)
    SEND_TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    SEND_TOKEN_PATH.write_text(creds.to_json()); SEND_TOKEN_PATH.chmod(0o600)
    return SEND_TOKEN_PATH


PLAIN_TEXT = ("Walton daily production email. Open in an HTML mail client, or see the dashboard at " + DASHBOARD_URL
              + " (classic site: " + CLASSIC_URL + ")")


def build_message(to: list[str], subject: str, html: str, bcc: list[str] | None = None) -> dict:
    msg = MIMEMultipart("alternative")
    msg["To"], msg["Subject"] = ", ".join(to), subject
    if bcc:
        msg["Bcc"] = ", ".join(bcc)      # Gmail delivers to Bcc and strips the header from what recipients see
    msg.attach(MIMEText(PLAIN_TEXT, "plain"))
    msg.attach(MIMEText(html, "html"))
    return {"raw": base64.urlsafe_b64encode(msg.as_bytes()).decode()}


def send(to: list[str], subject: str, html: str, bcc: list[str] | None = None) -> str:
    """Send through Gmail, or through the Outlook mailbox when MAIL_PROVIDER=microsoft."""
    if mail_provider() == "microsoft":
        from graph_mail import GraphMailbox
        return GraphMailbox.from_env().send(to, subject, html, text=PLAIN_TEXT, bcc=bcc)
    svc = gmail_send_service()
    r = svc.users().messages().send(userId="me", body=build_message(to, subject, html, bcc)).execute()
    return r.get("id", "")


def due_now(state_path: Path = STATE_PATH, now: datetime | None = None, send_hour: int = 7) -> tuple[bool, str]:
    """Once per weekday, at or after send_hour local time. No email on Saturday or Sunday:
    Monday's email covers Friday (the latest production day) and the whole previous week."""
    now = now or datetime.now()
    today = now.strftime("%Y-%m-%d")
    if now.weekday() >= 5:
        return False, "weekend"
    if now.hour < send_hour:
        return False, f"before {send_hour:02d}:00"
    st = json.loads(state_path.read_text()) if state_path.exists() else {}
    if st.get("last_sent") == today:
        return False, "already sent today"
    return True, "due"


def mark_sent(state_path: Path = STATE_PATH, now: datetime | None = None) -> None:
    now = now or datetime.now()
    state_path.write_text(json.dumps({"last_sent": now.strftime("%Y-%m-%d"), "at": now.strftime("%Y-%m-%dT%H:%M:%S")}, indent=1) + "\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--date", help="production day (default: yesterday, or the latest day with rows before it)")
    ap.add_argument("--out", type=Path, default=OUT_DIR, help="preview folder for <date>.html (gitignored)")
    ap.add_argument("--send", action="store_true", help="send through the Gmail API")
    ap.add_argument("--to", help="comma-separated recipients (default: env DIGEST_TO)")
    ap.add_argument("--bcc", help="comma-separated blind-copy recipients (default: env DIGEST_BCC)")
    ap.add_argument("--send-if-due", action="store_true", help="send once per day after DIGEST_SEND_HOUR (default 7) local time")
    ap.add_argument("--authorize", action="store_true", help="one-time browser consent for gmail.send")
    args = ap.parse_args(argv)
    if args.authorize:
        print(f"token saved to {authorize()}"); return 0
    if args.send_if_due:
        ok, why = due_now(send_hour=int(os.environ.get("DIGEST_SEND_HOUR", "7")))
        if not ok:
            print(f"digest not sent: {why}"); return 0
    df, status = load_inputs()
    day = pick_day(df, datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None)
    dg = build_digest(df, status, day)
    args.out.mkdir(parents=True, exist_ok=True)
    html_path = args.out / f"{day}.html"
    html_path.write_text(render_email(dg))
    filed = sum(1 for r in dg["reports"] if r["filed"])
    print(f"{dg['day_label']}: {dg['day_total']:,} lbs, EOS {filed}/3 shifts -> {html_path}")
    if args.send or args.send_if_due:
        to = [x.strip() for x in (args.to or os.environ.get("DIGEST_TO", "")).split(",") if x.strip()]
        bcc = [x.strip() for x in (args.bcc or os.environ.get("DIGEST_BCC", "")).split(",") if x.strip()]
        if not to:
            print("no recipients (use --to or DIGEST_TO)"); return 2
        mid = send(to, f"Walton production · {dg['day_label']} · {dg['day_total']:,} lbs", render_email(dg), bcc)
        print(f"sent {mid} to {', '.join(to)}" + (f" (bcc {len(bcc)})" if bcc else ""))
        if args.send_if_due:
            mark_sent()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
