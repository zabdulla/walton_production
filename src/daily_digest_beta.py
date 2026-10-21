#!/usr/bin/env python3
"""Beta daily email: the dashboard's "Pounds by machine" bars drawn inside the message.

The regular email (daily_digest.py) is unchanged; this builds the same message with one extra
card under the headline: one bar per machine for the day, in the dashboard's reading order, split
into 1st / 2nd / 3rd shift shades, with a tick at the machine's normal same weekday and the
difference beside the value.

No images and no attachments: the bars are plain table cells with background colours, which every
mail client draws (Gmail, Outlook, Apple Mail, phones). Shades are pre-mixed colours rather than
CSS opacity, and the tick is a 2px cell, because Outlook ignores opacity and positioning.

    python3 src/daily_digest_beta.py                      # preview reports/digest/<date>-beta.html
    python3 src/daily_digest_beta.py --date 2026-10-16    # another day
    python3 src/daily_digest_beta.py --send --to me@x.com # send through the same Gmail token as the digest
"""
from __future__ import annotations

import argparse
import html as _h
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import daily_digest as dd  # noqa: E402
from build_beta_dashboard import pretty, slot_color  # noqa: E402
from config import machine_rank  # noqa: E402

SHIFTS = ("1st", "2nd", "3rd")
SHADE = {"1st": 1.0, "2nd": 0.72, "3rd": 0.46}        # the dashboard's shades, mixed with the surface below
TRACK = "#f1efe9"                                      # the empty part of a bar
TICK = "#4b5563"                                       # the normal-weekday marker
NORMAL_DAYS = 4                                        # same weekdays averaged for "normal"
F = dd.F


def mix(hex_color: str, alpha: float, over: str = "#ffffff") -> str:
    """The colour an alpha-blended fill would show over a white surface, as a solid hex."""
    c = [int(hex_color[i:i + 2], 16) for i in (1, 3, 5)]
    o = [int(over[i:i + 2], 16) for i in (1, 3, 5)]
    return "#" + "".join(f"{round(a * alpha + b * (1 - alpha)):02x}" for a, b in zip(c, o))


def bars_for_day(df: pd.DataFrame, for_date: date) -> dict:
    """Pounds per machine and shift for the day, plus each machine's normal same weekday (the dashboard's basis)."""
    d = df.copy()
    d["lbs"] = dd._lbs(d)
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
    return dd._card(f'Pounds by machine · {e(day_label)}', meta, body)


def render_email_beta(dg: dict, b: dict) -> str:
    return dd.render_email(dg, extra_top=render_bars_card(b, dg["day_label"]))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--date", help="production day (default: yesterday, or the latest day with rows before it)")
    ap.add_argument("--out", type=Path, default=dd.OUT_DIR, help="preview folder for <date>-beta.html (gitignored)")
    ap.add_argument("--send", action="store_true", help="send through the Gmail API (the digest's token)")
    ap.add_argument("--to", help="comma-separated recipients (default: env DIGEST_BETA_TO)")
    args = ap.parse_args(argv)
    df, status = dd.load_inputs()
    day = dd.pick_day(df, datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None)
    dg = dd.build_digest(df, status, day)
    b = bars_for_day(df, day)
    html = render_email_beta(dg, b)
    args.out.mkdir(parents=True, exist_ok=True)
    html_path = args.out / f"{day}-beta.html"
    html_path.write_text(html)
    print(f"{dg['day_label']}: {b['total']:,} lbs over {len(b['rows'])} machines -> {html_path}")
    if args.send:
        to = [x.strip() for x in (args.to or os.environ.get("DIGEST_BETA_TO", "")).split(",") if x.strip()]
        if not to:
            print("no recipients (use --to or DIGEST_BETA_TO)"); return 2
        mid = dd.send(to, f"[beta] Walton production · {dg['day_label']} · {dg['day_total']:,} lbs", html)
        print(f"sent {mid} to {', '.join(to)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
