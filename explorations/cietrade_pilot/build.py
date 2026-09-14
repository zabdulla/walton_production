"""Build the Walton Daily Production Pilot page from the export archive.

    python3 explorations/cietrade_pilot/build.py
writes out/data.json, out/walton_daily_pilot.html (artifact fragment) and
out/walton_daily_pilot_standalone.html (open directly in a browser).
"""
from __future__ import annotations

import json
from pathlib import Path

import model

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


def payload(res: dict) -> dict:
    cells = res["cells"]
    rows = [dict(d=r.Date.strftime("%Y-%m-%d"), s=r.Shift, m=r.Machine, lbs=(None if r.lbs != r.lbs else round(float(r.lbs), 1)),
                 mode=r.mode, job=r.job, n=r.n, src=r.src) for r in cells.itertuples()]
    return dict(meta=res["meta"], rows=rows, exports=res["log"])


def main() -> None:
    res = model.run()
    OUT.mkdir(exist_ok=True)
    data = payload(res)
    (OUT / "data.json").write_text(json.dumps(data, default=str))
    page = (HERE / "template.html").read_text().replace("__DATA_JSON__", json.dumps(data, default=str))
    (OUT / "walton_daily_pilot.html").write_text(page)
    (OUT / "walton_daily_pilot_standalone.html").write_text(
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">\n'
        + page + "\n</head><body></body></html>" if False else
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
        + page[: page.index("</style>") + 8] + "</head><body>" + page[page.index("</style>") + 8:] + "</body></html>")
    print(f"\nwrote {OUT / 'walton_daily_pilot.html'} ({len(page) // 1024} KB, {len(rows := data['rows'])} rows, {len(data['exports'])} exports)")


if __name__ == "__main__":
    main()
