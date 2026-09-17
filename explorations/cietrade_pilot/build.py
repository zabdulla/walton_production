"""Build the Walton Daily Production Pilot page from the cieTrade observations.

    python3 explorations/cietrade_pilot/build.py
writes out/data.json, out/walton_daily_pilot.html (artifact fragment) and
out/walton_daily_pilot_standalone.html (open directly in a browser).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
sys.path.insert(0, str(HERE.parents[1] / "src"))
import cietrade_model as model  # noqa: E402

HEAD = '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'


def payload(res: dict) -> dict:
    rows = [dict(d=r.Date.strftime("%Y-%m-%d"), s=r.Shift, m=r.Machine, lbs=(None if r.lbs != r.lbs else round(float(r.lbs), 1)),
                 mode=r.mode, job=r.job, n=r.n, src=r.src) for r in res["cells"].itertuples()]
    return dict(meta=res["meta"], rows=rows, exports=res["log"])


def main() -> None:
    res = model.run()
    OUT.mkdir(exist_ok=True)
    data = payload(res)
    blob = json.dumps(data, default=str)
    (OUT / "data.json").write_text(blob)
    page = (HERE / "template.html").read_text().replace("__DATA_JSON__", blob)
    (OUT / "walton_daily_pilot.html").write_text(page)
    cut = page.index("</style>") + len("</style>")
    (OUT / "walton_daily_pilot_standalone.html").write_text(HEAD + page[:cut] + "</head><body>" + page[cut:] + "</body></html>")
    print(f"wrote {OUT / 'walton_daily_pilot.html'} ({len(page) // 1024} KB, {len(data['rows'])} rows, {len(data['exports'])} log entries)")


if __name__ == "__main__":
    main()
