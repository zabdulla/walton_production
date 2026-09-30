"""Beta dashboard: the embedded dataset and the page written end to end. No network."""
from __future__ import annotations

import json
from datetime import date

import pandas as pd

import build_beta_dashboard as bb


def _df() -> pd.DataFrame:
    rows = []
    for wk in range(30):
        for dow in range(5):
            d = pd.Timestamp("2026-09-28") - pd.Timedelta(weeks=wk) + pd.Timedelta(days=dow)
            if d > pd.Timestamp("2026-09-29"):
                continue
            for sh in ("1st", "2nd"):
                rows.append({"Date": d, "Shift": sh, "Machine_Name": "EXTRUDER", "Actual_Output": 5000 + 100 * wk, "Actual_Input": 0, "Machine_Hours": 7.0, "Man_Hours": 14.0, "Total_Expense": 350.0})
                rows.append({"Date": d, "Shift": sh, "Machine_Name": "GUILLOTINE", "Actual_Output": 0, "Actual_Input": 6000, "Machine_Hours": 0.0, "Man_Hours": 0.0, "Total_Expense": 0.0})
    rows.append({"Date": pd.Timestamp("2026-09-29"), "Shift": "unspecified", "Machine_Name": "GRINDER", "Actual_Output": 10, "Actual_Input": 0, "Machine_Hours": 0.0, "Man_Hours": 0.0, "Total_Expense": 0.0})
    return pd.DataFrame(rows)


STATUS = {"last_poll": "2026-09-30T06:00:00", "open_jobs": 3, "open_lbs": 12000, "closures": ["2026-09-07"], "end_of_shift": {"reports": [
    {"date": "2026-09-29", "shift": "1st", "by": "Tim", "machines": [
        {"machine": "EXTRUDER", "machine_hours": 7.0, "man_hours": 14.0, "operators": "Steven", "material": "BOPP", "downtime_min": 60, "reason": "Blades", "comment": ""}],
     "notes": ["clean-up"]},
    {"date": "2026-09-26", "shift": "3rd", "by": "Connor", "machines": [], "notes": []}]}}


def _load(tmp_path):
    p = tmp_path / "agg.xlsx"
    _df().to_excel(p, index=False)
    s = tmp_path / "status.json"
    s.write_text(json.dumps(STATUS))
    return bb.load(p, s)


def test_dataset_is_compact_and_on_the_support_basis(tmp_path) -> None:
    df, status = _load(tmp_path)
    D = bb.dataset(df, status, today=date(2026, 9, 30))
    assert D["data_through"] == "2026-09-29" and D["today"] == "2026-09-30"
    assert D["machines"] == ["EXTRUDER", "GRINDER", "GUILLOTINE"]                # fixed slot order, not rank
    assert D["dates"][0] == "2026-03-30" and D["dates"][-1] == "2026-09-29"       # 26 complete weeks plus the current one
    by = {(D["dates"][r[0]], D["shifts"][r[1]], D["machines"][r[2]]): r for r in D["rows"]}
    assert by[("2026-09-29", "1st", "GUILLOTINE")][3] == 6000                    # rolls count when it books no output
    assert by[("2026-09-29", "1st", "EXTRUDER")][3:] == [5000, 7.0, 14.0, 350.0]
    assert by[("2026-09-29", "unspecified", "GRINDER")][3] == 10
    assert [r["by"] for r in D["reports"]] == ["Connor", "Tim"] and D["closures"] == ["2026-09-07"]
    assert D["colors"]["EXTRUDER"] == "#2a78d6" and D["names"]["GUILLOTINE"] == "Guillotine"


def test_main_writes_the_page_with_data_and_every_view(tmp_path) -> None:
    p = tmp_path / "agg.xlsx"; _df().to_excel(p, index=False)
    s = tmp_path / "status.json"; s.write_text(json.dumps(STATUS))
    out = bb.main(p, tmp_path / "beta" / "index.html", s)
    page = out.read_text()
    for view in ("today", "week", "shifts", "machines", "more"):
        assert f'data-view="{view}"' in page
    for el in ("todayPager", "todaySeg", "weekPager", "weekSeg", "shiftsPager", "machSeg", "metricSeg"):
        assert f'id="{el}"' in page
    start = page.index('<script id="data" type="application/json">') + len('<script id="data" type="application/json">')
    D = json.loads(page[start:page.index("</script>", start)])
    assert D["reports"][1]["by"] == "Tim" and len(D["rows"]) > 250
    assert 'class="tabbar"' in page and "<\\/" not in page.replace("<\\/", "")     # JSON is safe inside <script>
