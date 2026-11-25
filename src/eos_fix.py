"""Correct one cell of the End of Shift log workbook in SharePoint, through Graph.

A supervisor types 7725 where 7.25 was meant and the row carries $193,000 of labor
cost until someone fixes it. This finds the row by Submission ID and machine and
writes one value, as the Excel edit would, so the next poll run reads it corrected.

    python3 src/eos_fix.py --submission 20261028-211835-2nd --machine "Auto tie" \
        --column "Total man hours" --value 7.25 [--dry-run]
    python3 src/eos_fix.py --submission 20261109-221902-3rd --machine "*" --column Shift --value 2nd
    python3 src/eos_fix.py --table Submissions --submission 20261109-221902-3rd --column Shift --value 2nd
    python3 src/eos_fix.py --fixes-json '[{"submission": ..., "machine": ..., "column": ..., "value": ...}, ...]'

``--machine "*"`` changes the column on every row of the submission; the Submissions
table has one row per submission and needs no machine.

Uses the same GRAPH_* / EOS_WORKBOOK_* settings as the reader (Files.ReadWrite.All).
The workflow eos-fix.yml runs it from the repository's secrets.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import canon_line  # noqa: E402
from eos_workbook import load_settings  # noqa: E402
from graph_client import GraphClient  # noqa: E402
from labor_sheet import normalize_machine  # noqa: E402

KEY_COLUMNS = ("Submission ID", "Machine")


def workbook_base(cfg: dict[str, str]) -> str:
    if cfg.get("drive_id") and cfg.get("item_id"):
        return f"/drives/{cfg['drive_id']}/items/{cfg['item_id']}/workbook"
    raise FileNotFoundError("eos_fix needs EOS_WORKBOOK_DRIVE_ID and EOS_WORKBOOK_ITEM_ID")


def parse_value(text: str) -> Any:
    """Numbers stay numbers (so Excel keeps the cell numeric); anything else is text."""
    try:
        f = float(text)
        return int(f) if f.is_integer() and "." not in text else f
    except ValueError:
        return text


def col_letter(n: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA."""
    s = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def same_machine(a: object, b: object) -> bool:
    """'Auto tie', 'AUTO-TIE BALER' and 'auto tie baler' are one machine."""
    if canon_line(a) == canon_line(b):
        return True
    na, nb = normalize_machine(str(a)), normalize_machine(str(b))
    return na is not None and na == nb


def find_rows(rows: list[dict], headers: list[str], submission: str, machine: str | None) -> list[dict]:
    """The rows of one submission: all of them for machine "*" or a table without a Machine
    column (Submissions), else exactly the one machine."""
    si = headers.index("Submission ID")
    hits = [r for r in rows if str(r["values"][0][si]).strip() == submission]
    if "Machine" in headers and machine not in (None, "", "*"):
        mi = headers.index("Machine")
        hits = [r for r in hits if same_machine(r["values"][0][mi], machine)]
        if len(hits) != 1:
            raise LookupError(f"{len(hits)} rows match submission {submission!r} and machine {machine!r} (need exactly one)")
    elif not hits:
        raise LookupError(f"no rows match submission {submission!r}")
    return hits


def fix_cells(client: Any, cfg: dict[str, str], table: str, submission: str, machine: str | None, column: str, value: Any,
              dry_run: bool = False) -> list[dict]:
    """Write ``value`` into ``column`` of the matching row(s); one dict per cell changed."""
    base = workbook_base(cfg)
    headers = [str(h) for h in client.get(f"{base}/tables/{table}/headerRowRange")["values"][0]]
    if column not in headers:
        raise LookupError(f"column {column!r} is not in {table}: {headers}")
    rows = client.get_all(f"{base}/tables/{table}/rows")
    ci = headers.index(column)
    out = []
    for row in find_rows(rows, headers, submission, machine):
        rng = client.get(f"{base}/tables/{table}/rows/itemAt(index={row['index']})/range")
        sheet, cells = rng["address"].split("!")                     # "Entries!A300:M300"
        m = re.match(r"([A-Z]+)(\d+)", cells.split(":")[0])
        start_col = sum((ord(c) - 64) * 26 ** i for i, c in enumerate(reversed(m.group(1)))) - 1
        cell = f"{col_letter(start_col + ci)}{m.group(2)}"
        res = dict(table=table, sheet=sheet.strip("'"), cell=cell, column=column, before=row["values"][0][ci], after=value,
                   submission=submission, machine=row["values"][0][headers.index("Machine")] if "Machine" in headers else None)
        if not dry_run:
            client.request("PATCH", f"{base}/worksheets/{res['sheet']}/range(address='{cell}')", json_body={"values": [[value]]})
        out.append(res)
    return out


def fix_cell(client: Any, cfg: dict[str, str], table: str, submission: str, machine: str, column: str, value: Any,
             dry_run: bool = False) -> dict:
    """One row, one cell (the single-fix form)."""
    return fix_cells(client, cfg, table, submission, machine, column, value, dry_run)[0]


def describe(res: dict, dry_run: bool) -> str:
    who = f"{res['submission']}" + (f" / {res['machine']}" if res.get("machine") else "")
    return ("[dry-run] would set" if dry_run else "set") + f" {res['sheet']}!{res['cell']} ({res['column']}) for {who}: {res['before']!r} -> {res['after']!r}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--table", default="Entries", choices=["Entries", "Submissions"])
    ap.add_argument("--submission", help="Submission ID of the row, e.g. 20261028-211835-2nd")
    ap.add_argument("--machine", help="Machine as written in the row (loosely matched), or * for every row of the submission")
    ap.add_argument("--column", help="Heading of the cell to change, e.g. 'Total man hours'")
    ap.add_argument("--value", help="New value; a number stays numeric")
    ap.add_argument("--fixes-json", help="A JSON list of {table?, submission, machine?, column, value} applied in order")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if a.fixes_json:
        fixes = json.loads(a.fixes_json)
    elif a.submission and a.column and a.value is not None:
        fixes = [dict(table=a.table, submission=a.submission, machine=a.machine, column=a.column, value=a.value)]
    else:
        ap.error("give --submission, --column and --value, or --fixes-json")
    client, cfg = GraphClient.from_env(), load_settings()
    for f in fixes:
        value = f["value"] if not isinstance(f["value"], str) else parse_value(f["value"])
        for res in fix_cells(client, cfg, f.get("table") or "Entries", f["submission"], f.get("machine"), f["column"], value, a.dry_run):
            print(describe(res, a.dry_run))
    return 0


if __name__ == "__main__":
    sys.exit(main())
