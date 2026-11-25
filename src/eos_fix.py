"""Correct one cell of the End of Shift log workbook in SharePoint, through Graph.

A supervisor types 7725 where 7.25 was meant and the row carries $193,000 of labor
cost until someone fixes it. This finds the row by Submission ID and machine and
writes one value, as the Excel edit would, so the next poll run reads it corrected.

    python3 src/eos_fix.py --submission 20261028-211835-2nd --machine "Auto tie" \
        --column "Total man hours" --value 7.25 [--dry-run]

Uses the same GRAPH_* / EOS_WORKBOOK_* settings as the reader (Files.ReadWrite.All).
The workflow eos-fix.yml runs it from the repository's secrets.
"""
from __future__ import annotations

import argparse
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


def find_row(rows: list[dict], headers: list[str], submission: str, machine: str) -> dict:
    si, mi = headers.index("Submission ID"), headers.index("Machine")
    hits = [r for r in rows if str(r["values"][0][si]).strip() == submission and same_machine(r["values"][0][mi], machine)]
    if len(hits) != 1:
        raise LookupError(f"{len(hits)} rows match submission {submission!r} and machine {machine!r} (need exactly one)")
    return hits[0]


def fix_cell(client: Any, cfg: dict[str, str], table: str, submission: str, machine: str, column: str, value: Any,
             dry_run: bool = False) -> dict:
    base = workbook_base(cfg)
    headers = [str(h) for h in client.get(f"{base}/tables/{table}/headerRowRange")["values"][0]]
    if column not in headers:
        raise LookupError(f"column {column!r} is not in {table}: {headers}")
    rows = client.get_all(f"{base}/tables/{table}/rows")
    row = find_row(rows, headers, submission, machine)
    ci = headers.index(column)
    rng = client.get(f"{base}/tables/{table}/rows/itemAt(index={row['index']})/range")
    sheet, cells = rng["address"].split("!")                     # "Entries!A300:M300"
    first = cells.split(":")[0]
    m = re.match(r"([A-Z]+)(\d+)", first)
    start_col = sum((ord(c) - 64) * 26 ** i for i, c in enumerate(reversed(m.group(1)))) - 1
    cell = f"{col_letter(start_col + ci)}{m.group(2)}"
    before = row["values"][0][ci]
    out = dict(table=table, sheet=sheet.strip("'"), cell=cell, column=column, before=before, after=value,
               submission=submission, machine=row["values"][0][headers.index("Machine")])
    if not dry_run:
        client.request("PATCH", f"{base}/worksheets/{out['sheet']}/range(address='{cell}')", json_body={"values": [[value]]})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--table", default="Entries", choices=["Entries", "Submissions"])
    ap.add_argument("--submission", required=True, help="Submission ID of the row, e.g. 20261028-211835-2nd")
    ap.add_argument("--machine", required=True, help="Machine as written in the row (loosely matched); any text for Submissions")
    ap.add_argument("--column", required=True, help="Heading of the cell to change, e.g. 'Total man hours'")
    ap.add_argument("--value", required=True, help="New value; a number stays numeric")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    res = fix_cell(GraphClient.from_env(), load_settings(), a.table, a.submission, a.machine, a.column, parse_value(a.value), a.dry_run)
    print(("[dry-run] would set" if a.dry_run else "set") + f" {res['sheet']}!{res['cell']} ({res['column']}) for {res['submission']} / {res['machine']}: "
          f"{res['before']!r} -> {res['after']!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
