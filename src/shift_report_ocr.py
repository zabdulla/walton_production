"""Digest photographed End-of-Shift reports into labor entries.

The paper sheet is a fixed grid — one row per machine (Auto tie, Baler 1/2, Big
densifier, New densifier, Extruder, Guillotine, Shredder, Shredder/Grinder) with
Machine Hours operated, Total Man Hours, Operator(s) and Comments — filled by
hand and photographed on a clipboard. Supervisors write the material being run
in brackets after the operators' names and sometimes add notes in the margin.

Two steps, runnable together or apart:

    python3 src/shift_report_ocr.py fetch             # Gmail -> data/shift_reports/*.jpg
    python3 src/shift_report_ocr.py extract --image photo.jpg [--dry-run]
    python3 src/shift_report_ocr.py run               # fetch, then extract anything new

Extraction uses Claude's vision through the Anthropic SDK (pip install anthropic;
credentials via ANTHROPIC_API_KEY or `ant auth login`). Every row carries a
confidence and the report a needs-review flag; low-confidence rows are still
landed but marked, so a human checks them rather than the whole sheet.
"""
from __future__ import annotations

import argparse
import base64
import io
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

from config import SHIFT_REPORT_DIR
from labor_entries import (
    LABOR_ENTRIES_PATH, empty_entries, empty_notes, make_entry, normalize_machine,
    normalize_shift, record, split_operator_material,
)

logger = logging.getLogger(__name__)

MODEL = "claude-opus-5"
DEFAULT_QUERY = 'subject:"end of shift" has:attachment'
IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}
MAX_EDGE = 2000  # px; phone photos are 4000+ and the grid reads fine at half that


# ---------------------------------------------------------------------------
# What the model returns
# ---------------------------------------------------------------------------

class ShiftRow(BaseModel):
    machine_label: str = Field(description="The row label exactly as printed on the sheet")
    machine_hours: float | None = Field(description="Machine Hours operated; null if blank")
    man_hours: float | None = Field(description="Total Man Hours (crew total); null if blank")
    operators: list[str] = Field(description="Operator first names as written, brackets removed")
    material: str | None = Field(description="Text written in brackets/parentheses after the names, e.g. BOPP")
    comment: str | None = Field(description="Comments cell text, null if blank")
    confidence: float = Field(description="0-1 confidence that the numbers and names were read correctly")
    uncertainty: str | None = Field(description="What was hard to read, if anything")


class ShiftReport(BaseModel):
    date: str | None = Field(description="Date as written, ISO YYYY-MM-DD; null if unreadable")
    date_confidence: float = Field(description="0-1; lower it when a digit could be read two ways")
    date_alternatives: list[str] = Field(description="Other plausible ISO dates if a digit is ambiguous")
    shift: str | None = Field(description="1st, 2nd or 3rd, from the heading")
    rows: list[ShiftRow] = Field(description="Only rows with something written in them")
    margin_notes: list[str] = Field(description="Any writing outside the grid, verbatim")
    needs_review: bool = Field(description="True if a human should check this sheet")
    review_reasons: list[str]


SYSTEM_PROMPT = """You read photographed End of Shift reports from a plastics recycling plant.

The sheet is a printed grid. The heading has a handwritten Date (M/D/YY) and the shift
(First/Second/Third or 1st/2nd/3rd). Each row is a machine: Auto tie, Baler 1, Baler 2,
Big densifier, Densifier or New Densifier, Extruder, Guillotine, Shredder, Shredder/Grinder.
Columns: Machine Hours operated, Total Man Hours, Operator(s), Comments.

Conventions:
- A blank row means the machine did not run. Return only rows with writing in them.
- Total Man Hours is the whole crew's hours added together (two operators for 7 hours = 14).
- Text in [brackets] or (parentheses) after the names is the material being run, not a person.
- Writing outside the grid (below it, in a margin) is a shift note about people or events not
  tied to one machine. Return it verbatim in margin_notes.
- Photos may be rotated or shadowed; read the printed labels to orient yourself.
- Handwritten digits can be ambiguous (8 vs 9, 1 vs 7, 5 vs 6). When a date digit could be read
  two ways, give your best reading, lower date_confidence, and list the alternatives.
- Do not guess names you cannot read; put what you can read and explain in uncertainty.
Set needs_review when the date is uncertain, any row is below 0.7 confidence, a row label is not
one of the known machines, or numbers are missing where names are present."""


# ---------------------------------------------------------------------------
# Image handling + model call
# ---------------------------------------------------------------------------

def prepare_image(path: Path) -> tuple[str, str]:
    """Return (base64 data, media type): EXIF-upright and capped at MAX_EDGE."""
    from PIL import Image, ImageOps
    img = Image.open(path)
    img = ImageOps.exif_transpose(img)
    if max(img.size) > MAX_EDGE:
        img.thumbnail((MAX_EDGE, MAX_EDGE))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=88)
    return base64.standard_b64encode(buf.getvalue()).decode("utf-8"), "image/jpeg"


def _client() -> Any:
    try:
        import anthropic
    except ImportError as exc:
        raise SystemExit("The Anthropic SDK is not installed: pip install anthropic") from exc
    return anthropic.Anthropic()


def extract_report(image_path: Path, client: Any = None, model: str = MODEL) -> ShiftReport:
    client = client or _client()
    data, media_type = prepare_image(image_path)
    response = client.messages.parse(
        model=model,
        max_tokens=16000,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": data}},
                {"type": "text", "text": "Read this End of Shift report."},
            ],
        }],
        output_format=ShiftReport,
    )
    if response.stop_reason == "refusal":
        raise RuntimeError(f"Model declined to read {image_path.name}: {response.stop_details}")
    return response.parsed_output


# ---------------------------------------------------------------------------
# Report -> landing rows
# ---------------------------------------------------------------------------

def report_to_entries(report: ShiftReport, source: str, captured_at: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    shift = normalize_shift(report.shift)
    date = report.date
    unsure_date = report.date_confidence < 0.8 or bool(report.date_alternatives)
    rows = []
    for r in report.rows:
        machine = normalize_machine(r.machine_label)
        names, material_from_names = split_operator_material(", ".join(r.operators))
        material = r.material or material_from_names
        reasons = []
        if machine is None:
            reasons.append(f"unknown machine label {r.machine_label!r}")
        if r.confidence < 0.7:
            reasons.append(f"low confidence {r.confidence:.2f}")
        if unsure_date:
            reasons.append("date uncertain" + (f" (could be {', '.join(report.date_alternatives)})" if report.date_alternatives else ""))
        if names and r.machine_hours is None and r.man_hours is None:
            reasons.append("operators named but no hours")
        rows.append(make_entry(
            Date=date, Shift=shift, Machine_Name=machine or r.machine_label,
            Machine_Hours=r.machine_hours, Man_Hours=r.man_hours,
            Operator=names, Material=material, Comment=r.comment,
            Source=source, Confidence=round(min(r.confidence, report.date_confidence), 2),
            Needs_Review=bool(reasons) or report.needs_review,
            Captured_At=captured_at or datetime.now().isoformat(timespec="seconds"),
        ))
        if reasons:
            rows[-1]["Comment"] = (rows[-1]["Comment"] + " | " if rows[-1]["Comment"] else "") + "REVIEW: " + "; ".join(reasons)
    notes = [{"Date": date, "Shift": shift, "Note": n, "Source": source,
              "Captured_At": captured_at or datetime.now().isoformat(timespec="seconds")}
             for n in report.margin_notes if n and n.strip()]
    return (pd.DataFrame(rows) if rows else empty_entries(), pd.DataFrame(notes) if notes else empty_notes())


# ---------------------------------------------------------------------------
# Gmail fetch
# ---------------------------------------------------------------------------

def fetch_images(query: str = DEFAULT_QUERY, days_back: int = 30, out_dir: Path = SHIFT_REPORT_DIR,
                 service: Any = None) -> list[Path]:
    """Save image attachments from matching emails. Skips files already present."""
    import fetch_emails as fe
    service = service or fe.get_service()
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    for stub in fe.list_messages(service, f"{query} newer_than:{days_back}d"):
        msg = fe.get_message(service, stub["id"])
        raw_date = fe.header_value(msg, "Date")
        try:
            from email.utils import parsedate_to_datetime
            stamp = parsedate_to_datetime(raw_date).strftime("%Y%m%d_%H%M")
        except Exception:
            stamp = "undated"
        n = 0
        for part in fe.iter_attachments(msg["payload"]):
            mime = part.get("mimeType", "")
            if mime not in IMAGE_TYPES:
                continue
            n += 1
            ext = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif"}[mime]
            target = out_dir / f"{stamp}_{stub['id'][:8]}_{n}.{ext}"
            if target.exists():
                continue
            target.write_bytes(fe.download_attachment_bytes(service, stub["id"], part["body"]["attachmentId"]))
            logger.info("Saved %s", target.name)
            saved.append(target)
    return saved


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _extract_paths(paths: list[Path], dry_run: bool, model: str, output: Path) -> int:
    client = _client()
    all_e, all_n = [], []
    for p in paths:
        sidecar = p.with_suffix(p.suffix + ".json")
        if sidecar.exists() and not dry_run:
            report = ShiftReport.model_validate_json(sidecar.read_text())
        else:
            report = extract_report(p, client=client, model=model)
            if not dry_run:
                sidecar.write_text(report.model_dump_json(indent=2))
        e, n = report_to_entries(report, source=f"image:{p.name}")
        print(f"{p.name}: {report.date} {report.shift} shift — {len(e)} machine row(s), "
              f"{len(n)} note(s){' — NEEDS REVIEW: ' + '; '.join(report.review_reasons) if report.needs_review else ''}")
        if dry_run and len(e):
            print(e[["Machine_Name", "Machine_Hours", "Man_Hours", "Operator", "Material", "Comment"]].to_string(index=False))
        all_e.append(e); all_n.append(n)
    entries = pd.concat(all_e, ignore_index=True) if all_e else empty_entries()
    notes = pd.concat(all_n, ignore_index=True) if all_n else empty_notes()
    result = record(entries, notes, path=output, dry_run=dry_run)
    print(f"{'[dry-run] ' if dry_run else ''}{result['new_entries']} row(s) -> {result['total_entries']} on file "
          f"({result['needs_review']} need review)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="Download report photos from Gmail")
    f.add_argument("--query", default=DEFAULT_QUERY); f.add_argument("--days", type=int, default=30)
    f.add_argument("--out", type=Path, default=SHIFT_REPORT_DIR)
    x = sub.add_parser("extract", help="Read photos into labor entries")
    x.add_argument("--image", type=Path, nargs="*", help="Specific photo(s)")
    x.add_argument("--dir", type=Path, default=SHIFT_REPORT_DIR, help="Or every unprocessed photo in a folder")
    x.add_argument("--model", default=MODEL); x.add_argument("--dry-run", action="store_true")
    x.add_argument("--output", type=Path, default=LABOR_ENTRIES_PATH)
    r = sub.add_parser("run", help="fetch, then extract anything new")
    r.add_argument("--query", default=DEFAULT_QUERY); r.add_argument("--days", type=int, default=30)
    r.add_argument("--model", default=MODEL); r.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.cmd == "fetch":
        saved = fetch_images(args.query, args.days, args.out)
        print(f"{len(saved)} new photo(s) saved to {args.out}")
        return 0
    if args.cmd == "extract":
        paths = list(args.image or []) or sorted(
            p for p in args.dir.glob("*") if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}
            and not p.with_suffix(p.suffix + ".json").exists())
        if not paths:
            print("Nothing to extract."); return 0
        return _extract_paths(paths, args.dry_run, args.model, args.output)
    if args.cmd == "run":
        saved = fetch_images(args.query, args.days)
        print(f"{len(saved)} new photo(s)")
        return _extract_paths(saved, args.dry_run, args.model, LABOR_ENTRIES_PATH) if saved else 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
