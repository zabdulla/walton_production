"""
Atomic file writes + rolling snapshots for data files.

The aggregated Excel files and published HTML dashboards represent ~2 years
of accumulated state. If any one of them is partially written (e.g., the
process is killed mid-write while the launchd Mac wakes from sleep), the
file becomes corrupted and the next run reads garbage.

This module provides three guarantees used across the pipeline:

1.  **Atomic replace.** Write to ``<path>.tmp`` first, then ``os.replace`` to
    swap into place. On POSIX this is an atomic operation — the destination
    either has the old contents or the new contents, never a half-written
    truncation.

2.  **Rolling snapshots.** Before overwriting, copy the previous version to
    ``data/snapshots/<stem>_<timestamp>.<ext>``. The orchestrator keeps the
    most recent N snapshots so we have a quick recovery point if a parsing
    bug or bad upstream Excel produces garbage downstream.

3.  **Sanity-check on growth.** Aggregated row counts should only grow (new
    weeks added) or stay flat. A 90 % drop in row count is almost always
    a bug, not real, so we refuse to overwrite.

All helpers raise on failure rather than swallowing — callers can decide
whether that's fatal.
"""
from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Atomic writes
# ---------------------------------------------------------------------------

def _tmp_path(path: Path) -> Path:
    """Compute a sibling temp path for atomic-rename writes."""
    return path.with_suffix(path.suffix + ".tmp")


def write_atomic_bytes(path: Path, data: bytes) -> None:
    """Write *data* to *path* atomically via tmp + ``os.replace``."""
    tmp = _tmp_path(path)
    tmp.write_bytes(data)
    tmp.replace(path)


def write_atomic_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    """Write *text* to *path* atomically."""
    tmp = _tmp_path(path)
    tmp.write_text(text, encoding=encoding)
    tmp.replace(path)


def write_atomic_excel(df, path: Path, **to_excel_kwargs) -> None:
    """Write a pandas DataFrame to *path* atomically.

    Uses pandas' ``to_excel`` against a sibling ``.tmp`` file, then renames.
    """
    tmp = _tmp_path(path)
    df.to_excel(tmp, **to_excel_kwargs)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def _snapshot_name(path: Path, when: datetime | None = None) -> str:
    """Build a snapshot filename: ``<stem>_<YYYY-MM-DDTHH-MM-SS><ext>``."""
    if when is None:
        when = datetime.now()
    ts = when.strftime("%Y-%m-%dT%H-%M-%S")
    return f"{path.stem}_{ts}{path.suffix}"


def take_snapshot(path: Path, snapshot_dir: Path) -> Path | None:
    """Copy *path* to a timestamped snapshot.

    Returns the snapshot path, or ``None`` if *path* doesn't yet exist
    (first-run case — nothing to snapshot).
    """
    if not path.exists():
        return None
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snap = snapshot_dir / _snapshot_name(path)
    shutil.copy2(path, snap)
    logger.debug(f"Snapshot: {path.name} → {snap.name}")
    return snap


def rotate_snapshots(snapshot_dir: Path, stem: str, suffix: str, keep: int = 7) -> int:
    """Keep the *keep* most recent snapshots matching *stem*_*<ext>*.

    Returns the number of older snapshots deleted. Safe if dir is missing.
    """
    if not snapshot_dir.exists():
        return 0
    pattern = f"{stem}_*{suffix}"
    snaps = sorted(snapshot_dir.glob(pattern),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    removed = 0
    for old in snaps[keep:]:
        try:
            old.unlink()
            removed += 1
        except OSError as e:
            logger.warning(f"Could not rotate {old.name}: {e}")
    return removed


def restore_from_snapshot(target: Path, snapshot: Path) -> None:
    """Restore *target* from *snapshot* using an atomic swap."""
    if not snapshot.exists():
        raise FileNotFoundError(f"Snapshot not found: {snapshot}")
    tmp = _tmp_path(target)
    shutil.copy2(snapshot, tmp)
    tmp.replace(target)
    logger.info(f"Restored {target.name} from snapshot {snapshot.name}")


# ---------------------------------------------------------------------------
# Sanity check
# ---------------------------------------------------------------------------

class GrowthSanityError(ValueError):
    """Raised when a new write would shrink an existing aggregated file."""


def check_growth(new_count: int, existing_path: Path, min_ratio: float = 0.9) -> str:
    """Verify *new_count* rows is at least *min_ratio* of the existing file.

    Returns a status message string. Raises ``GrowthSanityError`` if the
    new count is unexpectedly small (likely a bug — aggregated data should
    only grow).

    Skipped (returns "first write") if the file doesn't exist yet.
    """
    if not existing_path.exists():
        return "first write — no existing file to compare"
    try:
        import pandas as pd
        existing = pd.read_excel(existing_path)
        old_count = len(existing)
    except Exception as e:
        # If we can't read the old file, log and proceed — better to write
        # the new file than wedge the pipeline because the old one is broken.
        logger.warning(f"Sanity check could not read {existing_path.name}: {e}")
        return f"sanity check skipped (could not read existing: {e})"

    if old_count == 0:
        return "existing file was empty — no comparison"

    ratio = new_count / old_count
    if ratio < min_ratio:
        msg = (
            f"new row count {new_count:,} is only {ratio:.1%} of existing "
            f"{old_count:,} (threshold {min_ratio:.0%})"
        )
        raise GrowthSanityError(msg)
    return f"growth OK: {old_count:,} → {new_count:,} ({ratio:.1%})"


# ---------------------------------------------------------------------------
# High-level helper
# ---------------------------------------------------------------------------

def write_with_snapshot(
    path: Path,
    write_fn: Callable[[Path], None],
    snapshot_dir: Path,
    new_row_count: int | None = None,
    min_ratio: float = 0.9,
    keep_snapshots: int = 7,
) -> dict:
    """Coordinated atomic-write-with-snapshot-and-sanity-check.

    1.  If ``new_row_count`` is given, sanity-check against existing file.
    2.  Snapshot the current file (if any).
    3.  Atomically replace via ``write_fn(tmp_path)``.
    4.  Rotate snapshots, keeping the most recent *keep_snapshots*.

    Returns ``{"snapshot": Path | None, "growth_msg": str}``.
    Raises ``GrowthSanityError`` if the new count looks wrong.
    """
    growth_msg = ""
    if new_row_count is not None:
        growth_msg = check_growth(new_row_count, path, min_ratio=min_ratio)

    snap = take_snapshot(path, snapshot_dir)

    tmp = _tmp_path(path)
    write_fn(tmp)
    tmp.replace(path)

    rotate_snapshots(snapshot_dir, path.stem, path.suffix, keep=keep_snapshots)
    return {"snapshot": snap, "growth_msg": growth_msg}
