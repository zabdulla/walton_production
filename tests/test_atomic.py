"""Tests for src/atomic.py — atomic writes, snapshots, growth sanity."""
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from atomic import (
    GrowthSanityError,
    _snapshot_name,
    _tmp_path,
    check_growth,
    restore_from_snapshot,
    rotate_snapshots,
    take_snapshot,
    write_atomic_bytes,
    write_atomic_excel,
    write_atomic_text,
    write_with_snapshot,
)


# ---------------------------------------------------------------------------
# write_atomic_bytes / write_atomic_text
# ---------------------------------------------------------------------------

def test_write_atomic_bytes_creates_file(tmp_path: Path) -> None:
    target = tmp_path / "out.bin"
    write_atomic_bytes(target, b"hello world")
    assert target.read_bytes() == b"hello world"


def test_write_atomic_bytes_overwrites_existing(tmp_path: Path) -> None:
    target = tmp_path / "out.bin"
    target.write_bytes(b"old content")
    write_atomic_bytes(target, b"new content")
    assert target.read_bytes() == b"new content"


def test_write_atomic_bytes_leaves_no_tmp_file_on_success(tmp_path: Path) -> None:
    target = tmp_path / "out.bin"
    write_atomic_bytes(target, b"x")
    assert not _tmp_path(target).exists()


def test_write_atomic_text_handles_utf8(tmp_path: Path) -> None:
    target = tmp_path / "out.txt"
    write_atomic_text(target, "café — 日本語")
    assert target.read_text(encoding="utf-8") == "café — 日本語"


def test_write_atomic_excel_roundtrip(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    target = tmp_path / "out.xlsx"
    write_atomic_excel(df, target, index=False)
    read_back = pd.read_excel(target)
    pd.testing.assert_frame_equal(df, read_back)


def test_tmp_path_has_tmp_suffix() -> None:
    assert _tmp_path(Path("/x/y/foo.xlsx")) == Path("/x/y/foo.xlsx.tmp")
    assert _tmp_path(Path("foo.html")) == Path("foo.html.tmp")


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def test_snapshot_name_uses_iso_timestamp() -> None:
    when = datetime(2026, 5, 27, 13, 35, 42)
    assert _snapshot_name(Path("foo.xlsx"), when=when) == "foo_2026-05-27T13-35-42.xlsx"


def test_take_snapshot_copies_file(tmp_path: Path) -> None:
    src = tmp_path / "data.xlsx"
    src.write_bytes(b"original")
    snap_dir = tmp_path / "snapshots"

    snap = take_snapshot(src, snap_dir)

    assert snap is not None
    assert snap.parent == snap_dir
    assert snap.read_bytes() == b"original"
    # The original is untouched
    assert src.read_bytes() == b"original"


def test_take_snapshot_returns_none_when_source_missing(tmp_path: Path) -> None:
    snap = take_snapshot(tmp_path / "does_not_exist.xlsx", tmp_path / "snapshots")
    assert snap is None


def test_rotate_snapshots_keeps_most_recent(tmp_path: Path) -> None:
    snap_dir = tmp_path / "snapshots"
    snap_dir.mkdir()
    # Create 10 fake snapshots with monotonic mtimes
    for i in range(10):
        f = snap_dir / f"data_2026-05-27T13-{i:02d}-00.xlsx"
        f.write_bytes(b"x")
        # Set mtime so the loop ordering is deterministic
        ts = time.time() + i
        import os
        os.utime(f, (ts, ts))

    removed = rotate_snapshots(snap_dir, stem="data", suffix=".xlsx", keep=3)
    remaining = sorted(snap_dir.glob("data_*.xlsx"))
    assert removed == 7
    assert len(remaining) == 3
    # The 3 most-recent (highest i) should remain
    for f in remaining:
        assert any(f"-{j:02d}-00" in f.name for j in (7, 8, 9))


def test_rotate_snapshots_ignores_missing_dir(tmp_path: Path) -> None:
    # Should not raise
    assert rotate_snapshots(tmp_path / "nonexistent", "data", ".xlsx") == 0


def test_restore_from_snapshot_swaps_contents(tmp_path: Path) -> None:
    target = tmp_path / "data.xlsx"
    target.write_bytes(b"bad")
    snap = tmp_path / "snap.xlsx"
    snap.write_bytes(b"good")

    restore_from_snapshot(target, snap)
    assert target.read_bytes() == b"good"
    # Snapshot is preserved
    assert snap.read_bytes() == b"good"


def test_restore_from_snapshot_raises_when_snapshot_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        restore_from_snapshot(tmp_path / "target.xlsx", tmp_path / "missing.xlsx")


# ---------------------------------------------------------------------------
# Growth sanity check
# ---------------------------------------------------------------------------

def test_check_growth_first_write(tmp_path: Path) -> None:
    msg = check_growth(100, tmp_path / "new.xlsx")
    assert "first write" in msg


def test_check_growth_normal_growth(tmp_path: Path) -> None:
    existing = tmp_path / "data.xlsx"
    pd.DataFrame({"a": list(range(100))}).to_excel(existing, index=False)
    msg = check_growth(120, existing, min_ratio=0.9)
    assert "growth OK" in msg or "100" in msg


def test_check_growth_flat_is_ok(tmp_path: Path) -> None:
    existing = tmp_path / "data.xlsx"
    pd.DataFrame({"a": list(range(100))}).to_excel(existing, index=False)
    # Same count → ratio 1.0, above threshold
    msg = check_growth(100, existing, min_ratio=0.9)
    assert "OK" in msg or "growth" in msg.lower()


def test_check_growth_raises_on_shrink(tmp_path: Path) -> None:
    existing = tmp_path / "data.xlsx"
    pd.DataFrame({"a": list(range(100))}).to_excel(existing, index=False)
    with pytest.raises(GrowthSanityError):
        check_growth(50, existing, min_ratio=0.9)  # 50% of 100 < 90% threshold


def test_check_growth_allows_above_min_ratio(tmp_path: Path) -> None:
    existing = tmp_path / "data.xlsx"
    pd.DataFrame({"a": list(range(100))}).to_excel(existing, index=False)
    # Below default but above explicit min_ratio
    msg = check_growth(60, existing, min_ratio=0.5)
    assert "OK" in msg


def test_check_growth_empty_existing(tmp_path: Path) -> None:
    existing = tmp_path / "data.xlsx"
    pd.DataFrame({"a": []}).to_excel(existing, index=False)
    msg = check_growth(10, existing)
    assert "empty" in msg


# ---------------------------------------------------------------------------
# write_with_snapshot — the high-level helper
# ---------------------------------------------------------------------------

def test_write_with_snapshot_first_write_no_snapshot(tmp_path: Path) -> None:
    target = tmp_path / "data.xlsx"
    snap_dir = tmp_path / "snapshots"

    df = pd.DataFrame({"a": [1, 2, 3]})
    result = write_with_snapshot(
        target,
        lambda tmp: df.to_excel(tmp, index=False),
        snap_dir,
        new_row_count=3,
    )

    assert target.exists()
    assert result["snapshot"] is None  # no previous version to snapshot
    assert "first write" in result["growth_msg"]


def test_write_with_snapshot_subsequent_write_creates_snapshot(tmp_path: Path) -> None:
    target = tmp_path / "data.xlsx"
    snap_dir = tmp_path / "snapshots"

    pd.DataFrame({"a": [1, 2, 3]}).to_excel(target, index=False)

    df_new = pd.DataFrame({"a": [1, 2, 3, 4]})
    result = write_with_snapshot(
        target,
        lambda tmp: df_new.to_excel(tmp, index=False),
        snap_dir,
        new_row_count=4,
    )

    assert result["snapshot"] is not None
    assert result["snapshot"].exists()
    # New file in place
    assert len(pd.read_excel(target)) == 4
    # Snapshot is the OLD content (pre-overwrite)
    assert len(pd.read_excel(result["snapshot"])) == 3


def test_write_with_snapshot_blocks_shrink(tmp_path: Path) -> None:
    target = tmp_path / "data.xlsx"
    snap_dir = tmp_path / "snapshots"

    pd.DataFrame({"a": list(range(100))}).to_excel(target, index=False)

    with pytest.raises(GrowthSanityError):
        write_with_snapshot(
            target,
            lambda tmp: pd.DataFrame({"a": [1]}).to_excel(tmp, index=False),
            snap_dir,
            new_row_count=1,
            min_ratio=0.9,
        )
    # Target unchanged
    assert len(pd.read_excel(target)) == 100


def test_write_with_snapshot_rotates_old_snapshots(tmp_path: Path) -> None:
    target = tmp_path / "data.xlsx"
    snap_dir = tmp_path / "snapshots"

    # 10 successive writes (each older becomes a snapshot)
    for i in range(10):
        write_with_snapshot(
            target,
            lambda tmp, n=i: pd.DataFrame({"a": list(range(100 + n))}).to_excel(tmp, index=False),
            snap_dir,
            new_row_count=100 + i,
            keep_snapshots=3,
        )
        # Force monotonic timestamps so rotation is deterministic
        time.sleep(1.05)

    # Only the 3 most-recent snapshots remain
    remaining = list(snap_dir.glob("data_*.xlsx"))
    assert len(remaining) == 3
