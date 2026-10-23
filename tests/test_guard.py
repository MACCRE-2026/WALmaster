"""Tests for WALmaster Active Prevention Guard."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from core.guard import ActivePreventionGuard, GuardCycleResult


def test_guard_cycle_checkpoints_swelled_wal(tmp_path: Path) -> None:
    conv_dir = tmp_path / "conversations"
    conv_dir.mkdir()
    presence_dir = tmp_path / "presence"
    presence_dir.mkdir()
    sum_db = tmp_path / "conversation_summaries.db"

    # Setup summary DB in WAL mode
    with sqlite3.connect(sum_db) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE summaries (id TEXT PRIMARY KEY)")

    # Setup test conversation DB in WAL mode with frames
    cid = "test-guard-session"
    db_path = conv_dir / f"{cid}.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE steps (idx INTEGER PRIMARY KEY, data TEXT)")
        conn.execute("INSERT INTO steps VALUES (1, 'initial')")

    wal_path = conv_dir / f"{cid}.db-wal"
    assert wal_path.exists()

    # Create guard with a low threshold
    guard = ActivePreventionGuard(
        conv_dir=conv_dir,
        summaries_db=sum_db,
        presence_dir=presence_dir,
        max_wal_frames=1,  # Low threshold to trigger checkpoint
    )

    result: GuardCycleResult = guard.cycle()
    assert result.scanned_databases >= 1
    assert result.checkpoints_attempted >= 1
    assert result.checkpoint_errors == 0


def test_guard_cycle_reaps_stale_locks(tmp_path: Path) -> None:
    conv_dir = tmp_path / "conversations"
    conv_dir.mkdir()
    presence_dir = tmp_path / "presence"
    presence_dir.mkdir()
    sum_db = tmp_path / "conversation_summaries.db"

    with sqlite3.connect(sum_db) as conn:
        conn.execute("CREATE TABLE s (id INT)")

    # Create a stale lock file with an invalid PID (e.g. 99999999)
    stale_lock = presence_dir / "stale-session.lock"
    stale_lock.write_text('{"pid": 99999999, "image_name": "antigravity.exe"}', encoding="utf-8")

    guard = ActivePreventionGuard(
        conv_dir=conv_dir,
        summaries_db=sum_db,
        presence_dir=presence_dir,
        max_wal_frames=500,
    )

    result: GuardCycleResult = guard.cycle()
    assert result.locks_reaped >= 1
    assert not stale_lock.exists()
