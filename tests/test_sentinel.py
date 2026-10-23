"""Test suite for WALmaster Core Sentinel and Two-Tier Registry Reconciler.

Verifies:
1. Presence lock inspection and reaping:
   - Stale lock detection with dead PID.
   - Active lock verification with running allowed process image (e.g. python.exe).
   - PID reuse or runner mismatch handling (live process with non-matching executable image).
   - Corrupted or empty lock file handling.
   - Batch directory sweep and lock cleanup.
2. Two-tier registry reconciliation:
   - Synchronizing step count and last timestamp from conversation DB (`steps` table).
   - Synchronizing step count and last timestamp from conversation DB (`trajectory_meta` table).
   - Idempotent upsert verification into `conversation_summaries` table.
   - Missing conversation database handling.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from core.sentinel import (
    DEFAULT_ALLOWED_RUNNERS,
    LockReapStatus,
    Sentinel,
    inspect_presence_lock,
    reap_presence_locks,
    reconcile_two_tier_registry,
)


@contextmanager
def open_sqlite(db_path: Path | str) -> Generator[sqlite3.Connection, None, None]:
    """Deterministic SQLite context manager closing connection on exit."""
    conn: sqlite3.Connection = sqlite3.connect(db_path, timeout=10.0)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def temp_presence_dir(test_artifacts_dir: Path) -> Generator[Path, None, None]:
    """Provide an isolated presence lock directory on B: drive."""
    lock_dir: Path = test_artifacts_dir / "presence_test"
    lock_dir.mkdir(parents=True, exist_ok=True)
    try:
        yield lock_dir
    finally:
        for f in lock_dir.glob("*.lock"):
            try:
                f.unlink(missing_ok=True)
            except OSError:
                pass
        if lock_dir.exists():
            try:
                lock_dir.rmdir()
            except OSError:
                pass


def test_default_allowed_runners_configuration() -> None:
    """Verify default runners include canonical Electron and Python runners."""
    assert "antigravity.exe" in DEFAULT_ALLOWED_RUNNERS
    assert "code.exe" in DEFAULT_ALLOWED_RUNNERS
    assert "electron.exe" in DEFAULT_ALLOWED_RUNNERS
    assert "python.exe" in DEFAULT_ALLOWED_RUNNERS


def test_reap_stale_lock_with_dead_pid(temp_presence_dir: Path) -> None:
    """A lock owned by an extinct PID must be reaped and unlinked."""
    stale_lock: Path = temp_presence_dir / "dead_session.lock"
    payload: dict[str, object] = {
        "owner_id": "test_dead_session",
        "owner_pid": 9999999,
        "created_at_epoch": time.time() - 7200.0,
        "description": "Session from crashed process",
    }
    stale_lock.write_text(json.dumps(payload), encoding="utf-8")
    assert stale_lock.exists()

    status: LockReapStatus = inspect_presence_lock(stale_lock)
    assert status.reaped is True
    assert status.active is False
    assert status.is_alive is False
    assert status.pid == 9999999
    assert not stale_lock.exists()


def test_active_lock_held_by_allowed_runner(temp_presence_dir: Path) -> None:
    """A lock owned by an active allowed process must remain active and not be unlinked."""
    active_lock: Path = temp_presence_dir / "active_session.lock"
    current_pid: int = os.getpid()
    payload: dict[str, object] = {
        "owner_id": "current_test_runner",
        "owner_pid": current_pid,
        "created_at_epoch": time.time(),
        "description": "Active test session",
    }
    active_lock.write_text(json.dumps(payload), encoding="utf-8")
    assert active_lock.exists()

    status: LockReapStatus = inspect_presence_lock(active_lock)
    assert status.active is True
    assert status.reaped is False
    assert status.is_alive is True
    assert status.is_allowed_runner is True
    assert status.pid == current_pid
    assert active_lock.exists()


def test_reap_lock_when_process_image_mismatches_allowed(temp_presence_dir: Path) -> None:
    """A lock whose PID is alive but does not match allowed runners must be reaped."""
    mismatched_lock: Path = temp_presence_dir / "mismatched_session.lock"
    current_pid: int = os.getpid()
    payload: dict[str, object] = {
        "owner_id": "rogue_or_reused_pid_process",
        "owner_pid": current_pid,
        "created_at_epoch": time.time(),
        "description": "PID reused by non-allowed executable",
    }
    mismatched_lock.write_text(json.dumps(payload), encoding="utf-8")
    assert mismatched_lock.exists()

    # Restrict allowed runners to only antigravity.exe, excluding python.exe
    status: LockReapStatus = inspect_presence_lock(
        mismatched_lock, allowed_runners={"antigravity.exe", "code.exe"}
    )
    assert status.reaped is True
    assert status.active is False
    assert status.is_alive is True
    assert status.is_allowed_runner is False
    assert not mismatched_lock.exists()


def test_reap_corrupted_or_empty_lock_files(temp_presence_dir: Path) -> None:
    """Corrupted, unparseable, or empty lock files must be safely reaped."""
    empty_lock: Path = temp_presence_dir / "empty.lock"
    empty_lock.write_text("", encoding="utf-8")

    malformed_lock: Path = temp_presence_dir / "malformed.lock"
    malformed_lock.write_text("{this is not valid json!}", encoding="utf-8")

    no_pid_lock: Path = temp_presence_dir / "no_pid.lock"
    no_pid_lock.write_text(json.dumps({"some_key": "some_value"}), encoding="utf-8")

    for lock_file in (empty_lock, malformed_lock, no_pid_lock):
        status: LockReapStatus = inspect_presence_lock(lock_file)
        assert status.reaped is True
        assert status.active is False
        assert status.pid is None
        assert not lock_file.exists()


def test_raw_numeric_pid_lock_file(temp_presence_dir: Path) -> None:
    """Lock file containing only raw numeric PID string should be parsed correctly."""
    numeric_lock: Path = temp_presence_dir / "numeric.lock"
    numeric_lock.write_text(f"{os.getpid()}\n", encoding="utf-8")

    status: LockReapStatus = inspect_presence_lock(numeric_lock)
    assert status.pid == os.getpid()
    assert status.active is True
    assert status.reaped is False
    assert numeric_lock.exists()


def test_batch_reap_presence_locks_in_directory(temp_presence_dir: Path) -> None:
    """Directory sweep should reap dead/corrupted locks and preserve active ones."""
    # Dead lock
    dead_lock: Path = temp_presence_dir / "session1.lock"
    dead_lock.write_text(json.dumps({"owner_pid": 9999998}), encoding="utf-8")

    # Corrupt lock
    corrupt_lock: Path = temp_presence_dir / "session2.lock"
    corrupt_lock.write_text("corrupt_content", encoding="utf-8")

    # Active lock
    active_lock: Path = temp_presence_dir / "session3.lock"
    active_lock.write_text(json.dumps({"owner_pid": os.getpid()}), encoding="utf-8")

    results: list[LockReapStatus] = reap_presence_locks(temp_presence_dir)
    assert len(results) == 3

    reaped_paths: set[Path] = {r.lock_path for r in results if r.reaped}
    assert dead_lock in reaped_paths
    assert corrupt_lock in reaped_paths
    assert active_lock not in reaped_paths
    assert active_lock.exists()
    assert not dead_lock.exists()
    assert not corrupt_lock.exists()


def test_reconcile_two_tier_registry_from_steps(
    test_artifacts_dir: Path, ephemeral_db_path: Path
) -> None:
    """Reconciliation extracts max steps and timestamp from `steps` table and upserts summary."""
    conv_id: str = "conv-steps-test-01"
    summary_db_path: Path = test_artifacts_dir / "test_summaries_steps.db"
    summary_db_path.unlink(missing_ok=True)

    try:
        now: float = 1710000000.0
        with open_sqlite(ephemeral_db_path) as conn:
            conn.execute(
                """
                CREATE TABLE steps (
                    step_index INTEGER PRIMARY KEY,
                    step_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp REAL NOT NULL
                )
                """
            )
            for i in range(12):
                conn.execute(
                    "INSERT INTO steps (step_index, step_type, status, timestamp) VALUES (?, ?, ?, ?)",
                    (i, "action", "completed", now + (i * 10.0)),
                )
            conn.commit()

        updated_count: int = reconcile_two_tier_registry(conv_id, ephemeral_db_path, summary_db_path)
        assert updated_count == 12

        # Verify summary table contains the upserted row
        with open_sqlite(summary_db_path) as conn:
            row = conn.execute(
                "SELECT conversation_id, step_count, last_modified_time FROM conversation_summaries WHERE conversation_id = ?",
                (conv_id,),
            ).fetchone()
            assert row is not None
            assert row[0] == conv_id
            assert row[1] == 12
            assert row[2] == pytest.approx(now + (11 * 10.0))
    finally:
        summary_db_path.unlink(missing_ok=True)


def test_reconcile_two_tier_registry_from_trajectory_meta(
    test_artifacts_dir: Path, ephemeral_db_path: Path
) -> None:
    """Reconciliation extracts step_count and last_modified_time from `trajectory_meta`."""
    conv_id: str = "conv-meta-test-02"
    summary_db_path: Path = test_artifacts_dir / "test_summaries_meta.db"
    summary_db_path.unlink(missing_ok=True)

    try:
        expected_timestamp: float = 1715000500.25
        with open_sqlite(ephemeral_db_path) as conn:
            conn.execute(
                """
                CREATE TABLE trajectory_meta (
                    conversation_id TEXT PRIMARY KEY,
                    step_count INTEGER NOT NULL,
                    last_modified_time REAL NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO trajectory_meta (conversation_id, step_count, last_modified_time) VALUES (?, ?, ?)",
                (conv_id, 45, expected_timestamp),
            )
            conn.commit()

        count: int = reconcile_two_tier_registry(conv_id, ephemeral_db_path, summary_db_path)
        assert count == 45

        with open_sqlite(summary_db_path) as conn:
            row = conn.execute(
                "SELECT conversation_id, step_count, last_modified_time FROM conversation_summaries WHERE conversation_id = ?",
                (conv_id,),
            ).fetchone()
            assert row is not None
            assert row[0] == conv_id
            assert row[1] == 45
            assert row[2] == pytest.approx(expected_timestamp)
    finally:
        summary_db_path.unlink(missing_ok=True)


def test_reconcile_two_tier_registry_upsert_idempotency(
    test_artifacts_dir: Path, ephemeral_db_path: Path
) -> None:
    """Repeated reconciliations update existing summary record without duplicates."""
    conv_id: str = "conv-idempotent-03"
    summary_db_path: Path = test_artifacts_dir / "test_summaries_idempotent.db"
    summary_db_path.unlink(missing_ok=True)

    try:
        # Initial state: 3 steps
        with open_sqlite(ephemeral_db_path) as conn:
            conn.execute(
                "CREATE TABLE steps (step_index INTEGER PRIMARY KEY, timestamp REAL NOT NULL)"
            )
            for i in range(3):
                conn.execute("INSERT INTO steps VALUES (?, ?)", (i, 1000.0 + i))
            conn.commit()

        count1: int = reconcile_two_tier_registry(conv_id, ephemeral_db_path, summary_db_path)
        assert count1 == 3

        # Add 4 more steps
        with open_sqlite(ephemeral_db_path) as conn:
            for i in range(3, 7):
                conn.execute("INSERT INTO steps VALUES (?, ?)", (i, 1000.0 + i))
            conn.commit()

        count2: int = reconcile_two_tier_registry(conv_id, ephemeral_db_path, summary_db_path)
        assert count2 == 7

        # Verify summary db has exactly one row for this conv_id with step_count = 7
        with open_sqlite(summary_db_path) as conn:
            rows = conn.execute(
                "SELECT conversation_id, step_count, last_modified_time FROM conversation_summaries WHERE conversation_id = ?",
                (conv_id,),
            ).fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 7
            assert rows[0][2] == pytest.approx(1006.0)
    finally:
        summary_db_path.unlink(missing_ok=True)


def test_reconcile_missing_conv_db_raises_filenotfound(test_artifacts_dir: Path) -> None:
    """Non-existent conversation DB path must raise FileNotFoundError."""
    missing_path: Path = test_artifacts_dir / "non_existent_conv.db"
    summary_db_path: Path = test_artifacts_dir / "test_summaries_missing.db"

    with pytest.raises(FileNotFoundError):
        reconcile_two_tier_registry("conv-missing", missing_path, summary_db_path)


def test_sentinel_class_instance_usage(
    temp_presence_dir: Path, test_artifacts_dir: Path, ephemeral_db_path: Path
) -> None:
    """Verify Sentinel object-oriented interface works identically."""
    sentinel: Sentinel = Sentinel(allowed_runners={"python.exe", "custom_runner.exe"})
    assert "python.exe" in sentinel.allowed_runners
    assert "custom_runner.exe" in sentinel.allowed_runners

    # Check active lock with class instance
    lock_file: Path = temp_presence_dir / "oop_active.lock"
    lock_file.write_text(json.dumps({"owner_pid": os.getpid()}), encoding="utf-8")

    status: LockReapStatus = sentinel.inspect_presence_lock(lock_file)
    assert status.active is True
    assert status.reaped is False

    # Check reconcile with class instance
    with open_sqlite(ephemeral_db_path) as conn:
        conn.execute("CREATE TABLE steps (step_index INTEGER PRIMARY KEY, timestamp REAL)")
        conn.execute("INSERT INTO steps VALUES (1, 500.0)")
        conn.commit()

    summary_path: Path = test_artifacts_dir / "test_summaries_oop.db"
    summary_path.unlink(missing_ok=True)
    try:
        count: int = sentinel.reconcile_two_tier_registry("conv-oop", ephemeral_db_path, summary_path)
        assert count == 1
    finally:
        summary_path.unlink(missing_ok=True)
