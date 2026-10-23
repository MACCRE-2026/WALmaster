"""Lease Barrier and Concurrency Arbitration Test Suite.

Verifies that without the WALmaster arbiter barrier, concurrent access between
desktop Electron and cloud runner workers generates SQLITE_BUSY or locked states.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Final

import pytest

from core.arbiter import CheckpointResult, WALArbiter

SQLITE_BUSY_CODE: Final[int] = 1


def _init_wal_database(db_path: Path) -> None:
    """Initialize a clean WAL mode database."""
    with closing(sqlite3.connect(db_path, timeout=5.0)) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS session_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id TEXT NOT NULL,
                counter INTEGER NOT NULL,
                updated_at_epoch REAL NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO session_registry (worker_id, counter, updated_at_epoch) VALUES (?, ?, ?);",
            ("init_worker", 0, time.time()),
        )
        conn.commit()


def test_uncoordinated_checkpoint_during_active_write_returns_busy(ephemeral_db_path: Path) -> None:
    """Verify that attempting to checkpoint while an uncoordinated writer holds a lock returns SQLITE_BUSY."""
    _init_wal_database(ephemeral_db_path)

    # Worker 1: Desktop Electron process holds an uncommitted exclusive transaction
    conn_desktop: sqlite3.Connection = sqlite3.connect(ephemeral_db_path, timeout=0.1)
    try:
        conn_desktop.execute("BEGIN EXCLUSIVE;")
        conn_desktop.execute(
            "UPDATE session_registry SET counter = counter + 1 WHERE worker_id = 'init_worker';"
        )

        # Worker 2: Cloud runner or external checkpoint attempt without arbiter barrier
        with closing(sqlite3.connect(ephemeral_db_path, timeout=0.1)) as conn_cloud:
            res: tuple[int, int, int] | None = conn_cloud.execute(
                "PRAGMA wal_checkpoint(TRUNCATE);"
            ).fetchone()

            assert res is not None, "PRAGMA wal_checkpoint must return a 3-tuple result."
            busy_flag, _log_frames, _checkpointed_frames = res

            # Without coordination, SQLite must report SQLITE_BUSY (busy_flag == 1)
            assert busy_flag == SQLITE_BUSY_CODE, (
                f"Expected SQLITE_BUSY (1) during uncoordinated checkpoint, got {busy_flag}."
            )
    finally:
        conn_desktop.rollback()
        conn_desktop.close()


def test_concurrent_uncoordinated_writers_generate_locked_state(ephemeral_db_path: Path) -> None:
    """Verify that competing writers without arbitration encounter 'database is locked' errors."""
    _init_wal_database(ephemeral_db_path)

    # Worker 1 acquires exclusive write lock
    conn_worker1: sqlite3.Connection = sqlite3.connect(ephemeral_db_path, timeout=0.1)
    try:
        conn_worker1.execute("BEGIN EXCLUSIVE;")
        conn_worker1.execute("INSERT INTO session_registry (worker_id, counter, updated_at_epoch) VALUES (?, ?, ?);",
                             ("desktop_primary", 100, time.time()))

        # Worker 2 attempts immediate write with near-zero timeout
        with closing(sqlite3.connect(ephemeral_db_path, timeout=0.01)) as conn_worker2:
            with pytest.raises(sqlite3.OperationalError) as exc_info:
                conn_worker2.execute("BEGIN EXCLUSIVE;")

            error_msg: str = str(exc_info.value).lower()
            assert "database is locked" in error_msg or "busy" in error_msg
    finally:
        conn_worker1.rollback()
        conn_worker1.close()


def test_multithreaded_concurrent_worker_contention(ephemeral_db_path: Path) -> None:
    """Simulate two concurrent threads (desktop vs cloud runner) colliding on checkpointing and writing."""
    _init_wal_database(ephemeral_db_path)

    worker_errors: list[tuple[str, str]] = []
    worker1_started: threading.Event = threading.Event()
    stop_simulation: threading.Event = threading.Event()

    def desktop_thread_fn() -> None:
        """Simulates desktop Electron performing continuous writes."""
        conn: sqlite3.Connection = sqlite3.connect(ephemeral_db_path, timeout=0.05)
        try:
            conn.execute("BEGIN EXCLUSIVE;")
            conn.execute(
                "UPDATE session_registry SET counter = counter + 10 WHERE worker_id = 'init_worker';"
            )
            worker1_started.set()
            while not stop_simulation.is_set():
                time.sleep(0.01)
            conn.commit()
        except sqlite3.Error as exc:
            worker_errors.append(("desktop_thread", str(exc)))
        finally:
            conn.close()

    def cloud_thread_fn() -> None:
        """Simulates cloud runner attempting an uncoordinated checkpoint."""
        worker1_started.wait(timeout=2.0)
        conn: sqlite3.Connection = sqlite3.connect(ephemeral_db_path, timeout=0.05)
        try:
            res: tuple[int, int, int] | None = conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchone()
            if res and res[0] == SQLITE_BUSY_CODE:
                worker_errors.append(("cloud_thread", "SQLITE_BUSY"))
        except sqlite3.Error as exc:
            worker_errors.append(("cloud_thread", str(exc)))
        finally:
            conn.close()

    t_desktop: threading.Thread = threading.Thread(target=desktop_thread_fn, name="desktop_worker")
    t_cloud: threading.Thread = threading.Thread(target=cloud_thread_fn, name="cloud_worker")

    try:
        t_desktop.start()
        t_cloud.start()

        t_cloud.join(timeout=3.0)
        assert not t_cloud.is_alive(), "Cloud worker thread timed out."
    finally:
        stop_simulation.set()
        t_desktop.join(timeout=3.0)

    # Contention MUST have been detected by at least one worker
    assert len(worker_errors) > 0, "Concurrent uncoordinated access failed to produce expected contention!"
    detected_busy_or_locked: bool = any(
        "SQLITE_BUSY" in msg or "locked" in msg.lower() or "busy" in msg.lower()
        for _, msg in worker_errors
    )
    assert detected_busy_or_locked, f"Expected SQLITE_BUSY or locked state in: {worker_errors}"


def test_arbiter_barrier_detects_contention_and_aborts_handover(ephemeral_db_path: Path) -> None:
    """Verify that WALArbiter detects active writer contention and refuses to grant handover."""
    _init_wal_database(ephemeral_db_path)

    conn_desktop: sqlite3.Connection = sqlite3.connect(ephemeral_db_path, timeout=0.1)
    try:
        conn_desktop.execute("BEGIN EXCLUSIVE;")
        conn_desktop.execute(
            "UPDATE session_registry SET counter = 999 WHERE worker_id = 'init_worker';"
        )

        # Arbiter checks barrier for handover with fast timeout
        arbiter: WALArbiter = WALArbiter(ephemeral_db_path)
        checkpoint_result: CheckpointResult = arbiter.enforce_checkpoint_barrier(mode="TRUNCATE", timeout=0.1)

        # Handover must be rejected because database is busy
        assert checkpoint_result.success is False, "Barrier should fail when database is locked by active writer."
        assert checkpoint_result.busy == SQLITE_BUSY_CODE, f"Expected busy flag=1, got {checkpoint_result.busy}."
    finally:
        conn_desktop.rollback()
        conn_desktop.close()
