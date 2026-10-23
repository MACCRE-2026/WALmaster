"""Torn Page Injection and Concurrency Crash Harness.

Simulates torn writes, mid-frame truncations, and unfinalized WAL frame headers
to empirically establish the Red baseline for SQLite WAL failure states.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Final

from core.arbiter import CheckpointResult, WALArbiter

PAGE_SIZE: Final[int] = 4096
PAGE_32_INDEX: Final[int] = 32
PAGE_32_OFFSET: Final[int] = (PAGE_32_INDEX - 1) * PAGE_SIZE


def _populate_multipage_wal_db(db_path: Path, min_pages: int = 40) -> None:
    """Initialize an SQLite database in WAL mode and insert sufficient rows to span min_pages."""
    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute("PRAGMA page_size = 4096;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                step_number INTEGER NOT NULL,
                payload TEXT NOT NULL
            );
            """
        )
        # Each row is ~2KB, so 100 rows easily exceed 40 pages (160KB+)
        large_payload: str = "A" * 2000
        for i in range(1, 120):
            conn.execute(
                "INSERT INTO conversation_steps (conversation_id, step_number, payload) VALUES (?, ?, ?);",
                ("conv_alpha_test", i, large_payload),
            )
        conn.commit()


def test_torn_page_32_corruption_detected_by_integrity_check(ephemeral_db_path: Path) -> None:
    """Verify that corrupting Page 32 is detected by PRAGMA integrity_check or DatabaseError."""
    _populate_multipage_wal_db(ephemeral_db_path, min_pages=40)

    # Verify initial health
    with closing(sqlite3.connect(ephemeral_db_path)) as conn:
        pre_check: list[tuple[str]] = conn.execute("PRAGMA integrity_check;").fetchall()
        assert pre_check == [("ok",)], f"Expected clean db initially, got: {pre_check}"

    db_size: int = ephemeral_db_path.stat().st_size
    total_pages: int = db_size // PAGE_SIZE
    assert total_pages >= PAGE_32_INDEX, f"Database must have at least {PAGE_32_INDEX} pages, got {total_pages}"

    # Inject torn write directly at Page 32: corrupt the B-Tree leaf cell pointers
    corrupt_bytes: bytes = b"\x0a\x00\x00\x01\x00\x05\x00\x02\xff\xff\xde\xad\xbe\xef"
    with open(ephemeral_db_path, "r+b") as fh:
        fh.seek(PAGE_32_OFFSET)
        fh.write(corrupt_bytes)

    # Verification: PRAGMA integrity_check detects the corruption or raises DatabaseError
    corruption_detected: bool = False
    try:
        with closing(sqlite3.connect(ephemeral_db_path)) as conn:
            results: list[tuple[str]] = conn.execute("PRAGMA integrity_check;").fetchall()
            output_str: str = " ".join(r[0] for r in results if r and r[0])
            if output_str != "ok":
                corruption_detected = True
                assert (
                    "page 32" in output_str.lower()
                    or "error" in output_str.lower()
                    or "offset" in output_str.lower()
                ), f"Unexpected integrity output: {output_str}"

            # Querying corrupted page should also fail
            try:
                conn.execute("SELECT * FROM conversation_steps WHERE step_number = 32;").fetchall()
            except sqlite3.DatabaseError:
                corruption_detected = True
    except sqlite3.DatabaseError:
        corruption_detected = True

    assert corruption_detected, "PRAGMA integrity_check or raw SQLite failed to detect Page 32 corruption!"


def test_torn_write_mid_frame_truncation_detection(ephemeral_db_path: Path) -> None:
    """Verify that truncating a WAL frame mid-write causes SQLite I/O or DatabaseError."""
    wal_path: Path = ephemeral_db_path.with_name(ephemeral_db_path.name + "-wal")

    conn_writer: sqlite3.Connection = sqlite3.connect(ephemeral_db_path)
    try:
        conn_writer.execute("PRAGMA page_size = 4096;")
        conn_writer.execute("PRAGMA journal_mode = WAL;")
        conn_writer.execute("PRAGMA wal_autocheckpoint = 0;")
        conn_writer.execute("CREATE TABLE live_feed (id INTEGER PRIMARY KEY, msg TEXT);")
        conn_writer.execute("INSERT INTO live_feed (msg) VALUES ('frame_1');")
        conn_writer.commit()

        # Insert a second frame
        conn_writer.execute("INSERT INTO live_feed (msg) VALUES ('frame_2');")
        conn_writer.commit()

        assert wal_path.exists(), "WAL file must exist after uncheckpointed commit."
        wal_size_before: int = wal_path.stat().st_size
        assert wal_size_before > 32, "WAL file should contain header and multiple frames."

        # Simulate mid-write power loss / process crash by truncating 512 bytes mid-frame
        with open(wal_path, "r+b") as fh:
            fh.truncate(wal_size_before - 512)

        # Attempt to checkpoint or read using a separate reader connection
        detected_torn_write: bool = False
        try:
            with closing(sqlite3.connect(ephemeral_db_path)) as conn_reader:
                # Passive checkpoint on a torn WAL frame triggers SQLITE_IOERR_SHORT_READ
                conn_reader.execute("PRAGMA wal_checkpoint(PASSIVE);")
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
            detected_torn_write = True
            error_msg: str = str(exc).lower()
            assert "disk i/o error" in error_msg or "malformed" in error_msg or "locked" in error_msg

        assert detected_torn_write, "SQLite failed to flag torn WAL frame truncation during checkpoint!"
    finally:
        conn_writer.close()


def test_unfinalized_frame_header_injection_detected_by_arbiter(ephemeral_db_path: Path) -> None:
    """Verify that an unfinalized frame header or page corruption fails the WALmaster arbiter barrier."""
    _populate_multipage_wal_db(ephemeral_db_path, min_pages=40)

    # Invalidate Page 32 cell header
    with open(ephemeral_db_path, "r+b") as fh:
        fh.seek(PAGE_32_OFFSET)
        fh.write(b"\x00" * 32)

    arbiter: WALArbiter = WALArbiter(ephemeral_db_path)
    result: CheckpointResult = arbiter.enforce_checkpoint_barrier(mode="TRUNCATE")

    # Integrity must be flagged as false due to pre-checkpoint integrity check detecting corruption
    assert not result.integrity_ok, f"Arbiter should report integrity failure on corrupted DB: {result}"
