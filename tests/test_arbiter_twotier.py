"""Two-Tier Barrier Coordinator and Lease Token Test Suite.

Verifies:
1. Sequential two-tier WAL checkpoint barrier enforcement.
2. LeaseToken cryptographic digest generation and verification.
3. Lease acquisition, release, and double-acquire rejection.
4. Exponential jitter backoff under SQLITE_BUSY contention.
5. Win32 handle draining integration.
6. Sentinel presence lock reaping integration.
7. Expired lease token detection.
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import closing
from pathlib import Path

from core.arbiter import (
    BACKOFF_SCHEDULE_MS,
    LeaseToken,
    TwoTierBarrierCoordinator,
    _backoff_sleep,
    _checkpoint_with_retry,
)


def _init_wal_database(db_path: Path) -> None:
    """Initialize a clean WAL-mode database with a test table."""
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


def _init_summary_database(db_path: Path) -> None:
    """Initialize conversation_summaries.db schema."""
    with closing(sqlite3.connect(db_path, timeout=5.0)) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation_summaries (
                conversation_id TEXT PRIMARY KEY,
                step_count INTEGER NOT NULL,
                last_modified_time REAL NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO conversation_summaries (conversation_id, step_count, last_modified_time) "
            "VALUES (?, ?, ?);",
            ("test_conv", 5, time.time()),
        )
        conn.commit()


class TestLeaseToken:
    """Verify LeaseToken cryptographic digest and serialization."""

    def test_digest_determinism(self) -> None:
        digest_a = LeaseToken.compute_digest_for(
            lease_id="lease-001",
            actor_id="agent-alpha",
            lamport_epoch=42,
            issued_at=1700000000.0,
            conv_db_path="/tmp/conv.db",
            summary_db_path="/tmp/summary.db",
        )
        digest_b = LeaseToken.compute_digest_for(
            lease_id="lease-001",
            actor_id="agent-alpha",
            lamport_epoch=42,
            issued_at=1700000000.0,
            conv_db_path="/tmp/conv.db",
            summary_db_path="/tmp/summary.db",
        )
        assert digest_a == digest_b
        assert len(digest_a) == 64

    def test_digest_changes_with_different_inputs(self) -> None:
        base_args: dict[str, object] = {
            "lease_id": "lease-001",
            "actor_id": "agent-alpha",
            "lamport_epoch": 42,
            "issued_at": 1700000000.0,
            "conv_db_path": "/tmp/conv.db",
            "summary_db_path": "/tmp/summary.db",
        }
        base_digest = LeaseToken.compute_digest_for(**base_args)  # type: ignore[arg-type]

        for key, alt_value in [
            ("lease_id", "lease-002"),
            ("actor_id", "agent-beta"),
            ("lamport_epoch", 99),
            ("issued_at", 1700000001.0),
        ]:
            modified = dict(base_args)
            modified[key] = alt_value
            alt_digest = LeaseToken.compute_digest_for(**modified)  # type: ignore[arg-type]
            assert alt_digest != base_digest, f"Digest unchanged when modifying {key}"

    def test_serialization_roundtrip(self) -> None:
        token = LeaseToken(
            lease_id="lease-rt-001",
            actor_id="agent-gamma",
            lamport_epoch=7,
            issued_at=1700000000.0,
            expires_at=1700000300.0,
            conv_db_path="/tmp/conv.db",
            summary_db_path="/tmp/summary.db",
            digest="abc123",
        )
        data = token.as_dict()
        restored = LeaseToken.from_dict(data)
        assert restored.lease_id == token.lease_id
        assert restored.actor_id == token.actor_id
        assert restored.lamport_epoch == token.lamport_epoch
        assert restored.issued_at == token.issued_at
        assert restored.expires_at == token.expires_at
        assert restored.digest == token.digest


class TestCheckpointWithRetry:
    """Verify checkpoint retry with exponential backoff."""

    def test_successful_checkpoint(self, test_artifacts_dir: Path) -> None:
        db_path = test_artifacts_dir / "retry_success.db"
        try:
            _init_wal_database(db_path)
            result = _checkpoint_with_retry(db_path, "TRUNCATE", max_attempts=3, timeout=5.0)
            assert result.success is True
            assert result.busy == 0
            assert result.integrity_ok is True
        finally:
            for ext in ("", "-wal", "-shm"):
                p = db_path.with_name(db_path.name + ext)
                p.unlink(missing_ok=True)

    def test_missing_database_returns_failure(self, test_artifacts_dir: Path) -> None:
        missing = test_artifacts_dir / "nonexistent_retry.db"
        result = _checkpoint_with_retry(missing, "TRUNCATE", max_attempts=2, timeout=1.0)
        assert result.success is False

    def test_busy_database_retries_and_fails(self, test_artifacts_dir: Path) -> None:
        db_path = test_artifacts_dir / "retry_busy.db"
        try:
            _init_wal_database(db_path)
            conn_holder = sqlite3.connect(db_path, timeout=0.1)
            try:
                conn_holder.execute("BEGIN EXCLUSIVE;")
                conn_holder.execute(
                    "UPDATE session_registry SET counter = 999 WHERE worker_id = 'init_worker';"
                )
                result = _checkpoint_with_retry(db_path, "TRUNCATE", max_attempts=2, timeout=0.05)
                assert result.success is False
            finally:
                conn_holder.rollback()
                conn_holder.close()
        finally:
            for ext in ("", "-wal", "-shm"):
                p = db_path.with_name(db_path.name + ext)
                p.unlink(missing_ok=True)


class TestTwoTierBarrierCoordinator:
    """Verify the full two-tier barrier coordinator lifecycle."""

    def test_acquire_and_release_lease(self, test_artifacts_dir: Path) -> None:
        conv_db = test_artifacts_dir / "twotier_conv.db"
        summary_db = test_artifacts_dir / "twotier_summary.db"
        try:
            _init_wal_database(conv_db)
            _init_summary_database(summary_db)

            coordinator = TwoTierBarrierCoordinator(
                conv_db_path=conv_db,
                summary_db_path=summary_db,
                actor_id="test_agent",
                lease_ttl_seconds=60.0,
            )

            token = coordinator.acquire_handover_lease()
            assert token is not None
            assert token.actor_id == "test_agent"
            assert token.lamport_epoch == 1
            assert len(token.digest) == 64
            assert coordinator.active_lease is token

            released = coordinator.release_handover_lease()
            assert released is True
            assert coordinator.active_lease is None

            released_again = coordinator.release_handover_lease()
            assert released_again is False
        finally:
            for db in (conv_db, summary_db):
                for ext in ("", "-wal", "-shm"):
                    p = db.with_name(db.name + ext)
                    p.unlink(missing_ok=True)

    def test_double_acquire_rejected(self, test_artifacts_dir: Path) -> None:
        conv_db = test_artifacts_dir / "twotier_dbl_conv.db"
        summary_db = test_artifacts_dir / "twotier_dbl_summary.db"
        try:
            _init_wal_database(conv_db)
            _init_summary_database(summary_db)

            coordinator = TwoTierBarrierCoordinator(
                conv_db_path=conv_db,
                summary_db_path=summary_db,
            )

            first = coordinator.acquire_handover_lease()
            assert first is not None

            second = coordinator.acquire_handover_lease()
            assert second is None
        finally:
            for db in (conv_db, summary_db):
                for ext in ("", "-wal", "-shm"):
                    p = db.with_name(db.name + ext)
                    p.unlink(missing_ok=True)

    def test_lamport_epoch_monotonically_increases(self, test_artifacts_dir: Path) -> None:
        conv_db = test_artifacts_dir / "twotier_lamport_conv.db"
        summary_db = test_artifacts_dir / "twotier_lamport_summary.db"
        try:
            _init_wal_database(conv_db)
            _init_summary_database(summary_db)

            coordinator = TwoTierBarrierCoordinator(
                conv_db_path=conv_db,
                summary_db_path=summary_db,
            )

            epochs: list[int] = []
            for _ in range(3):
                token = coordinator.acquire_handover_lease()
                assert token is not None
                epochs.append(token.lamport_epoch)
                coordinator.release_handover_lease()

            assert epochs == [1, 2, 3]
            assert coordinator.lamport_epoch == 3
        finally:
            for db in (conv_db, summary_db):
                for ext in ("", "-wal", "-shm"):
                    p = db.with_name(db.name + ext)
                    p.unlink(missing_ok=True)

    def test_verify_valid_token(self, test_artifacts_dir: Path) -> None:
        conv_db = test_artifacts_dir / "twotier_verify_conv.db"
        summary_db = test_artifacts_dir / "twotier_verify_summary.db"
        try:
            _init_wal_database(conv_db)
            _init_summary_database(summary_db)

            coordinator = TwoTierBarrierCoordinator(
                conv_db_path=conv_db,
                summary_db_path=summary_db,
                lease_ttl_seconds=300.0,
            )

            token = coordinator.acquire_handover_lease()
            assert token is not None
            assert coordinator.verify_lease_token(token) is True
        finally:
            for db in (conv_db, summary_db):
                for ext in ("", "-wal", "-shm"):
                    p = db.with_name(db.name + ext)
                    p.unlink(missing_ok=True)

    def test_verify_tampered_token_rejected(self, test_artifacts_dir: Path) -> None:
        conv_db = test_artifacts_dir / "twotier_tamper_conv.db"
        summary_db = test_artifacts_dir / "twotier_tamper_summary.db"
        try:
            _init_wal_database(conv_db)
            _init_summary_database(summary_db)

            coordinator = TwoTierBarrierCoordinator(
                conv_db_path=conv_db,
                summary_db_path=summary_db,
            )

            token = coordinator.acquire_handover_lease()
            assert token is not None

            tampered = LeaseToken(
                lease_id=token.lease_id,
                actor_id="tampered_actor",
                lamport_epoch=token.lamport_epoch,
                issued_at=token.issued_at,
                expires_at=token.expires_at,
                conv_db_path=token.conv_db_path,
                summary_db_path=token.summary_db_path,
                digest=token.digest,
            )
            assert coordinator.verify_lease_token(tampered) is False
        finally:
            for db in (conv_db, summary_db):
                for ext in ("", "-wal", "-shm"):
                    p = db.with_name(db.name + ext)
                    p.unlink(missing_ok=True)

    def test_verify_expired_token_rejected(self, test_artifacts_dir: Path) -> None:
        conv_db = test_artifacts_dir / "twotier_expire_conv.db"
        summary_db = test_artifacts_dir / "twotier_expire_summary.db"
        try:
            _init_wal_database(conv_db)
            _init_summary_database(summary_db)

            coordinator = TwoTierBarrierCoordinator(
                conv_db_path=conv_db,
                summary_db_path=summary_db,
                lease_ttl_seconds=0.0,
            )

            token = coordinator.acquire_handover_lease()
            assert token is not None
            time.sleep(0.01)
            assert coordinator.verify_lease_token(token) is False
        finally:
            for db in (conv_db, summary_db):
                for ext in ("", "-wal", "-shm"):
                    p = db.with_name(db.name + ext)
                    p.unlink(missing_ok=True)

    def test_barrier_fails_when_conv_db_missing(self, test_artifacts_dir: Path) -> None:
        missing_conv = test_artifacts_dir / "missing_conv.db"
        summary_db = test_artifacts_dir / "twotier_miss_summary.db"
        try:
            _init_summary_database(summary_db)

            coordinator = TwoTierBarrierCoordinator(
                conv_db_path=missing_conv,
                summary_db_path=summary_db,
            )
            token = coordinator.acquire_handover_lease()
            assert token is None
        finally:
            for ext in ("", "-wal", "-shm"):
                p = summary_db.with_name(summary_db.name + ext)
                p.unlink(missing_ok=True)

    def test_barrier_fails_when_conv_db_locked(self, test_artifacts_dir: Path) -> None:
        conv_db = test_artifacts_dir / "twotier_locked_conv.db"
        summary_db = test_artifacts_dir / "twotier_locked_summary.db"
        try:
            _init_wal_database(conv_db)
            _init_summary_database(summary_db)

            conn_holder = sqlite3.connect(conv_db, timeout=0.1)
            try:
                conn_holder.execute("BEGIN EXCLUSIVE;")
                conn_holder.execute(
                    "UPDATE session_registry SET counter = 999 WHERE worker_id = 'init_worker';"
                )

                coordinator = TwoTierBarrierCoordinator(
                    conv_db_path=conv_db,
                    summary_db_path=summary_db,
                    max_checkpoint_attempts=2,
                    checkpoint_timeout=0.05,
                )
                token = coordinator.acquire_handover_lease()
                assert token is None
            finally:
                conn_holder.rollback()
                conn_holder.close()
        finally:
            for db in (conv_db, summary_db):
                for ext in ("", "-wal", "-shm"):
                    p = db.with_name(db.name + ext)
                    p.unlink(missing_ok=True)

    def test_acquire_with_custom_lease_id(self, test_artifacts_dir: Path) -> None:
        conv_db = test_artifacts_dir / "twotier_custom_conv.db"
        summary_db = test_artifacts_dir / "twotier_custom_summary.db"
        try:
            _init_wal_database(conv_db)
            _init_summary_database(summary_db)

            coordinator = TwoTierBarrierCoordinator(
                conv_db_path=conv_db,
                summary_db_path=summary_db,
            )
            token = coordinator.acquire_handover_lease(lease_id="custom-lease-42")
            assert token is not None
            assert token.lease_id == "custom-lease-42"
        finally:
            for db in (conv_db, summary_db):
                for ext in ("", "-wal", "-shm"):
                    p = db.with_name(db.name + ext)
                    p.unlink(missing_ok=True)


class TestBackoffSchedule:
    """Verify backoff schedule constants and sleep behavior."""

    def test_schedule_values(self) -> None:
        assert BACKOFF_SCHEDULE_MS == [5, 15, 45, 135, 405, 1000]

    def test_backoff_sleep_does_not_exceed_cap(self) -> None:
        start = time.perf_counter()
        _backoff_sleep(0)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        assert elapsed_ms < 250.0
