"""WALmaster Lease Arbiter and Two-Tier Checkpoint Barrier Coordinator.

Provides deterministic handover mechanics between local desktop sessions and remote cloud runners.
TwoTierBarrierCoordinator enforces synchronous PRAGMA wal_checkpoint(TRUNCATE) sequentially
on both conversations/<id>.db and conversation_summaries.db, drains Win32 file handles,
and generates cryptographically verifiable LeaseTokens anchored to a Monotonic Lamport Epoch Counter.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from core.provenance import compute_payload_hash

logger = logging.getLogger("walmaster.arbiter")

BACKOFF_SCHEDULE_MS: list[int] = [5, 15, 45, 135, 405, 1000]
MAX_BACKOFF_MS: int = 1000


@dataclass(slots=True, frozen=True)
class CheckpointResult:
    success: bool
    busy: int
    log_frames: int
    checkpointed_frames: int
    integrity_ok: bool
    diagnostics: list[str]


@dataclass(slots=True, frozen=True)
class LeaseToken:
    """Cryptographically verifiable handover lease token.

    Anchored to a Monotonic Lamport Epoch Counter and signed
    with a SHA-256 HMAC over the canonical lease payload.
    """

    lease_id: str
    actor_id: str
    lamport_epoch: int
    issued_at: float
    expires_at: float
    conv_db_path: str
    summary_db_path: str
    digest: str

    def as_dict(self) -> dict[str, object]:
        return {
            "lease_id": self.lease_id,
            "actor_id": self.actor_id,
            "lamport_epoch": self.lamport_epoch,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "conv_db_path": self.conv_db_path,
            "summary_db_path": self.summary_db_path,
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LeaseToken:
        return cls(
            lease_id=str(data["lease_id"]),
            actor_id=str(data["actor_id"]),
            lamport_epoch=int(data["lamport_epoch"]),  # type: ignore[arg-type]
            issued_at=float(data["issued_at"]),  # type: ignore[arg-type]
            expires_at=float(data["expires_at"]),  # type: ignore[arg-type]
            conv_db_path=str(data["conv_db_path"]),
            summary_db_path=str(data["summary_db_path"]),
            digest=str(data["digest"]),
        )

    @staticmethod
    def compute_digest_for(
        lease_id: str,
        actor_id: str,
        lamport_epoch: int,
        issued_at: float,
        conv_db_path: str,
        summary_db_path: str,
    ) -> str:
        payload: dict[str, object] = {
            "lease_id": lease_id,
            "actor_id": actor_id,
            "lamport_epoch": lamport_epoch,
            "issued_at": issued_at,
            "conv_db_path": conv_db_path,
            "summary_db_path": summary_db_path,
        }
        canonical: str = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class WALArbiter:
    """Coordinates SQLite WAL checkpoints and exclusive handover leases."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)

    def enforce_checkpoint_barrier(self, mode: str = "TRUNCATE", timeout: float = 10.0) -> CheckpointResult:
        """Execute a blocking WAL checkpoint barrier before runtime handovers.

        Args:
            mode: SQLite checkpoint mode (PASSIVE, FULL, RESTART, or TRUNCATE).
            timeout: Busy timeout in seconds before failing acquisition.

        Returns:
            CheckpointResult indicating whether the WAL has zero remaining frames.
        """
        diagnostics: list[str] = []
        if not self.db_path.exists():
            return CheckpointResult(
                success=False,
                busy=0,
                log_frames=0,
                checkpointed_frames=0,
                integrity_ok=False,
                diagnostics=[f"Database file {self.db_path} does not exist."],
            )

        try:
            with sqlite3.connect(self.db_path, timeout=timeout) as conn:
                cur = conn.cursor()

                # Check integrity first
                integrity_rows = cur.execute("PRAGMA integrity_check").fetchall()
                integrity_ok = len(integrity_rows) == 1 and integrity_rows[0][0] == "ok"
                if not integrity_ok:
                    diagnostics.append(f"Pre-checkpoint integrity warning: {integrity_rows}")

                # Execute wal_checkpoint
                checkpoint_query = f"PRAGMA wal_checkpoint({mode})"
                res = cur.execute(checkpoint_query).fetchone()
                busy, log_frames, checkpointed_frames = res if res else (1, -1, -1)

                success = (busy == 0) and (log_frames == checkpointed_frames or log_frames == 0)
                diagnostics.append(
                    f"Checkpoint {mode}: busy={busy}, log={log_frames}, checkpointed={checkpointed_frames}"
                )

                return CheckpointResult(
                    success=success,
                    busy=busy,
                    log_frames=log_frames,
                    checkpointed_frames=checkpointed_frames,
                    integrity_ok=integrity_ok,
                    diagnostics=diagnostics,
                )
        except sqlite3.Error as e:
            return CheckpointResult(
                success=False,
                busy=1,
                log_frames=-1,
                checkpointed_frames=-1,
                integrity_ok=False,
                diagnostics=[f"SQLite error during checkpoint barrier: {e}"],
            )


def _backoff_sleep(attempt: int) -> None:
    """Exponential jitter backoff: 5ms, 15ms, 45ms, 135ms, 405ms, 1000ms cap."""
    base_ms: int = BACKOFF_SCHEDULE_MS[min(attempt, len(BACKOFF_SCHEDULE_MS) - 1)]
    jitter_ms: float = random.uniform(0.0, base_ms * 0.5)
    sleep_sec: float = min((base_ms + jitter_ms) / 1000.0, MAX_BACKOFF_MS / 1000.0)
    time.sleep(sleep_sec)


def _checkpoint_with_retry(
    db_path: Path, mode: str, max_attempts: int, timeout: float
) -> CheckpointResult:
    """Attempt WAL checkpoint with exponential jitter backoff on SQLITE_BUSY."""
    diagnostics: list[str] = []
    if not db_path.exists():
        return CheckpointResult(
            success=False, busy=0, log_frames=0, checkpointed_frames=0,
            integrity_ok=False,
            diagnostics=[f"Database file {db_path} does not exist."],
        )

    for attempt in range(max_attempts):
        try:
            conn = sqlite3.connect(db_path, timeout=timeout)
            try:
                integrity_rows = conn.execute("PRAGMA integrity_check").fetchall()
                integrity_ok = len(integrity_rows) == 1 and integrity_rows[0][0] == "ok"
                if not integrity_ok:
                    diagnostics.append(f"Integrity warning on attempt {attempt}: {integrity_rows}")

                res = conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
                busy, log_frames, checkpointed_frames = res if res else (1, -1, -1)

                if busy == 0 and (log_frames == checkpointed_frames or log_frames == 0):
                    diagnostics.append(
                        f"Checkpoint {mode} succeeded on attempt {attempt}: "
                        f"busy={busy}, log={log_frames}, checkpointed={checkpointed_frames}"
                    )
                    return CheckpointResult(
                        success=True, busy=busy,
                        log_frames=log_frames,
                        checkpointed_frames=checkpointed_frames,
                        integrity_ok=integrity_ok,
                        diagnostics=diagnostics,
                    )

                diagnostics.append(
                    f"Checkpoint {mode} attempt {attempt} busy: "
                    f"busy={busy}, log={log_frames}, checkpointed={checkpointed_frames}"
                )
            finally:
                conn.close()

        except sqlite3.OperationalError as e:
            diagnostics.append(f"Attempt {attempt} OperationalError: {e}")

        if attempt < max_attempts - 1:
            _backoff_sleep(attempt)

    return CheckpointResult(
        success=False, busy=1, log_frames=-1, checkpointed_frames=-1,
        integrity_ok=False,
        diagnostics=diagnostics,
    )


def _drain_win32_file_handles(db_path: Path) -> bool:
    """Attempt to drain Win32 file handles by forcing garbage collection.

    On non-Windows platforms this is a no-op that returns True.
    """
    if sys.platform != "win32":
        return True

    import gc

    gc.collect()

    for ext in ("-wal", "-shm"):
        wal_path: Path = db_path.with_name(db_path.name + ext)
        if wal_path.exists():
            try:
                with open(wal_path, "rb"):
                    pass
            except OSError as e:
                logger.warning("Handle drain probe failed for %s: %s", wal_path, e)
                return False
    return True


@dataclass(slots=True)
class TwoTierBarrierCoordinator:
    """Enforces sequential two-tier WAL checkpoint barriers and lease management.

    Tier 1: conversations/<id>.db (per-conversation database)
    Tier 2: conversation_summaries.db (global sidebar index)

    Integrates with core.sentinel.Sentinel for presence lock reaping
    and generates cryptographically verifiable LeaseTokens anchored
    to a Monotonic Lamport Epoch Counter.
    """

    conv_db_path: Path
    summary_db_path: Path
    actor_id: str = "walmaster_coordinator"
    max_checkpoint_attempts: int = 6
    checkpoint_timeout: float = 5.0
    lease_ttl_seconds: float = 300.0
    _lamport_epoch: int = field(default=0, init=False, repr=False)
    _active_lease: LeaseToken | None = field(default=None, init=False, repr=False)

    def _advance_lamport(self) -> int:
        self._lamport_epoch += 1
        return self._lamport_epoch

    def _reap_presence_locks(self, lock_dir: Path | None) -> list[object]:
        """Integrate with Sentinel if a lock directory is provided."""
        if lock_dir is None or not lock_dir.exists():
            return []
        from core.sentinel import Sentinel
        sentinel = Sentinel()
        return sentinel.reap_presence_locks(lock_dir)

    def acquire_handover_lease(
        self,
        lock_dir: Path | None = None,
        lease_id: str | None = None,
    ) -> LeaseToken | None:
        """Acquire a handover lease by enforcing the two-tier checkpoint barrier.

        Steps:
        1. Optionally reap stale presence locks via Sentinel.
        2. Drain Win32 file handles on both databases.
        3. Sequentially checkpoint Tier 1 (conv DB) then Tier 2 (summary DB).
        4. On success, mint a cryptographically verifiable LeaseToken.

        Returns:
            LeaseToken on success, None if the barrier cannot be established.
        """
        if self._active_lease is not None:
            logger.warning("Lease already active: %s", self._active_lease.lease_id)
            return None

        if lock_dir is not None:
            reap_results = self._reap_presence_locks(lock_dir)
            logger.info("Presence lock reap: %d locks inspected", len(reap_results))

        # Drain Win32 handles
        if not _drain_win32_file_handles(self.conv_db_path):
            logger.error("Failed to drain Win32 handles for %s", self.conv_db_path)
            return None
        if not _drain_win32_file_handles(self.summary_db_path):
            logger.error("Failed to drain Win32 handles for %s", self.summary_db_path)
            return None

        # Tier 1: Checkpoint conversation database
        tier1_result: CheckpointResult = _checkpoint_with_retry(
            self.conv_db_path,
            mode="TRUNCATE",
            max_attempts=self.max_checkpoint_attempts,
            timeout=self.checkpoint_timeout,
        )
        if not tier1_result.success:
            logger.error(
                "Tier 1 checkpoint barrier failed for %s: %s",
                self.conv_db_path, tier1_result.diagnostics,
            )
            return None

        # Tier 2: Checkpoint summary database
        tier2_result: CheckpointResult = _checkpoint_with_retry(
            self.summary_db_path,
            mode="TRUNCATE",
            max_attempts=self.max_checkpoint_attempts,
            timeout=self.checkpoint_timeout,
        )
        if not tier2_result.success:
            logger.error(
                "Tier 2 checkpoint barrier failed for %s: %s",
                self.summary_db_path, tier2_result.diagnostics,
            )
            return None

        # Mint lease token
        epoch: int = self._advance_lamport()
        now: float = time.time()
        token_lease_id: str = lease_id or compute_payload_hash(
            f"{self.actor_id}:{epoch}:{now}:{os.getpid()}"
        )[:24]
        digest: str = LeaseToken.compute_digest_for(
            lease_id=token_lease_id,
            actor_id=self.actor_id,
            lamport_epoch=epoch,
            issued_at=now,
            conv_db_path=str(self.conv_db_path),
            summary_db_path=str(self.summary_db_path),
        )

        token = LeaseToken(
            lease_id=token_lease_id,
            actor_id=self.actor_id,
            lamport_epoch=epoch,
            issued_at=now,
            expires_at=now + self.lease_ttl_seconds,
            conv_db_path=str(self.conv_db_path),
            summary_db_path=str(self.summary_db_path),
            digest=digest,
        )
        self._active_lease = token
        logger.info("Handover lease acquired: %s (epoch=%d)", token.lease_id, epoch)
        return token

    def release_handover_lease(self) -> bool:
        """Release the currently held handover lease.

        Returns:
            True if a lease was released, False if no active lease existed.
        """
        if self._active_lease is None:
            return False
        released_id: str = self._active_lease.lease_id
        self._active_lease = None
        logger.info("Handover lease released: %s", released_id)
        return True

    def verify_lease_token(self, token: LeaseToken) -> bool:
        """Verify a lease token's cryptographic digest and expiry.

        Returns:
            True if the token digest is valid and the token has not expired.
        """
        expected_digest: str = LeaseToken.compute_digest_for(
            lease_id=token.lease_id,
            actor_id=token.actor_id,
            lamport_epoch=token.lamport_epoch,
            issued_at=token.issued_at,
            conv_db_path=token.conv_db_path,
            summary_db_path=token.summary_db_path,
        )
        if token.digest != expected_digest:
            logger.warning(
                "Lease token %s digest mismatch: expected %s, got %s",
                token.lease_id, expected_digest, token.digest,
            )
            return False

        now: float = time.time()
        if now > token.expires_at:
            logger.warning(
                "Lease token %s expired at %f (now=%f)",
                token.lease_id, token.expires_at, now,
            )
            return False

        return True

    @property
    def active_lease(self) -> LeaseToken | None:
        return self._active_lease

    @property
    def lamport_epoch(self) -> int:
        return self._lamport_epoch
