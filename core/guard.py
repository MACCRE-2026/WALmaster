"""WALmaster Active Prevention Guard.

Continuously monitors active SQLite databases across Antigravity's storage paths,
intercepts uncheckpointed WAL frame ballooning, enforces non-blocking incremental
checkpoints to prevent checkpoint starvation, and reaps orphaned presence locks.
"""
from __future__ import annotations

import datetime
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from core.arbiter import _checkpoint_with_retry, _drain_win32_file_handles
from core.sentinel import Sentinel

logger = logging.getLogger("walmaster.guard")

DEFAULT_CONV_DIR = Path.home() / ".gemini" / "antigravity" / "conversations"
DEFAULT_SUMMARIES_DB = Path.home() / ".gemini" / "antigravity" / "conversation_summaries.db"
DEFAULT_PRESENCE_DIR = Path.home() / ".gemini" / "antigravity" / "presence"


@dataclass(slots=True, frozen=True)
class GuardCycleResult:
    """Telemetry report produced by a single execution cycle of ActivePreventionGuard."""

    timestamp_iso: str
    scanned_databases: int
    checkpoints_attempted: int
    checkpoints_succeeded: int
    checkpoint_errors: int
    locks_inspected: int
    locks_reaped: int
    drained_bytes: int
    diagnostics: list[str] = field(default_factory=list)


class ActivePreventionGuard:
    """Proactive background guardian enforcing continuous WAL hygiene."""

    def __init__(
        self,
        *,
        conv_dir: Path | str | None = None,
        summaries_db: Path | str | None = None,
        presence_dir: Path | str | None = None,
        max_wal_frames: int = 500,
        critical_wal_frames: int = 2000,
        checkpoint_timeout: float = 3.0,
    ) -> None:
        self.conv_dir = Path(conv_dir) if conv_dir else DEFAULT_CONV_DIR
        self.summaries_db = Path(summaries_db) if summaries_db else DEFAULT_SUMMARIES_DB
        self.presence_dir = Path(presence_dir) if presence_dir else DEFAULT_PRESENCE_DIR
        self.max_wal_frames = max_wal_frames
        self.critical_wal_frames = critical_wal_frames
        self.checkpoint_timeout = checkpoint_timeout
        self.sentinel = Sentinel()

    def checkpoint_database(self, db_path: Path, frames: int) -> tuple[bool, int, str]:
        """Execute proportional checkpoint on a database with swelled WAL frames."""
        mode = "PASSIVE"
        if frames >= self.critical_wal_frames:
            mode = "RESTART"
            _drain_win32_file_handles(db_path)

        res = _checkpoint_with_retry(
            db_path,
            mode=mode,
            max_attempts=3,
            timeout=self.checkpoint_timeout,
        )

        drained_bytes = 0
        if res.success and res.checkpointed_frames > 0:
            drained_bytes = res.checkpointed_frames * 4096

        msg = f"{db_path.name}: {mode} checkpoint (success={res.success}, log={res.log_frames}, ckpt={res.checkpointed_frames})"
        return res.success, drained_bytes, msg

    def cycle(self) -> GuardCycleResult:
        """Execute a single prevention scan across presence locks and WAL files."""
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        diagnostics: list[str] = []

        # 1. Inspect and reap stale presence locks
        locks_inspected = 0
        locks_reaped = 0
        if self.presence_dir.exists():
            reap_statuses = self.sentinel.reap_presence_locks(self.presence_dir)
            locks_inspected = len(reap_statuses)
            for st in reap_statuses:
                if st.reaped:
                    locks_reaped += 1
                    diagnostics.append(f"Reaped stale presence lock: {st.lock_path.name} ({st.reason})")

        # 2. Scan conversation databases for swelled WAL files
        scanned_dbs = 0
        checkpoints_attempted = 0
        checkpoints_succeeded = 0
        checkpoint_errors = 0
        total_drained = 0

        target_dbs: list[Path] = []
        if self.conv_dir.exists():
            for f in self.conv_dir.glob("*.db-wal"):
                parent_db = f.with_name(f.name.removesuffix("-wal"))
                if parent_db.exists():
                    target_dbs.append(parent_db)

        # Include summary DB if it has an active WAL
        summary_wal = self.summaries_db.with_name(self.summaries_db.name + "-wal")
        if summary_wal.exists() and self.summaries_db.exists():
            target_dbs.append(self.summaries_db)

        scanned_dbs = len(target_dbs)

        for db_path in target_dbs:
            wal_path = db_path.with_name(db_path.name + "-wal")
            try:
                wal_size = wal_path.stat().st_size
                frames = wal_size // 4096
                if frames >= self.max_wal_frames:
                    checkpoints_attempted += 1
                    success, drained, detail = self.checkpoint_database(db_path, frames)
                    diagnostics.append(detail)
                    if success:
                        checkpoints_succeeded += 1
                        total_drained += drained
                    else:
                        checkpoint_errors += 1
            except OSError as e:
                diagnostics.append(f"Could not stat {wal_path.name}: {e}")

        return GuardCycleResult(
            timestamp_iso=now_iso,
            scanned_databases=scanned_dbs,
            checkpoints_attempted=checkpoints_attempted,
            checkpoints_succeeded=checkpoints_succeeded,
            checkpoint_errors=checkpoint_errors,
            locks_inspected=locks_inspected,
            locks_reaped=locks_reaped,
            drained_bytes=total_drained,
            diagnostics=diagnostics,
        )

    def run_guard_loop(
        self,
        interval_sec: float = 5.0,
        max_iterations: int | None = None,
    ) -> None:
        """Run continuous prevention guardian loop."""
        logger.info("Active Prevention Guard engaged (interval=%.1fs, max_frames=%d)...", interval_sec, self.max_wal_frames)
        iterations = 0
        while True:
            try:
                res = self.cycle()
                if res.checkpoints_attempted > 0 or res.locks_reaped > 0:
                    logger.info(
                        "Guard cycle: %d dbs, %d ckpts (success=%d), %d locks reaped, %d KB drained",
                        res.scanned_databases,
                        res.checkpoints_attempted,
                        res.checkpoints_succeeded,
                        res.locks_reaped,
                        res.drained_bytes // 1024,
                    )
                iterations += 1
                if max_iterations is not None and iterations >= max_iterations:
                    break
                time.sleep(interval_sec)
            except KeyboardInterrupt:
                logger.info("Active Prevention Guard halted by user interrupt.")
                break
            except Exception as err:
                logger.error("Guard error encountered in cycle: %s", err, exc_info=True)
                time.sleep(interval_sec)
