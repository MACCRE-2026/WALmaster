"""WALmaster Core Sentinel and Two-Tier Registry Reconciler.

Provides:
1. Win32 Process Image Presence Lock Reaper:
   - Scans and inspects presence lock files (presence/<id>.lock).
   - Validates owning PID vitality and process image via QueryFullProcessImageNameW.
   - Safely reaps/unlinks stale or non-matching lock files.
   - Preserves active locks held by verified Electron runners.
2. Two-Tier Registry Reconciler:
   - Synchronizes step counts and timestamps from conversation databases
     into the global sidebar index (conversation_summaries.db).
"""
from __future__ import annotations

import ctypes
import json
import logging
import os
import sqlite3
import sys
from collections.abc import Iterable
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path
from typing import Final

logger: logging.Logger = logging.getLogger("walmaster.sentinel")

# Win32 Process Access Flags
PROCESS_QUERY_LIMITED_INFORMATION: Final[int] = 0x1000
PROCESS_QUERY_INFORMATION: Final[int] = 0x0400
ERROR_ACCESS_DENIED: Final[int] = 5

# Canonical allowed runners for presence locks
DEFAULT_ALLOWED_RUNNERS: Final[frozenset[str]] = frozenset({
    "antigravity.exe",
    "code.exe",
    "electron.exe",
    "python.exe",
    "python3.exe",
    "pytest.exe",
})


@dataclass(slots=True, frozen=True)
class LockReapStatus:
    """Result of inspecting and optionally reaping a presence lock file."""

    lock_path: Path
    pid: int | None
    is_alive: bool
    image_name: str | None
    is_allowed_runner: bool
    reaped: bool
    active: bool
    reason: str


def query_process_image_win32(pid: int) -> tuple[bool, str | None]:
    """Inspect a Win32 process by PID using kernel32.

    Returns:
        tuple[is_alive, process_image_name_or_none]
    """
    if pid <= 0:
        return False, None

    kernel32 = ctypes.windll.kernel32
    handle: int | None = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        last_error: int = int(kernel32.GetLastError())
        if last_error == ERROR_ACCESS_DENIED:
            # Process is alive but owned by an elevated/system context
            return True, None
        return False, None

    try:
        buffer_size: int = 1024
        buf = ctypes.create_unicode_buffer(buffer_size)
        size = wintypes.DWORD(buffer_size)
        success: int = kernel32.QueryFullProcessImageNameW(handle, 0, buf, ctypes.byref(size))
        if success != 0 and buf.value:
            return True, Path(buf.value).name.lower()
        return True, None
    finally:
        kernel32.CloseHandle(handle)


def query_process_image(pid: int) -> tuple[bool, str | None]:
    """Inspect process vitality and executable name with Win32 / POSIX fallback.

    Returns:
        tuple[is_alive, image_name_or_none]
    """
    if pid <= 0:
        return False, None

    if sys.platform == "win32":
        try:
            return query_process_image_win32(pid)
        except (OSError, ValueError, AttributeError) as err:
            logger.warning("Win32 ctypes query failed for PID %d: %s", pid, err)

    # POSIX / fallback inspection
    try:
        os.kill(pid, 0)
        is_alive: bool = True
    except ProcessLookupError:
        return False, None
    except PermissionError:
        return True, None
    except OSError:
        return False, None

    proc_exe: Path = Path(f"/proc/{pid}/exe")
    if proc_exe.exists():
        try:
            return True, proc_exe.resolve().name.lower()
        except OSError:
            pass

    return is_alive, None


def is_process_alive_and_allowed(
    pid: int, allowed_runners: Iterable[str] | None = None
) -> tuple[bool, bool, str | None]:
    """Check if a process is alive and if its executable matches allowed runners.

    Returns:
        tuple[is_alive, is_allowed_runner, image_name]
    """
    allowed: frozenset[str] = (
        frozenset(r.lower() for r in allowed_runners)
        if allowed_runners is not None
        else DEFAULT_ALLOWED_RUNNERS
    )

    is_alive, image_name = query_process_image(pid)
    if not is_alive or not image_name:
        return is_alive, False, image_name

    img_lower: str = image_name.lower()
    img_stem: str = Path(image_name).stem.lower()

    for runner in allowed:
        r_lower: str = runner.lower()
        r_stem: str = Path(runner).stem.lower()
        if img_lower == r_lower or img_stem == r_stem or img_lower == f"{r_stem}.exe":
            return True, True, image_name

    return True, False, image_name


def extract_pid_from_lock_file(lock_path: Path) -> int | None:
    """Safely extract the owning PID from a lock file payload."""
    try:
        content: str = lock_path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None

    if not content:
        return None

    # 1. Try parsing JSON payload
    try:
        data: object = json.loads(content)
        if isinstance(data, dict):
            for key in ("owner_pid", "pid", "PID", "ownerPid"):
                if key in data:
                    try:
                        return int(data[key])
                    except (ValueError, TypeError):
                        pass
    except json.JSONDecodeError:
        pass

    # 2. Try parsing plain integer string
    try:
        return int(content)
    except ValueError:
        pass

    return None


def inspect_presence_lock(
    lock_path: Path | str,
    allowed_runners: Iterable[str] | None = None,
) -> LockReapStatus:
    """Inspect an individual presence lock file and reap if stale or mismatched.

    Args:
        lock_path: Path to the presence lock file.
        allowed_runners: Optional custom set of allowed executable image names.

    Returns:
        LockReapStatus describing the result of the inspection and action taken.
    """
    path: Path = Path(lock_path)
    if not path.exists():
        return LockReapStatus(
            lock_path=path,
            pid=None,
            is_alive=False,
            image_name=None,
            is_allowed_runner=False,
            reaped=False,
            active=False,
            reason=f"Lock file does not exist: {path}",
        )

    pid: int | None = extract_pid_from_lock_file(path)
    if pid is None:
        try:
            path.unlink(missing_ok=True)
            reaped = True
            is_active = False
            reason = "Corrupted, unparseable, or empty PID in lock file; reaped"
        except (PermissionError, OSError) as e:
            # If WinError 32 / PermissionError, an active process is actively holding the lock!
            reaped = False
            is_active = True
            reason = f"Actively held presence lock (held by active process handle: {e})"
            logger.debug("Presence lock %s is actively held by process: %s", path.name, e)
        return LockReapStatus(
            lock_path=path,
            pid=None,
            is_alive=is_active,
            image_name=None,
            is_allowed_runner=is_active,
            reaped=reaped,
            active=is_active,
            reason=reason,
        )

    is_alive, is_allowed, image_name = is_process_alive_and_allowed(pid, allowed_runners)

    if is_alive and is_allowed:
        return LockReapStatus(
            lock_path=path,
            pid=pid,
            is_alive=True,
            image_name=image_name,
            is_allowed_runner=True,
            reaped=False,
            active=True,
            reason=f"Active presence lock held by running runner PID {pid} ({image_name})",
        )

    # PID is dead or does not match an allowed runner
    try:
        path.unlink(missing_ok=True)
        reaped = True
    except OSError as e:
        reaped = False
        logger.error("Failed to unlink stale presence lock %s: %s", path, e)

    if not is_alive:
        reason = f"Owning PID {pid} is dead; stale presence lock reaped"
    else:
        reason = f"Process PID {pid} ({image_name}) does not match allowed runners; reaped"

    return LockReapStatus(
        lock_path=path,
        pid=pid,
        is_alive=is_alive,
        image_name=image_name,
        is_allowed_runner=is_allowed,
        reaped=reaped,
        active=False,
        reason=reason,
    )


def reap_presence_locks(
    lock_dir: Path | str,
    allowed_runners: Iterable[str] | None = None,
) -> list[LockReapStatus]:
    """Inspect and reap stale presence locks across a target directory.

    Args:
        lock_dir: Directory containing presence lock files (*.lock).
        allowed_runners: Optional custom set of allowed executable image names.

    Returns:
        List of LockReapStatus objects for each lock file inspected.
    """
    target_dir: Path = Path(lock_dir)
    if not target_dir.exists() or not target_dir.is_dir():
        return []

    results: list[LockReapStatus] = []
    for lock_file in target_dir.glob("*.lock"):
        if lock_file.is_file():
            results.append(inspect_presence_lock(lock_file, allowed_runners=allowed_runners))
    return results


def reconcile_two_tier_registry(
    conv_id: str,
    conv_db_path: Path | str,
    summary_db_path: Path | str,
) -> int:
    """Reconcile step count and timestamp from conversation DB into summary index.

    Reads the max step count and last timestamp from conv_db_path (tables: steps or
    trajectory_meta) and atomically upserts into summary_db_path (table: conversation_summaries).

    Args:
        conv_id: Conversation identifier.
        conv_db_path: Path to the conversation SQLite database.
        summary_db_path: Path to the global conversation_summaries.db.

    Returns:
        The updated step count.

    Raises:
        FileNotFoundError: If conv_db_path does not exist.
    """
    conv_path: Path = Path(conv_db_path)
    if not conv_path.exists():
        raise FileNotFoundError(f"Conversation database does not exist: {conv_path}")

    step_count: int = 0
    last_modified_time: float = 0.0

    # 1. Read step count and timestamp from conversation database
    conv_conn: sqlite3.Connection = sqlite3.connect(conv_path, timeout=10.0)
    try:
        tables_res = conv_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        existing_tables: set[str] = {row[0].lower() for row in tables_res}

        # Check trajectory_meta table
        if "trajectory_meta" in existing_tables:
            meta_cols_res = conv_conn.execute("PRAGMA table_info(trajectory_meta)").fetchall()
            meta_cols: set[str] = {row[1].lower() for row in meta_cols_res}

            where_clause: str = ""
            params: tuple[object, ...] = ()
            if "conversation_id" in meta_cols:
                where_clause = "WHERE conversation_id = ?"
                params = (conv_id,)

            step_col: str | None = None
            for candidate in ("step_count", "steps_count", "count", "step_number"):
                if candidate in meta_cols:
                    step_col = candidate
                    break

            if step_col:
                res = conv_conn.execute(
                    f"SELECT COALESCE(MAX({step_col}), 0) FROM trajectory_meta {where_clause}",
                    params,
                ).fetchone()
                if res and res[0] is not None:
                    step_count = max(step_count, int(res[0]))

            time_col: str | None = None
            for candidate in ("last_modified_time", "timestamp", "created_at", "updated_at"):
                if candidate in meta_cols:
                    time_col = candidate
                    break

            if time_col:
                res = conv_conn.execute(
                    f"SELECT COALESCE(MAX({time_col}), 0.0) FROM trajectory_meta {where_clause}",
                    params,
                ).fetchone()
                if res and res[0] is not None:
                    last_modified_time = max(last_modified_time, float(res[0]))

        # Check steps or conversation_steps table
        steps_table: str | None = None
        if "steps" in existing_tables:
            steps_table = "steps"
        elif "conversation_steps" in existing_tables:
            steps_table = "conversation_steps"

        if steps_table:
            steps_cols_res = conv_conn.execute(f"PRAGMA table_info({steps_table})").fetchall()
            steps_cols: set[str] = {row[1].lower() for row in steps_cols_res}

            # Count rows
            row_count_res = conv_conn.execute(f"SELECT COUNT(*) FROM {steps_table}").fetchone()
            row_count: int = int(row_count_res[0]) if row_count_res else 0

            max_col_count: int = 0
            if "step_count" in steps_cols:
                res = conv_conn.execute(
                    f"SELECT COALESCE(MAX(step_count), 0) FROM {steps_table}"
                ).fetchone()
                if res and res[0] is not None:
                    max_col_count = int(res[0])
            elif "step_number" in steps_cols:
                res = conv_conn.execute(
                    f"SELECT COALESCE(MAX(step_number), 0) FROM {steps_table}"
                ).fetchone()
                if res and res[0] is not None:
                    max_col_count = int(res[0])

            steps_count_val: int = max(row_count, max_col_count)
            step_count = max(step_count, steps_count_val)

            time_col_steps: str | None = None
            for candidate in (
                "timestamp",
                "created_at",
                "created_at_epoch",
                "last_modified_time",
                "time",
                "updated_at",
            ):
                if candidate in steps_cols:
                    time_col_steps = candidate
                    break

            if time_col_steps:
                res = conv_conn.execute(
                    f"SELECT COALESCE(MAX({time_col_steps}), 0.0) FROM {steps_table}"
                ).fetchone()
                if res and res[0] is not None:
                    last_modified_time = max(last_modified_time, float(res[0]))
    finally:
        conv_conn.close()

    if last_modified_time == 0.0:
        last_modified_time = conv_path.stat().st_mtime

    # 2. Atomically upsert into conversation_summaries
    summary_path: Path = Path(summary_db_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    summary_conn: sqlite3.Connection = sqlite3.connect(summary_path, timeout=10.0)
    try:
        with summary_conn:
            summary_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_summaries (
                    conversation_id TEXT PRIMARY KEY,
                    step_count INTEGER NOT NULL,
                    last_modified_time REAL NOT NULL
                )
                """
            )
            summary_conn.execute(
                """
                INSERT INTO conversation_summaries (conversation_id, step_count, last_modified_time)
                VALUES (?, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    step_count = excluded.step_count,
                    last_modified_time = excluded.last_modified_time
                """,
                (conv_id, step_count, last_modified_time),
            )
    finally:
        summary_conn.close()

    return step_count


class Sentinel:
    """Coordinates presence lock reaping and two-tier registry reconciliation."""

    def __init__(self, allowed_runners: Iterable[str] | None = None) -> None:
        self.allowed_runners: frozenset[str] = (
            frozenset(r.lower() for r in allowed_runners)
            if allowed_runners is not None
            else DEFAULT_ALLOWED_RUNNERS
        )

    def is_process_alive_and_allowed(self, pid: int) -> tuple[bool, bool, str | None]:
        """Check if PID is alive and matches an allowed runner image."""
        return is_process_alive_and_allowed(pid, self.allowed_runners)

    def inspect_presence_lock(self, lock_path: Path | str) -> LockReapStatus:
        """Inspect and conditionally reap an individual presence lock file."""
        return inspect_presence_lock(lock_path, self.allowed_runners)

    def reap_presence_locks(self, lock_dir: Path | str) -> list[LockReapStatus]:
        """Inspect and reap stale presence locks across a directory."""
        return reap_presence_locks(lock_dir, self.allowed_runners)

    def reconcile_two_tier_registry(
        self,
        conv_id: str,
        conv_db_path: Path | str,
        summary_db_path: Path | str,
    ) -> int:
        """Reconcile conversation DB state with the global summary registry."""
        return reconcile_two_tier_registry(conv_id, conv_db_path, summary_db_path)
