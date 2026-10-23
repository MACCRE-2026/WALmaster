"""Live Volatility Input Ring Buffer & Staging Spillover.

Absorbs write stream bursts, IPC/RPC events, and unscheduled process crashes.
Bounded in-memory ring buffer with automatic disk staging spillover,
synchronous journal flush, and post-crash startup recovery.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path
from types import TracebackType
from typing import Any, Final, Self

DEFAULT_MAX_MEMORY_BYTES: Final[int] = 32 * 1024 * 1024  # 32MB
DEFAULT_STAGING_DIR: Final[Path] = Path(r"B:\WALmaster\.development\cache\staging")


class LiveInputBuffer:
    """Shock-absorbing live input buffer with bounded in-memory capacity and disk staging.

    Invariants:
        1. In-memory buffer size never continuously exceeds max_memory_bytes.
        2. Excess volume spills over to atomic disk staging chunks with fsync.
        3. Flushing to journal synchronizes disk writes and cleans up staging chunks atomically.
        4. Staged chunks persist across abrupt crashes and are recoverable upon next startup.
    """

    def __init__(
        self,
        max_memory_bytes: int = DEFAULT_MAX_MEMORY_BYTES,
        staging_dir: Path | str = DEFAULT_STAGING_DIR,
        auto_flush_journal: Path | str | None = None,
    ) -> None:
        """Initialize the live input buffer."""
        self._max_memory_bytes: int = max_memory_bytes
        self._staging_dir: Path = Path(staging_dir)
        self._auto_flush_journal: Path | None = (
            Path(auto_flush_journal) if auto_flush_journal else None
        )

        self._staging_dir.mkdir(parents=True, exist_ok=True)

        self._memory_buffer: list[dict[str, Any]] = []
        self._current_memory_bytes: int = 0
        self._total_ingested: int = 0
        self._chunk_counter: int = 0
        self._lock: threading.Lock = threading.Lock()

    @property
    def pending_event_count(self) -> int:
        """Return the count of currently buffered in-memory events."""
        with self._lock:
            return len(self._memory_buffer)

    @property
    def current_memory_bytes(self) -> int:
        """Return the current memory footprint of buffered events in bytes."""
        with self._lock:
            return self._current_memory_bytes

    @property
    def max_memory_bytes(self) -> int:
        """Return the maximum memory threshold before disk spillover."""
        return self._max_memory_bytes

    @property
    def staging_dir(self) -> Path:
        """Return the staging directory path."""
        return self._staging_dir

    @property
    def staged_chunk_count(self) -> int:
        """Return the count of unfinalized staging chunk files on disk."""
        with self._lock:
            return len(list(self._staging_dir.glob("*.chunk")))

    @property
    def has_unfinalized_staging(self) -> bool:
        """Return True if any unfinalized staging chunks exist on disk."""
        return self.staged_chunk_count > 0

    def ingest(self, payload: dict[str, Any] | str | bytes) -> int:
        """Ingest a write payload/event into the buffer.

        Triggers automatic disk spillover if memory capacity is exceeded.
        Returns the sequential ingestion count.
        """
        event_data: dict[str, Any]
        if isinstance(payload, dict):
            event_data = payload
        elif isinstance(payload, str):
            trimmed = payload.strip()
            event_data = (
                json.loads(trimmed)
                if (trimmed.startswith("{") and trimmed.endswith("}"))
                else {"raw": payload}
            )
        elif isinstance(payload, bytes):
            decoded = payload.decode("utf-8").strip()
            event_data = (
                json.loads(decoded)
                if (decoded.startswith("{") and decoded.endswith("}"))
                else {"raw": decoded}
            )
        else:
            raise TypeError(f"Unsupported payload type for ingestion: {type(payload)}")

        encoded_bytes = json.dumps(event_data, sort_keys=True).encode("utf-8")
        payload_len = len(encoded_bytes)

        with self._lock:
            # Check if adding this payload exceeds max memory capacity
            if self._memory_buffer and (
                self._current_memory_bytes + payload_len > self._max_memory_bytes
            ):
                self._spill_to_disk()

            self._memory_buffer.append(event_data)
            self._current_memory_bytes += payload_len
            self._total_ingested += 1
            return self._total_ingested

    def _spill_to_disk(self) -> None:
        """Spill accumulated in-memory events to a disk staging chunk file.

        Caller MUST hold self._lock.
        Uses atomic rename and fsync to ensure zero corrupted chunks.
        """
        if not self._memory_buffer:
            return

        self._chunk_counter += 1
        timestamp_ns: int = time.time_ns()
        unique_suffix: str = uuid.uuid4().hex[:6]
        chunk_name = f"staging_{timestamp_ns:020d}_{self._chunk_counter:08d}_{unique_suffix}.chunk"
        tmp_path = self._staging_dir / f"{chunk_name}.tmp"
        final_path = self._staging_dir / chunk_name

        with open(tmp_path, "w", encoding="utf-8") as f:
            f.writelines(f"{json.dumps(event, sort_keys=True)}\n" for event in self._memory_buffer)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, final_path)
        self._memory_buffer.clear()
        self._current_memory_bytes = 0

    def flush_to_journal(self, journal_path: Path | str) -> int:
        """Synchronously flush in-memory and staged events to the target journal.

        Enforces atomic append-only disk write with flush + fsync.
        Unlinks processed staging chunks only after guaranteed persistence.
        Returns total count of events flushed.
        """
        jpath = Path(journal_path)
        jpath.parent.mkdir(parents=True, exist_ok=True)

        with self._lock:
            staged_chunks = sorted(self._staging_dir.glob("*.chunk"))
            flushed_count: int = 0

            with open(jpath, "a", encoding="utf-8") as jf:
                # 1. Stream pre-existing staged chunks in chronological order
                for chunk in staged_chunks:
                    with open(chunk, "r", encoding="utf-8") as cf:
                        for line in cf:
                            line_str = line.strip()
                            if line_str:
                                jf.write(line_str + "\n")
                                flushed_count += 1

                # 2. Append active in-memory events
                if self._memory_buffer:
                    jf.writelines(
                        f"{json.dumps(event, sort_keys=True)}\n" for event in self._memory_buffer
                    )
                    flushed_count += len(self._memory_buffer)

                # 3. Deterministic synchronous disk flush
                jf.flush()
                os.fsync(jf.fileno())

            # 4. Remove all successfully committed staging chunk files
            for chunk in staged_chunks:
                try:
                    chunk.unlink(missing_ok=True)
                except OSError:
                    pass

            # 5. Reset memory buffer
            self._memory_buffer.clear()
            self._current_memory_bytes = 0

            return flushed_count

    def recover_unfinalized_staging(self) -> list[dict[str, Any]]:
        """Read all events from unfinalized staging chunks on disk without modifying them."""
        with self._lock:
            staged_chunks = sorted(self._staging_dir.glob("*.chunk"))
            recovered_events: list[dict[str, Any]] = []

            for chunk in staged_chunks:
                with open(chunk, "r", encoding="utf-8") as cf:
                    for line in cf:
                        line_str = line.strip()
                        if line_str:
                            recovered_events.append(json.loads(line_str))

            return recovered_events

    def replay_and_flush(self, journal_path: Path | str) -> int:
        """Replay all unfinalized staging chunks directly into journal and unlink them.

        Returns total count of replayed events.
        """
        return self.flush_to_journal(journal_path)

    def close(self) -> None:
        """Close buffer and flush to auto_flush_journal or spill to disk."""
        with self._lock:
            if self._auto_flush_journal is not None:
                # Release lock before flush_to_journal to avoid re-entrancy deadlock
                pass

        if self._auto_flush_journal is not None:
            self.flush_to_journal(self._auto_flush_journal)
        else:
            with self._lock:
                if self._memory_buffer:
                    self._spill_to_disk()

    def __enter__(self) -> Self:
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager exit with deterministic cleanup."""
        self.close()
