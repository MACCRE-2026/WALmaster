"""WALmaster Event-Sourced Projection Engine & Snapshot Baselines.

Provides:
1. Streaming JSONL Event Projection:
   - Reads append-only `transcript.jsonl` events (steps, payloads, metadata).
   - Projects them into SQLite tables: `steps`, `trajectory_meta`, `gen_metadata`
     inside a single `BEGIN IMMEDIATE ... COMMIT` block.
   - Enforces deterministic sequential rowids.
   - Automatically runs `PRAGMA integrity_check` returning projection statistics.
2. Snapshot Baseline Management:
   - Manages snapshot baselines `snapshot_epoch_<N>.db`.
   - Checks for the nearest valid snapshot baseline, restores it, and replays
     only subsequent delta events from `transcript.jsonl`.
3. Malformed/Corrupt BLOB Cordoning:
   - Catches unparseable JSON or corrupted step payloads.
   - Appends corrupted records to `quarantine_diffs.jsonl` with Subjective
     Logic epistemic opinion tuples (b=0.0, d=0.9, u=0.1, a=0.5) and diagnostics.
   - Emits a clean fallback step in SQLite so the session remains viewable and uncorrupted.
"""
from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

logger: logging.Logger = logging.getLogger("walmaster.reprojection")


@dataclass(slots=True, frozen=True)
class EpistemicOpinion:
    """Subjective Logic opinion tuple omega = (b, d, u, a)."""

    b: float = 1.0  # belief
    d: float = 0.0  # disbelief
    u: float = 0.0  # uncertainty
    a: float = 0.5  # base rate

    def as_dict(self) -> dict[str, float]:
        """Convert opinion to dictionary representation."""
        return {"b": self.b, "d": self.d, "u": self.u, "a": self.a}


QUARANTINE_DEFAULT_OPINION: Final[EpistemicOpinion] = EpistemicOpinion(
    b=0.0,
    d=0.9,
    u=0.1,
    a=0.5,
)


@dataclass(slots=True, frozen=True)
class ProjectionStats:
    """Statistics captured during deterministic event projection."""

    steps_projected: int
    duration_ms: float
    integrity_ok: bool
    quarantined_count: int = 0
    snapshot_restored_epoch: int | None = None
    delta_events_replayed: int = 0
    diagnostics: list[str] = field(default_factory=list)


def _init_sqlite_schema(conn: sqlite3.Connection) -> None:
    """Initialize canonical Tier 1 SQLite tables and indices."""
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")

    # 1. steps table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS steps (
            id INTEGER PRIMARY KEY,
            conversation_id TEXT NOT NULL,
            step_index INTEGER NOT NULL,
            step_type TEXT NOT NULL,
            status TEXT NOT NULL,
            step_payload BLOB,
            created_at REAL,
            epoch INTEGER,
            lamport INTEGER,
            opinion_b REAL DEFAULT 1.0,
            opinion_d REAL DEFAULT 0.0,
            opinion_u REAL DEFAULT 0.0,
            opinion_a REAL DEFAULT 0.5
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_steps_status ON steps(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_steps_step_type ON steps(step_type);")

    # 2. trajectory_meta table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trajectory_meta (
            conversation_id TEXT PRIMARY KEY,
            title TEXT,
            step_count INTEGER DEFAULT 0,
            last_modified_time REAL,
            created_at REAL,
            metadata_blob BLOB
        );
        """
    )

    # 3. gen_metadata table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gen_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT,
            step_id INTEGER,
            model TEXT,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            total_tokens INTEGER,
            duration_ms REAL,
            metadata_json TEXT
        );
        """
    )

    # 4. executor_metadata table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS executor_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT,
            executor_id TEXT,
            runtime_env TEXT
        );
        """
    )

    # 5. parent_references table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS parent_references (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT,
            parent_step_id INTEGER,
            child_step_id INTEGER
        );
        """
    )

    # 6. trajectory_metadata_blob table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trajectory_metadata_blob (
            conversation_id TEXT PRIMARY KEY,
            payload BLOB
        );
        """
    )

    # 7. battle_mode_infos table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS battle_mode_infos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT,
            mode_payload TEXT
        );
        """
    )

    # Internal snapshot baseline tracking table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _walmaster_snapshot_meta (
            snapshot_epoch INTEGER PRIMARY KEY,
            max_step_index INTEGER,
            max_id INTEGER,
            created_at REAL
        );
        """
    )


class SnapshotManager:
    """Manages immutable snapshot baselines (snapshot_epoch_<N>.db)."""

    def __init__(self, snapshot_dir: Path | str) -> None:
        self.snapshot_dir: Path = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def create_snapshot(self, db_path: Path | str, epoch: int) -> Path:
        """Create an immutable snapshot baseline snapshot_epoch_<epoch>.db from db_path.

        Args:
            db_path: Path to the source SQLite database.
            epoch: Epoch or step boundary number for the snapshot baseline.

        Returns:
            Path to the created snapshot file.
        """
        source_db: Path = Path(db_path)
        if not source_db.exists():
            raise FileNotFoundError(f"Source database not found: {source_db}")

        snapshot_file: Path = self.snapshot_dir / f"snapshot_epoch_{epoch}.db"

        # Checkpoint source database to ensure zero uncheckpointed frames
        conn = sqlite3.connect(source_db, timeout=10.0, isolation_level=None)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _walmaster_snapshot_meta (
                    snapshot_epoch INTEGER PRIMARY KEY,
                    max_step_index INTEGER,
                    max_id INTEGER,
                    created_at REAL
                );
                """
            )
            max_id_row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM steps;").fetchone()
            max_step_row = conn.execute("SELECT COALESCE(MAX(step_index), 0) FROM steps;").fetchone()
            max_id: int = int(max_id_row[0]) if max_id_row else 0
            max_step: int = int(max_step_row[0]) if max_step_row else 0

            conn.execute(
                "INSERT OR REPLACE INTO _walmaster_snapshot_meta VALUES (?, ?, ?, ?);",
                (epoch, max_step, max_id, time.time()),
            )
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        finally:
            conn.close()

        # Copy clean checkpointed database to snapshot file
        shutil.copy2(source_db, snapshot_file)
        return snapshot_file

    def list_snapshots(self) -> list[tuple[int, Path]]:
        """List all valid snapshots sorted by epoch ascending."""
        if not self.snapshot_dir.exists():
            return []

        snapshots: list[tuple[int, Path]] = []
        for p in self.snapshot_dir.glob("snapshot_epoch_*.db"):
            if not p.is_file():
                continue
            stem: str = p.stem
            parts: list[str] = stem.split("_")
            if len(parts) >= 3 and parts[-1].isdigit():
                ep: int = int(parts[-1])
                snapshots.append((ep, p))

        snapshots.sort(key=lambda s: s[0])
        return snapshots

    def find_nearest_snapshot(self, target_epoch: int | None = None) -> tuple[int, Path] | None:
        """Find the nearest valid snapshot baseline with epoch <= target_epoch.

        Args:
            target_epoch: Optional maximum epoch boundary.

        Returns:
            Tuple of (epoch, path) if a valid snapshot is found, else None.
        """
        all_snaps: list[tuple[int, Path]] = self.list_snapshots()
        if not all_snaps:
            return None

        candidates: list[tuple[int, Path]] = all_snaps
        if target_epoch is not None:
            candidates = [s for s in all_snaps if s[0] <= target_epoch]

        if not candidates:
            return None

        # Prioritize largest epoch first
        candidates.sort(key=lambda s: s[0], reverse=True)

        for ep, path in candidates:
            conn: sqlite3.Connection | None = None
            try:
                conn = sqlite3.connect(path, timeout=5.0)
                res = conn.execute("PRAGMA integrity_check;").fetchall()
                if len(res) == 1 and res[0][0] == "ok":
                    return ep, path
            except (sqlite3.Error, OSError) as e:
                logger.warning("Snapshot %s failed verification: %s", path, e)
                continue
            finally:
                if conn is not None:
                    conn.close()

        return None


class ReprojectionEngine:
    """Event-sourced projection engine streaming JSONL logs into SQLite databases."""

    def __init__(
        self,
        snapshot_dir: Path | str | None = None,
        quarantine_path: Path | str | None = None,
        validate_payload: Callable[[object], bool] | None = None,
    ) -> None:
        self.snapshot_dir: Path | None = Path(snapshot_dir) if snapshot_dir is not None else None
        self.snapshot_mgr: SnapshotManager | None = (
            SnapshotManager(self.snapshot_dir) if self.snapshot_dir is not None else None
        )
        self.quarantine_path: Path | None = Path(quarantine_path) if quarantine_path is not None else None
        self.validate_payload: Callable[[object], bool] | None = validate_payload

    def create_snapshot_baseline(self, db_path: Path | str, epoch: int) -> Path:
        """Create a snapshot baseline from an existing database."""
        if self.snapshot_mgr is None:
            target_dir = Path(db_path).parent / "snapshots"
            self.snapshot_mgr = SnapshotManager(target_dir)
        return self.snapshot_mgr.create_snapshot(db_path, epoch)

    def _record_quarantine(
        self,
        quarantine_file: Path,
        reason: str,
        raw_record: object,
        diagnostics: dict[str, object],
        opinion: EpistemicOpinion = QUARANTINE_DEFAULT_OPINION,
    ) -> None:
        """Append an unparseable or corrupted record to quarantine_diffs.jsonl."""
        quarantine_file.parent.mkdir(parents=True, exist_ok=True)
        entry: dict[str, object] = {
            "timestamp": time.time(),
            "reason": reason,
            "raw_record": raw_record,
            "opinion": opinion.as_dict(),
            "diagnostics": diagnostics,
        }
        with open(quarantine_file, "a", encoding="utf-8") as qf:
            qf.write(json.dumps(entry) + "\n")

    def project_transcript(
        self,
        transcript_path: Path | str,
        target_db_path: Path | str,
        *,
        use_snapshots: bool = True,
        quarantine_path: Path | str | None = None,
    ) -> ProjectionStats:
        """Stream JSONL transcript events into SQLite target DB inside an atomic transaction.

        Args:
            transcript_path: Path to input transcript.jsonl.
            target_db_path: Path to output SQLite database.
            use_snapshots: Whether to look up and restore the nearest snapshot baseline.
            quarantine_path: Optional path for quarantine_diffs.jsonl.

        Returns:
            ProjectionStats summarizing the projection outcome.
        """
        start_time: float = time.perf_counter()
        t_path: Path = Path(transcript_path)
        db_path: Path = Path(target_db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        q_path: Path = (
            Path(quarantine_path)
            if quarantine_path is not None
            else (self.quarantine_path if self.quarantine_path is not None else db_path.parent / "quarantine_diffs.jsonl")
        )

        diagnostics: list[str] = []
        quarantined_count: int = 0
        restored_epoch: int | None = None
        delta_events_replayed: int = 0

        # Snapshot resolution
        nearest_snapshot: tuple[int, Path] | None = None
        if use_snapshots and self.snapshot_mgr is not None:
            nearest_snapshot = self.snapshot_mgr.find_nearest_snapshot()

        already_projected_steps: int = 0
        current_rowid: int = 0
        latest_conv_id: str = "default_session"

        if nearest_snapshot is not None:
            restored_epoch, snap_file = nearest_snapshot
            shutil.copy2(snap_file, db_path)

            # Read existing baseline bounds
            conn_base = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
            try:
                _init_sqlite_schema(conn_base)
                # Check _walmaster_snapshot_meta
                meta_row = conn_base.execute(
                    "SELECT max_step_index, max_id FROM _walmaster_snapshot_meta WHERE snapshot_epoch = ?;",
                    (restored_epoch,),
                ).fetchone()

                if meta_row:
                    max_step_index, max_id = int(meta_row[0]), int(meta_row[1])
                    already_projected_steps = max(max_step_index, max_id)
                    current_rowid = max_id
                else:
                    max_id_row = conn_base.execute("SELECT COALESCE(MAX(id), 0) FROM steps;").fetchone()
                    max_step_row = conn_base.execute("SELECT COALESCE(MAX(step_index), 0) FROM steps;").fetchone()
                    m_id: int = int(max_id_row[0]) if max_id_row else 0
                    m_step: int = int(max_step_row[0]) if max_step_row else 0
                    already_projected_steps = max(m_id, m_step)
                    current_rowid = m_id

                conv_id_row = conn_base.execute("SELECT conversation_id FROM steps LIMIT 1;").fetchone()
                if conv_id_row and conv_id_row[0]:
                    latest_conv_id = str(conv_id_row[0])
            finally:
                conn_base.close()
        else:
            # Clean start: remove any existing destination files
            for ext in ("", "-wal", "-shm"):
                p: Path = db_path.with_name(db_path.name + ext)
                if p.exists():
                    try:
                        p.unlink(missing_ok=True)
                    except OSError:
                        pass

        if not t_path.exists():
            # Initialize empty schema and return
            conn_empty = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
            try:
                _init_sqlite_schema(conn_empty)
                res = conn_empty.execute("PRAGMA integrity_check;").fetchall()
                integrity_ok = len(res) == 1 and res[0][0] == "ok"
            finally:
                conn_empty.close()

            dur = (time.perf_counter() - start_time) * 1000.0
            return ProjectionStats(
                steps_projected=0,
                duration_ms=dur,
                integrity_ok=integrity_ok,
                quarantined_count=0,
                snapshot_restored_epoch=None,
                delta_events_replayed=0,
                diagnostics=["Transcript file does not exist"],
            )

        # Open SQLite connection with manual transaction control
        conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
        try:
            _init_sqlite_schema(conn)

            # Single atomic transaction block: BEGIN IMMEDIATE ... COMMIT
            conn.execute("BEGIN IMMEDIATE;")
            try:
                with open(t_path, "r", encoding="utf-8") as f:
                    for line_no, raw_line in enumerate(f, start=1):
                        line_str: str = raw_line.strip()
                        if not line_str:
                            continue

                        # 1. Attempt JSON deserialization
                        record: dict[str, Any]
                        try:
                            parsed = json.loads(line_str)
                            if not isinstance(parsed, dict):
                                raise TypeError(f"Record on line {line_no} is not a JSON object")
                            record = parsed
                        except (json.JSONDecodeError, TypeError, ValueError) as json_err:
                            # Malformed JSONL record -> Cordon to quarantine
                            quarantined_count += 1
                            self._record_quarantine(
                                quarantine_file=q_path,
                                reason=f"Malformed JSON: {json_err}",
                                raw_record=line_str,
                                diagnostics={"line_number": line_no, "error_type": "JSONDecodeError"},
                            )

                            # Emit clean fallback step
                            current_rowid += 1
                            fallback_payload: str = json.dumps({
                                "status": "quarantined",
                                "error": "Corrupted record cordoned to quarantine_diffs.jsonl",
                                "details": str(json_err),
                                "line_number": line_no,
                            })
                            conn.execute(
                                """
                                INSERT INTO steps (
                                    id, conversation_id, step_index, step_type, status,
                                    step_payload, created_at, epoch, lamport,
                                    opinion_b, opinion_d, opinion_u, opinion_a
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                """,
                                (
                                    current_rowid,
                                    latest_conv_id,
                                    current_rowid,
                                    "FALLBACK_QUARANTINED",
                                    "QUARANTINED",
                                    fallback_payload,
                                    time.time(),
                                    0,
                                    0,
                                    QUARANTINE_DEFAULT_OPINION.b,
                                    QUARANTINE_DEFAULT_OPINION.d,
                                    QUARANTINE_DEFAULT_OPINION.u,
                                    QUARANTINE_DEFAULT_OPINION.a,
                                ),
                            )
                            continue

                        # 2. Extract Event / Record Type
                        event_type: str = str(record.get("event_type") or record.get("type") or "")

                        # Handle trajectory_meta event
                        if event_type in ("trajectory_meta", "meta"):
                            c_id: str = str(record.get("conversation_id") or latest_conv_id)
                            latest_conv_id = c_id
                            title_val: str | None = (
                                str(record["title"]) if record.get("title") is not None else None
                            )
                            step_cnt: int = int(record.get("step_count") or 0)
                            created_ts: float = float(record.get("created_at") or time.time())
                            modified_ts: float = float(record.get("last_modified_time") or created_ts)

                            conn.execute(
                                """
                                INSERT INTO trajectory_meta (
                                    conversation_id, title, step_count, last_modified_time, created_at
                                ) VALUES (?, ?, ?, ?, ?)
                                ON CONFLICT(conversation_id) DO UPDATE SET
                                    title = COALESCE(excluded.title, trajectory_meta.title),
                                    step_count = MAX(trajectory_meta.step_count, excluded.step_count),
                                    last_modified_time = MAX(trajectory_meta.last_modified_time, excluded.last_modified_time);
                                """,
                                (c_id, title_val, step_cnt, modified_ts, created_ts),
                            )
                            continue

                        # Handle gen_metadata event
                        if event_type in ("gen_metadata", "generation"):
                            c_id = str(record.get("conversation_id") or latest_conv_id)
                            step_id_val: int | None = (
                                int(record["step_id"]) if record.get("step_id") is not None else None
                            )
                            model_val: str | None = (
                                str(record["model"]) if record.get("model") is not None else None
                            )
                            prompt_tokens: int | None = (
                                int(record["prompt_tokens"]) if record.get("prompt_tokens") is not None else None
                            )
                            comp_tokens: int | None = (
                                int(record["completion_tokens"]) if record.get("completion_tokens") is not None else None
                            )
                            tot_tokens: int | None = (
                                int(record["total_tokens"]) if record.get("total_tokens") is not None else None
                            )
                            dur_ms: float | None = (
                                float(record["duration_ms"]) if record.get("duration_ms") is not None else None
                            )
                            meta_json_str: str | None = (
                                json.dumps(record["metadata"]) if "metadata" in record else None
                            )

                            conn.execute(
                                """
                                INSERT INTO gen_metadata (
                                    conversation_id, step_id, model, prompt_tokens,
                                    completion_tokens, total_tokens, duration_ms, metadata_json
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                                """,
                                (
                                    c_id,
                                    step_id_val,
                                    model_val,
                                    prompt_tokens,
                                    comp_tokens,
                                    tot_tokens,
                                    dur_ms,
                                    meta_json_str,
                                ),
                            )
                            continue

                        # 3. Extract Step Record
                        step_dict: dict[str, Any]
                        if "step" in record and isinstance(record["step"], dict):
                            step_dict = dict(record["step"])
                            # Inherit top-level metadata if omitted in step
                            for inherited_key in ("epoch", "lamport", "conversation_id", "opinion"):
                                if inherited_key not in step_dict and inherited_key in record:
                                    step_dict[inherited_key] = record[inherited_key]
                        else:
                            step_dict = record

                        step_idx_val: int = int(step_dict.get("step_index") or step_dict.get("index") or 0)
                        ep_val: int = int(step_dict.get("epoch") or 0)

                        # Delta Replay Check: If already covered by restored snapshot baseline, skip
                        if already_projected_steps > 0:
                            if step_idx_val > 0 and step_idx_val <= already_projected_steps:
                                continue
                            if (
                                restored_epoch is not None
                                and ep_val > 0
                                and ep_val <= restored_epoch
                                and step_idx_val <= already_projected_steps
                            ):
                                continue

                        delta_events_replayed += 1

                        conv_id: str = str(
                            step_dict.get("conversation_id") or latest_conv_id or "default_session"
                        )
                        latest_conv_id = conv_id

                        s_type: str = str(
                            step_dict.get("step_type") or step_dict.get("type") or "AGENT_STEP"
                        )
                        status_str: str = str(step_dict.get("status") or "DONE")
                        created_at_val: float = float(step_dict.get("created_at") or time.time())
                        lamport_val: int = int(step_dict.get("lamport") or 0)

                        # Parse opinion
                        op_dict = step_dict.get("opinion")
                        opinion_b = 1.0
                        opinion_d = 0.0
                        opinion_u = 0.0
                        opinion_a = 0.5
                        if isinstance(op_dict, dict):
                            opinion_b = float(op_dict.get("b", 1.0))
                            opinion_d = float(op_dict.get("d", 0.0))
                            opinion_u = float(op_dict.get("u", 0.0))
                            opinion_a = float(op_dict.get("a", 0.5))

                        raw_payload = step_dict.get("step_payload", step_dict.get("payload"))

                        # Check for corrupted payload or explicit corrupt flag
                        is_payload_corrupt: bool = bool(step_dict.get("corrupt") or step_dict.get("is_corrupt"))

                        if not is_payload_corrupt and self.validate_payload is not None:
                            try:
                                if not self.validate_payload(raw_payload):
                                    is_payload_corrupt = True
                            except (TypeError, ValueError, AttributeError, RuntimeError):
                                is_payload_corrupt = True

                        if is_payload_corrupt:
                            # Cordon corrupt payload
                            quarantined_count += 1
                            self._record_quarantine(
                                quarantine_file=q_path,
                                reason="Corrupt payload or unparseable BLOB",
                                raw_record=raw_line,
                                diagnostics={
                                    "line_number": line_no,
                                    "step_index": step_idx_val,
                                    "error_type": "CorruptPayloadError",
                                },
                            )

                            current_rowid += 1
                            fallback_msg: str = json.dumps({
                                "status": "quarantined",
                                "error": "Corrupted payload cordoned to quarantine_diffs.jsonl",
                                "original_index": step_idx_val,
                                "line_number": line_no,
                            })
                            conn.execute(
                                """
                                INSERT INTO steps (
                                    id, conversation_id, step_index, step_type, status,
                                    step_payload, created_at, epoch, lamport,
                                    opinion_b, opinion_d, opinion_u, opinion_a
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                """,
                                (
                                    current_rowid,
                                    conv_id,
                                    current_rowid,
                                    "FALLBACK_QUARANTINED",
                                    "QUARANTINED",
                                    fallback_msg,
                                    created_at_val,
                                    ep_val,
                                    lamport_val,
                                    QUARANTINE_DEFAULT_OPINION.b,
                                    QUARANTINE_DEFAULT_OPINION.d,
                                    QUARANTINE_DEFAULT_OPINION.u,
                                    QUARANTINE_DEFAULT_OPINION.a,
                                ),
                            )
                            continue

                        # Clean valid step
                        current_rowid += 1
                        normalized_payload: str | bytes | None
                        if isinstance(raw_payload, (bytes, bytearray)):
                            normalized_payload = bytes(raw_payload)
                        elif isinstance(raw_payload, str):
                            normalized_payload = raw_payload
                        elif raw_payload is not None:
                            normalized_payload = json.dumps(raw_payload)
                        else:
                            normalized_payload = None

                        conn.execute(
                            """
                            INSERT INTO steps (
                                id, conversation_id, step_index, step_type, status,
                                step_payload, created_at, epoch, lamport,
                                opinion_b, opinion_d, opinion_u, opinion_a
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                            """,
                            (
                                current_rowid,
                                conv_id,
                                current_rowid,
                                s_type,
                                status_str,
                                normalized_payload,
                                created_at_val,
                                ep_val,
                                lamport_val,
                                opinion_b,
                                opinion_d,
                                opinion_u,
                                opinion_a,
                            ),
                        )

                # Update trajectory_meta with final step count and latest modified timestamp
                conn.execute(
                    """
                    INSERT INTO trajectory_meta (
                        conversation_id, step_count, last_modified_time
                    ) VALUES (?, ?, ?)
                    ON CONFLICT(conversation_id) DO UPDATE SET
                        step_count = MAX(trajectory_meta.step_count, excluded.step_count),
                        last_modified_time = MAX(trajectory_meta.last_modified_time, excluded.last_modified_time);
                    """,
                    (latest_conv_id, current_rowid, time.time()),
                )

                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise

            # Automatic integrity check on projected database
            integrity_rows = conn.execute("PRAGMA integrity_check;").fetchall()
            integrity_ok: bool = len(integrity_rows) == 1 and integrity_rows[0][0] == "ok"
            if not integrity_ok:
                diagnostics.append(f"PRAGMA integrity_check failed: {integrity_rows}")

            count_row = conn.execute("SELECT COUNT(*) FROM steps;").fetchone()
            total_projected_steps: int = int(count_row[0]) if count_row else 0
        finally:
            conn.close()

        dur_ms = (time.perf_counter() - start_time) * 1000.0
        return ProjectionStats(
            steps_projected=total_projected_steps,
            duration_ms=dur_ms,
            integrity_ok=integrity_ok,
            quarantined_count=quarantined_count,
            snapshot_restored_epoch=restored_epoch,
            delta_events_replayed=delta_events_replayed,
            diagnostics=diagnostics,
        )


def reproject_from_jsonl(
    transcript_path: Path | str,
    target_db_path: Path | str,
    snapshot_dir: Path | str | None = None,
    quarantine_path: Path | str | None = None,
    use_snapshots: bool = True,
) -> ProjectionStats:
    """Convenience function to reproject events from transcript.jsonl into a SQLite database.

    Args:
        transcript_path: Path to the source JSONL event transcript.
        target_db_path: Path to the target SQLite database.
        snapshot_dir: Directory containing or receiving snapshot baselines.
        quarantine_path: Destination path for cordoned records.
        use_snapshots: Whether to check for and restore valid snapshot baselines.

    Returns:
        ProjectionStats with execution timings and integrity outcome.
    """
    engine: ReprojectionEngine = ReprojectionEngine(
        snapshot_dir=snapshot_dir,
        quarantine_path=quarantine_path,
    )
    return engine.project_transcript(
        transcript_path=transcript_path,
        target_db_path=target_db_path,
        use_snapshots=use_snapshots,
    )


def create_snapshot_baseline(
    db_path: Path | str,
    snapshot_dir: Path | str,
    epoch: int,
) -> Path:
    """Convenience function to snapshot a SQLite database at a given epoch."""
    manager: SnapshotManager = SnapshotManager(snapshot_dir)
    return manager.create_snapshot(db_path, epoch)
