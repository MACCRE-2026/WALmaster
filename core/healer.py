"""WALmaster Sovereign Session Healer.

Repairs corrupted Antigravity SQLite databases using event-sourced logs,
reaps stale presence locks, aligns two-tier UI registries, and purges sync conflict files.
"""
from __future__ import annotations

import datetime
import gc
import json
import logging
import re
import shutil
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.sentinel import Sentinel
from core.summary_sync import (
    make_timestamp_proto,
    parse_proto_fields,
    patch_raw_summary,
    serialize_proto_fields,
)

logger = logging.getLogger("walmaster.healer")

DEFAULT_CONVERSATIONS_DIR = Path.home() / ".gemini" / "antigravity" / "conversations"
DEFAULT_BRAIN_DIR = Path.home() / ".gemini" / "antigravity" / "brain"
DEFAULT_PRESENCE_DIR = Path.home() / ".gemini" / "antigravity" / "presence"
DEFAULT_SUMMARIES_DB = Path.home() / ".gemini" / "antigravity" / "conversation_summaries.db"
DEFAULT_AGYHUB_PROTO = Path.home() / ".gemini" / "antigravity" / "agyhub_summaries_proto.pb"

CANONICAL_SCHEMAS: dict[str, str] = {
    "trajectory_meta": (
        "CREATE TABLE `trajectory_meta` (`trajectory_id` text,`cascade_id` text,"
        "`trajectory_type` integer,`source` integer,PRIMARY KEY (`trajectory_id`))"
    ),
    "steps": (
        "CREATE TABLE `steps` (`idx` integer,`step_type` integer NOT NULL DEFAULT 0,"
        "`status` integer NOT NULL DEFAULT 0,`has_subtrajectory` numeric NOT NULL DEFAULT false,"
        "`metadata` blob,`error_details` blob,`permissions` blob,`task_details` blob,"
        "`render_info` blob,`step_payload` blob,`step_format` integer NOT NULL DEFAULT 0,"
        "PRIMARY KEY (`idx`))"
    ),
    "gen_metadata": (
        "CREATE TABLE `gen_metadata` (`idx` integer,`data` blob,`size` integer NOT NULL DEFAULT 0,"
        "PRIMARY KEY (`idx`))"
    ),
    "executor_metadata": (
        "CREATE TABLE `executor_metadata` (`idx` integer,`data` blob,PRIMARY KEY (`idx`))"
    ),
    "parent_references": (
        "CREATE TABLE `parent_references` (`idx` integer,`data` blob,PRIMARY KEY (`idx`))"
    ),
    "trajectory_metadata_blob": (
        "CREATE TABLE `trajectory_metadata_blob` (`id` text DEFAULT 'main',`data` blob,"
        "PRIMARY KEY (`id`))"
    ),
    "battle_mode_infos": (
        "CREATE TABLE `battle_mode_infos` (`idx` integer,`data` blob,PRIMARY KEY (`idx`))"
    ),
}

STEP_TYPE_MAP: dict[str, int] = {
    "USER_INPUT": 14,
    "PLANNER_RESPONSE": 15,
    "GENERIC": 132,
    "SYSTEM_MESSAGE": 101,
    "CHECKPOINT": 102,
}


@dataclass(slots=True, frozen=True)
class HealResult:
    session_id: str
    success: bool
    total_steps: int
    salvaged_steps: int
    synthesized_steps: int
    integrity_ok: bool
    presence_lock_reaped: bool
    summaries_upserted: bool
    purged_conflicts: list[str]
    backup_path: Path | None
    diagnostics: list[str] = field(default_factory=list)


def synthesize_step_metadata(
    created_at_iso: str,
    root_trajectory_id: str,
    step_idx: int,
    turn_idx: int,
    conversation_id: str,
) -> bytes:
    """Synthesize a canonical binary Protobuf message for steps.metadata."""
    sec = int(time.time())
    nanos = 0
    if created_at_iso:
        try:
            clean_iso = created_at_iso.replace("Z", "+00:00")
            dt = datetime.datetime.fromisoformat(clean_iso)
            sec = int(dt.timestamp())
            nanos = dt.microsecond * 1000
        except (ValueError, TypeError):
            pass

    # Tag 1: google.protobuf.Timestamp
    ts_bytes = make_timestamp_proto(sec, nanos)

    # Tag 20: TrajectoryBinding submessage
    binding_fields: dict[int, list[tuple[str, Any]]] = {
        1: [("bytes", root_trajectory_id.encode("ascii"))],
        2: [("varint", step_idx)],
        3: [("varint", turn_idx)],
        4: [("bytes", conversation_id.encode("ascii"))],
    }
    binding_bytes = serialize_proto_fields(binding_fields)

    # Top-level step metadata
    step_fields: dict[int, list[tuple[str, Any]]] = {
        1: [("bytes", ts_bytes)],
        3: [("varint", 2)],  # status completed
        20: [("bytes", binding_bytes)],
    }
    return serialize_proto_fields(step_fields)


def synthesize_trajectory_metadata_blob(
    ws_uri: str,
    project_id: str | None,
    conversation_id: str,
    created_at_sec: int,
) -> bytes:
    """Synthesize a canonical binary Protobuf message for trajectory_metadata_blob."""
    ws_clean = ws_uri.replace("%3A", ":")
    encoded_ws = ws_uri if "%3A" in ws_uri else ws_uri.replace(":", "%3A", 1)
    # CortexWorkspaceMetadata submessage
    ws_sub = serialize_proto_fields({
        1: [("bytes", ws_clean.encode("utf-8"))],
        2: [("bytes", ws_clean.encode("utf-8"))],
        3: [("bytes", b"")],
        4: [("bytes", b"main")],
    })
    # Timestamp submessage
    ts_sub = make_timestamp_proto(created_at_sec, 0)

    top_fields: dict[int, list[tuple[str, Any]]] = {
        1: [("bytes", ws_sub)],
        2: [("bytes", ts_sub)],
        6: [("bytes", conversation_id.encode("ascii"))],
        7: [("bytes", encoded_ws.encode("utf-8"))],
    }
    if project_id:
        top_fields[18] = [("bytes", project_id.encode("ascii"))]

    return serialize_proto_fields(top_fields)


class SovereignSessionHealer:
    """Repairs corrupted Antigravity conversations and aligns the two-tier UI registry."""

    def __init__(
        self,
        conversations_dir: Path | str = DEFAULT_CONVERSATIONS_DIR,
        brain_dir: Path | str = DEFAULT_BRAIN_DIR,
        presence_dir: Path | str = DEFAULT_PRESENCE_DIR,
        summaries_db: Path | str = DEFAULT_SUMMARIES_DB,
        agyhub_proto: Path | str = DEFAULT_AGYHUB_PROTO,
    ) -> None:
        self.conversations_dir = Path(conversations_dir)
        self.brain_dir = Path(brain_dir)
        self.presence_dir = Path(presence_dir)
        self.summaries_db = Path(summaries_db)
        self.agyhub_proto = Path(agyhub_proto)
        self.sentinel = Sentinel()

    @staticmethod
    def _extract_metadata_from_blob(blob: bytes) -> tuple[str | None, str | None]:
        """Extract project_id and workspace_uri from a trajectory_metadata_blob Protobuf."""
        if not blob:
            return None, None

        project_id: str | None = None
        workspace_uri: str | None = None

        # 1. Deterministic Protobuf decoding
        try:
            fields = parse_proto_fields(blob)
            if fields.get(18):
                val = fields[18][0][1]
                if isinstance(val, bytes) and len(val) == 36:
                    project_id = val.decode("ascii", errors="ignore")

            if fields.get(1):
                ws_sub = fields[1][0][1]
                if isinstance(ws_sub, bytes):
                    ws_fields = parse_proto_fields(ws_sub)
                    if ws_fields.get(1):
                        ws_val = ws_fields[1][0][1]
                        if isinstance(ws_val, bytes):
                            workspace_uri = ws_val.decode("utf-8", errors="ignore")
            elif fields.get(7):
                ws_val7 = fields[7][0][1]
                if isinstance(ws_val7, bytes):
                    workspace_uri = ws_val7.decode("utf-8", errors="ignore")
        except (ValueError, KeyError, IndexError) as decode_err:
            logger.debug("Protobuf metadata extraction notice: %s", decode_err)

        # 2. Byte pattern search fallback
        if not project_id:
            m_proj = re.search(rb"\x92\x01\$([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", blob)
            if m_proj:
                project_id = m_proj.group(1).decode("ascii")
            else:
                uuids = re.findall(rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", blob)
                if uuids:
                    project_id = uuids[-1].decode("ascii")

        if not workspace_uri:
            m_ws = re.search(rb"(file:///[a-zA-Z0-9%_/-]+)", blob)
            if m_ws:
                ws_cand = m_ws.group(1).decode("utf-8", errors="ignore")
                workspace_uri = ws_cand.removesuffix("z")

        return project_id, workspace_uri

    def _upsert_summary(
        self,
        s_conn: sqlite3.Connection,
        session_id: str,
        title: str,
        total_steps: int,
        now_str: str,
        project_id: str | None = None,
        workspace_uris: str | None = None,
        raw_summary: bytes | None = None,
    ) -> None:
        """Dynamically upsert into conversation_summaries respecting schema constraints."""
        cur = s_conn.cursor()
        cur.execute("PRAGMA table_info(conversation_summaries)")
        cols = {row[1]: row for row in cur.fetchall()}

        if not cols:
            return

        cur.execute(
            "SELECT * FROM conversation_summaries WHERE conversation_id = ?",
            (session_id,),
        )
        existing_row = cur.fetchone()
        existing_dict: dict[str, Any] = {}
        if existing_row:
            existing_dict = {col: existing_row[i] for i, col in enumerate(cols.keys())}

        # Format timestamps in Antigravity standard space format (YYYY-MM-DD HH:MM:SS)
        clean_now = now_str.replace("T", " ")

        data_to_set: dict[str, Any] = {
            "conversation_id": session_id,
            "title": title or existing_dict.get("title") or "Restored Conversation",
            "step_count": total_steps,
            "last_modified_time": clean_now,
        }

        if "preview" in cols:
            data_to_set["preview"] = existing_dict.get("preview") or title or "Restored Conversation"
        if "last_user_input_time" in cols:
            prev_input_time = existing_dict.get("last_user_input_time")
            if prev_input_time and "T" in str(prev_input_time):
                prev_input_time = str(prev_input_time).replace("T", " ")
            data_to_set["last_user_input_time"] = prev_input_time or clean_now

        chosen_ws = workspace_uris or existing_dict.get("workspace_uris")
        if "workspace_uris" in cols:
            data_to_set["workspace_uris"] = chosen_ws or '["file:///b%3A/CRSDL-MACCRE"]'
        if "status" in cols:
            col_type = str(cols["status"][2]).upper()
            if "INT" in col_type:
                data_to_set["status"] = 1
            else:
                data_to_set["status"] = "CASCADE_RUN_STATUS_IDLE"
        if "app_data_dir" in cols:
            data_to_set["app_data_dir"] = existing_dict.get("app_data_dir") or "antigravity"

        chosen_pid = project_id or existing_dict.get("project_id")
        if "project_id" in cols and chosen_pid:
            data_to_set["project_id"] = chosen_pid

        if "raw_summary" in cols:
            existing_raw = existing_dict.get("raw_summary") or b""
            target_raw = raw_summary if raw_summary is not None else existing_raw
            if target_raw:
                data_to_set["raw_summary"] = target_raw

        col_names = list(data_to_set.keys())
        placeholders = ", ".join(["?"] * len(col_names))
        quoted_cols = ", ".join([f"`{c}`" for c in col_names])

        update_clauses = [
            f"`{c}` = excluded.`{c}`"
            for c in col_names
            if c != "conversation_id"
        ]
        update_str = ", ".join(update_clauses)

        sql = f"""
            INSERT INTO conversation_summaries ({quoted_cols})
            VALUES ({placeholders})
            ON CONFLICT(conversation_id) DO UPDATE SET
            {update_str}
        """
        cur.execute(sql, [data_to_set[c] for c in col_names])

    def heal_session(
        self,
        session_id: str,
        *,
        backup: bool = True,
        live_apply: bool = False,
        scratch_dir: Path | str | None = None,
    ) -> HealResult:
        """Surgically heal a conversation database and align the two-tier UI registry.

        Args:
            session_id: The UUID of the conversation.
            backup: Whether to create a .corrupt.bak backup of the original database.
            live_apply: If True, replaces the live database in ~/.gemini/antigravity/conversations/.
            scratch_dir: Isolated directory for building the pristine database.

        Returns:
            HealResult detailing repaired steps, integrity checks, and presence locks.
        """
        diagnostics: list[str] = []
        target_db = self.conversations_dir / f"{session_id}.db"
        brain_log = self.brain_dir / session_id / ".system_generated" / "logs" / "transcript.jsonl"
        lock_file = self.presence_dir / f"{session_id}.lock"

        if not brain_log.exists():
            return HealResult(
                session_id=session_id,
                success=False,
                total_steps=0,
                salvaged_steps=0,
                synthesized_steps=0,
                integrity_ok=False,
                presence_lock_reaped=False,
                summaries_upserted=False,
                purged_conflicts=[],
                backup_path=None,
                diagnostics=[f"Canonical event log not found: {brain_log}"],
            )

        # 1. Read canonical events from JSONL
        jsonl_steps: list[dict[str, Any]] = []
        with open(brain_log, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line_str = line.strip()
                if line_str:
                    try:
                        jsonl_steps.append(json.loads(line_str))
                    except json.JSONDecodeError as err:
                        diagnostics.append(f"Skipped unparseable JSONL line: {err}")

        total_steps = len(jsonl_steps)
        title = "Restored Conversation"
        for st in jsonl_steps:
            if st.get("type") == "USER_INPUT" and st.get("content"):
                c = str(st.get("content", "")).strip()
                title = (c[:60] + "...") if len(c) > 60 else c
                break

        # 2. Extract salvageable valid records from existing DB
        salvaged_steps: dict[int, tuple[Any, ...]] = {}
        salvaged_meta: list[tuple[Any, ...]] = []
        salvaged_blob: list[tuple[Any, ...]] = []
        salvaged_gen: dict[int, tuple[Any, ...]] = {}
        salvaged_exec: dict[int, tuple[Any, ...]] = {}

        if target_db.exists():
            src_conn: sqlite3.Connection | None = None
            try:
                src_conn = sqlite3.connect(f"file:{target_db.as_posix()}?mode=ro", uri=True)
                cur = src_conn.cursor()
                try:
                    cur.execute("SELECT trajectory_id, cascade_id, trajectory_type, source FROM trajectory_meta")
                    salvaged_meta = cur.fetchall()
                except sqlite3.Error as meta_err:
                    logger.debug("Meta salvage note: %s", meta_err)

                try:
                    cur.execute("SELECT id, data FROM trajectory_metadata_blob")
                    salvaged_blob = cur.fetchall()
                    if salvaged_blob and (not salvaged_blob[0][1] or len(salvaged_blob[0][1]) == 0):
                        salvaged_blob = []
                except sqlite3.Error as blob_err:
                    logger.debug("Blob salvage note: %s", blob_err)

                try:
                    cur.execute("SELECT idx, data, size FROM gen_metadata")
                    for r in cur.fetchall():
                        salvaged_gen[r[0]] = r
                except sqlite3.Error as gen_err:
                    logger.debug("gen_metadata salvage note: %s", gen_err)

                try:
                    cur.execute("SELECT idx, data FROM executor_metadata")
                    for r in cur.fetchall():
                        salvaged_exec[r[0]] = r
                except sqlite3.Error as exec_err:
                    logger.debug("executor_metadata salvage note: %s", exec_err)

                # Probe steps individually to bypass corrupted B-tree page pointers
                for idx in range(total_steps):
                    try:
                        cur.execute(
                            "SELECT idx, step_type, status, has_subtrajectory, metadata, error_details, permissions, task_details, render_info, step_payload, step_format FROM steps WHERE idx = ?",
                            (idx,),
                        )
                        row = cur.fetchone()
                        if row:
                            salvaged_steps[idx] = row
                    except sqlite3.Error:
                        pass
            except sqlite3.Error as src_err:
                diagnostics.append(f"Notice: Source DB read-only connect encountered: {src_err}")
            finally:
                if src_conn is not None:
                    src_conn.close()
                gc.collect()

        # Fallback 1: check if existing .bak has the intact trajectory_metadata_blob
        bak_file = target_db.with_suffix(".db.corrupt.bak")
        has_valid_blob = bool(salvaged_blob and salvaged_blob[0][1] and len(salvaged_blob[0][1]) > 0)
        if not has_valid_blob and bak_file.exists():
            try:
                with sqlite3.connect(f"file:{bak_file.as_posix()}?mode=ro", uri=True) as b_conn:
                    b_cur = b_conn.cursor()
                    b_cur.execute("SELECT id, data FROM trajectory_metadata_blob")
                    b_rows = b_cur.fetchall()
                    if b_rows and b_rows[0][1] and len(b_rows[0][1]) > 0:
                        salvaged_blob = b_rows
                        has_valid_blob = True
                        diagnostics.append(f"Salvaged {len(b_rows[0][1])}-byte trajectory_metadata_blob from backup: {bak_file.name}")
            except sqlite3.Error as bak_err:
                logger.debug("Backup blob salvage note: %s", bak_err)

        # Fallback 2: Synthesize minimal valid trajectory_metadata_blob if missing
        if not has_valid_blob:
            proj_hint: str | None = None
            ws_hint: str = "file:///b:/CRSDL-MACCRE"
            if self.summaries_db.exists():
                try:
                    with sqlite3.connect(f"file:{self.summaries_db.as_posix()}?mode=ro", uri=True) as s_conn:
                        s_cur = s_conn.cursor()
                        s_cur.execute("SELECT project_id, workspace_uris FROM conversation_summaries WHERE conversation_id = ?", (session_id,))
                        s_row = s_cur.fetchone()
                        if s_row:
                            if s_row[0]:
                                proj_hint = s_row[0]
                            if s_row[1]:
                                try:
                                    ws_list = json.loads(s_row[1])
                                    if ws_list and isinstance(ws_list, list):
                                        ws_hint = str(ws_list[0])
                                except (json.JSONDecodeError, TypeError) as parse_err:
                                    logger.debug("Failed to parse workspace_uris hint: %s", parse_err)
                except sqlite3.Error as s_err:
                    logger.debug("Failed to read summaries DB for hint: %s", s_err)

            synth_blob_bytes = synthesize_trajectory_metadata_blob(
                ws_uri=ws_hint,
                project_id=proj_hint,
                conversation_id=session_id,
                created_at_sec=int(time.time()),
            )
            salvaged_blob = [("main", synth_blob_bytes)]
            diagnostics.append(f"Synthesized minimal trajectory_metadata_blob (project_id={proj_hint})")

        # Determine root trajectory ID
        root_trajectory_id: str | None = None
        if salvaged_meta and salvaged_meta[0][0] and salvaged_meta[0][0] != session_id:
            root_trajectory_id = str(salvaged_meta[0][0])
        elif bak_file.exists():
            try:
                with sqlite3.connect(f"file:{bak_file.as_posix()}?mode=ro", uri=True) as b_conn:
                    b_cur = b_conn.cursor()
                    b_cur.execute("SELECT trajectory_id FROM trajectory_meta LIMIT 1")
                    b_row = b_cur.fetchone()
                    if b_row and b_row[0] and b_row[0] != session_id:
                        root_trajectory_id = str(b_row[0])
            except (sqlite3.Error, OSError):
                pass

        if not root_trajectory_id and self.summaries_db.exists():
            # Check conversation_summaries.db raw_summary Field 4
            try:
                with sqlite3.connect(f"file:{self.summaries_db.as_posix()}?mode=ro", uri=True) as s_conn:
                    s_cur = s_conn.cursor()
                    s_cur.execute("SELECT raw_summary FROM conversation_summaries WHERE conversation_id = ?", (session_id,))
                    s_r = s_cur.fetchone()
                    if s_r and s_r[0]:
                        f_map = parse_proto_fields(s_r[0])
                        if f_map.get(4):
                            cand_root = f_map[4][0][1].decode("ascii", errors="ignore")
                            if cand_root and cand_root != session_id:
                                root_trajectory_id = cand_root
            except (sqlite3.Error, OSError):
                pass

        if not root_trajectory_id:
            root_trajectory_id = str(uuid.uuid4())

        # 3. Build pristine database in scratch directory
        scratch = Path(scratch_dir) if scratch_dir else Path(r"B:\WALmaster\.development\artifacts\scratch")
        scratch.mkdir(parents=True, exist_ok=True)
        healed_tmp = scratch / f"healed_{session_id}_{int(time.time() * 1000)}.db"

        if healed_tmp.exists():
            healed_tmp.unlink()

        synthesized_count = 0
        dst = sqlite3.connect(healed_tmp)
        try:
            cur = dst.cursor()
            for ddl in CANONICAL_SCHEMAS.values():
                cur.execute(ddl)

            # Metadata tables
            if salvaged_meta and salvaged_meta[0][0] != session_id:
                cur.executemany("INSERT INTO trajectory_meta VALUES (?, ?, ?, ?)", salvaged_meta)
            else:
                cur.execute("INSERT INTO trajectory_meta VALUES (?, ?, 4, 1)", (root_trajectory_id, session_id))

            if salvaged_blob:
                cur.executemany("INSERT INTO trajectory_metadata_blob VALUES (?, ?)", salvaged_blob)

            for gen_row in salvaged_gen.values():
                cur.execute("INSERT INTO gen_metadata VALUES (?, ?, ?)", gen_row)

            for exec_row in salvaged_exec.values():
                cur.execute("INSERT INTO executor_metadata VALUES (?, ?)", exec_row)

            # Synthesize continuous, sequential steps 0..total_steps-1
            healed_rows: list[tuple[Any, ...]] = []
            for idx in range(total_steps):
                if idx in salvaged_steps:
                    row = salvaged_steps[idx]
                    if row[4] is None:
                        # Backfill missing step metadata
                        st = jsonl_steps[idx] if idx < len(jsonl_steps) else {}
                        meta_bytes = synthesize_step_metadata(
                            created_at_iso=st.get("created_at", ""),
                            root_trajectory_id=root_trajectory_id,
                            step_idx=idx,
                            turn_idx=idx // 2,
                            conversation_id=session_id,
                        )
                        row = (row[0], row[1], row[2], row[3], meta_bytes, row[5], row[6], row[7], row[8], row[9], row[10])
                    healed_rows.append(row)
                else:
                    st = jsonl_steps[idx]
                    st_type = STEP_TYPE_MAP.get(st.get("type", "GENERIC"), 132)
                    st_status = 3 if st.get("status") == "DONE" else 2
                    payload_json = json.dumps(st).encode("utf-8")
                    meta_bytes = synthesize_step_metadata(
                        created_at_iso=st.get("created_at", ""),
                        root_trajectory_id=root_trajectory_id,
                        step_idx=idx,
                        turn_idx=idx // 2,
                        conversation_id=session_id,
                    )
                    healed_rows.append((
                        idx, st_type, st_status, 0, meta_bytes, None, None, None, None, payload_json, 0
                    ))
                    synthesized_count += 1

            cur.executemany(
                "INSERT INTO steps (idx, step_type, status, has_subtrajectory, metadata, error_details, permissions, task_details, render_info, step_payload, step_format) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                healed_rows,
            )

            # Re-create canonical indexes
            cur.execute("CREATE INDEX IF NOT EXISTS `idx_steps_status` ON `steps`(`status`)")
            cur.execute("CREATE INDEX IF NOT EXISTS `idx_steps_step_type` ON `steps`(`step_type`)")
            dst.commit()

            cur.execute("PRAGMA integrity_check")
            check_rows = cur.fetchall()
            integrity_ok = len(check_rows) == 1 and check_rows[0][0].lower() == "ok"
            if not integrity_ok:
                diagnostics.append(f"Healed database integrity warning: {check_rows}")
        finally:
            dst.close()
            gc.collect()

        # 4. Backup original corrupted database
        backup_path: Path | None = None
        if backup and target_db.exists():
            backup_path = target_db.with_suffix(".db.corrupt.bak")
            should_backup = True
            if backup_path.exists() and backup_path.stat().st_size > 0:
                try:
                    with sqlite3.connect(f"file:{backup_path.as_posix()}?mode=ro", uri=True) as b_conn:
                        b_cur = b_conn.cursor()
                        b_cur.execute("SELECT count(*) FROM trajectory_metadata_blob WHERE length(data) > 0")
                        has_blob_cnt = b_cur.fetchone()[0]
                        if has_blob_cnt > 0:
                            # Existing backup already preserves intact metadata blob
                            should_backup = False
                            diagnostics.append(f"Preserved existing backup with valid metadata: {backup_path.name}")
                except (sqlite3.Error, OSError) as bak_chk_err:
                    logger.debug("Could not verify existing backup metadata: %s", bak_chk_err)
            if should_backup:
                try:
                    shutil.copy2(target_db, backup_path)
                    diagnostics.append(f"Backup created at: {backup_path}")
                except OSError as b_err:
                    diagnostics.append(f"Failed to create backup: {b_err}")

        # 5. Purge sync conflict files
        purged_conflicts: list[str] = []
        if target_db.parent.exists():
            for item in target_db.parent.iterdir():
                if item.is_file() and item.name.startswith(session_id):
                    if item in (target_db, backup_path):
                        continue
                    if " (1)" in item.name or " (2)" in item.name or item.name.endswith(".conflict") or item.name.endswith(".tmp"):
                        try:
                            item.unlink(missing_ok=True)
                            purged_conflicts.append(item.name)
                        except OSError as p_err:
                            diagnostics.append(f"Could not purge conflict {item.name}: {p_err}")

        # 6. Apply live if requested
        if live_apply and integrity_ok:
            try:
                # Force garbage collection to release any dangling local handles
                gc.collect()
                time.sleep(0.05)

                # Remove active WAL and SHM so SQLite starts completely fresh
                wal_file = target_db.parent / f"{target_db.name}-wal"
                shm_file = target_db.parent / f"{target_db.name}-shm"

                for transient in (wal_file, shm_file):
                    if transient.exists():
                        for attempt in range(6):
                            try:
                                transient.unlink(missing_ok=True)
                                break
                            except OSError:
                                gc.collect()
                                time.sleep(0.05 * (attempt + 1))

                shutil.copy2(healed_tmp, target_db)
                diagnostics.append(f"Live database successfully replaced: {target_db}")
            except OSError as live_err:
                diagnostics.append(f"Live replacement failed: {live_err}")
                return HealResult(
                    session_id=session_id,
                    success=False,
                    total_steps=total_steps,
                    salvaged_steps=len(salvaged_steps),
                    synthesized_steps=synthesized_count,
                    integrity_ok=integrity_ok,
                    presence_lock_reaped=False,
                    summaries_upserted=False,
                    purged_conflicts=purged_conflicts,
                    backup_path=backup_path,
                    diagnostics=diagnostics,
                )
            finally:
                healed_tmp.unlink(missing_ok=True)

        # 7. Reap stale presence lock
        presence_lock_reaped = False
        if lock_file.exists():
            try:
                lock_file.unlink(missing_ok=True)
                presence_lock_reaped = True
                diagnostics.append(f"Reaped stale presence lock: {lock_file.name}")
            except OSError as l_err:
                diagnostics.append(f"Could not unlink presence lock: {l_err}")

        # 8. Upsert into conversation_summaries.db and align Tier 3 Proto Registry
        summaries_upserted = False
        if self.summaries_db.exists():
            try:
                now_dt = datetime.datetime.now(datetime.timezone.utc)
                now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S") + f".{now_dt.microsecond:07d}+00:00"
                proj_id: str | None = None
                ws_uri: str | None = None
                if salvaged_blob and salvaged_blob[0][1]:
                    proj_id, ws_uri = self._extract_metadata_from_blob(salvaged_blob[0][1])

                if not proj_id or not ws_uri:
                    from core.summary_sync import resolve_conversation_workspace
                    res_ws, res_pid, _ = resolve_conversation_workspace(
                        session_id,
                        conv_dir=self.conversations_dir,
                        brain_dir=self.brain_dir,
                    )
                    proj_id = proj_id or res_pid
                    ws_uri = ws_uri or res_ws

                # Ensure canonical %3A workspace URI
                if ws_uri and ":" in ws_uri and "%3A" not in ws_uri:
                    ws_uri = ws_uri.replace(":", "%3A", 1)

                ws_uris_json = json.dumps([ws_uri]) if ws_uri else json.dumps(["file:///b%3A/CRSDL-MACCRE"])
                patched_raw_blob = patch_raw_summary(
                    b"",
                    ws_uri or "file:///b%3A/CRSDL-MACCRE",
                    int(now_dt.timestamp()),
                    metadata_blob=salvaged_blob[0][1] if salvaged_blob else None,
                    session_id=session_id,
                    title=title,
                    step_count=total_steps,
                    root_trajectory_id=root_trajectory_id or session_id,
                    project_id=proj_id,
                    created_at_sec=int(now_dt.timestamp()),
                )
                with sqlite3.connect(self.summaries_db, timeout=10.0) as s_conn:
                    self._upsert_summary(
                        s_conn,
                        session_id,
                        title,
                        total_steps,
                        now_str,
                        project_id=proj_id,
                        workspace_uris=ws_uris_json,
                        raw_summary=patched_raw_blob,
                    )
                    s_conn.commit()
                    summaries_upserted = True
                    diagnostics.append(f"Upserted conversation_summaries.db (step_count={total_steps}, title='{title}', project_id='{proj_id}')")

                # Align Tier-3 Protobuf Registry
                from core.summary_sync import sync_conversation_metadata
                sync_res = sync_conversation_metadata(
                    session_id,
                    summaries_db=self.summaries_db,
                    agyhub_proto=self.agyhub_proto,
                    conv_dir=self.conversations_dir,
                    brain_dir=self.brain_dir,
                )
                diagnostics.append(
                    f"Tier-3 Sync aligned (project_id={sync_res.get('project_id')}, "
                    f"tier1={sync_res.get('tier1_updated')}, "
                    f"tier2={sync_res.get('tier2_updated')}, "
                    f"tier3={sync_res.get('tier3_updated')})"
                )
            except (sqlite3.Error, OSError) as sum_err:
                diagnostics.append(f"Failed to upsert conversation_summaries.db or sync Tier-3: {sum_err}")

        return HealResult(
            session_id=session_id,
            success=integrity_ok,
            total_steps=total_steps,
            salvaged_steps=len(salvaged_steps),
            synthesized_steps=synthesized_count,
            integrity_ok=integrity_ok,
            presence_lock_reaped=presence_lock_reaped,
            summaries_upserted=summaries_upserted,
            purged_conflicts=purged_conflicts,
            backup_path=backup_path,
            diagnostics=diagnostics,
        )
