"""Tests for WALmaster to CRSDL-MACCRE Bridge Module."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from core.bridge import (
    heal_and_restore_for_maccre,
    patch_maccre_recovery,
    scan_and_heal_all_corrupted,
)


def test_bridge_heals_session_and_formats_dict(tmp_path: Path):
    conv_dir = tmp_path / "conversations"
    brain_dir = tmp_path / "brain"
    presence_dir = tmp_path / "presence"
    summaries_db = tmp_path / "conversation_summaries.db"
    scratch_dir = tmp_path / "scratch"

    conv_dir.mkdir(parents=True)
    brain_dir.mkdir(parents=True)
    presence_dir.mkdir(parents=True)
    scratch_dir.mkdir(parents=True)

    session_id = "test-bridge-session-001"

    # Setup mock brain transcript
    log_dir = brain_dir / session_id / ".system_generated" / "logs"
    log_dir.mkdir(parents=True)
    transcript = log_dir / "transcript.jsonl"
    with open(transcript, "w", encoding="utf-8") as f:
        f.write(json.dumps({"step_index": 0, "type": "USER_INPUT", "content": "Hello"}) + "\n")
        f.write(json.dumps({"step_index": 1, "type": "PLANNER_RESPONSE", "content": "World"}) + "\n")

    # Setup presence lock
    lock = presence_dir / f"{session_id}.lock"
    lock.write_text("dummy", encoding="utf-8")

    # Setup summaries table
    with sqlite3.connect(summaries_db) as conn:
        conn.execute(
            """
            CREATE TABLE `conversation_summaries` (
                `conversation_id` text PRIMARY KEY,
                `title` text,
                `preview` text,
                `step_count` integer NOT NULL DEFAULT 0,
                `last_modified_time` text,
                `workspace_uris` text NOT NULL DEFAULT '[]',
                `status` text NOT NULL DEFAULT 'CASCADE_RUN_STATUS_IDLE'
            )
            """
        )

    res = heal_and_restore_for_maccre(
        session_id,
        conversations_dir=conv_dir,
        brain_dir=brain_dir,
        presence_dir=presence_dir,
        summaries_db=summaries_db,
        live_apply=True,
        backup=True,
        scratch_dir=scratch_dir,
    )

    assert isinstance(res, dict)
    assert res["session_id"] == session_id
    assert res["success"] is True
    assert res["integrity_ok"] is True
    assert res["total_steps"] == 2
    assert res["presence_lock_reaped"] is True
    assert res["summaries_upserted"] is True
    assert (conv_dir / f"{session_id}.db").exists()


def test_bridge_scan_and_heal_all_corrupted(tmp_path: Path):
    conv_dir = tmp_path / "conversations"
    brain_dir = tmp_path / "brain"
    presence_dir = tmp_path / "presence"
    summaries_db = tmp_path / "conversation_summaries.db"
    scratch_dir = tmp_path / "scratch"

    conv_dir.mkdir(parents=True)
    brain_dir.mkdir(parents=True)
    presence_dir.mkdir(parents=True)
    scratch_dir.mkdir(parents=True)

    # 1 valid session
    val_id = "session-valid"
    with sqlite3.connect(conv_dir / f"{val_id}.db") as conn:
        conn.execute("CREATE TABLE t (id int)")
        conn.execute("INSERT INTO t VALUES (1)")

    # 1 corrupt session
    corrupt_id = "session-corrupt"
    corrupt_file = conv_dir / f"{corrupt_id}.db"
    corrupt_file.write_bytes(b"NOT AN SQLITE FILE INVALID TRUNCATED HEADER")

    # Brain transcript for corrupt session
    log_dir = brain_dir / corrupt_id / ".system_generated" / "logs"
    log_dir.mkdir(parents=True)
    with open(log_dir / "transcript.jsonl", "w", encoding="utf-8") as f:
        f.write(json.dumps({"step_index": 0, "type": "USER_INPUT", "content": "Fix me"}) + "\n")

    # Setup summaries table
    with sqlite3.connect(summaries_db) as conn:
        conn.execute(
            """
            CREATE TABLE `conversation_summaries` (
                `conversation_id` text PRIMARY KEY,
                `title` text,
                `preview` text,
                `step_count` integer NOT NULL DEFAULT 0,
                `last_modified_time` text,
                `workspace_uris` text NOT NULL DEFAULT '[]',
                `status` text NOT NULL DEFAULT 'CASCADE_RUN_STATUS_IDLE'
            )
            """
        )

    healed_list = scan_and_heal_all_corrupted(
        conversations_dir=conv_dir,
        brain_dir=brain_dir,
        presence_dir=presence_dir,
        summaries_db=summaries_db,
        live_apply=True,
        backup=True,
        scratch_dir=scratch_dir,
    )

    assert len(healed_list) == 1
    assert healed_list[0]["session_id"] == corrupt_id
    assert healed_list[0]["success"] is True


def test_bridge_patch_maccre_recovery():
    # Calling patch_maccre_recovery should run gracefully regardless of whether maccre_core is installed
    result = patch_maccre_recovery()
    assert isinstance(result, bool)
