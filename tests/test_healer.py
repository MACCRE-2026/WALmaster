"""Tests for WALmaster SovereignSessionHealer."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from core.healer import SovereignSessionHealer


@pytest.fixture
def healer_environment(tmp_path: Path):
    conv_dir = tmp_path / "conversations"
    brain_dir = tmp_path / "brain"
    presence_dir = tmp_path / "presence"
    summaries_db = tmp_path / "conversation_summaries.db"

    conv_dir.mkdir(parents=True)
    brain_dir.mkdir(parents=True)
    presence_dir.mkdir(parents=True)

    # Initialize empty summaries database
    with sqlite3.connect(summaries_db) as conn:
        conn.execute(
            """
            CREATE TABLE `conversation_summaries` (
                `conversation_id` text PRIMARY KEY,
                `title` text,
                `preview` text,
                `step_count` integer NOT NULL DEFAULT 0,
                `last_modified_time` text,
                `last_user_input_time` text,
                `workspace_uris` text,
                `project_id` text,
                `status` integer NOT NULL DEFAULT 0
            )
            """
        )

    return {
        "conv_dir": conv_dir,
        "brain_dir": brain_dir,
        "presence_dir": presence_dir,
        "summaries_db": summaries_db,
    }


def test_healer_reconstructs_and_reaps_lock(healer_environment, tmp_path: Path):
    conv_dir = healer_environment["conv_dir"]
    brain_dir = healer_environment["brain_dir"]
    presence_dir = healer_environment["presence_dir"]
    summaries_db = healer_environment["summaries_db"]

    session_id = "test-session-uuid-1234"

    # 1. Setup mock brain with 10 steps in transcript.jsonl
    session_brain = brain_dir / session_id / ".system_generated" / "logs"
    session_brain.mkdir(parents=True)
    transcript_file = session_brain / "transcript.jsonl"

    steps_data = []
    for i in range(10):
        step_type = "USER_INPUT" if i == 0 else "PLANNER_RESPONSE"
        steps_data.append({
            "step_index": i,
            "type": step_type,
            "status": "DONE",
            "content": f"User prompt {i}" if i == 0 else f"Model response {i}",
        })

    with open(transcript_file, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(s) + "\n" for s in steps_data)

    # 2. Setup mock presence lock
    lock_file = presence_dir / f"{session_id}.lock"
    lock_file.write_bytes(b"")

    # 3. Setup mock existing corrupt database with missing steps
    corrupt_db = conv_dir / f"{session_id}.db"
    with sqlite3.connect(corrupt_db) as conn:
        conn.execute(
            "CREATE TABLE `steps` (`idx` integer,`step_type` integer,`status` integer,`has_subtrajectory` numeric,`metadata` blob,`error_details` blob,`permissions` blob,`task_details` blob,`render_info` blob,`step_payload` blob,`step_format` integer,PRIMARY KEY (`idx`))"
        )
        # Insert only step 0 and 1
        conn.execute("INSERT INTO steps VALUES (0, 14, 3, 0, NULL, NULL, NULL, NULL, NULL, ?, 0)", (b"step0",))
        conn.execute("INSERT INTO steps VALUES (1, 15, 3, 0, NULL, NULL, NULL, NULL, NULL, ?, 0)", (b"step1",))

    # 4. Instantiate healer and execute healing
    healer = SovereignSessionHealer(
        conversations_dir=conv_dir,
        brain_dir=brain_dir,
        presence_dir=presence_dir,
        summaries_db=summaries_db,
    )

    scratch_dir = tmp_path / "scratch"
    res = healer.heal_session(session_id, live_apply=True, scratch_dir=scratch_dir)

    assert res.success is True
    assert res.total_steps == 10
    assert res.salvaged_steps == 2
    assert res.synthesized_steps == 8
    assert res.integrity_ok is True
    assert res.presence_lock_reaped is True
    assert res.summaries_upserted is True
    assert not lock_file.exists()

    # 5. Verify target DB has all 10 steps and clean integrity
    with sqlite3.connect(corrupt_db) as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check")
        assert cur.fetchone()[0] == "ok"
        cur.execute("SELECT count(*) FROM steps")
        assert cur.fetchone()[0] == 10
        cur.execute("SELECT idx FROM steps ORDER BY idx")
        indices = [r[0] for r in cur.fetchall()]
        assert indices == list(range(10))

    # 6. Verify summaries DB has record
    with sqlite3.connect(summaries_db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT conversation_id, step_count, title FROM conversation_summaries WHERE conversation_id = ?", (session_id,))
        row = cur.fetchone()
        assert row is not None
        assert row[0] == session_id
        assert row[1] == 10
        assert row[2] == "User prompt 0"


def test_healer_salvages_metadata_blob_from_bak_and_preserves_project_id(healer_environment, tmp_path: Path):
    conv_dir = healer_environment["conv_dir"]
    brain_dir = healer_environment["brain_dir"]
    presence_dir = healer_environment["presence_dir"]
    summaries_db = healer_environment["summaries_db"]

    session_id = "test-session-metadata-salvage"

    # 1. Setup mock brain with transcript
    session_brain = brain_dir / session_id / ".system_generated" / "logs"
    session_brain.mkdir(parents=True)
    transcript_file = session_brain / "transcript.jsonl"
    with open(transcript_file, "w", encoding="utf-8") as f:
        f.write(json.dumps({"step_index": 0, "type": "USER_INPUT", "status": "DONE", "content": "Test prompt"}) + "\n")

    # 2. Setup mock existing corrupt database with EMPTY trajectory_metadata_blob
    corrupt_db = conv_dir / f"{session_id}.db"
    with sqlite3.connect(corrupt_db) as conn:
        conn.execute("CREATE TABLE `steps` (`idx` integer,`step_type` integer,`status` integer,`has_subtrajectory` numeric,`metadata` blob,`error_details` blob,`permissions` blob,`task_details` blob,`render_info` blob,`step_payload` blob,`step_format` integer,PRIMARY KEY (`idx`))")
        conn.execute("CREATE TABLE `trajectory_metadata_blob` (`id` text DEFAULT 'main',`data` blob,PRIMARY KEY (`id`))")
        conn.execute("INSERT INTO trajectory_metadata_blob VALUES ('main', X'')")

    # 3. Setup mock .db.corrupt.bak with INTACT trajectory_metadata_blob (containing project ID & workspace)
    bak_db = conv_dir / f"{session_id}.db.corrupt.bak"
    # Construct synthetic proto containing workspace URI and project ID in field 18
    # Field 18 tag: 0x92 0x01, len 36 ($):
    mock_project_id = "130124a4-3973-4ff8-8326-01a5c3b46475"
    mock_ws = b"file:///b:/ProjectAvocado"
    mock_blob = (
        b"\n\x1d\n\x19" + mock_ws + b"\x1a\x00\x12\x0c\x08\xb1\xde\xd5\xd6\x06\x10\xf4"
        b"\x92\x01$" + mock_project_id.encode("ascii")
    )
    with sqlite3.connect(bak_db) as conn:
        conn.execute("CREATE TABLE `trajectory_metadata_blob` (`id` text DEFAULT 'main',`data` blob,PRIMARY KEY (`id`))")
        conn.execute("INSERT INTO trajectory_metadata_blob VALUES ('main', ?)", (mock_blob,))

    # 4. Instantiate healer and heal
    healer = SovereignSessionHealer(
        conversations_dir=conv_dir,
        brain_dir=brain_dir,
        presence_dir=presence_dir,
        summaries_db=summaries_db,
    )
    scratch_dir = tmp_path / "scratch"
    res = healer.heal_session(session_id, live_apply=True, scratch_dir=scratch_dir)

    assert res.success is True

    # 5. Verify target DB has salvaged the intact blob from the .bak
    with sqlite3.connect(corrupt_db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, data FROM trajectory_metadata_blob")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "main"
        assert row[1] == mock_blob
        assert len(row[1]) == len(mock_blob)

    # 6. Verify summaries DB was updated with project_id and proper timestamp format
    with sqlite3.connect(summaries_db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT project_id, last_modified_time FROM conversation_summaries WHERE conversation_id = ?", (session_id,))
        sum_row = cur.fetchone()
        assert sum_row is not None
        assert sum_row[0] == mock_project_id
        # Timestamp must match "YYYY-MM-DD HH:MM:SS" space format, NOT "T"
        assert " " in sum_row[1]
        assert "T" not in sum_row[1]

    # 7. Verify .bak file was NOT overwritten with the 0-byte corrupt DB
    with sqlite3.connect(bak_db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT length(data) FROM trajectory_metadata_blob")
        assert cur.fetchone()[0] == len(mock_blob)

