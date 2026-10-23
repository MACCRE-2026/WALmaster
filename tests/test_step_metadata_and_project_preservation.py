"""Unit and integration tests for step metadata synthesis and project folder preservation."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from core.healer import (
    SovereignSessionHealer,
    synthesize_step_metadata,
    synthesize_trajectory_metadata_blob,
)
from core.summary_sync import parse_proto_fields


def test_synthesize_step_metadata_has_valid_protobuf_tags() -> None:
    """Ensure synthesized step metadata contains Tag 1 (Timestamp) and Tag 20 (Binding)."""
    created_at_iso = "2026-10-22T14:30:00.123456Z"
    root_traj_id = "240197e6-4aeb-4a8f-9ce8-90ea18d05f82"
    conv_id = "55b5eb19-b6c0-49fd-8958-84d0b52c7987"
    step_idx = 42
    turn_idx = 10

    blob = synthesize_step_metadata(
        created_at_iso=created_at_iso,
        root_trajectory_id=root_traj_id,
        step_idx=step_idx,
        turn_idx=turn_idx,
        conversation_id=conv_id,
    )

    assert isinstance(blob, bytes)
    assert len(blob) > 0

    fields = parse_proto_fields(blob)
    # Tag 1: Timestamp submessage
    assert 1 in fields
    ts_bytes = fields[1][0][1]
    ts_fields = parse_proto_fields(ts_bytes)
    assert 1 in ts_fields  # seconds varint
    assert ts_fields[1][0][1] > 0

    # Tag 20: TrajectoryBinding submessage
    assert 20 in fields
    binding_bytes = fields[20][0][1]
    binding_fields = parse_proto_fields(binding_bytes)
    assert 1 in binding_fields  # trajectory_id
    assert binding_fields[1][0][1] == root_traj_id.encode("ascii")
    assert 2 in binding_fields  # step_idx
    assert binding_fields[2][0][1] == step_idx
    assert 4 in binding_fields  # conversation_id
    assert binding_fields[4][0][1] == conv_id.encode("ascii")


def test_synthesize_trajectory_metadata_blob_preserves_project_id_tag_18() -> None:
    """Ensure synthesized metadata blob contains valid Tag 18 project_id without varint corruption."""
    ws_uri = "file:///b:/WALmaster"
    project_id = "3de8a35e-8d05-4f91-bd4c-0a785b017993"
    conv_id = "55b5eb19-b6c0-49fd-8958-84d0b52c7987"
    created_at_sec = 1761144000

    blob = synthesize_trajectory_metadata_blob(
        ws_uri=ws_uri,
        project_id=project_id,
        conversation_id=conv_id,
        created_at_sec=created_at_sec,
    )

    assert isinstance(blob, bytes)
    fields = parse_proto_fields(blob)

    # Tag 1: CortexWorkspaceMetadata
    assert 1 in fields
    # Tag 18: project_id (must not be swallowed by varint continuation)
    assert 18 in fields
    assert fields[18][0][1] == project_id.encode("ascii")


def test_healer_synthesizes_step_metadata_and_retains_project_binding(tmp_path: Path) -> None:
    """Integration test verifying healer populates steps.metadata and trajectory_meta hierarchy."""
    conv_dir = tmp_path / "conversations"
    brain_dir = tmp_path / "brain"
    presence_dir = tmp_path / "presence"
    summaries_db = tmp_path / "conversation_summaries.db"
    agyhub_proto = tmp_path / "agyhub_summaries_proto.pb"

    conv_dir.mkdir(parents=True)
    brain_dir.mkdir(parents=True)
    presence_dir.mkdir(parents=True)
    agyhub_proto.write_bytes(b"")

    session_id = "test-session-uuid-1234"
    proj_id = "3de8a35e-8d05-4f91-bd4c-0a785b017993"
    ws_uri = "file:///b:/WALmaster"

    # Setup summaries DB
    with sqlite3.connect(summaries_db) as conn:
        conn.execute("""
            CREATE TABLE conversation_summaries (
                conversation_id text PRIMARY KEY,
                title text NOT NULL DEFAULT '',
                preview text NOT NULL DEFAULT '',
                step_count integer NOT NULL DEFAULT 0,
                last_modified_time datetime NOT NULL,
                workspace_uris text NOT NULL,
                status text NOT NULL DEFAULT '',
                source text NOT NULL DEFAULT '',
                project_id text NOT NULL DEFAULT '',
                agent_name text NOT NULL DEFAULT '',
                parent_conversation_id text NOT NULL DEFAULT '',
                nesting_depth integer NOT NULL DEFAULT 0,
                battle_id text NOT NULL DEFAULT '',
                winning_conversation_id text NOT NULL DEFAULT '',
                not_fully_idle numeric NOT NULL DEFAULT false,
                killed numeric NOT NULL DEFAULT false,
                last_user_input_time datetime NOT NULL,
                last_user_input_step_index integer NOT NULL DEFAULT -1,
                app_data_dir text NOT NULL DEFAULT '',
                raw_summary blob,
                group_id text NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "INSERT INTO conversation_summaries (conversation_id, title, project_id, workspace_uris, last_modified_time, last_user_input_time) VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))",
            (session_id, "Test Healing", proj_id, json.dumps([ws_uri])),
        )

    # Setup transcript with 3 steps
    log_dir = brain_dir / session_id / ".system_generated" / "logs"
    log_dir.mkdir(parents=True)
    transcript_file = log_dir / "transcript.jsonl"
    with open(transcript_file, "w", encoding="utf-8") as f:
        f.write(json.dumps({"step_index": 0, "type": "USER_INPUT", "content": "Hello", "created_at": "2026-10-22T10:00:00Z"}) + "\n")
        f.write(json.dumps({"step_index": 1, "type": "PLANNER_RESPONSE", "content": "Hi", "created_at": "2026-10-22T10:00:05Z"}) + "\n")
        f.write(json.dumps({"step_index": 2, "type": "USER_INPUT", "content": "Do work", "created_at": "2026-10-22T10:00:10Z"}) + "\n")

    healer = SovereignSessionHealer(
        conversations_dir=conv_dir,
        brain_dir=brain_dir,
        presence_dir=presence_dir,
        summaries_db=summaries_db,
        agyhub_proto=agyhub_proto,
    )

    res = healer.heal_session(session_id, live_apply=True, backup=False)
    assert res.success is True
    assert res.total_steps == 3

    # Check live healed DB
    healed_db_path = conv_dir / f"{session_id}.db"
    assert healed_db_path.exists()

    with sqlite3.connect(healed_db_path) as conn:
        # 1. Verify steps.metadata is NOT NULL and contains valid Timestamp
        cur = conn.cursor()
        cur.execute("SELECT idx, metadata FROM steps ORDER BY idx ASC")
        rows = cur.fetchall()
        assert len(rows) == 3
        for idx, meta in rows:
            assert meta is not None
            assert len(meta) > 0
            fields = parse_proto_fields(meta)
            assert 1 in fields  # Timestamp present

        # 2. Verify trajectory_metadata_blob has Tag 18 with project_id
        cur.execute("SELECT data FROM trajectory_metadata_blob WHERE id = 'main'")
        blob_row = cur.fetchone()
        assert blob_row is not None
        blob_fields = parse_proto_fields(blob_row[0])
        assert 18 in blob_fields
        assert blob_fields[18][0][1] == proj_id.encode("ascii")

        # 3. Verify trajectory_meta hierarchy
        cur.execute("SELECT trajectory_id, cascade_id, trajectory_type, source FROM trajectory_meta")
        t_meta = cur.fetchone()
        assert t_meta is not None
        assert t_meta[1] == session_id
        assert t_meta[2] == 4
        assert t_meta[3] == 1

    # Check conversation_summaries.db has raw_summary populated
    with sqlite3.connect(summaries_db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT project_id, length(raw_summary) FROM conversation_summaries WHERE conversation_id = ?", (session_id,))
        s_row = cur.fetchone()
        assert s_row is not None
        assert s_row[0] == proj_id
        assert s_row[1] is not None and s_row[1] > 0
