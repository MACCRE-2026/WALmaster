"""Test full CascadeTrajectorySummary protobuf construction and multi-tier synchronization."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from core.summary_sync import (
    build_full_trajectory_summary,
    deserialize_agyhub_proto,
    make_workspace_proto,
    parse_proto_fields,
    patch_raw_summary,
    sync_conversation_metadata,
)


def test_make_workspace_proto_contains_all_four_tags() -> None:
    ws_uri = "file:///b:/WALmaster"
    data = make_workspace_proto(ws_uri)
    f = parse_proto_fields(data)
    assert 1 in f
    assert f[1][0][1] == ws_uri.encode("utf-8")
    assert 2 in f
    assert f[2][0][1] == ws_uri.encode("utf-8")
    assert 3 in f
    assert f[3][0][1] == b""
    assert 4 in f
    assert f[4][0][1] == b"main"


def test_build_full_trajectory_summary_matches_canonical_schema() -> None:
    raw = build_full_trajectory_summary(
        session_id="55b5eb19-b6c0-49fd-8958-84d0b52c7987",
        title="Multi-Agent Project Planning",
        step_count=713,
        root_trajectory_id="d6256433-dfc1-4e7a-9a07-4a4849af3e75",
        ws_uri="file:///b%3A/WALmaster",
        project_id="3de8a35e-8d05-4f91-bd4c-0a785b017993",
        created_at_sec=1729608666,
        last_modified_sec=1729612000,
        last_user_input_step_index=701,
    )

    f = parse_proto_fields(raw)
    assert sorted(f.keys()) == [1, 2, 3, 4, 5, 7, 9, 10, 15, 16, 17, 22, 28]
    assert f[1][0][1] == b"Multi-Agent Project Planning"
    assert f[2][0][1] == 713
    assert f[4][0][1] == b"d6256433-dfc1-4e7a-9a07-4a4849af3e75"
    assert f[5][0][1] == 1
    assert f[16][0][1] == 701
    assert f[22][0][1] == 4
    assert f[28][0][1] == 4

    # Check Tag 9 (workspace)
    f9 = parse_proto_fields(f[9][0][1])
    assert f9[1][0][1] == b"file:///b:/WALmaster"
    assert f9[2][0][1] == b"file:///b:/WALmaster"
    assert f9[4][0][1] == b"main"

    # Check Tag 17 (CortexTrajectoryMetadata)
    f17 = parse_proto_fields(f[17][0][1])
    assert f17[6][0][1] == b"55b5eb19-b6c0-49fd-8958-84d0b52c7987"
    assert f17[7][0][1] == b"file:///b%3A/WALmaster"
    assert f17[18][0][1] == b"3de8a35e-8d05-4f91-bd4c-0a785b017993"


def test_patch_raw_summary_synthesizes_missing_canonical_fields() -> None:
    # Given an empty or minimal raw summary
    raw = patch_raw_summary(
        b"",
        "file:///b%3A/CRSDL-MACCRE",
        1729615000,
        session_id="bdc84193-9009-4ecd-b035-5d2e3289049c",
        title="Recover Interrupted State",
        step_count=150,
        root_trajectory_id="018ba7a1-6a3a-4505-b177-88c75f350c80",
        project_id="5213403d-0b3b-425f-8d93-930e20114c4e",
    )
    f = parse_proto_fields(raw)
    assert 1 in f
    assert f[1][0][1] == b"Recover Interrupted State"
    assert 4 in f
    assert f[4][0][1] == b"018ba7a1-6a3a-4505-b177-88c75f350c80"
    assert 9 in f
    assert 17 in f
    f17 = parse_proto_fields(f[17][0][1])
    assert f17[18][0][1] == b"5213403d-0b3b-425f-8d93-930e20114c4e"


def test_sync_conversation_metadata_recovers_empty_blob_from_bak(tmp_path: Path) -> None:
    conv_dir = tmp_path / "conversations"
    conv_dir.mkdir(parents=True)
    summaries_db = tmp_path / "conversation_summaries.db"
    proto_p = tmp_path / "agyhub_summaries_proto.pb"

    cid = "98a54762-a31b-47d7-920a-4571175dffce"
    live_db = conv_dir / f"{cid}.db"
    bak_db = conv_dir / f"{cid}.db.corrupt.bak"

    # Create live_db with 0-byte blob
    with sqlite3.connect(live_db) as conn:
        conn.execute("CREATE TABLE trajectory_metadata_blob (id TEXT PRIMARY KEY, data BLOB)")
        conn.execute("CREATE TABLE trajectory_meta (trajectory_id TEXT, cascade_id TEXT, trajectory_type INTEGER, source INTEGER)")
        conn.execute("INSERT INTO trajectory_metadata_blob VALUES ('main', X'')")
        conn.execute("INSERT INTO trajectory_meta VALUES ('12ae5df8', ?, 4, 1)", (cid,))

    # Create bak_db with valid blob
    valid_blob = build_full_trajectory_summary(
        session_id=cid,
        title="Polymorphic Sci-Fi Story Engine",
        step_count=2020,
        root_trajectory_id="12ae5df8",
        ws_uri="file:///b%3A/ProjectAvocado",
        project_id="130124a4-3973-4ff8-8326-01a5c3b46475",
        created_at_sec=1729600000,
        last_modified_sec=1729610000,
    )
    with sqlite3.connect(bak_db) as conn:
        conn.execute("CREATE TABLE trajectory_metadata_blob (id TEXT PRIMARY KEY, data BLOB)")
        conn.execute("INSERT INTO trajectory_metadata_blob VALUES ('main', ?)", (valid_blob,))

    # Create empty summaries_db and proto
    with sqlite3.connect(summaries_db) as conn:
        conn.execute("""
            CREATE TABLE conversation_summaries (
                conversation_id TEXT PRIMARY KEY,
                title TEXT,
                step_count INTEGER,
                project_id TEXT,
                workspace_uris TEXT,
                last_modified_time TEXT,
                raw_summary BLOB
            )
        """)
        conn.execute(
            "INSERT INTO conversation_summaries VALUES (?, 'Polymorphic Sci-Fi', 2020, '', '', '2026-10-22 18:30:08+00:00', X'')",
            (cid,)
        )
    proto_p.write_bytes(b"")

    res = sync_conversation_metadata(
        cid,
        summaries_db=summaries_db,
        agyhub_proto=proto_p,
        conv_dir=conv_dir,
    )
    assert res["project_id"] == "130124a4-3973-4ff8-8326-01a5c3b46475"
    assert res["workspace_uri"] == "file:///b%3A/ProjectAvocado"

    # Verify live_db blob was recovered from bak_db
    with sqlite3.connect(live_db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT length(data) FROM trajectory_metadata_blob WHERE id='main'")
        assert cur.fetchone()[0] > 0

    # Verify summaries_db updated
    with sqlite3.connect(summaries_db) as conn:
        cur = conn.cursor()
        cur.execute("SELECT project_id, workspace_uris, length(raw_summary) FROM conversation_summaries WHERE conversation_id=?", (cid,))
        row = cur.fetchone()
        assert row[0] == "130124a4-3973-4ff8-8326-01a5c3b46475"
        assert json.loads(row[1]) == ["file:///b%3A/ProjectAvocado"]
        assert row[2] > 0

    # Verify proto updated
    entries = dict(deserialize_agyhub_proto(proto_p.read_bytes()))
    assert cid in entries
    raw_proto = entries[cid]
    f = parse_proto_fields(raw_proto)
    assert 9 in f
    assert 17 in f
    f17 = parse_proto_fields(f[17][0][1])
    assert f17[18][0][1] == b"130124a4-3973-4ff8-8326-01a5c3b46475"
