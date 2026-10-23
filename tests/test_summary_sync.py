"""Tests for WALmaster Tier-3 Protobuf & Relational Registry Synchronization."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from core.summary_sync import (
    deserialize_agyhub_proto,
    make_proto_tag,
    make_proto_varint,
    make_timestamp_proto,
    make_workspace_proto,
    parse_proto_fields,
    patch_raw_summary,
    serialize_agyhub_proto,
    serialize_proto_fields,
    sync_conversation_metadata,
)


def test_proto_varint_and_tag_encoding() -> None:
    # Single byte varint
    assert make_proto_varint(1) == b"\x01"
    assert make_proto_varint(127) == b"\x7f"
    # Multi-byte varint
    assert make_proto_varint(300) == b"\xac\x02"

    # Field 1, wire type 2 (length-delimited): (1 << 3) | 2 = 10 (0x0a)
    assert make_proto_tag(1, 2) == b"\n"
    # Field 2, wire type 2: (2 << 3) | 2 = 18 (0x12)
    assert make_proto_tag(2, 2) == b"\x12"
    # Field 3, wire type 0 (varint): (3 << 3) | 0 = 24 (0x18)
    assert make_proto_tag(3, 0) == b"\x18"


def test_timestamp_and_workspace_proto_creation() -> None:
    ts_bytes = make_timestamp_proto(1700000000, nanos=12345)
    parsed_ts = parse_proto_fields(ts_bytes)
    assert 1 in parsed_ts
    assert parsed_ts[1][0] == ("varint", 1700000000)
    assert 2 in parsed_ts
    assert parsed_ts[2][0] == ("varint", 12345)

    ws_bytes = make_workspace_proto("file:///b:/WALmaster")
    parsed_ws = parse_proto_fields(ws_bytes)
    assert 1 in parsed_ws
    assert parsed_ws[1][0] == ("bytes", b"file:///b:/WALmaster")
    assert 3 in parsed_ws
    assert parsed_ws[3][0] == ("bytes", b"")


def test_patch_raw_summary_replaces_corrupt_timestamps_and_adds_workspace() -> None:
    # Construct a degraded raw_summary with corrupt negative year 0001 varint
    # and no Field 9
    bad_marker = b"\x80\x92\xb8\xc3\x98\xfe\xff\xff\xff\x01"
    degraded_fields: dict[int, list[tuple[str, bytes]]] = {
        1: [("bytes", b"Test Degraded Conversation")],
        2: [("varint", 42)],
        3: [("bytes", b"\x08" + bad_marker)],
        10: [("bytes", b"\x08" + bad_marker)],
    }
    degraded_raw = serialize_proto_fields(degraded_fields)

    patched = patch_raw_summary(degraded_raw, "file:///b:/WALmaster", 1729600000)
    pf = parse_proto_fields(patched)

    # Field 1 (Title) preserved
    assert pf[1][0][1] == b"Test Degraded Conversation"
    # Field 2 (Step count) preserved
    assert pf[2][0][1] == 42
    # Field 9 (Workspace) added
    assert 9 in pf
    assert b"file:///b:/WALmaster" in pf[9][0][1]
    # Field 3 and 10 patched with valid timestamp, bad marker eliminated
    assert bad_marker not in pf[3][0][1]
    assert bad_marker not in pf[10][0][1]


def test_agyhub_proto_serialization_roundtrip(tmp_path: Path) -> None:
    entries = [
        ("cid-1111", b"\n\x04test\x12\x05hello"),
        ("cid-2222", b"\n\x06second\x12\x05world"),
    ]
    proto_bytes = serialize_agyhub_proto(entries)
    deserialized = deserialize_agyhub_proto(proto_bytes)

    assert len(deserialized) == 2
    assert deserialized[0] == ("cid-1111", b"\n\x04test\x12\x05hello")
    assert deserialized[1] == ("cid-2222", b"\n\x06second\x12\x05world")


def test_sync_conversation_metadata_end_to_end(tmp_path: Path) -> None:
    cid = "55b5eb19-b6c0-49fd-8958-84d0b52c7987"
    conv_dir = tmp_path / "conversations"
    conv_dir.mkdir()
    brain_dir = tmp_path / "brain"
    brain_dir.mkdir()
    sum_db = tmp_path / "conversation_summaries.db"
    proto_p = tmp_path / "agyhub_summaries_proto.pb"

    # Setup Tier 1: .db and .bak
    live_db = conv_dir / f"{cid}.db"
    bak_db = conv_dir / f"{cid}.db.corrupt.bak"

    with sqlite3.connect(live_db) as conn:
        conn.execute("CREATE TABLE trajectory_metadata_blob (id TEXT PRIMARY KEY, data BLOB)")
        conn.execute("INSERT INTO trajectory_metadata_blob VALUES ('main', X'')")

    with sqlite3.connect(bak_db) as conn:
        conn.execute("CREATE TABLE trajectory_metadata_blob (id TEXT PRIMARY KEY, data BLOB)")
        sample_bak_blob = b"\n\x18\n\x14file:///b:/WALmaster\x1a\x00\x12\x0b\x08\xbf\xd3\xe0\xd6\x06\x10\xe0\xfd\x8e\x0e"
        conn.execute("INSERT INTO trajectory_metadata_blob VALUES ('main', ?)", (sample_bak_blob,))

    # Setup Tier 2: summaries.db and conversations_fts
    with sqlite3.connect(sum_db) as conn:
        conn.execute(
            """
            CREATE TABLE conversation_summaries (
                conversation_id TEXT PRIMARY KEY,
                title TEXT,
                step_count INTEGER,
                last_modified_time TEXT,
                workspace_uris TEXT,
                project_id TEXT,
                raw_summary BLOB
            )
            """
        )
        conn.execute(
            """
            INSERT INTO conversation_summaries VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (cid, "Test Title", 713, "0001-01-01 00:00:00+00:00", "[]", "", b"\n\nTest Title"),
        )
        conn.execute(
            """
            CREATE TABLE conversations_fts (
                conversation_id TEXT,
                chunk_index INTEGER,
                workspace TEXT,
                project TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO conversations_fts VALUES (?, -1, '', '')
            """,
            (cid,),
        )

    # Setup Tier 3: proto
    proto_data = serialize_agyhub_proto([(cid, b"\n\nTest Title")], mod_times={cid: 1700000000})
    proto_p.write_bytes(proto_data)

    # Execute sync
    res = sync_conversation_metadata(
        cid,
        summaries_db=sum_db,
        agyhub_proto=proto_p,
        conv_dir=conv_dir,
        brain_dir=brain_dir,
    )

    assert res["session_id"] == cid
    assert res["project_id"] == "3de8a35e-8d05-4f91-bd4c-0a785b017993"
    assert res["tier1_updated"] is True
    assert res["tier2_updated"] is True
    assert res["tier3_updated"] is True

    # Verify Tier 1 live DB restored from bak
    with sqlite3.connect(live_db) as conn:
        live_blob = conn.execute("SELECT data FROM trajectory_metadata_blob WHERE id='main'").fetchone()[0]
        assert len(live_blob) > 0
        assert b"file:///b:/WALmaster" in live_blob

    # Verify Tier 2 summaries DB updated
    with sqlite3.connect(sum_db) as conn:
        row = conn.execute("SELECT project_id, workspace_uris, last_modified_time, raw_summary FROM conversation_summaries WHERE conversation_id=?", (cid,)).fetchone()
        assert row[0] == "3de8a35e-8d05-4f91-bd4c-0a785b017993"
        assert "file:///b%3A/WALmaster" in row[1]
        assert "0001-01-01" not in row[2]
        assert b"file:///b:/WALmaster" in row[3]

        # Verify conversations_fts was updated
        fts_row = conn.execute("SELECT workspace, project FROM conversations_fts WHERE chunk_index = -1 AND conversation_id=?", (cid,)).fetchone()
        assert fts_row[0] == "WALmaster"
        assert fts_row[1] == "WALmaster"

    # Verify Tier 3 proto updated
    entries = deserialize_agyhub_proto(proto_p.read_bytes())
    assert len(entries) == 1
    assert entries[0][0] == cid
    assert b"file:///b:/WALmaster" in entries[0][1]
    pf_entry = parse_proto_fields(entries[0][1])
    assert 17 in pf_entry
    f17_sub = parse_proto_fields(pf_entry[17][0][1])
    assert 18 in f17_sub
    assert f17_sub[18][0][1] == b"3de8a35e-8d05-4f91-bd4c-0a785b017993"
    assert 7 in f17_sub
    assert f17_sub[7][0][1] == b"file:///b%3A/WALmaster"
