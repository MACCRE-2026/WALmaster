"""Tests for Event-Sourced Projection Engine & Snapshot Baselines.

Verifies:
1. Streaming JSONL Event Projection (steps, trajectory_meta, gen_metadata, PRAGMA integrity_check).
2. Snapshot baseline management and accelerated delta replay.
3. Malformed JSON lines and corrupted payload cordoning into quarantine_diffs.jsonl with clean fallback steps.
4. Deterministic sequential rowids and index alignment.
"""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path

import pytest

from core.reprojection import (
    EpistemicOpinion,
    ProjectionStats,
    ReprojectionEngine,
    SnapshotManager,
    create_snapshot_baseline,
    reproject_from_jsonl,
)


@pytest.fixture
def reprojection_sandbox(test_artifacts_dir: Path) -> Path:
    """Create an isolated sandbox directory for reprojection tests."""
    sandbox: Path = test_artifacts_dir / f"reproj_sandbox_{int(time.time() * 1000)}"
    sandbox.mkdir(parents=True, exist_ok=True)
    return sandbox


def test_epistemic_opinion_structure() -> None:
    """Verify EpistemicOpinion creation and dictionary export."""
    op = EpistemicOpinion(b=0.8, d=0.1, u=0.1, a=0.5)
    assert op.as_dict() == {"b": 0.8, "d": 0.1, "u": 0.1, "a": 0.5}


def test_streaming_projection_from_jsonl(reprojection_sandbox: Path) -> None:
    """Verify clean streaming projection from transcript.jsonl into SQLite."""
    transcript_path: Path = reprojection_sandbox / "transcript.jsonl"
    target_db_path: Path = reprojection_sandbox / "conversation.db"

    # Write events to transcript.jsonl
    events: list[dict[str, object]] = [
        {
            "event_type": "trajectory_meta",
            "conversation_id": "conv_alpha",
            "title": "Alpha Session",
            "created_at": 1700000000.0,
        },
        {
            "event_type": "step",
            "epoch": 1,
            "lamport": 10,
            "conversation_id": "conv_alpha",
            "step_index": 1,
            "step_type": "USER_INPUT",
            "status": "DONE",
            "step_payload": "Hello agent",
            "created_at": 1700000001.0,
        },
        {
            "event_type": "gen_metadata",
            "conversation_id": "conv_alpha",
            "step_id": 1,
            "model": "claude-3-5-sonnet",
            "prompt_tokens": 12,
            "completion_tokens": 8,
            "total_tokens": 20,
            "duration_ms": 150.0,
        },
        {
            "epoch": 1,
            "lamport": 11,
            "step": {
                "conversation_id": "conv_alpha",
                "index": 2,
                "step_type": "TOOL_EXECUTION",
                "status": "DONE",
                "payload": "Executed tool ls",
                "created_at": 1700000002.0,
            },
        },
        {
            "event_type": "step",
            "epoch": 1,
            "lamport": 12,
            "conversation_id": "conv_alpha",
            "step_index": 3,
            "step_type": "AGENT_RESPONSE",
            "status": "DONE",
            "step_payload": "Here are your files",
            "created_at": 1700000003.0,
        },
    ]

    with open(transcript_path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(ev) + "\n" for ev in events)

    stats: ProjectionStats = reproject_from_jsonl(
        transcript_path=transcript_path,
        target_db_path=target_db_path,
        use_snapshots=False,
    )

    assert stats.integrity_ok is True
    assert stats.steps_projected == 3
    assert stats.quarantined_count == 0
    assert stats.duration_ms >= 0.0

    # Verify tables and sequential rowids
    with closing(sqlite3.connect(target_db_path)) as conn:
        # Check integrity
        integrity = conn.execute("PRAGMA integrity_check").fetchall()
        assert integrity == [("ok",)]

        # Check steps
        steps = conn.execute("SELECT id, step_index, step_type, status, step_payload FROM steps ORDER BY id ASC").fetchall()
        assert len(steps) == 3
        # Enforce deterministic sequential rowids 1, 2, 3
        assert steps[0][0] == 1
        assert steps[1][0] == 2
        assert steps[2][0] == 3
        assert steps[0][1] == 1
        assert steps[0][2] == "USER_INPUT"
        assert steps[1][2] == "TOOL_EXECUTION"
        assert steps[2][2] == "AGENT_RESPONSE"

        # Check trajectory_meta
        meta = conn.execute("SELECT conversation_id, title, step_count FROM trajectory_meta").fetchall()
        assert len(meta) == 1
        assert meta[0][0] == "conv_alpha"
        assert meta[0][1] == "Alpha Session"
        assert meta[0][2] == 3

        # Check gen_metadata
        gen = conn.execute("SELECT conversation_id, model, total_tokens FROM gen_metadata").fetchall()
        assert len(gen) == 1
        assert gen[0][0] == "conv_alpha"
        assert gen[0][1] == "claude-3-5-sonnet"
        assert gen[0][2] == 20

        # Check index alignment
        indices = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        index_names = {idx[0] for idx in indices}
        assert "idx_steps_status" in index_names
        assert "idx_steps_step_type" in index_names


def test_snapshot_baseline_creation_and_delta_replay(reprojection_sandbox: Path) -> None:
    """Verify snapshot baseline creation and accelerated delta replay."""
    transcript_path: Path = reprojection_sandbox / "transcript_large.jsonl"
    target_db_path: Path = reprojection_sandbox / "conversation_large.db"
    snapshot_dir: Path = reprojection_sandbox / "snapshots"

    # Create 100 steps in transcript
    total_steps: int = 100
    with open(transcript_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({
            "event_type": "trajectory_meta",
            "conversation_id": "conv_scale",
            "title": "Scale Session",
            "created_at": 1700000000.0,
        }) + "\n")
        f.writelines(
            json.dumps({
                "event_type": "step",
                "epoch": 1,
                "lamport": i,
                "conversation_id": "conv_scale",
                "step_index": i,
                "step_type": "TOOL_EXECUTION" if i % 2 == 0 else "AGENT_RESPONSE",
                "status": "DONE",
                "step_payload": f"Step payload {i} " + ("x" * 200),
                "created_at": 1700000000.0 + i,
            }) + "\n"
            for i in range(1, total_steps + 1)
        )

    # 1. Project initial 60 steps to a baseline DB
    temp_initial_db: Path = reprojection_sandbox / "initial_temp.db"
    initial_transcript: Path = reprojection_sandbox / "transcript_initial_60.jsonl"
    with open(initial_transcript, "w", encoding="utf-8") as f_out, open(transcript_path, "r", encoding="utf-8") as f_in:
        lines = f_in.readlines()
        f_out.writelines(lines[:61])  # 1 meta + 60 steps

    engine = ReprojectionEngine(snapshot_dir=snapshot_dir)
    init_stats = engine.project_transcript(initial_transcript, temp_initial_db, use_snapshots=False)
    assert init_stats.steps_projected == 60
    assert init_stats.integrity_ok is True

    # 2. Test create_snapshot_baseline standalone helper and SnapshotManager
    snap_path = create_snapshot_baseline(temp_initial_db, snapshot_dir, epoch=60)
    assert snap_path.exists()
    assert snap_path.name == "snapshot_epoch_60.db"

    snap_mgr = SnapshotManager(snapshot_dir)
    snapshots = snap_mgr.list_snapshots()
    assert len(snapshots) == 1
    assert snapshots[0][0] == 60
    nearest = snap_mgr.find_nearest_snapshot(100)
    assert nearest is not None and nearest[0] == 60

    # 3. Project full 100-step transcript with snapshots enabled
    delta_stats: ProjectionStats = engine.project_transcript(
        transcript_path=transcript_path,
        target_db_path=target_db_path,
        use_snapshots=True,
    )

    assert delta_stats.integrity_ok is True
    assert delta_stats.steps_projected == 100
    assert delta_stats.snapshot_restored_epoch == 60
    assert delta_stats.delta_events_replayed == 40

    # 4. Verify all 100 steps exist with sequential rowids 1..100
    with closing(sqlite3.connect(target_db_path)) as conn:
        count_res = conn.execute("SELECT COUNT(*) FROM steps").fetchone()
        assert count_res is not None and count_res[0] == 100

        min_max_id = conn.execute("SELECT MIN(id), MAX(id) FROM steps").fetchone()
        assert min_max_id == (1, 100)

        # Confirm integrity
        assert conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]


def test_malformed_records_cordoned_into_quarantine(reprojection_sandbox: Path) -> None:
    """Verify corrupted lines and malformed payloads are quarantined with clean fallback steps."""
    transcript_path: Path = reprojection_sandbox / "transcript_corrupt.jsonl"
    target_db_path: Path = reprojection_sandbox / "conversation_corrupt.db"
    quarantine_path: Path = reprojection_sandbox / "quarantine_diffs.jsonl"

    lines: list[str] = [
        # Valid Step 1
        json.dumps({
            "event_type": "step",
            "conversation_id": "conv_quarantine",
            "step_index": 1,
            "step_type": "USER_INPUT",
            "status": "DONE",
            "step_payload": "Valid input",
            "created_at": 1700000001.0,
        }),
        # Corrupt line 2: completely unparseable JSON
        '{"event_type": "step", "conversation_id": "conv_quarantine", "step_index": 2, UNPARSEABLE_JSON_GARBAGE',
        # Valid Step 3
        json.dumps({
            "event_type": "step",
            "conversation_id": "conv_quarantine",
            "step_index": 3,
            "step_type": "AGENT_RESPONSE",
            "status": "DONE",
            "step_payload": "Valid response",
            "created_at": 1700000003.0,
        }),
        # Corrupted Payload Step 4: Marked corrupted / unparseable BLOB
        json.dumps({
            "event_type": "step",
            "conversation_id": "conv_quarantine",
            "step_index": 4,
            "step_type": "TOOL_EXECUTION",
            "status": "DONE",
            "step_payload": {"corrupt_blob": True, "raw_hex": "0xDEADBEEF"},
            "corrupt": True,
            "created_at": 1700000004.0,
        }),
        # Valid Step 5
        json.dumps({
            "event_type": "step",
            "conversation_id": "conv_quarantine",
            "step_index": 5,
            "step_type": "AGENT_RESPONSE",
            "status": "DONE",
            "step_payload": "Final valid step",
            "created_at": 1700000005.0,
        }),
    ]

    with open(transcript_path, "w", encoding="utf-8") as f:
        f.writelines(line + "\n" for line in lines)

    # Custom payload validator that flags corrupt_blob
    def payload_validator(payload: object) -> bool:
        return not (isinstance(payload, dict) and payload.get("corrupt_blob"))

    engine = ReprojectionEngine(
        quarantine_path=quarantine_path,
        validate_payload=payload_validator,
    )

    stats: ProjectionStats = engine.project_transcript(
        transcript_path=transcript_path,
        target_db_path=target_db_path,
        use_snapshots=False,
    )

    # Must complete successfully without crashing
    assert stats.integrity_ok is True
    assert stats.steps_projected == 5
    assert stats.quarantined_count == 2

    # Verify quarantine file
    assert quarantine_path.exists()
    quarantine_records: list[dict[str, object]] = []
    with open(quarantine_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                quarantine_records.append(json.loads(line))

    assert len(quarantine_records) == 2
    # Check epistemic opinion tuple is present: b=0.0, d=0.9, u=0.1, a=0.5
    for q_rec in quarantine_records:
        opinion = q_rec.get("opinion")
        assert opinion == {"b": 0.0, "d": 0.9, "u": 0.1, "a": 0.5}
        assert "reason" in q_rec
        assert "diagnostics" in q_rec

    # Verify SQLite DB has 5 rows with strictly sequential rowids 1, 2, 3, 4, 5
    with closing(sqlite3.connect(target_db_path)) as conn:
        rows = conn.execute("SELECT id, step_type, status FROM steps ORDER BY id ASC").fetchall()
        assert len(rows) == 5
        assert [r[0] for r in rows] == [1, 2, 3, 4, 5]

        # Row 2 (corrupted JSON) and Row 4 (corrupted payload) must be clean fallback steps
        assert rows[1][1] == "FALLBACK_QUARANTINED"
        assert rows[1][2] == "QUARANTINED"
        assert rows[3][1] == "FALLBACK_QUARANTINED"
        assert rows[3][2] == "QUARANTINED"

        # Row 1, 3, 5 are normal
        assert rows[0][1] == "USER_INPUT"
        assert rows[2][1] == "AGENT_RESPONSE"
        assert rows[4][1] == "AGENT_RESPONSE"


def test_deterministic_sequential_rowids_repaired(reprojection_sandbox: Path) -> None:
    """Verify that scrambled step indices in JSONL are normalized to strictly sequential rowids."""
    transcript_path: Path = reprojection_sandbox / "transcript_scrambled.jsonl"
    target_db_path: Path = reprojection_sandbox / "conversation_scrambled.db"

    # Out of order step indices in events
    events: list[dict[str, object]] = [
        {"conversation_id": "conv_scrambled", "step_index": 99, "step_type": "A", "status": "DONE"},
        {"conversation_id": "conv_scrambled", "step_index": 12, "step_type": "B", "status": "DONE"},
        {"conversation_id": "conv_scrambled", "step_index": 500, "step_type": "C", "status": "DONE"},
    ]

    with open(transcript_path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(ev) + "\n" for ev in events)

    stats = reproject_from_jsonl(transcript_path, target_db_path, use_snapshots=False)
    assert stats.steps_projected == 3

    with closing(sqlite3.connect(target_db_path)) as conn:
        rows = conn.execute("SELECT id, step_index, step_type FROM steps ORDER BY id ASC").fetchall()
        assert [r[0] for r in rows] == [1, 2, 3]
