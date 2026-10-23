"""Tests for WALmaster Live Concurrency Monitor & Telemetry."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from telemetry.monitor import LiveConcurrencyMonitor


def test_probe_conversation_detects_wal_and_presence_lock(tmp_path: Path):
    conv_dir = tmp_path / "conversations"
    brain_dir = tmp_path / "brain"
    presence_dir = tmp_path / "presence"
    summaries_db = tmp_path / "conversation_summaries.db"
    telemetry_dir = tmp_path / "telemetry"

    conv_dir.mkdir(parents=True)
    brain_dir.mkdir(parents=True)
    presence_dir.mkdir(parents=True)
    telemetry_dir.mkdir(parents=True)

    session_id = "test-telem-001"

    # Setup database with steps table
    db_file = conv_dir / f"{session_id}.db"
    with sqlite3.connect(db_file) as conn:
        conn.execute("CREATE TABLE steps (idx int PRIMARY KEY, payload text)")
        conn.execute("INSERT INTO steps VALUES (0, 'hello'), (1, 'world')")

    # Setup WAL file of 16384 bytes (4 frames)
    wal_file = conv_dir / f"{session_id}.db-wal"
    wal_file.write_bytes(b"\x00" * 16384)

    # Setup presence lock with a dead mock PID
    lock_file = presence_dir / f"{session_id}.lock"
    lock_file.write_text("99999999", encoding="utf-8")

    # Setup summaries table
    with sqlite3.connect(summaries_db) as s_conn:
        s_conn.execute("CREATE TABLE conversation_summaries (conversation_id text PRIMARY KEY, title text, step_count int)")
        s_conn.execute("INSERT INTO conversation_summaries VALUES (?, 'Telemetry Test Session', 2)", (session_id,))

    # Setup brain transcript with 2 steps
    log_dir = brain_dir / session_id / ".system_generated" / "logs"
    log_dir.mkdir(parents=True)
    with open(log_dir / "transcript.jsonl", "w", encoding="utf-8") as f:
        f.write(json.dumps({"step_index": 0}) + "\n")
        f.write(json.dumps({"step_index": 1}) + "\n")

    monitor = LiveConcurrencyMonitor(
        conversations_dir=conv_dir,
        brain_dir=brain_dir,
        presence_dir=presence_dir,
        summaries_db=summaries_db,
        telemetry_dir=telemetry_dir,
    )

    metric = monitor.probe_conversation(session_id)

    assert metric.session_id == session_id
    assert metric.title == "Telemetry Test Session"
    assert metric.wal_size_bytes == 16384
    assert metric.uncheckpointed_frames_est == 4
    assert metric.has_presence_lock is True
    assert metric.presence_lock_pid == 99999999
    assert metric.db_step_count == 2
    assert metric.summary_step_count == 2
    assert metric.transcript_step_count == 2
    assert metric.step_discrepancy == 0
    assert metric.integrity_status == "OK"


def test_collect_snapshot_and_markdown_report(tmp_path: Path):
    conv_dir = tmp_path / "conversations"
    brain_dir = tmp_path / "brain"
    presence_dir = tmp_path / "presence"
    summaries_db = tmp_path / "conversation_summaries.db"
    telemetry_dir = tmp_path / "telemetry"

    conv_dir.mkdir(parents=True)
    brain_dir.mkdir(parents=True)
    presence_dir.mkdir(parents=True)
    telemetry_dir.mkdir(parents=True)

    session_id = "test-telem-snapshot"
    db_file = conv_dir / f"{session_id}.db"
    with sqlite3.connect(db_file) as conn:
        conn.execute("CREATE TABLE steps (idx int)")

    wal_file = conv_dir / f"{session_id}.db-wal"
    wal_file.write_bytes(b"\x00" * 8192)

    monitor = LiveConcurrencyMonitor(
        conversations_dir=conv_dir,
        brain_dir=brain_dir,
        presence_dir=presence_dir,
        summaries_db=summaries_db,
        telemetry_dir=telemetry_dir,
    )

    snapshot = monitor.collect_snapshot(focus_session_ids=[session_id])
    assert snapshot.total_wal_bytes == 8192
    assert snapshot.max_wal_session_id == session_id
    assert snapshot.max_wal_bytes == 8192
    assert len(snapshot.metrics) == 1

    # Log to jsonl
    monitor.log_snapshot_to_jsonl(snapshot)
    assert monitor.stream_log_path.exists()

    # Generate markdown report
    report = monitor.generate_markdown_report(snapshot)
    assert "# WALmaster Concurrency & Telemetry Snapshot" in report
    assert f"`{session_id}`" in report
    assert monitor.snapshot_md_path.exists()


def test_run_monitoring_loop_bounded_iterations(tmp_path: Path):
    conv_dir = tmp_path / "conversations"
    brain_dir = tmp_path / "brain"
    presence_dir = tmp_path / "presence"
    summaries_db = tmp_path / "conversation_summaries.db"
    telemetry_dir = tmp_path / "telemetry"

    conv_dir.mkdir(parents=True)
    brain_dir.mkdir(parents=True)
    presence_dir.mkdir(parents=True)
    telemetry_dir.mkdir(parents=True)

    monitor = LiveConcurrencyMonitor(
        conversations_dir=conv_dir,
        brain_dir=brain_dir,
        presence_dir=presence_dir,
        summaries_db=summaries_db,
        telemetry_dir=telemetry_dir,
    )

    # Run for 2 iterations with 0.01s sleep
    monitor.run_monitoring_loop(interval_sec=0.01, max_iterations=2)
    assert monitor.stream_log_path.exists()
    assert monitor.snapshot_md_path.exists()
