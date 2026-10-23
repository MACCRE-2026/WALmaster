"""Tests for Live Volatility Input Ring Buffer and Staging Spillover.

Verification matrix:
1. LiveInputBuffer ingestion and memory tracking.
2. Automatic disk spillover upon exceeding max_memory_bytes.
3. Synchronous, atomic flush to journal (transcript.jsonl).
4. Post-crash startup recovery and replay of unfinalized staging chunks.
5. Deterministic context management and handle teardown.
6. Concurrent ingestion thread safety.
"""
from __future__ import annotations

import json
import shutil
import threading
import uuid
from pathlib import Path

import pytest

from core.buffer import LiveInputBuffer


@pytest.fixture
def ephemeral_staging_dir(test_artifacts_dir: Path) -> Path:
    """Provide an isolated, ephemeral staging directory on B: drive."""
    dir_path = test_artifacts_dir / f"staging_{uuid.uuid4().hex[:8]}"
    dir_path.mkdir(parents=True, exist_ok=True)
    try:
        yield dir_path
    finally:
        if dir_path.exists():
            shutil.rmtree(dir_path, ignore_errors=True)


def test_buffer_basic_ingest(ephemeral_staging_dir: Path) -> None:
    """Verify basic ingestion, memory tracking, and event counting."""
    buf = LiveInputBuffer(
        max_memory_bytes=1024 * 1024,
        staging_dir=ephemeral_staging_dir,
    )

    assert buf.pending_event_count == 0
    assert buf.current_memory_bytes == 0

    idx1 = buf.ingest({"step": 1, "type": "tool_call", "payload": "sample data"})
    assert idx1 == 1
    assert buf.pending_event_count == 1
    assert buf.current_memory_bytes > 0

    idx2 = buf.ingest({"step": 2, "type": "tool_result", "payload": "result text"})
    assert idx2 == 2
    assert buf.pending_event_count == 2
    assert buf.staged_chunk_count == 0


def test_buffer_spillover_threshold(ephemeral_staging_dir: Path) -> None:
    """Verify that buffer spills over to disk staging files when max_memory_bytes is exceeded."""
    # Set small memory threshold to force spillover
    max_bytes = 250
    buf = LiveInputBuffer(
        max_memory_bytes=max_bytes,
        staging_dir=ephemeral_staging_dir,
    )

    # Ingest events that will exceed 250 bytes
    payload_a = {"event": "payload_a", "data": "x" * 150}
    payload_b = {"event": "payload_b", "data": "y" * 150}

    buf.ingest(payload_a)
    assert buf.staged_chunk_count == 0

    # Ingesting payload_b should trigger spillover of the accumulated in-memory items
    buf.ingest(payload_b)

    assert buf.staged_chunk_count >= 1
    # Check that disk staging file was created
    staging_files = list(ephemeral_staging_dir.glob("*.chunk"))
    assert len(staging_files) >= 1

    # Verify content in staging chunk
    with open(staging_files[0], "r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) >= 1
        record = json.loads(lines[0])
        assert record["event"] == "payload_a"


def test_buffer_flush_to_journal(
    ephemeral_staging_dir: Path,
    test_artifacts_dir: Path,
) -> None:
    """Verify synchronous flush of both staged and in-memory events to journal."""
    journal_path = test_artifacts_dir / f"test_transcript_{uuid.uuid4().hex[:8]}.jsonl"

    try:
        # Buffer with 200 bytes threshold to guarantee both staged chunks and memory items
        buf = LiveInputBuffer(
            max_memory_bytes=200,
            staging_dir=ephemeral_staging_dir,
        )

        # Ingest 10 events
        for i in range(10):
            buf.ingest({"seq": i, "content": f"message_{i}" * 10})

        # We must have at least one staged chunk on disk
        assert buf.staged_chunk_count > 0

        # Flush to journal
        flushed_count = buf.flush_to_journal(journal_path)
        assert flushed_count == 10

        # Staging chunks must have been unlinked
        assert buf.staged_chunk_count == 0
        assert len(list(ephemeral_staging_dir.glob("*.chunk"))) == 0
        assert buf.current_memory_bytes == 0
        assert buf.pending_event_count == 0

        # Verify all 10 records are in the journal in chronological order
        with open(journal_path, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]

        assert len(lines) == 10
        for i, record in enumerate(lines):
            assert record["seq"] == i
    finally:
        if journal_path.exists():
            journal_path.unlink(missing_ok=True)


def test_buffer_crash_recovery_on_startup(
    ephemeral_staging_dir: Path,
    test_artifacts_dir: Path,
) -> None:
    """Verify that an interrupted/crashed process's staging chunks are recovered and replayed."""
    journal_path = test_artifacts_dir / f"recovered_transcript_{uuid.uuid4().hex[:8]}.jsonl"

    try:
        # Simulate process 1: ingests events, creates staging chunks, and terminates abruptly without flush
        buf_1 = LiveInputBuffer(
            max_memory_bytes=150,
            staging_dir=ephemeral_staging_dir,
        )
        for i in range(8):
            buf_1.ingest({"seq": i, "note": "pre_crash_data" * 5})

        assert buf_1.staged_chunk_count > 0
        staged_chunks_before = list(ephemeral_staging_dir.glob("*.chunk"))
        assert len(staged_chunks_before) > 0

        # Simulate process crash / new startup:
        # Instantiate fresh LiveInputBuffer with the same staging dir
        buf_recovered = LiveInputBuffer(
            max_memory_bytes=1024 * 1024,
            staging_dir=ephemeral_staging_dir,
        )

        assert buf_recovered.has_unfinalized_staging is True
        unfinalized_events = buf_recovered.recover_unfinalized_staging()
        assert len(unfinalized_events) >= 1

        # Replay and flush unfinalized chunks to the journal
        replayed_count = buf_recovered.replay_and_flush(journal_path)
        assert replayed_count == len(unfinalized_events)

        # Chunks must now be cleared
        assert buf_recovered.has_unfinalized_staging is False
        assert len(list(ephemeral_staging_dir.glob("*.chunk"))) == 0

        # Verify recovered journal content
        with open(journal_path, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        assert len(lines) == replayed_count
        assert lines[0]["seq"] == 0
    finally:
        if journal_path.exists():
            journal_path.unlink(missing_ok=True)


def test_buffer_context_manager(
    ephemeral_staging_dir: Path,
    test_artifacts_dir: Path,
) -> None:
    """Verify deterministic context manager behaviour."""
    journal_path = test_artifacts_dir / f"ctx_transcript_{uuid.uuid4().hex[:8]}.jsonl"

    try:
        with LiveInputBuffer(
            max_memory_bytes=500,
            staging_dir=ephemeral_staging_dir,
            auto_flush_journal=journal_path,
        ) as buf:
            buf.ingest({"id": "ctx_1", "val": 100})
            buf.ingest({"id": "ctx_2", "val": 200})

        # Upon exiting the with block, auto_flush_journal flushed the events
        assert journal_path.exists()
        with open(journal_path, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        assert len(lines) == 2
        assert lines[0]["id"] == "ctx_1"
        assert lines[1]["id"] == "ctx_2"
    finally:
        if journal_path.exists():
            journal_path.unlink(missing_ok=True)


def test_buffer_concurrent_ingest(
    ephemeral_staging_dir: Path,
    test_artifacts_dir: Path,
) -> None:
    """Verify thread safety during high-concurrency ingestion."""
    journal_path = test_artifacts_dir / f"concurrent_transcript_{uuid.uuid4().hex[:8]}.jsonl"

    try:
        buf = LiveInputBuffer(
            max_memory_bytes=500,
            staging_dir=ephemeral_staging_dir,
        )

        num_threads = 4
        items_per_thread = 25
        total_items = num_threads * items_per_thread

        def worker(thread_id: int) -> None:
            for i in range(items_per_thread):
                buf.ingest({
                    "thread_id": thread_id,
                    "item": i,
                    "payload": f"t{thread_id}_item_{i}" * 5,
                })

        threads = [
            threading.Thread(target=worker, args=(t,))
            for t in range(num_threads)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Flush everything to journal
        flushed = buf.flush_to_journal(journal_path)
        assert flushed == total_items

        with open(journal_path, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        assert len(lines) == total_items
    finally:
        if journal_path.exists():
            journal_path.unlink(missing_ok=True)
