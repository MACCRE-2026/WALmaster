"""Pytest fixtures and configuration for WALmaster test suites.

Strictly isolates all ephemeral SQLite test databases and locks inside:
B:\\WALmaster\\.development\\artifacts\\tests\\
"""
from __future__ import annotations

import gc
import uuid
from collections.abc import Generator
from pathlib import Path

import pytest

EPHEMERAL_TEST_DIR: Path = Path(r"B:\WALmaster\.development\artifacts\tests")


@pytest.fixture(scope="session")
def test_artifacts_dir() -> Path:
    """Ensure and return the canonical ephemeral test directory on B: drive."""
    EPHEMERAL_TEST_DIR.mkdir(parents=True, exist_ok=True)
    return EPHEMERAL_TEST_DIR


@pytest.fixture
def ephemeral_db_path(test_artifacts_dir: Path) -> Generator[Path, None, None]:
    """Provide an isolated, ephemeral SQLite database path with deterministic cleanup."""
    unique_id: str = uuid.uuid4().hex[:12]
    db_file: Path = test_artifacts_dir / f"test_sim_{unique_id}.db"

    try:
        yield db_file
    finally:
        gc.collect()
        for ext in ("", "-wal", "-shm"):
            target: Path = db_file.with_name(db_file.name + ext)
            if target.exists():
                try:
                    target.unlink(missing_ok=True)
                except OSError:
                    pass


@pytest.fixture
def mock_presence_lock_path(test_artifacts_dir: Path) -> Generator[Path, None, None]:
    """Provide the canonical mock presence lock path with strict cleanup."""
    lock_file: Path = test_artifacts_dir / "mock_presence.lock"
    if lock_file.exists():
        lock_file.unlink(missing_ok=True)

    try:
        yield lock_file
    finally:
        if lock_file.exists():
            try:
                lock_file.unlink(missing_ok=True)
            except OSError:
                pass
