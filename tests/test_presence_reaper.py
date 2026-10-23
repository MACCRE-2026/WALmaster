"""Presence Lock Reaper and Collision Detection Test Suite.

Verifies that uncoordinated access to an SQLite resource detects lock collisions
when a mock presence lock file exists at B:\\WALmaster\\.development\\artifacts\\tests\\mock_presence.lock.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pytest

LOCK_PATH: Final[Path] = Path(r"B:\WALmaster\.development\artifacts\tests\mock_presence.lock")


@dataclass(slots=True, frozen=True)
class PresenceMetadata:
    """Metadata payload stored inside a presence lock file."""

    owner_id: str
    owner_pid: int
    created_at_epoch: float
    description: str

    def to_json(self) -> str:
        """Serialize metadata to a JSON string."""
        return json.dumps(
            {
                "owner_id": self.owner_id,
                "owner_pid": self.owner_pid,
                "created_at_epoch": self.created_at_epoch,
                "description": self.description,
            }
        )

    @classmethod
    def from_json(cls, raw: str) -> PresenceMetadata:
        """Parse metadata from a JSON string."""
        data = json.loads(raw)
        return cls(
            owner_id=str(data["owner_id"]),
            owner_pid=int(data["owner_pid"]),
            created_at_epoch=float(data["created_at_epoch"]),
            description=str(data.get("description", "")),
        )


class PresenceCollisionError(Exception):
    """Raised when an uncoordinated process encounters an active presence lock."""


class UncoordinatedPresenceClient:
    """Simulates an uncoordinated desktop/cloud client attempting presence acquisition."""

    def __init__(self, client_id: str) -> None:
        self.client_id: str = client_id

    def acquire(self, lock_path: Path) -> int:
        """Attempt atomic acquisition via standard OS exclusive creation.

        Returns:
            Open file descriptor if acquisition succeeded.

        Raises:
            PresenceCollisionError: If the lock file already exists.
        """
        try:
            fd: int = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError as err:
            raise PresenceCollisionError(
                f"Client '{self.client_id}' detected collision on active lock: {lock_path}"
            ) from err

        meta: PresenceMetadata = PresenceMetadata(
            owner_id=self.client_id,
            owner_pid=os.getpid(),
            created_at_epoch=time.time(),
            description=f"Acquired by {self.client_id}",
        )
        os.write(fd, meta.to_json().encode("utf-8"))
        return fd


def reap_presence_lock(lock_path: Path) -> bool:
    """Nullify/remove an abandoned or stale presence lock file."""
    if lock_path.exists():
        lock_path.unlink()
        return True
    return False


def test_presence_lock_collision_detection(mock_presence_lock_path: Path) -> None:
    """Verify that when mock_presence.lock exists, standard uncoordinated access detects the collision."""
    # Step 1: Create the mock presence lock file at canonical path
    assert mock_presence_lock_path == LOCK_PATH
    initial_client: UncoordinatedPresenceClient = UncoordinatedPresenceClient("worker_desktop_1")
    fd_initial: int = initial_client.acquire(mock_presence_lock_path)
    try:
        assert mock_presence_lock_path.exists(), "Mock presence lock file must exist on disk."

        # Step 2: Uncoordinated second worker attempts access
        competing_client: UncoordinatedPresenceClient = UncoordinatedPresenceClient("worker_cloud_runner")
        with pytest.raises(PresenceCollisionError) as exc_info:
            fd_conflict: int = competing_client.acquire(mock_presence_lock_path)
            os.close(fd_conflict)  # pragma: no cover

        assert "detected collision" in str(exc_info.value)
        assert str(mock_presence_lock_path) in str(exc_info.value)
    finally:
        os.close(fd_initial)


def test_raw_os_file_collision_semantics(mock_presence_lock_path: Path) -> None:
    """Verify raw OS FileExistsError when an uncoordinated process bypasses coordinated wrappers."""
    # Seed the mock presence lock file
    mock_presence_lock_path.write_text("{\"stale_owner\": 9999}", encoding="utf-8")
    assert mock_presence_lock_path.exists()

    with pytest.raises(FileExistsError):
        fd: int = os.open(mock_presence_lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)  # pragma: no cover


def test_presence_reaper_cleans_stale_lock(mock_presence_lock_path: Path) -> None:
    """Verify that reaping a stale presence lock allows subsequent access to proceed cleanly."""
    # Create stale lock file
    stale_meta: PresenceMetadata = PresenceMetadata(
        owner_id="dead_process",
        owner_pid=999999,
        created_at_epoch=time.time() - 3600.0,
        description="Abandoned session from crash",
    )
    mock_presence_lock_path.write_text(stale_meta.to_json(), encoding="utf-8")
    assert mock_presence_lock_path.exists()

    # Collision is actively detected
    new_client: UncoordinatedPresenceClient = UncoordinatedPresenceClient("worker_reconnected")
    with pytest.raises(PresenceCollisionError):
        fd_bad: int = new_client.acquire(mock_presence_lock_path)
        os.close(fd_bad)  # pragma: no cover

    # Execute Reaper
    reaped: bool = reap_presence_lock(mock_presence_lock_path)
    assert reaped is True
    assert not mock_presence_lock_path.exists()

    # Verify clean access is now acquired without error
    fd_good: int = new_client.acquire(mock_presence_lock_path)
    try:
        assert mock_presence_lock_path.exists()
        content: str = mock_presence_lock_path.read_text(encoding="utf-8")
        recovered_meta: PresenceMetadata = PresenceMetadata.from_json(content)
        assert recovered_meta.owner_id == "worker_reconnected"
    finally:
        os.close(fd_good)
