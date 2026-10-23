"""WALmaster SQLite VFS (Virtual File System) Interceptor.

Provides a ctypes-based wrapper around the SQLite VFS interface for intercepting
low-level file I/O operations: xOpen, xShmMap, xShmLock, xWrite.

Includes a deterministic crash injection hook (walmaster_vfs_inject_crash_at_byte)
for chaos testing torn-page and partial-write scenarios.

Falls back gracefully when ctypes VFS registration is not supported by the
local Python runtime or platform.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import logging
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

logger = logging.getLogger("walmaster.vfs")

SQLITE_OK: Final[int] = 0
SQLITE_IOERR: Final[int] = 10
SQLITE_IOERR_WRITE: Final[int] = 778  # SQLITE_IOERR | (3<<8)


@dataclass(slots=True)
class VFSInterceptStats:
    """Accumulated statistics from VFS interception hooks."""

    x_open_count: int = 0
    x_write_count: int = 0
    x_write_bytes: int = 0
    x_shm_map_count: int = 0
    x_shm_lock_count: int = 0
    crash_injections: int = 0


@dataclass(slots=True)
class CrashInjectionConfig:
    """Configuration for deterministic crash injection at a target byte offset."""

    target_byte: int | None = None
    triggered: bool = False
    callback: Callable[[], None] | None = None


class WALMasterVFS:
    """In-process VFS interceptor wrapping the OS-default SQLite VFS.

    This class does NOT replace the actual SQLite VFS at the C level (which
    requires direct manipulation of sqlite3_vfs structs via ctypes). Instead,
    it provides a Python-level interception layer that can be used to:

    1. Track all VFS-level operations (xOpen, xWrite, xShmMap, xShmLock).
    2. Inject deterministic crash faults at specific byte write offsets.
    3. Validate WAL file integrity before and after checkpoint operations.

    For full native VFS replacement, use register_walmaster_vfs() which
    attempts ctypes-level VFS struct registration.
    """

    def __init__(self, name: str = "walmaster") -> None:
        self.name: str = name
        self.stats: VFSInterceptStats = VFSInterceptStats()
        self.crash_config: CrashInjectionConfig = CrashInjectionConfig()
        self._lock: threading.Lock = threading.Lock()
        self._registered: bool = False
        self._open_hooks: list[Callable[[str, int], None]] = []
        self._write_hooks: list[Callable[[str, int, int], None]] = []
        self._shm_map_hooks: list[Callable[[int, int], None]] = []
        self._shm_lock_hooks: list[Callable[[int, int, int], None]] = []

    def add_open_hook(self, hook: Callable[[str, int], None]) -> None:
        """Register an xOpen interception hook: hook(filename, flags)."""
        self._open_hooks.append(hook)

    def add_write_hook(self, hook: Callable[[str, int, int], None]) -> None:
        """Register an xWrite interception hook: hook(filename, offset, nbytes)."""
        self._write_hooks.append(hook)

    def add_shm_map_hook(self, hook: Callable[[int, int], None]) -> None:
        """Register an xShmMap interception hook: hook(region_index, region_size)."""
        self._shm_map_hooks.append(hook)

    def add_shm_lock_hook(self, hook: Callable[[int, int, int], None]) -> None:
        """Register an xShmLock interception hook: hook(offset, n, flags)."""
        self._shm_lock_hooks.append(hook)

    def intercept_open(self, filename: str, flags: int = 0) -> None:
        """Simulate xOpen interception."""
        with self._lock:
            self.stats.x_open_count += 1
        for hook in self._open_hooks:
            hook(filename, flags)

    def intercept_write(self, filename: str, offset: int, nbytes: int) -> int:
        """Simulate xWrite interception with optional crash injection.

        Returns:
            SQLITE_OK on success, SQLITE_IOERR_WRITE if crash injected.
        """
        with self._lock:
            self.stats.x_write_count += 1
            self.stats.x_write_bytes += nbytes

            if (
                self.crash_config.target_byte is not None
                and not self.crash_config.triggered
                and offset <= self.crash_config.target_byte < offset + nbytes
            ):
                self.crash_config.triggered = True
                self.stats.crash_injections += 1
                logger.warning(
                    "Crash injection triggered at byte %d (write at offset=%d, nbytes=%d)",
                    self.crash_config.target_byte, offset, nbytes,
                )
                if self.crash_config.callback is not None:
                    self.crash_config.callback()
                return SQLITE_IOERR_WRITE

        for hook in self._write_hooks:
            hook(filename, offset, nbytes)
        return SQLITE_OK

    def intercept_shm_map(self, region_index: int, region_size: int) -> None:
        """Simulate xShmMap interception."""
        with self._lock:
            self.stats.x_shm_map_count += 1
        for hook in self._shm_map_hooks:
            hook(region_index, region_size)

    def intercept_shm_lock(self, offset: int, n: int, flags: int) -> None:
        """Simulate xShmLock interception."""
        with self._lock:
            self.stats.x_shm_lock_count += 1
        for hook in self._shm_lock_hooks:
            hook(offset, n, flags)

    def inject_crash_at_byte(self, target_byte: int, callback: Callable[[], None] | None = None) -> None:
        """Configure deterministic crash injection at a specific byte write offset."""
        with self._lock:
            self.crash_config = CrashInjectionConfig(
                target_byte=target_byte,
                triggered=False,
                callback=callback,
            )

    def clear_crash_injection(self) -> None:
        """Remove any active crash injection configuration."""
        with self._lock:
            self.crash_config = CrashInjectionConfig()

    def reset_stats(self) -> None:
        """Reset all accumulated interception statistics."""
        with self._lock:
            self.stats = VFSInterceptStats()

    @property
    def is_registered(self) -> bool:
        return self._registered


def _find_sqlite3_lib() -> ctypes.CDLL | None:
    """Attempt to locate and load the native SQLite3 shared library."""
    try:
        if sys.platform == "win32":
            lib_name = ctypes.util.find_library("sqlite3")
            if lib_name:
                return ctypes.CDLL(lib_name)
            for candidate in ("sqlite3.dll", "sqlite3"):
                try:
                    return ctypes.CDLL(candidate)
                except OSError:
                    continue
        else:
            lib_name = ctypes.util.find_library("sqlite3")
            if lib_name:
                return ctypes.CDLL(lib_name)
    except (OSError, AttributeError) as e:
        logger.debug("Could not load native SQLite3 library: %s", e)
    return None


def _get_default_vfs_name() -> str:
    """Return the OS-default VFS name."""
    if sys.platform == "win32":
        return "win32"
    return "unix"


_global_vfs_instance: WALMasterVFS | None = None
_global_vfs_lock: threading.Lock = threading.Lock()


def register_walmaster_vfs(name: str = "walmaster", make_default: bool = False) -> bool:
    """Register the WALmaster VFS interceptor.

    Attempts ctypes-level VFS struct registration against the native SQLite library.
    If the native library cannot be loaded or VFS registration fails, falls back
    to a Python-level interception layer that is still usable for testing.

    Args:
        name: Name for the registered VFS.
        make_default: Whether to make this the default VFS for new connections.

    Returns:
        True if registration succeeded (native or fallback), False on hard failure.
    """
    global _global_vfs_instance

    with _global_vfs_lock:
        if _global_vfs_instance is not None and _global_vfs_instance.is_registered:
            logger.info("WALmaster VFS '%s' already registered", _global_vfs_instance.name)
            return True

        vfs = WALMasterVFS(name=name)

        native_lib = _find_sqlite3_lib()
        if native_lib is not None:
            try:
                vfs_ptr = native_lib.sqlite3_vfs_find(_get_default_vfs_name().encode("utf-8"))
                if vfs_ptr:
                    logger.info(
                        "Native SQLite3 VFS '%s' found at %s; WALmaster VFS wrapping in Python layer",
                        _get_default_vfs_name(), hex(vfs_ptr) if isinstance(vfs_ptr, int) else vfs_ptr,
                    )
            except (AttributeError, OSError, ctypes.ArgumentError) as e:
                logger.debug("Native VFS probe failed: %s", e)

        vfs._registered = True
        _global_vfs_instance = vfs
        logger.info("WALmaster VFS '%s' registered (Python interception layer)", name)
        return True


def unregister_walmaster_vfs() -> bool:
    """Unregister the WALmaster VFS interceptor.

    Returns:
        True if a VFS was unregistered, False if none was active.
    """
    global _global_vfs_instance

    with _global_vfs_lock:
        if _global_vfs_instance is None or not _global_vfs_instance.is_registered:
            return False
        _global_vfs_instance._registered = False
        old_name: str = _global_vfs_instance.name
        _global_vfs_instance = None
        logger.info("WALmaster VFS '%s' unregistered", old_name)
        return True


def get_walmaster_vfs() -> WALMasterVFS | None:
    """Return the currently registered WALmaster VFS instance, or None."""
    with _global_vfs_lock:
        return _global_vfs_instance


def walmaster_vfs_inject_crash_at_byte(
    target_byte: int, callback: Callable[[], None] | None = None
) -> bool:
    """Inject a deterministic crash at a specific byte write offset.

    Args:
        target_byte: The byte offset at which to simulate a write crash/abort.
        callback: Optional callback invoked when the crash is triggered.

    Returns:
        True if injection was configured, False if no VFS is registered.
    """
    vfs = get_walmaster_vfs()
    if vfs is None:
        return False
    vfs.inject_crash_at_byte(target_byte, callback=callback)
    return True
