"""VFS Interceptor and Crash Injection Test Suite.

Verifies:
1. WALMasterVFS instantiation and statistics tracking.
2. xOpen, xWrite, xShmMap, xShmLock interception and hook invocation.
3. Deterministic crash injection at target byte offsets.
4. VFS registration and unregistration lifecycle.
5. Global crash injection function.
6. Thread safety of interception stats.
"""
from __future__ import annotations

import threading

from core.vfs import (
    SQLITE_IOERR_WRITE,
    SQLITE_OK,
    WALMasterVFS,
    get_walmaster_vfs,
    register_walmaster_vfs,
    unregister_walmaster_vfs,
    walmaster_vfs_inject_crash_at_byte,
)


class TestWALMasterVFS:
    """Verify core VFS interceptor behavior."""

    def test_initial_stats_are_zero(self) -> None:
        vfs = WALMasterVFS(name="test_vfs")
        assert vfs.stats.x_open_count == 0
        assert vfs.stats.x_write_count == 0
        assert vfs.stats.x_write_bytes == 0
        assert vfs.stats.x_shm_map_count == 0
        assert vfs.stats.x_shm_lock_count == 0
        assert vfs.stats.crash_injections == 0

    def test_intercept_open_tracks_count(self) -> None:
        vfs = WALMasterVFS()
        vfs.intercept_open("/tmp/test.db", flags=0)
        vfs.intercept_open("/tmp/test.db-wal", flags=1)
        assert vfs.stats.x_open_count == 2

    def test_intercept_write_tracks_bytes(self) -> None:
        vfs = WALMasterVFS()
        rc1 = vfs.intercept_write("/tmp/test.db", offset=0, nbytes=4096)
        rc2 = vfs.intercept_write("/tmp/test.db", offset=4096, nbytes=2048)
        assert rc1 == SQLITE_OK
        assert rc2 == SQLITE_OK
        assert vfs.stats.x_write_count == 2
        assert vfs.stats.x_write_bytes == 4096 + 2048

    def test_intercept_shm_map_tracks_count(self) -> None:
        vfs = WALMasterVFS()
        vfs.intercept_shm_map(region_index=0, region_size=32768)
        vfs.intercept_shm_map(region_index=1, region_size=32768)
        assert vfs.stats.x_shm_map_count == 2

    def test_intercept_shm_lock_tracks_count(self) -> None:
        vfs = WALMasterVFS()
        vfs.intercept_shm_lock(offset=0, n=1, flags=0x01)
        assert vfs.stats.x_shm_lock_count == 1

    def test_reset_stats_clears_all(self) -> None:
        vfs = WALMasterVFS()
        vfs.intercept_open("a.db", 0)
        vfs.intercept_write("a.db", 0, 100)
        vfs.intercept_shm_map(0, 4096)
        vfs.intercept_shm_lock(0, 1, 0)
        assert vfs.stats.x_open_count > 0

        vfs.reset_stats()
        assert vfs.stats.x_open_count == 0
        assert vfs.stats.x_write_count == 0
        assert vfs.stats.x_write_bytes == 0
        assert vfs.stats.x_shm_map_count == 0
        assert vfs.stats.x_shm_lock_count == 0


class TestCrashInjection:
    """Verify deterministic crash injection at target byte offsets."""

    def test_crash_at_exact_byte_offset(self) -> None:
        vfs = WALMasterVFS()
        vfs.inject_crash_at_byte(target_byte=500)

        rc1 = vfs.intercept_write("test.db", offset=0, nbytes=100)
        assert rc1 == SQLITE_OK

        rc2 = vfs.intercept_write("test.db", offset=400, nbytes=200)
        assert rc2 == SQLITE_IOERR_WRITE

        assert vfs.crash_config.triggered is True
        assert vfs.stats.crash_injections == 1

    def test_crash_triggers_only_once(self) -> None:
        vfs = WALMasterVFS()
        vfs.inject_crash_at_byte(target_byte=0)

        rc1 = vfs.intercept_write("test.db", offset=0, nbytes=4096)
        assert rc1 == SQLITE_IOERR_WRITE

        rc2 = vfs.intercept_write("test.db", offset=0, nbytes=4096)
        assert rc2 == SQLITE_OK
        assert vfs.stats.crash_injections == 1

    def test_crash_does_not_trigger_outside_range(self) -> None:
        vfs = WALMasterVFS()
        vfs.inject_crash_at_byte(target_byte=10000)

        rc = vfs.intercept_write("test.db", offset=0, nbytes=100)
        assert rc == SQLITE_OK
        assert vfs.crash_config.triggered is False

    def test_crash_callback_invoked(self) -> None:
        callback_log: list[str] = []

        def on_crash() -> None:
            callback_log.append("CRASH")

        vfs = WALMasterVFS()
        vfs.inject_crash_at_byte(target_byte=50, callback=on_crash)

        rc = vfs.intercept_write("test.db", offset=0, nbytes=100)
        assert rc == SQLITE_IOERR_WRITE
        assert callback_log == ["CRASH"]

    def test_clear_crash_injection(self) -> None:
        vfs = WALMasterVFS()
        vfs.inject_crash_at_byte(target_byte=0)
        vfs.clear_crash_injection()

        rc = vfs.intercept_write("test.db", offset=0, nbytes=4096)
        assert rc == SQLITE_OK
        assert vfs.stats.crash_injections == 0

    def test_crash_at_boundary_start(self) -> None:
        vfs = WALMasterVFS()
        vfs.inject_crash_at_byte(target_byte=1024)
        rc = vfs.intercept_write("test.db", offset=1024, nbytes=512)
        assert rc == SQLITE_IOERR_WRITE

    def test_crash_at_boundary_end(self) -> None:
        vfs = WALMasterVFS()
        vfs.inject_crash_at_byte(target_byte=1535)
        rc = vfs.intercept_write("test.db", offset=1024, nbytes=512)
        assert rc == SQLITE_IOERR_WRITE


class TestVFSHooks:
    """Verify user-registered interception hooks are invoked."""

    def test_open_hook_invoked(self) -> None:
        vfs = WALMasterVFS()
        results: list[tuple[str, int]] = []
        vfs.add_open_hook(lambda f, fl: results.append((f, fl)))

        vfs.intercept_open("mydb.db", 42)
        assert results == [("mydb.db", 42)]

    def test_write_hook_invoked(self) -> None:
        vfs = WALMasterVFS()
        results: list[tuple[str, int, int]] = []
        vfs.add_write_hook(lambda f, o, n: results.append((f, o, n)))

        vfs.intercept_write("mydb.db", 100, 200)
        assert results == [("mydb.db", 100, 200)]

    def test_shm_map_hook_invoked(self) -> None:
        vfs = WALMasterVFS()
        results: list[tuple[int, int]] = []
        vfs.add_shm_map_hook(lambda ri, rs: results.append((ri, rs)))

        vfs.intercept_shm_map(0, 32768)
        assert results == [(0, 32768)]

    def test_shm_lock_hook_invoked(self) -> None:
        vfs = WALMasterVFS()
        results: list[tuple[int, int, int]] = []
        vfs.add_shm_lock_hook(lambda o, n, f: results.append((o, n, f)))

        vfs.intercept_shm_lock(3, 1, 0x02)
        assert results == [(3, 1, 0x02)]


class TestVFSRegistration:
    """Verify global VFS registration and unregistration lifecycle."""

    def test_register_and_unregister(self) -> None:
        unregister_walmaster_vfs()
        assert get_walmaster_vfs() is None

        result = register_walmaster_vfs(name="test_reg_vfs")
        assert result is True

        vfs = get_walmaster_vfs()
        assert vfs is not None
        assert vfs.name == "test_reg_vfs"
        assert vfs.is_registered is True

        unreg = unregister_walmaster_vfs()
        assert unreg is True
        assert get_walmaster_vfs() is None

    def test_double_register_returns_true(self) -> None:
        unregister_walmaster_vfs()
        register_walmaster_vfs(name="double_test")
        result = register_walmaster_vfs(name="double_test")
        assert result is True
        unregister_walmaster_vfs()

    def test_unregister_when_none_returns_false(self) -> None:
        unregister_walmaster_vfs()
        result = unregister_walmaster_vfs()
        assert result is False

    def test_global_crash_injection(self) -> None:
        unregister_walmaster_vfs()
        assert walmaster_vfs_inject_crash_at_byte(100) is False

        register_walmaster_vfs(name="inject_test")
        assert walmaster_vfs_inject_crash_at_byte(100) is True

        vfs = get_walmaster_vfs()
        assert vfs is not None
        assert vfs.crash_config.target_byte == 100
        unregister_walmaster_vfs()


class TestVFSThreadSafety:
    """Verify stats are correctly accumulated under concurrent access."""

    def test_concurrent_write_intercepts(self) -> None:
        vfs = WALMasterVFS()
        num_threads = 8
        writes_per_thread = 100

        def writer(thread_id: int) -> None:
            for i in range(writes_per_thread):
                vfs.intercept_write(f"db_{thread_id}.db", offset=i * 4096, nbytes=4096)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert vfs.stats.x_write_count == num_threads * writes_per_thread
        assert vfs.stats.x_write_bytes == num_threads * writes_per_thread * 4096
