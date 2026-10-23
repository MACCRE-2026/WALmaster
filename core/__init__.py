"""WALmaster Core Module."""
from __future__ import annotations

from core.arbiter import (
    BACKOFF_SCHEDULE_MS,
    MAX_BACKOFF_MS,
    CheckpointResult,
    LeaseToken,
    TwoTierBarrierCoordinator,
    WALArbiter,
)
from core.bridge import (
    heal_and_restore_for_maccre,
    patch_maccre_recovery,
    scan_and_heal_all_corrupted,
)
from core.buffer import (
    DEFAULT_MAX_MEMORY_BYTES,
    DEFAULT_STAGING_DIR,
    LiveInputBuffer,
)
from core.healer import (
    HealResult,
    SovereignSessionHealer,
)
from core.provenance import (
    BinaryReceipt,
    QuarantineRecord,
    SLSAAttestation,
    SubjectiveOpinion,
    append_attestation,
    append_quarantine_record,
    compute_merkle_root,
    compute_payload_hash,
    fuse_opinions,
    read_attestations,
    read_quarantine_records,
)
from core.reprojection import (
    QUARANTINE_DEFAULT_OPINION,
    EpistemicOpinion,
    ProjectionStats,
    ReprojectionEngine,
    SnapshotManager,
    create_snapshot_baseline,
    reproject_from_jsonl,
)
from core.sentinel import (
    DEFAULT_ALLOWED_RUNNERS,
    LockReapStatus,
    Sentinel,
    inspect_presence_lock,
    reap_presence_locks,
    reconcile_two_tier_registry,
)
from core.vfs import (
    SQLITE_IOERR_WRITE,
    SQLITE_OK,
    CrashInjectionConfig,
    VFSInterceptStats,
    WALMasterVFS,
    get_walmaster_vfs,
    register_walmaster_vfs,
    unregister_walmaster_vfs,
    walmaster_vfs_inject_crash_at_byte,
)

__version__: str = "0.2.0"

__all__: list[str] = [
    "BACKOFF_SCHEDULE_MS",
    "DEFAULT_ALLOWED_RUNNERS",
    "DEFAULT_MAX_MEMORY_BYTES",
    "DEFAULT_STAGING_DIR",
    "MAX_BACKOFF_MS",
    "QUARANTINE_DEFAULT_OPINION",
    "SQLITE_IOERR_WRITE",
    "SQLITE_OK",
    "BinaryReceipt",
    "CheckpointResult",
    "CrashInjectionConfig",
    "EpistemicOpinion",
    "HealResult",
    "LeaseToken",
    "LiveInputBuffer",
    "LockReapStatus",
    "ProjectionStats",
    "QuarantineRecord",
    "ReprojectionEngine",
    "SLSAAttestation",
    "Sentinel",
    "SnapshotManager",
    "SovereignSessionHealer",
    "SubjectiveOpinion",
    "TwoTierBarrierCoordinator",
    "VFSInterceptStats",
    "WALArbiter",
    "WALMasterVFS",
    "__version__",
    "append_attestation",
    "append_quarantine_record",
    "compute_merkle_root",
    "compute_payload_hash",
    "create_snapshot_baseline",
    "fuse_opinions",
    "get_walmaster_vfs",
    "heal_and_restore_for_maccre",
    "inspect_presence_lock",
    "patch_maccre_recovery",
    "read_attestations",
    "read_quarantine_records",
    "reap_presence_locks",
    "reconcile_two_tier_registry",
    "register_walmaster_vfs",
    "reproject_from_jsonl",
    "scan_and_heal_all_corrupted",
    "unregister_walmaster_vfs",
    "walmaster_vfs_inject_crash_at_byte",
]
