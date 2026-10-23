# WALmaster: Sovereign Concurrency, Lease Arbitration & Event-Sourced Storage Engine
## Agent Handover & Technical Specification

**Author/Origin:** Conductor Agent (CRSDL-MACCRE Ecosystem)  
**Target:** Dedicated Research & Engineering Agent for WALmaster  
**Workspace Anchor:** `B:\CRSDL-MACCRE\walmaster` (Can be linked/deployed to `B:\WALmaster`)

---

## 1. Executive Summary & Problem Formulation

In modern desktop AI developer environments (specifically Google Antigravity 2.15+ and broader Electron-based runtimes like VS Code, Obsidian, and Slack), users experience frequent SQLite database corruption characterized by:
```text
sqlite3.DatabaseError: database disk image is malformed
PRAGMA integrity_check: Tree 8 page 32 cell 1: Rowid 17 out of order
Tree 7 page 7 cell 1: 2nd reference to page 32
Tree 4 page 4 cell 4: 2nd reference to page 33
```
Along with telltale sync collision files in `%USERPROFILE%\.gemini\antigravity\`:
```text
conversation_summaries (1).db-wal
conversation_summaries (2).db-wal
presence/<conversation-id>.lock
```

This failure escalates dramatically when users switch between **local desktop Antigravity** and the **remote cloud connection** introduced in version 2.15.0.

### The Mission of WALmaster:
To eliminate band-aids and solve this at the root architectural level:
1. **Model the Concurrency Physics:** Instrument and reproduce exact SQLite WAL torn page writes and lock contention across desktop and cloud environments.
2. **Implement Lease Arbitration:** Build a distributed lease and barrier coordinator that guarantees zero-torn-write handovers between local Electron processes and remote cloud runners.
3. **Event-Sourced Projection Engine:** Separate the persistent append-only event log (`transcript.jsonl`) from the ephemeral read projection (`conversations/<id>.db`), making UI databases 100% self-healing and zero-loss.
4. **Publish Verifiable Systems Evidence:** Deliver reproducible benchmarks and whitepapers capable of serving as a reference architecture for Google Antigravity and the broader Electron/embedded systems community.

---

## 2. Root Cause Analysis: SQLite vs. Cloud/Electron Architecture

### A. Is SQLite Broken?
**No.** SQLite's Write-Ahead Logging (WAL) architecture behaves exactly as designed. The SQLite specification (by D. Richard Hipp) explicitly states:
> *"All processes sharing a database in WAL mode must reside on the same physical host and must have direct access to shared memory (`.db-shm`). Running WAL mode over network filesystems or sync clients is strictly unsupported."*

### B. The Real Culprits:
1. **The Shared Memory (`.db-shm`) Black Hole:**
   When an Electron application transitions execution between desktop and cloud, the two execution runtimes do not share physical RAM. If both ends hold open handles or attempt to update WAL indices without a shared POSIX/Win32 memory bus, index pages drift and rowids fall out of order.
2. **Missing Handover Checkpoint Barrier:**
   When Antigravity shifts context to cloud inference or a remote runner, it does not enforce a blocking:
   ```sql
   PRAGMA wal_checkpoint(TRUNCATE);
   ```
   accompanied by releasing OS file handles. As a result, the remote runner interacts with stale or mid-commit WAL files.
3. **Sync Daemon Splintering:**
   File sync bridges (Google Drive, OneDrive, or internal Antigravity rsync daemons) do not synchronize `.db`, `.db-wal`, and `.db-shm` as an atomic transaction. They sync files individually, generating conflict copies (`conversation_summaries (1).db-wal`) that leave SQLite permanently locked or corrupt.
4. **Abrupt Process Termination in Electron:**
   When the desktop application crashes, is killed, or loses power, open SQLite handles leave `presence/<id>.lock` files orphaned on disk and pending WAL frames uncheckpointed.

---

## 3. Prior Art & Codebase Discoveries in CRSDL-MACCRE

The existing CRSDL-MACCRE repository already contains critical discoveries regarding why restored conversations often refuse to appear in the Antigravity UI:

### Key Inspection Target: `maccre_core/transcripts/recovery.py`
In this module, our team discovered that **Antigravity operates a Two-Tier UI Registry**:
* **Tier 1 (Payload):** `~/.gemini/antigravity/conversations/<conversation-id>.db`
  Stores the trajectory metadata and protobuf BLOB step payloads.
* **Tier 2 (Sidebar Index):** `~/.gemini/antigravity/conversation_summaries.db`
  The Electron sidebar **never queries individual conversation databases** to render the session list. It queries `conversation_summaries.db` for `conversation_id`, `step_count`, and `last_modified_time`.
* **Tier 3 (Presence Lock):** `~/.gemini/antigravity/presence/<conversation-id>.lock`
  If a stale 0-byte `.lock` file exists from an interrupted run, Antigravity treats the conversation as currently owned by another worker and suppresses UI updates.

**Why UI Restoration was a fight:**
If you only repair or rebuild `conversations/<id>.db`, the UI will completely ignore it because:
1. `conversation_summaries.db` has not been synchronized with the updated step count and timestamp.
2. Stale `presence/<id>.lock` files block ownership acquisition.
3. Transient conflict files (`*(1).db-wal`) confuse the Electron main process SQLite driver (`better-sqlite3`).

---

## 4. Architectural Roadmap for the WALmaster Agent

The next agent taking over `WALmaster` should proceed across these prioritized phases:

### Phase 1: Test Suite & Concurrency Simulator (`walmaster/tests/`)
* **Target:** Build a deterministic simulation harness that models:
  * Two competing processes writing to a WAL database without shared memory.
  * Sudden simulated crash / process abort (`os._exit` / `TerminateProcess`) during a 10MB burst.
  * Verifiable empirical reproduction of the exact `Tree 8 page 32 cell 1: Rowid 17 out of order` corruption.

### Phase 2: Handover Barrier & Distributed Lease Arbiter (`walmaster/core/arbiter.py`)
* Implement a rock-solid lease arbiter:
  * **Pre-Handover Contract:** Synchronous `PRAGMA wal_checkpoint(TRUNCATE)` -> verify 0 remaining WAL frames -> release OS locks -> emit cryptographically signed handover token.
  * **Acquisition Gate:** Remote runner verifies handover token before acquiring database handle.
  * **Post-Handover Sync:** On return to desktop, re-read and invalidate stale cache.

### Phase 3: The Event-Sourced Projection Engine (`walmaster/core/reprojection.py`)
* Decouple the UI database from being the source of truth:
  * **Canonical Truth:** The append-only, tamper-evident log ([`transcript.jsonl`](file:///b:/CRSDL-MACCRE/maccre_core/transcripts/)).
  * **Projection Engine:** Reads JSONL -> populates SQLite tables (`steps`, `trajectory_meta`, `gen_metadata`) -> updates `conversation_summaries.db` -> purges `.lock` files.
  * **Self-Healing Hook:** An automated startup probe (`PRAGMA integrity_check`). If any error is detected, the corrupt `.db` is quarantined, and a fresh `.db` is projected from the log in <500ms.

### Phase 4: Telemetry & Whitepaper Documentation (`walmaster/docs/`)
* Compile an empirical benchmark report:
  * Performance metrics: Write throughput, latency, and recovery time.
  * Failure analysis: Before vs After WALmaster lease arbiter under 1,000 simulated network dropouts.
  * Ready-to-publish engineering brief for the Google Antigravity and open-source Electron communities.

---

## 5. Sovereign Workspace & Host Storage Directives

1. **Drive `B:\` Sovereignty:** All test databases, virtual environments, and temporary simulation directories MUST reside strictly on `B:\` (e.g. `B:\CRSDL-MACCRE\walmaster\scratch\` or `B:\WALmaster`). Never write test databases to `C:\`.
2. **Standard-Library Seams:** Build the core lock arbiter and recovery engine using 100% Python standard library (`sqlite3`, `fcntl` / `ctypes` Win32 `LockFileEx`, `hashlib`, `json`).
3. **Empirical Red/Green TDD:** Every bug fix and lease protocol must have an automated test that turns red when concurrency breaks and green when protected by the arbiter.
