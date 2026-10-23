"""WALmaster Live Concurrency Monitor & Telemetry Collector.

Monitors active SQLite conversations, WAL ballooning, checkpoint lag,
two-tier registry drift, and presence lock arbitration in hybrid edge-to-cloud
Antigravity workflows.
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from core.provenance import SubjectiveOpinion
from core.sentinel import DEFAULT_ALLOWED_RUNNERS, query_process_image_win32

logger = logging.getLogger("walmaster.telemetry")

DEFAULT_CONV_DIR = Path.home() / ".gemini" / "antigravity" / "conversations"
DEFAULT_BRAIN_DIR = Path.home() / ".gemini" / "antigravity" / "brain"
DEFAULT_PRESENCE_DIR = Path.home() / ".gemini" / "antigravity" / "presence"
DEFAULT_SUMMARIES_DB = Path.home() / ".gemini" / "antigravity" / "conversation_summaries.db"
DEFAULT_TELEMETRY_DIR = Path(r"B:\WALmaster\telemetry")


@dataclass(slots=True, frozen=True)
class ConversationMetric:
    """Telemetry metrics collected for a single conversation."""

    session_id: str
    title: str
    db_size_bytes: int
    wal_size_bytes: int
    shm_size_bytes: int
    uncheckpointed_frames_est: int
    has_presence_lock: bool
    presence_lock_pid: int | None
    presence_lock_exclusive: bool
    presence_image_name: str | None
    is_allowed_runner: bool
    db_step_count: int
    summary_step_count: int
    transcript_step_count: int
    step_discrepancy: int
    integrity_status: str
    has_sync_conflicts: bool
    sync_conflict_files: list[str]
    timestamp_iso: str
    health_opinion: tuple[float, float, float, float]  # (belief, disbelief, uncertainty, base_rate)


@dataclass(slots=True)
class SystemTelemetrySnapshot:
    """Whole-system snapshot of active database concurrency and telemetry."""

    timestamp_iso: str
    total_tracked_conversations: int
    active_conversations_count: int
    total_wal_bytes: int
    max_wal_session_id: str
    max_wal_bytes: int
    active_locks_count: int
    corrupted_sessions: list[str] = field(default_factory=list)
    metrics: list[ConversationMetric] = field(default_factory=list)


class LiveConcurrencyMonitor:
    """Watches active databases and records empirical concurrency telemetry."""

    def __init__(
        self,
        conversations_dir: Path | str | None = None,
        brain_dir: Path | str | None = None,
        presence_dir: Path | str | None = None,
        summaries_db: Path | str | None = None,
        telemetry_dir: Path | str | None = None,
    ) -> None:
        self.conv_dir = Path(conversations_dir) if conversations_dir else DEFAULT_CONV_DIR
        self.brain_dir = Path(brain_dir) if brain_dir else DEFAULT_BRAIN_DIR
        self.presence_dir = Path(presence_dir) if presence_dir else DEFAULT_PRESENCE_DIR
        self.summaries_db = Path(summaries_db) if summaries_db else DEFAULT_SUMMARIES_DB
        self.telemetry_dir = Path(telemetry_dir) if telemetry_dir else DEFAULT_TELEMETRY_DIR
        self.telemetry_dir.mkdir(parents=True, exist_ok=True)

        self.stream_log_path = self.telemetry_dir / "live_concurrency_stream.jsonl"
        self.snapshot_md_path = self.telemetry_dir / "TELEMETRY_SNAPSHOT.md"

    def probe_conversation(self, session_id: str) -> ConversationMetric:
        """Probe an individual conversation for low-level storage metrics."""
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        pure_id = session_id.replace(".db", "").strip()

        db_path = self.conv_dir / f"{pure_id}.db"
        wal_path = self.conv_dir / f"{pure_id}.db-wal"
        shm_path = self.conv_dir / f"{pure_id}.db-shm"
        lock_path = self.presence_dir / f"{pure_id}.lock"

        db_size = db_path.stat().st_size if db_path.exists() else 0
        wal_size = wal_path.stat().st_size if wal_path.exists() else 0
        shm_size = shm_path.stat().st_size if shm_path.exists() else 0
        frames_est = max(0, wal_size // 4096)

        # Inspect presence lock
        has_lock = lock_path.exists()
        lock_pid: int | None = None
        is_exclusive = False
        image_name: str | None = None
        is_runner = False

        if has_lock:
            try:
                raw_bytes = lock_path.read_bytes()
                txt = raw_bytes.decode("utf-8", errors="ignore").strip()
                if txt.isdigit():
                    lock_pid = int(txt)
            except PermissionError:
                is_exclusive = True
            except OSError:
                pass

            if lock_pid is not None and lock_pid > 0:
                is_alive, img = query_process_image_win32(lock_pid)
                if is_alive:
                    image_name = img
                    if img:
                        proc_stem = Path(img).name.lower()
                        is_runner = any(proc_stem == r.lower() for r in DEFAULT_ALLOWED_RUNNERS)

        # Inspect summary DB
        title = "(No Title)"
        summary_steps = -1
        if self.summaries_db.exists():
            s_conn: sqlite3.Connection | None = None
            try:
                s_conn = sqlite3.connect(f"file:{self.summaries_db.as_posix()}?mode=ro", uri=True, timeout=2.0)
                row = s_conn.execute(
                    "SELECT title, step_count FROM conversation_summaries WHERE conversation_id = ?",
                    (pure_id,),
                ).fetchone()
                if row:
                    title = str(row[0] or "(No Title)")
                    summary_steps = int(row[1])
            except (sqlite3.Error, OSError):
                pass
            finally:
                if s_conn is not None:
                    s_conn.close()

        # Inspect DB steps & integrity
        db_steps = -1
        integrity_status = "UNKNOWN"
        if db_path.exists():
            conn: sqlite3.Connection | None = None
            try:
                conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=2.0)
                cur = conn.cursor()
                cur.execute("SELECT count(*) FROM steps")
                r_cnt = cur.fetchone()
                db_steps = int(r_cnt[0]) if r_cnt else -1

                chk = cur.execute("PRAGMA integrity_check(1)").fetchall()
                if chk and len(chk) == 1 and chk[0][0].lower() == "ok":
                    integrity_status = "OK"
                else:
                    integrity_status = str(chk[0][0]) if chk else "FAILED"
            except sqlite3.Error as e:
                integrity_status = f"ERR: {e}"
            finally:
                if conn is not None:
                    conn.close()

        # Inspect brain transcript steps
        transcript_path = self.brain_dir / pure_id / ".system_generated" / "logs" / "transcript.jsonl"
        transcript_steps = 0
        if transcript_path.exists():
            try:
                with open(transcript_path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            transcript_steps += 1
            except OSError:
                pass

        step_discrepancy = (transcript_steps - db_steps) if (transcript_steps > 0 and db_steps >= 0) else 0

        # Check sync conflict files
        sync_conflicts: list[str] = []
        if self.conv_dir.exists():
            for item in self.conv_dir.glob(f"{pure_id}*"):
                if item.name in (f"{pure_id}.db", f"{pure_id}.db-wal", f"{pure_id}.db-shm", f"{pure_id}.db.corrupt.bak"):
                    continue
                if " (1)" in item.name or " (2)" in item.name or ".conflict" in item.name or ".tmp" in item.name:
                    sync_conflicts.append(item.name)

        has_conflicts = len(sync_conflicts) > 0

        # Compute Subjective Opinion Tuple omega = (b, d, u, a)
        if integrity_status == "OK" and step_discrepancy == 0 and not has_conflicts and wal_size < 1_000_000:
            opinion = SubjectiveOpinion(b=0.95, d=0.01, u=0.04, a=0.5)
        elif integrity_status == "OK" and (wal_size >= 1_000_000 or step_discrepancy > 0):
            # Ballooning WAL or minor sync drift raises uncertainty
            opinion = SubjectiveOpinion(b=0.70, d=0.10, u=0.20, a=0.5)
        elif "ERR" in integrity_status or integrity_status != "OK":
            opinion = SubjectiveOpinion(b=0.05, d=0.85, u=0.10, a=0.5)
        else:
            opinion = SubjectiveOpinion(b=0.50, d=0.10, u=0.40, a=0.5)

        return ConversationMetric(
            session_id=pure_id,
            title=title,
            db_size_bytes=db_size,
            wal_size_bytes=wal_size,
            shm_size_bytes=shm_size,
            uncheckpointed_frames_est=frames_est,
            has_presence_lock=has_lock,
            presence_lock_pid=lock_pid,
            presence_lock_exclusive=is_exclusive,
            presence_image_name=image_name,
            is_allowed_runner=is_runner,
            db_step_count=db_steps,
            summary_step_count=summary_steps,
            transcript_step_count=transcript_steps,
            step_discrepancy=step_discrepancy,
            integrity_status=integrity_status,
            has_sync_conflicts=has_conflicts,
            sync_conflict_files=sync_conflicts,
            timestamp_iso=now_iso,
            health_opinion=(opinion.b, opinion.d, opinion.u, opinion.a),
        )

    def collect_snapshot(self, focus_session_ids: list[str] | None = None) -> SystemTelemetrySnapshot:
        """Collect whole-system snapshot focusing on active/ballooning databases."""
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Find active candidates: presence locks, active WALs, or requested focus IDs
        active_ids: set[str] = set()
        if focus_session_ids:
            for s in focus_session_ids:
                active_ids.add(s.replace(".db", "").strip())

        if self.presence_dir.exists():
            for lk in self.presence_dir.glob("*.lock"):
                active_ids.add(lk.stem)

        # Include conversations with WAL > 0
        total_wal_bytes = 0
        max_wal_session = ""
        max_wal_bytes = 0

        for wal_file in self.conv_dir.glob("*.db-wal"):
            sid = wal_file.name.replace(".db-wal", "")
            sz = wal_file.stat().st_size
            total_wal_bytes += sz
            if sz > max_wal_bytes:
                max_wal_bytes = sz
                max_wal_session = sid
            if sz > 0:
                active_ids.add(sid)

        # Total count of all registered conversations
        all_dbs = [p for p in self.conv_dir.glob("*.db") if not p.name.endswith(".bak")]

        metrics: list[ConversationMetric] = []
        corrupted: list[str] = []

        for cid in sorted(active_ids):
            m = self.probe_conversation(cid)
            metrics.append(m)
            if m.integrity_status != "OK":
                corrupted.append(cid)

        # Sort metrics by WAL size descending
        metrics.sort(key=lambda x: x.wal_size_bytes, reverse=True)

        return SystemTelemetrySnapshot(
            timestamp_iso=now_iso,
            total_tracked_conversations=len(all_dbs),
            active_conversations_count=len(active_ids),
            total_wal_bytes=total_wal_bytes,
            max_wal_session_id=max_wal_session,
            max_wal_bytes=max_wal_bytes,
            active_locks_count=sum(1 for m in metrics if m.has_presence_lock),
            corrupted_sessions=corrupted,
            metrics=metrics,
        )

    def log_snapshot_to_jsonl(self, snapshot: SystemTelemetrySnapshot, output_path: Path | None = None) -> None:
        """Append telemetry snapshot record to stream JSONL."""
        target = output_path if output_path else self.stream_log_path
        target.parent.mkdir(parents=True, exist_ok=True)
        record = asdict(snapshot)
        with open(target, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def generate_markdown_report(self, snapshot: SystemTelemetrySnapshot, output_path: Path | None = None) -> str:
        """Render a readable markdown report summarizing current telemetry."""
        lines: list[str] = [
            "# WALmaster Concurrency & Telemetry Snapshot",
            f"**Timestamp (UTC):** `{snapshot.timestamp_iso}`  ",
            f"**Total Registered Conversations:** {snapshot.total_tracked_conversations}  ",
            f"**Active Concurrent Conversations:** {snapshot.active_conversations_count}  ",
            f"**Total Aggregate WAL Size:** {snapshot.total_wal_bytes / (1024 * 1024):.2f} MB  ",
            f"**Peak WAL Buffer Session:** `{snapshot.max_wal_session_id}` ({snapshot.max_wal_bytes / (1024 * 1024):.2f} MB)  ",
            f"**Active Presence Locks:** {snapshot.active_locks_count}  ",
            f"**Corrupted Sessions Detected:** {len(snapshot.corrupted_sessions)}  ",
            "",
            "---",
            "",
            "## Active Conversation Telemetry Table",
            "",
            "| Session ID | Title | WAL Size | Est. Frames | Lock Status | DB / Trans Steps | Integrity | Health Opinion (b, d, u) |",
            "|---|---|---|---|---|---|---|---|",
        ]

        for m in snapshot.metrics:
            wal_mb = m.wal_size_bytes / (1024 * 1024)
            lock_desc = "EXCLUSIVE" if m.presence_lock_exclusive else ("LOCKED" if m.has_presence_lock else "FREE")
            title_trunc = m.title[:30].replace("|", "-")
            opinion_str = f"({m.health_opinion[0]:.2f}, {m.health_opinion[1]:.2f}, {m.health_opinion[2]:.2f})"
            lines.append(
                f"| `{m.session_id}` | {title_trunc} | **{wal_mb:.2f} MB** | {m.uncheckpointed_frames_est:,} | {lock_desc} | {m.db_step_count} / {m.transcript_step_count} | {m.integrity_status} | `{opinion_str}` |"
            )

        lines.extend([
            "",
            "---",
            "",
            "## Failing Mechanisms Diagnosis & Whitepaper Observations",
            "",
            "1. **Checkpoint Starvation under Remote Relay:**",
            "   When the desktop Antigravity 2.0 acts as the edge relay server for `antigravity.google.com`, SQLite connections remain continuously open to service client RPCs. SQLite never reaches a quiescent 0-connection state to execute an automatic passive checkpoint, allowing WAL files to balloon into tens of megabytes.",
            "",
            "2. **Uncheckpointed WAL Crash Vulnerability:**",
            "   A 27+ MB WAL represents thousands of uncommitted B-tree page frames. If the host machine sleeps, drops power, or the desktop process terminates abruptly, SQLite must replay these thousands of frames on subsequent startup without valid shared memory (`.db-shm`), directly triggering `error code 11` (torn B-tree initialization).",
            "",
            "3. **Exclusive Win32 Presence Deadlocks:**",
            "   Electron runners holding exclusive file handles prevent multi-tab concurrency and crash recovery tools from safely inspecting session vitality.",
            "",
        ])

        report_str = "\n".join(lines)
        if output_path:
            output_path.write_text(report_str, encoding="utf-8")
        else:
            self.snapshot_md_path.write_text(report_str, encoding="utf-8")

        return report_str

    def run_monitoring_loop(
        self,
        interval_sec: float = 5.0,
        max_iterations: int | None = None,
        focus_session_id: str | None = None,
    ) -> None:
        """Run telemetry probe loop for the specified interval and iterations."""
        focus_list = [focus_session_id] if focus_session_id else None
        iterations = 0

        logger.info("Starting live concurrency telemetry monitor (interval=%.1fs)...", interval_sec)
        while True:
            try:
                snapshot = self.collect_snapshot(focus_session_ids=focus_list)
                self.log_snapshot_to_jsonl(snapshot)
                self.generate_markdown_report(snapshot)

                iterations += 1
                if max_iterations is not None and iterations >= max_iterations:
                    logger.info("Completed %d telemetry monitor iterations.", iterations)
                    break

                time.sleep(interval_sec)
            except KeyboardInterrupt:
                logger.info("Telemetry monitor halted by user interrupt.")
                break
            except (sqlite3.Error, OSError, json.JSONDecodeError) as e:
                logger.error("Error in telemetry monitoring loop: %s", e)
                time.sleep(interval_sec)


def main() -> None:
    """CLI entrypoint for WALmaster telemetry monitor."""
    parser = argparse.ArgumentParser(description="WALmaster Live Concurrency Monitor")
    parser.add_argument("--once", action="store_true", help="Capture a single telemetry snapshot and exit")
    parser.add_argument("--interval", type=float, default=5.0, help="Sampling interval in seconds (default: 5.0)")
    parser.add_argument("--iterations", type=int, default=None, help="Max iterations before exiting")
    parser.add_argument("--session", type=str, default="55b5eb19-b6c0-49fd-8958-84d0b52c7987", help="Focus session ID")
    parser.add_argument("--json", action="store_true", help="Print snapshot JSON to stdout")

    args = parser.parse_args()

    monitor = LiveConcurrencyMonitor()

    if args.once:
        snapshot = monitor.collect_snapshot(focus_session_ids=[args.session])
        monitor.log_snapshot_to_jsonl(snapshot)
        report = monitor.generate_markdown_report(snapshot)
        if args.json:
            print(json.dumps(asdict(snapshot), indent=2))
        else:
            print(report)
    else:
        monitor.run_monitoring_loop(
            interval_sec=args.interval,
            max_iterations=args.iterations,
            focus_session_id=args.session,
        )


if __name__ == "__main__":
    main()
