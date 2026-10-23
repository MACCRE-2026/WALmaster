"""WALmaster Conversation Delta & Activity Tracker.

Tracks changes across conversations (step increments, WAL surges, presence locks)
to determine which sessions were touched during testing sessions.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

CONV_DIR = Path.home() / ".gemini" / "antigravity" / "conversations"
BRAIN_DIR = Path.home() / ".gemini" / "antigravity" / "brain"
SUMMARIES_DB = Path.home() / ".gemini" / "antigravity" / "conversation_summaries.db"
BASELINE_FILE = Path(r"B:\WALmaster\telemetry\baseline_state.json")


@dataclass(slots=True)
class SessionState:
    session_id: str
    title: str
    db_size: int
    wal_size: int
    db_steps: int
    summary_steps: int
    last_modified: str
    has_wal: bool


def capture_current_state() -> dict[str, SessionState]:
    """Capture current state of all conversations."""
    summaries: dict[str, tuple[str, int, str]] = {}
    if SUMMARIES_DB.exists():
        try:
            with sqlite3.connect(f"file:{SUMMARIES_DB.as_posix()}?mode=ro", uri=True) as conn:
                for row in conn.execute("SELECT conversation_id, title, step_count, last_modified_time FROM conversation_summaries"):
                    summaries[row[0]] = (row[1] or "(No Title)", row[2] or 0, str(row[3] or ""))
        except (sqlite3.Error, OSError):
            pass

    state: dict[str, SessionState] = {}
    for db_path in CONV_DIR.glob("*.db"):
        if db_path.name.endswith(".bak"):
            continue
        cid = db_path.stem
        wal_path = CONV_DIR / f"{cid}.db-wal"
        db_sz = db_path.stat().st_size
        wal_sz = wal_path.stat().st_size if wal_path.exists() else 0

        title, sum_steps, lmod = summaries.get(cid, ("(No Title)", -1, ""))

        # Read steps count from DB
        db_steps = -1
        try:
            with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=1.0) as conn:
                r = conn.execute("SELECT count(*) FROM steps").fetchone()
                db_steps = r[0] if r else -1
        except (sqlite3.Error, OSError):
            pass

        state[cid] = SessionState(
            session_id=cid,
            title=title,
            db_size=db_sz,
            wal_size=wal_sz,
            db_steps=db_steps,
            summary_steps=sum_steps,
            last_modified=lmod,
            has_wal=wal_sz > 0,
        )

    return state


def save_baseline(output_path: Path = BASELINE_FILE) -> None:
    """Save current state as the baseline for delta tracking."""
    state = capture_current_state()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    serializable = {k: asdict(v) for k, v in state.items()}
    output_path.write_text(json.dumps(serializable, indent=2), encoding="utf-8")
    print(f"Baseline saved with {len(state)} conversations at {output_path}")


def compare_with_baseline(baseline_path: Path = BASELINE_FILE) -> list[dict[str, Any]]:
    """Compare current state with saved baseline and report touched conversations."""
    if not baseline_path.exists():
        print(f"Baseline file {baseline_path} not found. Capturing baseline now.")
        save_baseline(baseline_path)
        return []

    with open(baseline_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    current = capture_current_state()
    deltas: list[dict[str, Any]] = []

    for cid, cur in current.items():
        base_dict = raw.get(cid)
        if not base_dict:
            deltas.append({
                "session_id": cid,
                "title": cur.title,
                "type": "NEW_SESSION",
                "step_delta": cur.db_steps,
                "wal_delta_bytes": cur.wal_size,
                "current_steps": cur.db_steps,
                "current_wal_bytes": cur.wal_size,
            })
            continue

        step_diff = cur.db_steps - base_dict.get("db_steps", -1)
        wal_diff = cur.wal_size - base_dict.get("wal_size", 0)
        lmod_changed = cur.last_modified != base_dict.get("last_modified", "")

        if step_diff != 0 or wal_diff != 0 or lmod_changed:
            deltas.append({
                "session_id": cid,
                "title": cur.title,
                "type": "TOUCHED",
                "step_delta": step_diff,
                "wal_delta_bytes": wal_diff,
                "current_steps": cur.db_steps,
                "baseline_steps": base_dict.get("db_steps", -1),
                "current_wal_bytes": cur.wal_size,
                "baseline_wal_bytes": base_dict.get("wal_size", 0),
            })

    return deltas


def main() -> None:
    parser = argparse.ArgumentParser(description="WALmaster Conversation Delta Tracker")
    parser.add_argument("--save-baseline", action="store_true", help="Capture and save baseline now")
    parser.add_argument("--diff", action="store_true", help="Compare current state with baseline")

    args = parser.parse_args()

    if args.save_baseline:
        save_baseline()
    elif args.diff:
        deltas = compare_with_baseline()
        print(f"\n--- TOUCHED CONVERSATIONS ({len(deltas)}) ---")
        if not deltas:
            print("No conversations have been modified since baseline.")
        for d in deltas:
            print(f"Session {d['session_id'][:8]} | Steps: {d.get('baseline_steps')} -> {d['current_steps']} (delta {d['step_delta']}) | WAL: {d['current_wal_bytes'] / (1024*1024):.2f} MB | {d['title'][:35]}")
    else:
        # Default behavior: if baseline exists, show diff; else save baseline
        if BASELINE_FILE.exists():
            deltas = compare_with_baseline()
            print(f"Detected {len(deltas)} touched conversations since baseline.")
            for d in deltas:
                print(f"  {d['session_id']}: delta_steps={d['step_delta']} wal_mb={d['current_wal_bytes'] / (1024*1024):.2f} title='{d['title'][:30]}'")
        else:
            save_baseline()


if __name__ == "__main__":
    main()
