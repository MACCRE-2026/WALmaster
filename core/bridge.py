"""WALmaster to CRSDL-MACCRE Bridge Module.

Provides deterministic session healing, presence lock reaping, two-tier
reconciliation, and seamless drop-in integration with CRSDL-MACCRE recovery tooling.
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any

from core.healer import HealResult, SovereignSessionHealer

logger = logging.getLogger("walmaster.bridge")

DEFAULT_CONV_DIR = Path.home() / ".gemini" / "antigravity" / "conversations"
DEFAULT_BRAIN_DIR = Path.home() / ".gemini" / "antigravity" / "brain"
DEFAULT_PRESENCE_DIR = Path.home() / ".gemini" / "antigravity" / "presence"
DEFAULT_SUMMARIES_DB = Path.home() / ".gemini" / "antigravity" / "conversation_summaries.db"
DEFAULT_SCRATCH_DIR = Path(r"B:\WALmaster\.development\artifacts\scratch")


def heal_and_restore_for_maccre(
    session_id: str,
    *,
    conversations_dir: Path | str | None = None,
    brain_dir: Path | str | None = None,
    presence_dir: Path | str | None = None,
    summaries_db: Path | str | None = None,
    live_apply: bool = True,
    backup: bool = True,
    scratch_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Heal a session using WALmaster and return a structured dictionary.

    Compatible with CRSDL-MACCRE recovery result format.
    """
    pure_id = session_id.replace(".db", "").strip()
    conv_path = Path(conversations_dir) if conversations_dir else DEFAULT_CONV_DIR
    b_path = Path(brain_dir) if brain_dir else DEFAULT_BRAIN_DIR
    p_path = Path(presence_dir) if presence_dir else DEFAULT_PRESENCE_DIR
    s_db = Path(summaries_db) if summaries_db else DEFAULT_SUMMARIES_DB
    sc_dir = Path(scratch_dir) if scratch_dir else DEFAULT_SCRATCH_DIR

    healer = SovereignSessionHealer(
        conversations_dir=conv_path,
        brain_dir=b_path,
        presence_dir=p_path,
        summaries_db=s_db,
    )

    result: HealResult = healer.heal_session(
        pure_id,
        live_apply=live_apply,
        backup=backup,
        scratch_dir=sc_dir,
    )

    return {
        "session_id": result.session_id,
        "success": result.success,
        "integrity_ok": result.integrity_ok,
        "total_steps": result.total_steps,
        "salvaged_steps": result.salvaged_steps,
        "synthesized_steps": result.synthesized_steps,
        "presence_lock_reaped": result.presence_lock_reaped,
        "summaries_upserted": result.summaries_upserted,
        "purged_conflicts": result.purged_conflicts,
        "backup_path": str(result.backup_path) if result.backup_path else None,
        "diagnostics": result.diagnostics,
    }


def scan_and_heal_all_corrupted(
    *,
    conversations_dir: Path | str | None = None,
    brain_dir: Path | str | None = None,
    presence_dir: Path | str | None = None,
    summaries_db: Path | str | None = None,
    live_apply: bool = True,
    backup: bool = True,
    scratch_dir: Path | str | None = None,
) -> list[dict[str, Any]]:
    """Scan conversations directory for all corrupted or inconsistent databases and heal them."""
    conv_path = Path(conversations_dir) if conversations_dir else DEFAULT_CONV_DIR
    if not conv_path.exists():
        logger.warning("Conversations directory does not exist: %s", conv_path)
        return []

    corrupted_ids: list[str] = []
    for db_file in conv_path.glob("*.db"):
        if db_file.name.endswith(".corrupt.bak") or db_file.name.endswith(".bak"):
            continue
        try:
            with sqlite3.connect(f"file:{db_file.as_posix()}?mode=ro", uri=True, timeout=2.0) as conn:
                chk = conn.execute("PRAGMA integrity_check").fetchall()
                if not (len(chk) == 1 and chk[0][0].lower() == "ok"):
                    corrupted_ids.append(db_file.stem)
        except (sqlite3.Error, OSError):
            corrupted_ids.append(db_file.stem)

    logger.info("Discovered %d corrupted databases out of total scans.", len(corrupted_ids))

    results: list[dict[str, Any]] = []
    for sid in corrupted_ids:
        res = heal_and_restore_for_maccre(
            sid,
            conversations_dir=conv_path,
            brain_dir=brain_dir,
            presence_dir=presence_dir,
            summaries_db=summaries_db,
            live_apply=live_apply,
            backup=backup,
            scratch_dir=scratch_dir,
        )
        results.append(res)

    return results


def patch_maccre_recovery() -> bool:
    """Monkey-patch CRSDL-MACCRE's restore_conversation to use WALmaster SovereignSessionHealer."""
    try:
        import maccre_core.transcripts.recovery as m_rec

        def patched_restore_conversation(
            session_id: str,
            live_apply: bool = True,
            backup: bool = True,
            **kwargs: Any,
        ) -> Any:
            logger.info("WALmaster bridge intercepting maccre_core restore_conversation for %s", session_id)
            res = heal_and_restore_for_maccre(
                session_id,
                live_apply=live_apply,
                backup=backup,
            )
            # If original function exists and caller expects its return object or datacenter side-effects,
            # we can run original in passive mode or populate expected structure
            return res

        m_rec.restore_conversation = patched_restore_conversation  # type: ignore[assignment]
        logger.info("Successfully patched maccre_core.transcripts.recovery.restore_conversation with WALmaster bridge.")
        return True
    except ImportError:
        logger.warning("maccre_core is not in python path; skipping runtime monkey-patch.")
        return False


def main() -> None:
    """CLI entrypoint for WALmaster bridge."""
    parser = argparse.ArgumentParser(description="WALmaster to CRSDL-MACCRE Bridge")
    parser.add_argument("session_id", nargs="?", help="Specific session ID to heal")
    parser.add_argument("--heal-all", action="store_true", help="Scan and heal all corrupted sessions")
    parser.add_argument("--no-live", action="store_true", help="Do not overwrite live files (dry run)")
    parser.add_argument("--no-backup", action="store_true", help="Do not create backup files")
    parser.add_argument("--json", action="store_true", help="Output result as JSON")

    args = parser.parse_args()

    live_apply = not args.no_live
    backup = not args.no_backup

    if args.heal_all:
        results = scan_and_heal_all_corrupted(live_apply=live_apply, backup=backup)
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            print(f"Healed {len(results)} corrupted sessions.")
            for r in results:
                print(f"Session {r['session_id']}: success={r['success']} steps={r['total_steps']}")
    elif args.session_id:
        result = heal_and_restore_for_maccre(args.session_id, live_apply=live_apply, backup=backup)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"Session {result['session_id']}: success={result['success']} steps={result['total_steps']}")
            for diag in result["diagnostics"]:
                print("  -", diag)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
