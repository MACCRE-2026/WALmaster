"""WALmaster Unified Command-Line Interface.

The sovereign management CLI for SQLite WAL concurrency, autonomous healing,
multi-tier registry synchronization, and active prevention guarding in Google Antigravity.
"""
from __future__ import annotations

import argparse
import logging
import sys

from core.bridge import heal_and_restore_for_maccre, scan_and_heal_all_corrupted
from core.guard import ActivePreventionGuard
from core.summary_sync import sync_all_summaries, sync_conversation_metadata
from telemetry.monitor import LiveConcurrencyMonitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("walmaster.cli")


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for WALmaster CLI."""
    parser = argparse.ArgumentParser(
        prog="walmaster",
        description="Sovereign SQLite WAL Concurrency & Autonomous Recovery Engine for Google Antigravity",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # Command: guard
    guard_parser = subparsers.add_parser("guard", help="Run active background prevention guardian")
    guard_parser.add_argument("--interval", type=float, default=5.0, help="Check interval in seconds (default: 5.0)")
    guard_parser.add_argument("--max-wal-frames", type=int, default=500, help="Max uncheckpointed WAL frames before PASSIVE checkpoint (default: 500)")
    guard_parser.add_argument("--critical-wal-frames", type=int, default=2000, help="Critical WAL frames before RESTART checkpoint (default: 2000)")
    guard_parser.add_argument("--iterations", type=int, default=None, help="Max iterations before exit (default: infinite)")

    # Command: heal
    heal_parser = subparsers.add_parser("heal", help="Heal corrupted conversation databases from transcript event logs")
    heal_parser.add_argument("session", nargs="?", default=None, help="Specific conversation UUID to heal")
    heal_parser.add_argument("--all", action="store_true", help="Scan and heal all corrupted conversations")

    # Command: sync
    sync_parser = subparsers.add_parser("sync", help="Synchronize conversations across all 3 persistence tiers")
    sync_parser.add_argument("session", nargs="?", default=None, help="Specific conversation UUID to sync")
    sync_parser.add_argument("--all", action="store_true", help="Synchronize all displaced conversations in registry")

    # Command: status
    subparsers.add_parser("status", help="Display fleet-wide concurrency, WAL frame metrics, and health telemetry")

    return parser


def main() -> int:
    """Main CLI execution router."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "guard":
        guard = ActivePreventionGuard(
            max_wal_frames=args.max_wal_frames,
            critical_wal_frames=args.critical_wal_frames,
        )
        guard.run_guard_loop(interval_sec=args.interval, max_iterations=args.iterations)
        return 0

    elif args.command == "heal":
        if args.all or not args.session:
            logger.info("Scanning and healing all corrupted conversations across fleet...")
            results = scan_and_heal_all_corrupted()
            logger.info("Fleet scan completed. Healed %d conversations.", len(results))
            return 0
        else:
            logger.info("Healing session: %s", args.session)
            res = heal_and_restore_for_maccre(args.session)
            print(f"Result: success={res['success']}, integrity={res['integrity_ok']}, total_steps={res['total_steps']}")
            return 0 if res["success"] else 1

    elif args.command == "sync":
        if args.all or not args.session:
            logger.info("Executing fleet-wide 3-tier synchronization...")
            res = sync_all_summaries()
            logger.info("Sync complete. Checked: %d, Synchronized: %d", res["total_checked"], res["synchronized"])
            return 0
        else:
            logger.info("Synchronizing conversation: %s", args.session)
            res = sync_conversation_metadata(args.session)
            print(f"Result: project_id={res['project_id']}, tier1={res['tier1_updated']}, tier2={res['tier2_updated']}, tier3={res['tier3_updated']}")
            return 0

    elif args.command == "status":
        monitor = LiveConcurrencyMonitor()
        snapshot = monitor.collect_snapshot()
        report = monitor.generate_markdown_report(snapshot)
        print(report)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
