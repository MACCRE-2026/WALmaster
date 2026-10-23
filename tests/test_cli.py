"""Tests for WALmaster unified CLI."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from cli import build_parser, main


def test_cli_parser_commands() -> None:
    parser = build_parser()

    # Test guard command parsing
    args = parser.parse_args(["guard", "--interval", "2.0", "--max-wal-frames", "300"])
    assert args.command == "guard"
    assert args.interval == 2.0
    assert args.max_wal_frames == 300

    # Test heal command parsing
    args = parser.parse_args(["heal", "some-uuid"])
    assert args.command == "heal"
    assert args.session == "some-uuid"

    # Test sync command parsing
    args = parser.parse_args(["sync", "--all"])
    assert args.command == "sync"
    assert args.all is True

    # Test status command parsing
    args = parser.parse_args(["status"])
    assert args.command == "status"


def test_cli_status_execution(tmp_path: Path) -> None:
    conv_dir = tmp_path / "conversations"
    conv_dir.mkdir()
    presence_dir = tmp_path / "presence"
    presence_dir.mkdir()

    with patch("sys.argv", ["walmaster", "status"]):
        exit_code = main()
        assert exit_code == 0
