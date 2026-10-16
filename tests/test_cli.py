"""CLI parsing tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from creek.cli import build_parser, main


def test_config_is_required() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


def test_defaults() -> None:
    args = build_parser().parse_args(["--config", "plc.json"])
    assert args.redis_url == "redis://127.0.0.1:6379/0"
    assert args.key_prefix == "plc:"
    assert args.command_stream == "plc:commands"
    assert args.consumer_group == "creek-plc"
    assert args.timestamps is False
    assert args.dry_run is False


def test_overrides() -> None:
    args = build_parser().parse_args(
        [
            "--config",
            "plc.json",
            "--redis-url",
            "redis://redis:6379/2",
            "--key-prefix",
            "plant:",
            "--command-stream",
            "plant:cmds",
            "--consumer-group",
            "g",
            "--timestamps",
            "--dry-run",
        ]
    )
    assert args.redis_url == "redis://redis:6379/2"
    assert args.key_prefix == "plant:"
    assert args.command_stream == "plant:cmds"
    assert args.consumer_group == "g"
    assert args.timestamps is True
    assert args.dry_run is True


def test_missing_config_exits_two(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as info:
        main(["--config", str(tmp_path / "missing.json")])
    assert info.value.code == 2
