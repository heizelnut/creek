"""Command-line entry point for the creek-plc daemon."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
from collections.abc import Sequence
from pathlib import Path

from creek import __version__
from creek.daemon import Daemon, RedisConfig
from creek.plc import PlcConfig, PlcConfigError


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser (flags documented in ``AGENTS.md`` §3)."""
    parser = argparse.ArgumentParser(
        prog="creek-plc",
        description="Mirror PLC tags into Redis and forward writes from a Redis Stream.",
    )
    parser.add_argument("--config", type=Path, required=True, help="JSON PLC tag map")
    parser.add_argument(
        "--redis-url", default="redis://127.0.0.1:6379/0", help="Redis connection URL"
    )
    parser.add_argument("--key-prefix", default="plc:", help="Redis key/channel prefix")
    parser.add_argument(
        "--command-stream", default="plc:commands", help="Redis Stream carrying PLC writes"
    )
    parser.add_argument("--consumer-group", default="creek-plc", help="Stream consumer group")
    parser.add_argument(
        "--timestamps", action="store_true", help="also write <key>.ts (unix seconds)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="poll PLCs and log changes; do not touch Redis"
    )
    parser.add_argument("--log-level", default="info", help="debug|info|warning|error")
    parser.add_argument("--version", action="version", version=f"creek-plc {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse flags, run the daemon, and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        config = PlcConfig.from_file(args.config)
    except PlcConfigError as exc:
        parser.error(str(exc))
    redis_config = RedisConfig(
        url=args.redis_url,
        key_prefix=args.key_prefix,
        command_stream=args.command_stream,
        consumer_group=args.consumer_group,
        timestamps=args.timestamps,
    )
    daemon = Daemon(config, redis_config, dry_run=args.dry_run)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_run(daemon))
    return 0


async def _run(daemon: Daemon) -> None:
    await daemon.start()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # pragma: no cover - Windows
            loop.add_signal_handler(sig, daemon.request_stop)
    await daemon.serve_forever()
    await daemon.close()
