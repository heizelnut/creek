"""Command-line entry point for the Creek server."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import re
import signal
from collections.abc import Sequence
from pathlib import Path

from creek import __version__
from creek.cluster import Cluster
from creek.server import Config, Server

_DURATION = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|h)?$")
_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(text: str) -> float:
    """Parse ``300``, ``5m``, ``500ms``, ``2h`` into seconds."""
    match = _DURATION.match(text.strip())
    if match is None:
        raise ValueError(f"invalid duration {text!r}")
    return float(match.group(1)) * _UNITS[match.group(2) or "s"]


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser (flags documented in ``AGENTS.md`` §3)."""
    parser = argparse.ArgumentParser(prog="creek", description="Creek server")
    parser.add_argument("--ws-addr", default=":7100", help="WebSocket + static listener")
    parser.add_argument("--tcp-addr", default=":7101", help="RESP (TCP) listener")
    parser.add_argument("--web-dir", type=Path, default=None, help="static files served at /")
    parser.add_argument("--data-dir", type=Path, default=None, help="WAL and snapshot directory")
    parser.add_argument("--auth-file", type=Path, default=None, help="JSON token file")
    parser.add_argument(
        "--fsync", choices=("always", "everysec", "no"), default="everysec", help="WAL fsync policy"
    )
    parser.add_argument("--snapshot-interval", default="5m", help="snapshot cadence (e.g. 5m)")
    parser.add_argument("--replicaof", default=None, help="follow a primary (host:port)")
    parser.add_argument("--replica-token", default=None, help="AUTH token for the primary")
    parser.add_argument("--cluster-config", type=Path, default=None, help="JSON slot topology")
    parser.add_argument("--plc-config", type=Path, default=None, help="JSON PLC tag map")
    parser.add_argument(
        "--auto-failover-after",
        type=float,
        default=None,
        help="seconds without a primary before a replica promotes itself",
    )
    parser.add_argument("--log-level", default="info", help="debug|info|warning|error")
    parser.add_argument("--version", action="version", version=f"creek {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse flags, run the server, and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        snapshot_interval = parse_duration(args.snapshot_interval)
    except ValueError as exc:
        parser.error(str(exc))
    replicaof: tuple[str, int, str | None] | None = None
    if args.replicaof is not None:
        host, _, port_text = args.replicaof.rpartition(":")
        try:
            replicaof = (host or "127.0.0.1", int(port_text), args.replica_token)
        except ValueError:
            parser.error(f"invalid --replicaof {args.replicaof!r}")
    cluster = Cluster.from_file(args.cluster_config) if args.cluster_config else None
    config = Config(
        ws_addr=args.ws_addr,
        tcp_addr=args.tcp_addr,
        web_dir=args.web_dir,
        data_dir=args.data_dir,
        auth_file=args.auth_file,
        fsync=args.fsync,
        snapshot_interval=snapshot_interval,
        replicaof=replicaof,
        auto_failover_after=args.auto_failover_after,
        cluster=cluster,
        plc_config=args.plc_config,
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_run(config))
    return 0


async def _run(config: Config) -> None:
    server = Server(config)
    await server.start()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # pragma: no cover - Windows
            loop.add_signal_handler(sig, server.request_stop)
    logging.getLogger("creek").info("creek ready: ws=:%d tcp=:%d", server.ws_port, server.tcp_port)
    await server.serve_forever()
    await server.close()
