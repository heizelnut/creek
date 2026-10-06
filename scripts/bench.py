#!/usr/bin/env python3
"""Baseline load/benchmark harness for Creek.

Starts a Creek server in a child process (so the load generator and the server
don't fight over one event loop), drives it with concurrent clients over either
transport, and reports throughput plus latency percentiles.

    uv run python scripts/bench.py --command ping --clients 10 --requests 1000
    uv run python scripts/bench.py --transport tcp --command set
    uv run python scripts/bench.py --command publish --subscribers 5
    uv run python scripts/bench.py --no-server          # use a running server

Numbers are only comparable on the same machine, with the same client/server
split, and after a warmup. Treat them as a regression baseline, not a promise.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import socket
import subprocess
import sys
import time
from collections.abc import Sequence

import websockets

CRLF = b"\r\n"


def frame(*parts: str) -> bytes:
    """Encode a RESP array command."""
    out = bytearray(f"*{len(parts)}\r\n".encode())
    for part in parts:
        encoded = part.encode()
        out += f"${len(encoded)}\r\n".encode() + encoded + CRLF
    return bytes(out)


async def read_reply(reader: asyncio.StreamReader) -> None:
    """Consume exactly one RESP2 reply from a TCP stream."""
    line = await reader.readline()
    if not line:
        raise EOFError("connection closed")
    kind = line[:1]
    if kind == b"$":
        length = int(line[1:])
        if length >= 0:
            await reader.readexactly(length + 2)
    elif kind == b"*":
        for _ in range(int(line[1:])):
            await read_reply(reader)


def request(command: str, index: int, iteration: int) -> bytes:
    if command == "ping":
        return frame("PING")
    if command == "set":
        return frame("SET", f"bench:{index}", str(iteration))
    if command == "get":
        return frame("GET", "bench:key")
    if command == "publish":
        return frame("PUBLISH", "bench:chan", str(iteration))
    raise ValueError(f"unknown command {command!r}")


async def run_ws_client(
    port: int, command: str, requests: int, index: int, latencies: list[float]
) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as ws:
        for iteration in range(requests):
            started = time.perf_counter()
            await ws.send(request(command, index, iteration))
            await ws.recv()
            latencies.append(time.perf_counter() - started)


async def run_tcp_client(
    port: int, command: str, requests: int, index: int, latencies: list[float]
) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        for iteration in range(requests):
            started = time.perf_counter()
            writer.write(request(command, index, iteration))
            await writer.drain()
            await read_reply(reader)
            latencies.append(time.perf_counter() - started)
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()


async def subscribe_ws(port: int, channel: str) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{port}/ws") as ws:
        await ws.send(frame("SUBSCRIBE", channel))
        await ws.recv()  # confirmation
        with contextlib.suppress(Exception):
            while True:
                await ws.recv()  # drain deliveries so the buffer never fills


async def subscribe_tcp(port: int, channel: str) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(frame("SUBSCRIBE", channel))
        await writer.drain()
        await read_reply(reader)
        with contextlib.suppress(Exception):
            while True:
                await read_reply(reader)
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()


def percentile(sorted_values: Sequence[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    position = (len(sorted_values) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), 0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise SystemExit(f"server did not start on port {port}")


def start_server(args: argparse.Namespace) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "creek",
            "--tcp-addr",
            f"127.0.0.1:{args.tcp_port}",
            "--ws-addr",
            f"127.0.0.1:{args.ws_port}",
            "--log-level",
            "warning",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


async def benchmark(args: argparse.Namespace) -> None:
    port = args.ws_port if args.transport == "ws" else args.tcp_port
    run_client = run_ws_client if args.transport == "ws" else run_tcp_client
    subscribe = subscribe_ws if args.transport == "ws" else subscribe_tcp

    if args.command == "get":
        # Make sure the key exists before measuring reads.
        reader, writer = await asyncio.open_connection("127.0.0.1", args.tcp_port)
        writer.write(frame("SET", "bench:key", "value"))
        await writer.drain()
        await read_reply(reader)
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()

    subscribers: list[asyncio.Task[None]] = []
    if args.command == "publish":
        subscribers = [
            asyncio.create_task(subscribe(port, "bench:chan")) for _ in range(args.subscribers)
        ]
        await asyncio.sleep(0.2)  # let subscriptions register

    latencies: list[float] = []
    started = time.perf_counter()
    await asyncio.gather(
        *(
            run_client(port, args.command, args.requests, index, latencies)
            for index in range(args.clients)
        )
    )
    elapsed = time.perf_counter() - started

    for task in subscribers:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    latencies.sort()
    total = args.clients * args.requests
    print(
        f"transport={args.transport} command={args.command} clients={args.clients} requests={total}"
    )
    if args.command == "publish":
        print(f"subscribers={args.subscribers}")
    print(f"throughput : {total / elapsed:,.0f} ops/s")
    print(f"elapsed    : {elapsed:.3f} s")
    for label, fraction in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99), ("max", 1.0)):
        print(f"latency {label}: {percentile(latencies, fraction) * 1000:.3f} ms")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Creek benchmark harness")
    parser.add_argument("--transport", choices=("ws", "tcp"), default="ws")
    parser.add_argument("--command", choices=("ping", "set", "get", "publish"), default="ping")
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--requests", type=int, default=1000, help="per client")
    parser.add_argument("--subscribers", type=int, default=5, help="for --command publish")
    parser.add_argument("--ws-port", type=int, default=17100)
    parser.add_argument("--tcp-port", type=int, default=17101)
    parser.add_argument("--no-server", action="store_true", help="use a running server")
    args = parser.parse_args(argv)

    server = None
    if not args.no_server:
        server = start_server(args)
        wait_for_port(args.ws_port if args.transport == "ws" else args.tcp_port)
    try:
        asyncio.run(benchmark(args))
    finally:
        if server is not None:
            server.terminate()
            server.wait(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
