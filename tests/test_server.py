"""Integration tests: both listeners on ephemeral ports.

Both transports carry the same RESP2 bytes, so the assertions look alike.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
import websockets

from creek.server import Config, Server


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    instance = Server(Config(ws_addr="127.0.0.1:0", tcp_addr="127.0.0.1:0"))
    await instance.start()
    try:
        yield instance
    finally:
        await instance.close()


async def test_tcp_ping(server: Server) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", server.tcp_port)
    try:
        writer.write(b"PING\r\n")
        await writer.drain()
        assert await reader.readuntil(b"\r\n") == b"+PONG\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


async def test_tcp_bulk_echo(server: Server) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", server.tcp_port)
    try:
        writer.write(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
        await writer.drain()
        assert await reader.readuntil(b"\r\n") == b"$5\r\n"
        assert await reader.readexactly(7) == b"hello\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


async def test_tcp_pipelined_commands(server: Server) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", server.tcp_port)
    try:
        writer.write(b"PING\r\nPING\r\n")
        await writer.drain()
        assert await reader.readuntil(b"\r\n") == b"+PONG\r\n"
        assert await reader.readuntil(b"\r\n") == b"+PONG\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


async def test_tcp_unknown_command_returns_error(server: Server) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", server.tcp_port)
    try:
        writer.write(b"NOPE\r\n")
        await writer.drain()
        assert (await reader.readuntil(b"\r\n")).startswith(b"-ERR unknown command")
    finally:
        writer.close()
        await writer.wait_closed()


async def test_ws_ping(server: Server) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{server.ws_port}/ws") as ws:
        await ws.send(b"PING\r\n")
        assert await ws.recv() == b"+PONG\r\n"


async def test_ws_bulk_echo(server: Server) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{server.ws_port}/ws") as ws:
        await ws.send(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
        assert await ws.recv() == b"$5\r\nhello\r\n"


async def test_ws_unknown_command(server: Server) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{server.ws_port}/ws") as ws:
        await ws.send(b"NOPE\r\n")
        assert (await ws.recv()).startswith(b"-ERR unknown command")


async def test_ws_json_is_not_special(server: Server) -> None:
    # There is no JSON layer: a JSON blob is just an unknown inline command.
    async with websockets.connect(f"ws://127.0.0.1:{server.ws_port}/ws") as ws:
        await ws.send(b'{"cmd":"PING"}')
        assert (await ws.recv()).startswith(b"-ERR unknown command")


async def test_ws_quit_closes(server: Server) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{server.ws_port}/ws") as ws:
        await ws.send(b"QUIT\r\n")
        assert await ws.recv() == b"+OK\r\n"
        with pytest.raises(websockets.exceptions.ConnectionClosed):
            await ws.recv()


async def test_store_is_shared_across_connections_and_transports(server: Server) -> None:
    # Write over TCP ...
    reader, writer = await asyncio.open_connection("127.0.0.1", server.tcp_port)
    try:
        writer.write(b"SET shared hello\r\n")
        await writer.drain()
        assert await reader.readuntil(b"\r\n") == b"+OK\r\n"
    finally:
        writer.close()
        await writer.wait_closed()
    # ... read it back over WebSocket.
    async with websockets.connect(f"ws://127.0.0.1:{server.ws_port}/ws") as ws:
        await ws.send(b"GET shared\r\n")
        assert await ws.recv() == b"*2\r\n$3\r\nstr\r\n$5\r\nhello\r\n"


async def test_tcp_typed_value_and_ttl(server: Server) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", server.tcp_port)
    try:
        writer.write(b"SET temp 21.5 TYPE f32 EX 60\r\n")
        await writer.drain()
        assert await reader.readuntil(b"\r\n") == b"+OK\r\n"
        writer.write(b"TTL temp\r\n")
        await writer.drain()
        ttl = await reader.readuntil(b"\r\n")
        assert ttl.startswith(b":")
        assert int(ttl[1:].strip()) in (59, 60)
    finally:
        writer.close()
        await writer.wait_closed()
