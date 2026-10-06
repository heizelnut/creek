"""Client library tests, over both transports."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from creek.client import connect
from creek.errors import CommandError
from creek.server import Config, Server


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    instance = Server(Config(ws_addr="127.0.0.1:0", tcp_addr="127.0.0.1:0"))
    await instance.start()
    try:
        yield instance
    finally:
        await instance.close()


async def test_websocket_client(server: Server) -> None:
    async with await connect(f"ws://127.0.0.1:{server.ws_port}/ws") as client:
        assert await client.ping() == "PONG"
        assert await client.set("k", "v") == "OK"
        assert await client.get("k") == ["str", "v"]
        assert await client.delete("k") == 1
        assert await client.get("k") is None


async def test_tcp_client(server: Server) -> None:
    async with await connect(f"tcp://127.0.0.1:{server.tcp_port}") as client:
        assert await client.ping() == "PONG"
        await client.set("n", "41", type="i32")
        assert await client.execute("INCR", "n") == 42


async def test_client_typed_set(server: Server) -> None:
    async with await connect(f"ws://127.0.0.1:{server.ws_port}/ws") as client:
        await client.set("temp", "21.5", type="f32", ex=60)
        assert await client.get("temp") == ["f32", "21.5"]
        assert await client.execute("TTL", "temp") in (59, 60)


async def test_client_error(server: Server) -> None:
    async with await connect(f"ws://127.0.0.1:{server.ws_port}/ws") as client:
        with pytest.raises(CommandError) as info:
            await client.execute("NOPE")
        assert info.value.code == "ERR"


async def test_tcp_error_is_consumed(server: Server) -> None:
    # Regression: a TCP error reply must be consumed, not re-raised forever.
    async with await connect(f"tcp://127.0.0.1:{server.tcp_port}") as client:
        with pytest.raises(CommandError):
            await client.execute("NOPE")
        assert await client.ping() == "PONG"


async def test_client_pubsub(server: Server) -> None:
    async with (
        await connect(f"ws://127.0.0.1:{server.ws_port}/ws") as subscriber,
        await connect(f"ws://127.0.0.1:{server.ws_port}/ws") as publisher,
    ):
        received: list[tuple[str, str, str]] = []

        async def collect() -> None:
            async for message in subscriber.subscribe("news"):
                received.append(message)
                return

        task = asyncio.create_task(collect())
        await asyncio.sleep(0.05)  # let the subscription register
        await publisher.publish("news", "hello")
        await asyncio.wait_for(task, 1.0)
        assert received == [("news", "str", "hello")]
