"""End-to-end daemon tests against a Modbus simulator and a fake Redis."""

from __future__ import annotations

import asyncio

from creek.daemon import Daemon, RedisConfig
from creek.mirror import MemoryMirror
from creek.plc import Connection, ModbusSimulator, PlcConfig, Tag
from support import FakeRedis


def _config(simulator: ModbusSimulator) -> PlcConfig:
    return PlcConfig(
        connections={
            "line1": Connection(
                name="line1",
                driver="modbus",
                host="127.0.0.1",
                port=simulator.port,
                poll_interval=0.01,
            )
        },
        tags=[
            Tag(
                key="motor.speed",
                connection="line1",
                address="HR0",
                type="u16",
                writable=True,
                scale=0.1,
            )
        ],
    )


async def _wait_for(predicate, *, attempts: int = 300) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


async def test_daemon_mirrors_and_writes() -> None:
    simulator = ModbusSimulator()
    await simulator.start()
    simulator.holding[0] = 1500
    redis = FakeRedis()
    daemon = Daemon(_config(simulator), RedisConfig(), redis=redis)
    await daemon.start()
    try:
        await _wait_for(lambda: redis.strings.get("plc:motor.speed") == "150.0")
        assert redis.strings["plc:motor.speed.q"] == "good"
        assert ("plc:motor.speed", "150.0") in redis.published

        redis.add("plc:commands", {"key": "motor.speed", "value": "200"})
        await _wait_for(lambda: simulator.holding[0] == 2000)
        await _wait_for(lambda: redis.strings.get("plc:motor.speed") == "200.0")
    finally:
        await daemon.close()
        await simulator.close()


async def test_daemon_closes_injected_redis() -> None:
    redis = FakeRedis()
    daemon = Daemon(PlcConfig(connections={}, tags=[]), RedisConfig(), redis=redis)
    await daemon.start()
    await daemon.close()
    assert redis.closed is True


async def test_daemon_dry_run_uses_memory_mirror() -> None:
    daemon = Daemon(PlcConfig(connections={}, tags=[]), RedisConfig(), dry_run=True)
    await daemon.start()
    try:
        assert isinstance(daemon.mirror, MemoryMirror)
        assert daemon.consumer is None
    finally:
        await daemon.close()


async def test_serve_forever_stops_on_request() -> None:
    daemon = Daemon(PlcConfig(connections={}, tags=[]), RedisConfig(), dry_run=True)
    await daemon.start()
    task = asyncio.create_task(daemon.serve_forever())
    daemon.request_stop()
    await asyncio.wait_for(task, 1.0)
    await daemon.close()


def test_default_consumer_name_is_process_specific() -> None:
    from creek.daemon import default_consumer_name

    name = default_consumer_name()
    assert "-" in name
    assert str(__import__("os").getpid()) in name
