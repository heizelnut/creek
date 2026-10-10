"""PLC manager, commands, and quality tests (M9d/M9e)."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence
from pathlib import Path

import pytest

from creek.auth import Identity
from creek.client import connect
from creek.commands import Context, execute
from creek.errors import CommandError
from creek.plc import (
    Connection,
    FakeDriver,
    ModbusSimulator,
    PlcConfig,
    PlcManager,
    Poller,
    Tag,
    parse_tag_value,
)
from creek.protocol import Command, SimpleString
from creek.pubsub import Bus
from creek.server import Config, Server
from creek.store import Store
from creek.value import Value


def _connection(*, interval: float = 1.0, **params: int | str) -> Connection:
    return Connection(
        name="line1",
        driver="fake",
        host="127.0.0.1",
        port=0,
        namespace="line1",
        poll_interval=interval,
        params=dict(params),
    )


def _manager(clock) -> tuple[PlcManager, FakeDriver]:
    driver = FakeDriver({"HR0": Value.parse("u16", "1500"), "HR1": Value.parse("u16", "2")})
    config = PlcConfig(
        connections={"line1": _connection()},
        tags=[
            Tag("speed", "line1", "HR0", "u16", writable=True, scale=0.1),
            Tag("temp", "line1", "HR1", "u16"),
        ],
    )
    manager = PlcManager(
        config, Store(), Bus(), drivers={"fake": lambda connection: driver}, clock=clock
    )
    return manager, driver


def _context(manager: PlcManager) -> Context:
    return Context(identity=Identity("op", "line1", frozenset({"all"})), plc=manager)


# --- commands -------------------------------------------------------------


async def test_plc_list_tags_info(clock) -> None:
    manager, _ = _manager(clock)
    manager.start()
    try:
        await manager.connections["line1"].poller.poll_once()
        context = _context(manager)
        assert await execute(Command("PLC", ("LIST",)), context) == ["line1"]
        assert await execute(Command("PLC", ("TAGS",)), context) == ["speed", "temp"]
        report = await execute(Command("PLC", ("INFO",)), context)
        assert "line1" in report
        assert "link=up" in report
    finally:
        await manager.close()


async def test_plc_write_scales_and_refreshes(clock) -> None:
    manager, driver = _manager(clock)
    manager.start()
    try:
        context = _context(manager)
        # engineering 20.0 -> raw 200 (scale 0.1)
        assert await execute(Command("PLC", ("WRITE", "speed", "20")), context) == SimpleString(
            "OK"
        )
        assert ("HR0", Value.parse("u16", "200")) in driver.writes
        mirrored = manager.connections["line1"].poller.store.get("line1", "speed")
        assert mirrored is not None
        assert mirrored.data == pytest.approx(20.0)
    finally:
        await manager.close()


async def test_plc_write_rejects_non_writable(clock) -> None:
    manager, _ = _manager(clock)
    manager.start()
    try:
        with pytest.raises(CommandError) as info:
            await execute(Command("PLC", ("WRITE", "temp", "5")), _context(manager))
        assert "not writable" in str(info.value)
    finally:
        await manager.close()


async def test_plc_write_unknown_tag(clock) -> None:
    manager, _ = _manager(clock)
    manager.start()
    try:
        with pytest.raises(CommandError):
            await execute(Command("PLC", ("WRITE", "nope", "5")), _context(manager))
    finally:
        await manager.close()


async def test_plc_not_configured() -> None:
    with pytest.raises(CommandError):
        await execute(Command("PLC", ("LIST",)), Context())


async def test_plc_write_is_audited(clock, caplog) -> None:
    manager, _ = _manager(clock)
    manager.start()
    try:
        with caplog.at_level(logging.INFO, logger="creek.audit"):
            await execute(Command("PLC", ("WRITE", "speed", "20")), _context(manager))
        records = [record for record in caplog.records if record.name == "creek.audit"]
        assert records
        assert records[-1].tag == "speed"
    finally:
        await manager.close()


def test_parse_tag_value() -> None:
    assert parse_tag_value(Tag("s", "c", "HR0", "u16", scale=0.1), "20") == Value.parse(
        "u16", "200"
    )
    assert parse_tag_value(Tag("t", "c", "HR1", "u16"), "5") == Value.parse("u16", "5")


# --- quality / staleness --------------------------------------------------


class _FlakyDriver:
    def __init__(self) -> None:
        self.fail = False

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def read(self, tags: Sequence[Tag]) -> dict[str, Value]:
        if self.fail:
            raise ConnectionError("offline")
        return {"speed": Value.parse("u16", "100")}

    async def write(self, tag: Tag, value: Value) -> None: ...


async def test_quality_marks_stale_on_failure(clock) -> None:
    driver = _FlakyDriver()
    connection = _connection(interval=0.01)
    tag = Tag("speed", "line1", "HR0", "u16")
    store, bus = Store(), Bus()
    poller = Poller(connection, [tag], driver, store, bus, clock=clock)

    await poller.poll_once()
    assert poller.link == "up"
    assert store.get("line1", "speed.q") == Value.parse("str", "good")

    driver.fail = True
    poller.start()
    try:
        for _ in range(200):
            if poller.link == "down":
                break
            await asyncio.sleep(0.01)
        assert poller.link == "down"
        assert poller.errors >= 1
        assert store.get("line1", "speed.q") == Value.parse("str", "stale")
        # the last known value is kept, not cleared
        assert store.get("line1", "speed") == Value.parse("u16", "100")
    finally:
        await poller.stop()


async def test_quality_can_be_disabled(clock) -> None:
    connection = _connection(quality="false")
    tag = Tag("speed", "line1", "HR0", "u16")
    store, bus = Store(), Bus()
    poller = Poller(connection, [tag], _FlakyDriver(), store, bus, clock=clock)
    await poller.poll_once()
    assert store.get("line1", "speed.q") is None


# --- end to end through the server ----------------------------------------


async def test_server_serves_plc_tags(tmp_path: Path) -> None:
    simulator = ModbusSimulator()
    await simulator.start()
    simulator.holding[0] = 1500
    config_path = tmp_path / "plc.json"
    config_path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "line1",
                        "driver": "modbus",
                        "host": "127.0.0.1",
                        "port": simulator.port,
                        "namespace": "default",
                        "poll_interval": 0.01,
                    }
                ],
                "tags": [
                    {
                        "key": "motor.speed",
                        "connection": "line1",
                        "address": "HR0",
                        "type": "u16",
                        "scale": 0.1,
                        "writable": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    server = Server(Config(ws_addr="127.0.0.1:0", tcp_addr="127.0.0.1:0", plc_config=config_path))
    await server.start()
    client = await connect(f"tcp://127.0.0.1:{server.tcp_port}")
    try:
        for _ in range(200):
            if await client.get("motor.speed") is not None:
                break
            await asyncio.sleep(0.02)
        assert await client.get("motor.speed") == ["f64", "150.0"]

        assert await client.execute("PLC", "WRITE", "motor.speed", "200") == "OK"
        assert simulator.holding[0] == 2000
        assert await client.get("motor.speed") == ["f64", "200.0"]
    finally:
        await client.close()
        await server.close()
        await simulator.close()
