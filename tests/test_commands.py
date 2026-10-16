"""Command-stream tests: parsing, applying writes, and acknowledgement."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any

import pytest

from creek.commands import CommandConsumer, CommandError, parse_entry
from creek.mirror import MemoryMirror
from creek.plc import Connection, FakeDriver, PlcConfig, PlcManager, Tag
from creek.value import Value
from support import FakeRedis


def _connection() -> Connection:
    return Connection(name="line1", driver="fake", host="127.0.0.1", port=0)


def _manager() -> tuple[PlcManager, FakeDriver]:
    driver = FakeDriver({"HR0": Value.parse("u16", "1500")})
    config = PlcConfig(
        connections={"line1": _connection()},
        tags=[
            Tag("speed", "line1", "HR0", "u16", writable=True, scale=0.1),
            Tag("temp", "line1", "HR1", "u16"),
        ],
    )
    manager = PlcManager(config, MemoryMirror(), drivers={"fake": lambda connection: driver})
    manager.start()
    return manager, driver


# --- parsing --------------------------------------------------------------


def test_parse_entry() -> None:
    command = parse_entry("1-0", {"key": "speed", "value": "20"})
    assert (command.key, command.value, command.id) == ("speed", "20", "1-0")


def test_parse_entry_decodes_bytes() -> None:
    command = parse_entry(b"2-0", {b"key": b"speed", b"value": b"20"})
    assert (command.key, command.value) == ("speed", "20")


def test_parse_entry_requires_key() -> None:
    with pytest.raises(CommandError):
        parse_entry("1-0", {"value": "20"})


def test_parse_entry_requires_value() -> None:
    with pytest.raises(CommandError):
        parse_entry("1-0", {"key": "speed"})


# --- handling -------------------------------------------------------------


async def test_handle_applies_and_acks() -> None:
    manager, driver = _manager()
    redis = FakeRedis()
    consumer = CommandConsumer(manager, redis)
    try:
        await consumer.handle("1-0", {"key": "speed", "value": "20"})
        # engineering 20.0 -> raw 200 (scale 0.1)
        assert ("HR0", Value.parse("u16", "200")) in driver.writes
        assert consumer.writes == 1
        assert consumer.failures == 0
        assert "1-0" in redis.acked
    finally:
        await manager.close()


async def test_handle_refreshes_mirror() -> None:
    manager, _ = _manager()
    redis = FakeRedis()
    consumer = CommandConsumer(manager, redis)
    try:
        await consumer.handle("1-0", {"key": "speed", "value": "20"})
        mirrored = manager.connections["line1"].poller.mirror
        assert isinstance(mirrored, MemoryMirror)
        assert mirrored.values["speed"].data == pytest.approx(20.0)
    finally:
        await manager.close()


async def test_handle_rejects_unknown_tag(caplog: pytest.LogCaptureFixture) -> None:
    manager, _ = _manager()
    consumer = CommandConsumer(manager, FakeRedis())
    try:
        with caplog.at_level(logging.WARNING, logger="creek.audit"):
            await consumer.handle("2-0", {"key": "nope", "value": "1"})
        assert consumer.writes == 0
        assert consumer.failures == 1
        assert any("rejected" in record.message for record in caplog.records)
    finally:
        await manager.close()


async def test_handle_rejects_read_only_tag() -> None:
    manager, _ = _manager()
    consumer = CommandConsumer(manager, FakeRedis())
    try:
        await consumer.handle("3-0", {"key": "temp", "value": "5"})
        assert consumer.failures == 1
    finally:
        await manager.close()


async def test_handle_acks_malformed_entries() -> None:
    manager, _ = _manager()
    redis = FakeRedis()
    consumer = CommandConsumer(manager, redis)
    try:
        await consumer.handle("4-0", {"nope": "x"})
        assert "4-0" in redis.acked
        assert consumer.failures == 1
    finally:
        await manager.close()


async def test_success_is_audited(caplog: pytest.LogCaptureFixture) -> None:
    manager, _ = _manager()
    consumer = CommandConsumer(manager, FakeRedis())
    try:
        with caplog.at_level(logging.INFO, logger="creek.audit"):
            await consumer.handle("5-0", {"key": "speed", "value": "20"})
        records = [record for record in caplog.records if record.name == "creek.audit"]
        assert records
        assert records[-1].key == "speed"
    finally:
        await manager.close()


# --- the read loop --------------------------------------------------------


async def test_consumer_drains_the_stream() -> None:
    manager, driver = _manager()
    redis = FakeRedis()
    redis.add("plc:commands", {"key": "speed", "value": "20"})
    consumer = CommandConsumer(manager, redis, consumer="c1", block_ms=10)
    await consumer.start()
    try:
        for _ in range(200):
            if consumer.writes:
                break
            await asyncio.sleep(0.01)
        assert consumer.writes == 1
        assert ("HR0", Value.parse("u16", "200")) in driver.writes
    finally:
        await consumer.stop()
        await manager.close()


async def test_start_tolerates_existing_group() -> None:
    manager, _ = _manager()
    redis = FakeRedis()
    await redis.xgroup_create("plc:commands", "creek-plc")
    consumer = CommandConsumer(manager, redis, block_ms=10)
    await consumer.start()
    await consumer.stop()
    await manager.close()


async def test_consumer_survives_redis_errors() -> None:
    class FlakyRedis(FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        async def xreadgroup(
            self,
            groupname: str,
            consumername: str,
            streams: dict[str, str],
            count: int | None = None,
            block: int | None = None,
        ) -> list[Any]:
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("redis down")
            return await super().xreadgroup(groupname, consumername, streams, count, block)

    manager, _ = _manager()
    redis = FlakyRedis()
    redis.add("plc:commands", {"key": "speed", "value": "20"})
    consumer = CommandConsumer(manager, redis, block_ms=10)
    await consumer.start()
    try:
        for _ in range(200):
            if consumer.writes:
                break
            await asyncio.sleep(0.01)
        assert consumer.writes == 1
    finally:
        await consumer.stop()
        await manager.close()


def test_command_error_is_a_creek_error() -> None:
    from creek.errors import CreekError

    assert issubclass(CommandError, CreekError)


def test_handle_accepts_mapping() -> None:
    # A Mapping (not just dict) must work.
    fields: Mapping[str, str] = {"key": "speed", "value": "1"}
    assert parse_entry("1-0", fields).key == "speed"
