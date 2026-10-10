"""PLC foundation tests: tag map, scaling, and polling (M9a)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from creek.plc import Connection, FakeDriver, PlcConfig, PlcConfigError, Poller, Tag, apply_scale
from creek.protocol import encode_message
from creek.pubsub import Bus, Subscriber
from creek.store import Store
from creek.value import Value


def _tag(key: str, address: str, type: str = "u16", **kwargs: object) -> Tag:
    return Tag(key=key, connection="line1", address=address, type=type, **kwargs)  # type: ignore[arg-type]


def _connection(**kwargs: object) -> Connection:
    defaults: dict[str, object] = {
        "name": "line1",
        "driver": "modbus",
        "host": "127.0.0.1",
        "port": 502,
        "namespace": "line1",
    }
    defaults.update(kwargs)
    return Connection(**defaults)  # type: ignore[arg-type]


# --- config ---------------------------------------------------------------


def test_config_from_file(tmp_path: Path) -> None:
    path = tmp_path / "plc.json"
    path.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "line1",
                        "driver": "modbus",
                        "host": "10.0.0.10",
                        "port": 502,
                        "namespace": "line1",
                        "poll_interval": 0.5,
                        "params": {"unit": 1},
                    }
                ],
                "tags": [
                    {
                        "key": "speed",
                        "connection": "line1",
                        "address": "HR0",
                        "type": "u16",
                        "scale": 0.1,
                    },
                    {
                        "key": "run",
                        "connection": "line1",
                        "address": "C0",
                        "type": "bool",
                        "writable": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    config = PlcConfig.from_file(path)
    assert config.connections["line1"].port == 502
    assert config.connections["line1"].params == {"unit": 1}
    assert config.tags[0].scale == 0.1
    assert config.tags[1].writable is True
    assert [tag.key for tag in config.tags_for("line1")] == ["speed", "run"]


def test_config_rejects_unknown_connection(tmp_path: Path) -> None:
    path = tmp_path / "plc.json"
    path.write_text(
        json.dumps(
            {"connections": [], "tags": [{"key": "x", "connection": "nope", "address": "HR0"}]}
        ),
        encoding="utf-8",
    )
    with pytest.raises(PlcConfigError):
        PlcConfig.from_file(path)


def test_config_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(PlcConfigError):
        PlcConfig.from_file(tmp_path / "missing.json")


# --- scaling --------------------------------------------------------------


def test_apply_scale() -> None:
    raw = Value.parse("u16", "1500")
    assert apply_scale(_tag("s", "HR0"), raw) == raw  # unscaled passes through
    scaled = apply_scale(_tag("s", "HR0", scale=0.1), raw)
    assert scaled.type.tag == "f64"
    assert scaled.data == pytest.approx(150.0)
    offset = apply_scale(_tag("s", "HR0", offset=-10.0), raw)
    assert offset.data == pytest.approx(1490.0)


def test_apply_scale_ignores_non_numeric() -> None:
    flag = Value.parse("bool", "true")
    assert apply_scale(_tag("f", "C0", scale=2.0), flag) == flag


# --- polling --------------------------------------------------------------


async def test_poll_mirrors_into_store_and_publishes() -> None:
    store, bus = Store(), Bus()
    subscriber = Subscriber("line1")
    bus.subscribe(subscriber, "speed")
    driver = FakeDriver({"HR0": Value.parse("u16", "1500")})
    poller = Poller(_connection(), [_tag("speed", "HR0")], driver, store, bus)

    changed = await poller.poll_once()

    assert changed["speed"] == Value.parse("u16", "1500")
    assert store.get("line1", "speed") == Value.parse("u16", "1500")
    assert await subscriber.next_frame() == encode_message("speed", "u16", "1500")


async def test_poll_publishes_only_changes() -> None:
    store, bus = Store(), Bus()
    subscriber = Subscriber("line1")
    bus.subscribe(subscriber, "speed")
    driver = FakeDriver({"HR0": Value.parse("u16", "1500")})
    poller = Poller(_connection(), [_tag("speed", "HR0")], driver, store, bus)

    await poller.poll_once()
    assert await subscriber.next_frame() == encode_message("speed", "u16", "1500")

    assert await poller.poll_once() == {}  # unchanged
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(subscriber.next_frame(), 0.02)


async def test_poll_applies_scale_before_storing() -> None:
    store, bus = Store(), Bus()
    driver = FakeDriver({"HR0": Value.parse("u16", "1500")})
    poller = Poller(_connection(), [_tag("speed", "HR0", scale=0.1)], driver, store, bus)
    await poller.poll_once()
    stored = store.get("line1", "speed")
    assert stored is not None
    assert stored.type.tag == "f64"
    assert stored.data == pytest.approx(150.0)


async def test_poller_loop_survives_errors() -> None:
    class FlakyDriver:
        def __init__(self) -> None:
            self.calls = 0

        async def connect(self) -> None: ...

        async def close(self) -> None: ...

        async def read(self, tags):  # type: ignore[no-untyped-def]
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("plc offline")
            return {"speed": Value.parse("u16", "7")}

        async def write(self, tag, value): ...  # type: ignore[no-untyped-def]

    store, bus = Store(), Bus()
    driver = FlakyDriver()
    poller = Poller(_connection(), [_tag("speed", "HR0")], driver, store, bus, interval=0.01)
    poller.start()
    try:
        for _ in range(100):
            if store.get("line1", "speed") is not None:
                break
            await asyncio.sleep(0.01)
        assert store.get("line1", "speed") == Value.parse("u16", "7")
        assert poller.errors == 1
    finally:
        await poller.stop()


# --- driver ---------------------------------------------------------------


async def test_fake_driver_read_and_write() -> None:
    driver = FakeDriver({"HR0": Value.parse("u16", "1")})
    await driver.connect()
    assert driver.connected is True
    tag = _tag("speed", "HR0")
    assert await driver.read([tag]) == {"speed": Value.parse("u16", "1")}
    await driver.write(tag, Value.parse("u16", "2"))
    assert await driver.read([tag]) == {"speed": Value.parse("u16", "2")}
    assert driver.writes == [("HR0", Value.parse("u16", "2"))]
