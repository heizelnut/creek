"""Poller tests: mirroring, scaling, quality, and backoff."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from creek.mirror import MemoryMirror
from creek.plc import Connection, FakeDriver, Poller, Tag, apply_scale, parse_tag_value
from creek.value import Value


def _tag(key: str, address: str, type: str = "u16", **kwargs: object) -> Tag:
    return Tag(key=key, connection="line1", address=address, type=type, **kwargs)  # type: ignore[arg-type]


def _connection(**kwargs: object) -> Connection:
    defaults: dict[str, object] = {
        "name": "line1",
        "driver": "modbus",
        "host": "127.0.0.1",
        "port": 502,
    }
    defaults.update(kwargs)
    return Connection(**defaults)  # type: ignore[arg-type]


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


def test_parse_tag_value() -> None:
    assert parse_tag_value(Tag("s", "c", "HR0", "u16", scale=0.1), "20") == Value.parse(
        "u16", "200"
    )
    assert parse_tag_value(Tag("t", "c", "HR1", "u16"), "5") == Value.parse("u16", "5")


# --- polling --------------------------------------------------------------


async def test_poll_mirrors_changes_only() -> None:
    mirror = MemoryMirror()
    driver = FakeDriver({"HR0": Value.parse("u16", "1500")})
    poller = Poller(_connection(), [_tag("speed", "HR0")], driver, mirror)

    changed = await poller.poll_once()
    assert changed["speed"] == Value.parse("u16", "1500")
    assert mirror.values["speed"] == Value.parse("u16", "1500")

    assert await poller.poll_once() == {}  # unchanged
    assert mirror.puts == [("speed", Value.parse("u16", "1500"))]


async def test_poll_applies_scale_before_mirroring() -> None:
    mirror = MemoryMirror()
    driver = FakeDriver({"HR0": Value.parse("u16", "1500")})
    poller = Poller(_connection(), [_tag("speed", "HR0", scale=0.1)], driver, mirror)
    await poller.poll_once()
    stored = mirror.values["speed"]
    assert stored.type.tag == "f64"
    assert stored.data == pytest.approx(150.0)


async def test_remember_suppresses_republish() -> None:
    mirror = MemoryMirror()
    driver = FakeDriver({"HR0": Value.parse("u16", "5")})
    poller = Poller(_connection(), [_tag("speed", "HR0")], driver, mirror)
    poller.remember("speed", Value.parse("u16", "5"))
    changed = await poller.poll_once()
    assert "speed" not in changed
    assert mirror.puts == []


async def test_poller_loop_survives_errors() -> None:
    class FlakyDriver:
        def __init__(self) -> None:
            self.calls = 0

        async def connect(self) -> None: ...

        async def close(self) -> None: ...

        async def read(self, tags: Sequence[Tag]) -> dict[str, Value]:
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("plc offline")
            return {"speed": Value.parse("u16", "7")}

        async def write(self, tag: Tag, value: Value) -> None: ...

    mirror = MemoryMirror()
    driver = FlakyDriver()
    poller = Poller(_connection(), [_tag("speed", "HR0")], driver, mirror, interval=0.01)
    poller.start()
    try:
        for _ in range(200):
            if "speed" in mirror.values:
                break
            await asyncio.sleep(0.01)
        assert mirror.values["speed"] == Value.parse("u16", "7")
        assert poller.errors == 1
    finally:
        await poller.stop()


# --- quality / staleness --------------------------------------------------


class _TogglingDriver:
    """Returns a fixed value until ``fail`` is set, then raises."""

    def __init__(self, value: Value) -> None:
        self.value = value
        self.fail = False

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def read(self, tags: Sequence[Tag]) -> dict[str, Value]:
        if self.fail:
            raise ConnectionError("offline")
        return {tag.key: self.value for tag in tags}

    async def write(self, tag: Tag, value: Value) -> None: ...


async def test_quality_good_then_stale() -> None:
    mirror = MemoryMirror()
    driver = _TogglingDriver(Value.parse("u16", "100"))
    poller = Poller(_connection(), [_tag("speed", "HR0")], driver, mirror)

    await poller.poll_once()
    assert poller.link == "up"
    assert mirror.qualities["speed"] == "good"

    driver.fail = True
    poller.start()
    try:
        for _ in range(200):
            if poller.link == "down":
                break
            await asyncio.sleep(0.01)
        assert poller.link == "down"
        assert poller.errors >= 1
        assert mirror.qualities["speed"] == "stale"
        # the last known value is kept, not cleared
        assert mirror.values["speed"] == Value.parse("u16", "100")
    finally:
        await poller.stop()


async def test_quality_can_be_disabled() -> None:
    mirror = MemoryMirror()
    driver = FakeDriver({"HR0": Value.parse("u16", "1")})
    poller = Poller(
        _connection(params={"quality": "false"}), [_tag("speed", "HR0")], driver, mirror
    )
    await poller.poll_once()
    assert mirror.qualities == {}
