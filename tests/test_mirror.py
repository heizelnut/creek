"""Redis mirror tests."""

from __future__ import annotations

from creek.mirror import MemoryMirror, RedisMirror
from creek.plc import Tag
from creek.value import Value
from support import FakeRedis


def _tag(key: str = "motor.speed") -> Tag:
    return Tag(key, "line1", "HR0", "u16")


async def test_put_sets_key_and_publishes() -> None:
    redis = FakeRedis()
    mirror = RedisMirror(redis, prefix="plc:")
    await mirror.put(_tag(), Value.parse("u16", "1500"))
    assert redis.strings == {"plc:motor.speed": "1500"}
    assert redis.published == [("plc:motor.speed", "1500")]


async def test_put_honours_prefix() -> None:
    redis = FakeRedis()
    mirror = RedisMirror(redis, prefix="plant/a/")
    await mirror.put(_tag(), Value.parse("u16", "1"))
    assert "plant/a/motor.speed" in redis.strings


async def test_quality_writes_companion_key() -> None:
    redis = FakeRedis()
    mirror = RedisMirror(redis)
    await mirror.quality(_tag(), "stale")
    assert redis.strings == {"plc:motor.speed.q": "stale"}
    assert redis.published == [("plc:motor.speed.q", "stale")]


async def test_timestamps_are_opt_in() -> None:
    redis = FakeRedis()
    mirror = RedisMirror(redis, timestamps=True, clock=lambda: 1000.5)
    await mirror.put(_tag(), Value.parse("u16", "1"))
    assert redis.strings["plc:motor.speed.ts"] == "1000.500"


async def test_no_timestamp_by_default() -> None:
    redis = FakeRedis()
    mirror = RedisMirror(redis, clock=lambda: 1000.5)
    await mirror.put(_tag(), Value.parse("u16", "1"))
    assert "plc:motor.speed.ts" not in redis.strings


async def test_value_text_forms() -> None:
    redis = FakeRedis()
    mirror = RedisMirror(redis)
    await mirror.put(_tag("f"), Value.parse("f64", "1.5"))
    await mirror.put(_tag("b"), Value.parse("bool", "true"))
    assert redis.strings["plc:f"] == "1.5"
    assert redis.strings["plc:b"] == "true"


async def test_memory_mirror_records() -> None:
    mirror = MemoryMirror()
    tag = _tag()
    value = Value.parse("u16", "7")
    await mirror.put(tag, value)
    await mirror.quality(tag, "good")
    assert mirror.values["motor.speed"] == value
    assert mirror.qualities["motor.speed"] == "good"
    assert mirror.puts == [("motor.speed", value)]
    assert mirror.quality_puts == [("motor.speed", "good")]
