"""Mirror polled PLC state into Redis (``PLC.md`` §3).

The daemon writes each tag to a Redis string key (its *last value*) and publishes the same
value on a channel named after the key, so consumers can either read the current state or
subscribe to changes. Quality is exposed as a companion ``<key>.q`` key/channel.

The :class:`Mirror` protocol is the seam between the polling/command logic and Redis; tests and
``--dry-run`` use :class:`MemoryMirror`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from creek.value import Value

if TYPE_CHECKING:
    from creek.plc.tags import Tag

QUALITY_GOOD = "good"
QUALITY_STALE = "stale"


class PipelineLike(Protocol):
    """The subset of a redis-py pipeline the mirror uses."""

    def set(self, name: str, value: str) -> PipelineLike: ...

    def publish(self, channel: str, message: str) -> PipelineLike: ...

    async def execute(self) -> Any: ...


class RedisLike(Protocol):
    """The subset of a redis client the daemon uses (redis.asyncio.Redis satisfies it)."""

    def pipeline(self) -> PipelineLike: ...

    async def xgroup_create(
        self, name: str, groupname: str, id: str = "0", mkstream: bool = False
    ) -> Any: ...

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...

    async def xack(self, name: str, groupname: str, *ids: str) -> Any: ...

    async def aclose(self) -> None: ...


@runtime_checkable
class Mirror(Protocol):
    """Where polled PLC values and quality flags are written."""

    async def put(self, tag: Tag, value: Value) -> None:
        """Record ``tag``'s current value."""
        ...

    async def quality(self, tag: Tag, quality: str) -> None:
        """Record ``tag``'s quality (``good`` / ``stale``)."""
        ...


class RedisMirror:
    """Writes values to Redis keys and publishes changes on matching channels."""

    def __init__(
        self,
        redis: RedisLike,
        *,
        prefix: str = "plc:",
        timestamps: bool = False,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = redis
        self._prefix = prefix
        self._timestamps = timestamps
        self._clock = clock

    def key(self, name: str) -> str:
        """The Redis key for a tag key (or companion like ``<key>.q``)."""
        return f"{self._prefix}{name}"

    async def put(self, tag: Tag, value: Value) -> None:
        """Set ``<prefix><key>`` and publish the value on that channel."""
        key = self.key(tag.key)
        text = value.text()
        pipe = self._redis.pipeline()
        pipe.set(key, text)
        if self._timestamps:
            pipe.set(f"{key}.ts", f"{self._clock():.3f}")
        pipe.publish(key, text)
        await pipe.execute()

    async def quality(self, tag: Tag, quality: str) -> None:
        """Set ``<prefix><key>.q`` and publish the quality on that channel."""
        key = self.key(f"{tag.key}.q")
        pipe = self._redis.pipeline()
        pipe.set(key, quality)
        pipe.publish(key, quality)
        await pipe.execute()


class MemoryMirror:
    """An in-memory :class:`Mirror` for ``--dry-run`` and tests."""

    def __init__(self) -> None:
        self.values: dict[str, Value] = {}
        self.qualities: dict[str, str] = {}
        self.puts: list[tuple[str, Value]] = []
        self.quality_puts: list[tuple[str, str]] = []

    async def put(self, tag: Tag, value: Value) -> None:
        self.values[tag.key] = value
        self.puts.append((tag.key, value))

    async def quality(self, tag: Tag, quality: str) -> None:
        self.qualities[tag.key] = quality
        self.quality_puts.append((tag.key, quality))


__all__ = [
    "QUALITY_GOOD",
    "QUALITY_STALE",
    "MemoryMirror",
    "Mirror",
    "PipelineLike",
    "RedisLike",
    "RedisMirror",
]
