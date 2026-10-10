"""Poll a PLC connection and mirror its tags into the store and pub/sub (``PLC.md`` §3).

The mirror is written with the non-journaling ``Store.restore``: PLC state is derived and
re-polled on restart, so it must not bloat the WAL. Each changed tag is published on a channel
named after its key.

Quality (M9e): while a connection is healthy every tag is ``good``; when a poll fails the tags
become ``stale``. Quality is exposed as a companion ``<key>.q`` key (disable with
``params.quality = "false"``), so a SCADA can subscribe to it. A failed poll keeps the last known
value rather than clearing it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable, Sequence

from creek.plc.driver import PlcDriver
from creek.plc.tags import Connection, Tag
from creek.pubsub import Bus
from creek.store import Store
from creek.value import Value, ValueType

log = logging.getLogger("creek.plc")

_GOOD = "good"
_STALE = "stale"
_MAX_BACKOFF = 60.0


def apply_scale(tag: Tag, value: Value) -> Value:
    """Apply a tag's scale/offset; unscaled (and non-numeric) tags pass through."""
    if tag.scale == 1.0 and tag.offset == 0.0:
        return value
    if value.type is ValueType.BOOL or not isinstance(value.data, (int, float)):
        return value
    return Value(ValueType.F64, float(value.data) * tag.scale + tag.offset)


def parse_tag_value(tag: Tag, text: str) -> Value:
    """Parse a written value: engineering units if the tag is scaled, else its raw type."""
    if tag.scale == 1.0 and tag.offset == 0.0:
        return Value.parse(tag.type, text)
    engineering = float(text)
    raw = (engineering - tag.offset) / tag.scale
    vtype = ValueType.from_tag(tag.type)
    if vtype.is_integer_type:
        return Value.from_int(vtype, round(raw))
    return Value(vtype, raw)


class Poller:
    """Reads a connection's tags on an interval and mirrors changes into Creek."""

    def __init__(
        self,
        connection: Connection,
        tags: Sequence[Tag],
        driver: PlcDriver,
        store: Store,
        bus: Bus,
        *,
        interval: float | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.connection = connection
        self.tags = list(tags)
        self.driver = driver
        self.store = store
        self.bus = bus
        self.interval = connection.poll_interval if interval is None else interval
        self.polls = 0
        self.errors = 0
        self.link = "down"
        self.last_success: float | None = None
        self._quality: dict[str, str] = {}
        self._clock = clock
        self._task: asyncio.Task[None] | None = None

    @property
    def owner(self) -> str:
        """The namespace this connection's tags live under."""
        return self.connection.namespace

    @property
    def quality_enabled(self) -> bool:
        """Whether to publish companion ``<key>.q`` quality keys."""
        return str(self.connection.params.get("quality", "true")).lower() != "false"

    def start(self) -> None:
        """Start the polling loop."""
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the polling loop."""
        task = self._task
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._task = None

    async def _run(self) -> None:
        backoff = 0.0
        while True:
            try:
                await self.poll_once()
                backoff = 0.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # a bad poll must not kill the loop
                self.errors += 1
                self.link = "down"
                self._mark_stale()
                backoff = self.interval if backoff == 0.0 else min(backoff * 2, _MAX_BACKOFF)
                log.warning(
                    "plc poll failed",
                    extra={
                        "connection": self.connection.name,
                        "error": str(exc),
                        "backoff": backoff,
                    },
                )
            await asyncio.sleep(self.interval + backoff)

    async def poll_once(self) -> dict[str, Value]:
        """Read once; update the store, publish changes, and refresh quality."""
        values = await self.driver.read(self.tags)
        changed: dict[str, Value] = {}
        for tag in self.tags:
            if tag.key not in values:
                continue
            value = apply_scale(tag, values[tag.key])
            if self.store.get(self.owner, tag.key) != value:
                self.store.restore(self.owner, tag.key, value, None)
                self.bus.publish(self.owner, tag.key, value)
                changed[tag.key] = value
            self._set_quality(tag, _GOOD, changed)
        self.link = "up"
        self.last_success = self._clock()
        self.polls += 1
        return changed

    def _set_quality(self, tag: Tag, quality: str, changed: dict[str, Value]) -> None:
        if not self.quality_enabled or self._quality.get(tag.key) == quality:
            return
        self._quality[tag.key] = quality
        key = f"{tag.key}.q"
        value = Value(ValueType.STR, quality)
        self.store.restore(self.owner, key, value, None)
        self.bus.publish(self.owner, key, value)
        changed[key] = value

    def _mark_stale(self) -> None:
        for tag in self.tags:
            self._set_quality(tag, _STALE, {})


__all__ = ["Poller", "apply_scale", "parse_tag_value"]
