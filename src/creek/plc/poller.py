"""Poll a PLC connection and mirror its tags into the store and pub/sub (``PLC.md`` §3).

The mirror is written with the non-journaling ``Store.restore``: PLC state is derived and
re-polled on restart, so it must not bloat the WAL. Each changed tag is published on a channel
named after its key.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Sequence

from creek.plc.driver import PlcDriver
from creek.plc.tags import Connection, Tag
from creek.pubsub import Bus
from creek.store import Store
from creek.value import Value, ValueType

log = logging.getLogger("creek.plc")


def apply_scale(tag: Tag, value: Value) -> Value:
    """Apply a tag's scale/offset; unscaled (and non-numeric) tags pass through."""
    if tag.scale == 1.0 and tag.offset == 0.0:
        return value
    if value.type is ValueType.BOOL or not isinstance(value.data, (int, float)):
        return value
    return Value(ValueType.F64, float(value.data) * tag.scale + tag.offset)


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
    ) -> None:
        self.connection = connection
        self.tags = list(tags)
        self.driver = driver
        self.store = store
        self.bus = bus
        self.interval = connection.poll_interval if interval is None else interval
        self.errors = 0
        self._task: asyncio.Task[None] | None = None

    @property
    def owner(self) -> str:
        """The namespace this connection's tags live under."""
        return self.connection.namespace

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
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.errors += 1
                log.warning(
                    "plc poll failed",
                    extra={"connection": self.connection.name, "error": str(exc)},
                )
            await asyncio.sleep(self.interval)

    async def poll_once(self) -> dict[str, Value]:
        """Read once; update the store and publish the tags that changed."""
        values = await self.driver.read(self.tags)
        changed: dict[str, Value] = {}
        for tag in self.tags:
            if tag.key not in values:
                continue
            value = apply_scale(tag, values[tag.key])
            if self.store.get(self.owner, tag.key) == value:
                continue
            self.store.restore(self.owner, tag.key, value, None)
            self.bus.publish(self.owner, tag.key, value)
            changed[tag.key] = value
        return changed


__all__ = ["Poller", "apply_scale"]
