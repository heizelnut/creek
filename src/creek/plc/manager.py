"""Own the PLC connections: drivers, pollers, and the write path (``PLC.md`` §3)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from creek.errors import CreekError
from creek.mirror import Mirror
from creek.plc.driver import PlcDriver
from creek.plc.modbus import ModbusDriver
from creek.plc.poller import Poller, apply_scale
from creek.plc.s7 import S7Driver
from creek.plc.tags import Connection, PlcConfig, Tag
from creek.value import Value

log = logging.getLogger("creek.plc")

DriverFactory = Callable[[Connection], PlcDriver]

_DRIVERS: dict[str, DriverFactory] = {"modbus": ModbusDriver, "s7": S7Driver}


class PlcError(CreekError):
    """A PLC configuration, tag, or write error."""


@dataclass
class PlcConnection:
    """A live PLC connection: its config, driver, poller, and tags by key."""

    connection: Connection
    driver: PlcDriver
    poller: Poller
    tags: dict[str, Tag] = field(default_factory=dict)


class PlcManager:
    """Creates and owns the drivers and pollers for a PLC config."""

    def __init__(
        self,
        config: PlcConfig,
        mirror: Mirror,
        *,
        drivers: dict[str, DriverFactory] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.config = config
        self._mirror = mirror
        self._drivers = drivers if drivers is not None else _DRIVERS
        self._clock = clock
        self.connections: dict[str, PlcConnection] = {}
        self._tags: dict[str, Tag] = {}

    def start(self) -> None:
        """Create the drivers and pollers and start polling."""
        for connection in self.config.connections.values():
            driver = self._make_driver(connection)
            tags = self.config.tags_for(connection.name)
            poller = Poller(connection, tags, driver, self._mirror, clock=self._clock)
            entry = PlcConnection(connection, driver, poller, {tag.key: tag for tag in tags})
            self.connections[connection.name] = entry
            self._tags.update(entry.tags)
            poller.start()

    async def close(self) -> None:
        """Stop all pollers and close all drivers."""
        for entry in self.connections.values():
            await entry.poller.stop()
            await entry.driver.close()
        self.connections.clear()
        self._tags.clear()

    def tag(self, key: str) -> Tag | None:
        """Look up a tag by its key."""
        return self._tags.get(key)

    def tags(self) -> list[Tag]:
        """All tags across all connections."""
        return list(self._tags.values())

    async def write(self, key: str, value: Value) -> None:
        """Write a tag to its PLC, then read it back into the mirror."""
        tag = self._tags.get(key)
        if tag is None:
            raise PlcError(f"unknown PLC tag {key!r}")
        if not tag.writable:
            raise PlcError(f"tag {key!r} is not writable")
        entry = self.connections[tag.connection]
        await entry.driver.write(tag, value)
        await self._refresh(entry, tag)

    async def _refresh(self, entry: PlcConnection, tag: Tag) -> None:
        values = await entry.driver.read([tag])
        if tag.key in values:
            value = apply_scale(tag, values[tag.key])
            entry.poller.remember(tag.key, value)
            await self._mirror.put(tag, value)

    def _make_driver(self, connection: Connection) -> PlcDriver:
        factory = self._drivers.get(connection.driver)
        if factory is None:
            raise PlcError(f"unknown PLC driver {connection.driver!r}")
        return factory(connection)


__all__ = ["PlcConnection", "PlcError", "PlcManager"]
