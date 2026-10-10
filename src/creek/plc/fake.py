"""An in-memory PLC driver for tests and dry runs."""

from __future__ import annotations

from collections.abc import Sequence

from creek.plc.tags import Tag
from creek.value import Value


class FakeDriver:
    """A driver backed by a dict of ``address -> value``."""

    def __init__(self, values: dict[str, Value] | None = None) -> None:
        self.values: dict[str, Value] = dict(values or {})
        self.writes: list[tuple[str, Value]] = []
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def read(self, tags: Sequence[Tag]) -> dict[str, Value]:
        return {tag.key: self.values[tag.address] for tag in tags if tag.address in self.values}

    async def write(self, tag: Tag, value: Value) -> None:
        self.values[tag.address] = value
        self.writes.append((tag.address, value))


__all__ = ["FakeDriver"]
