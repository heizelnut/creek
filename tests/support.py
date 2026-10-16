"""In-memory test doubles for Redis (no external services)."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any


class FakePipeline:
    """A redis-py pipeline stand-in that records and then applies queued commands."""

    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._ops: list[tuple[str, str, str]] = []

    def set(self, name: str, value: str) -> FakePipeline:
        self._ops.append(("set", name, value))
        return self

    def publish(self, channel: str, message: str) -> FakePipeline:
        self._ops.append(("publish", channel, message))
        return self

    async def execute(self) -> list[bool]:
        for kind, first, second in self._ops:
            if kind == "set":
                self._redis.strings[first] = second
            else:
                self._redis.published.append((first, second))
        count = len(self._ops)
        self._ops = []
        return [True] * count


class FakeRedis:
    """The subset of ``redis.asyncio.Redis`` the daemon uses, backed by dicts/lists."""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.published: list[tuple[str, str]] = []
        self.entries: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.groups: dict[tuple[str, str], set[str]] = {}
        self.acked: set[str] = set()
        self.closed = False
        self._next_id = 0

    # --- strings / pub-sub ------------------------------------------------

    def pipeline(self) -> FakePipeline:
        return FakePipeline(self)

    # --- streams ----------------------------------------------------------

    def add(self, stream: str, fields: Mapping[str, str]) -> str:
        """Append an entry (test helper standing in for ``XADD``)."""
        self._next_id += 1
        record_id = f"{self._next_id}-0"
        self.entries.setdefault(stream, []).append((record_id, dict(fields)))
        return record_id

    async def xgroup_create(
        self, name: str, groupname: str, id: str = "0", mkstream: bool = False
    ) -> bool:
        if (name, groupname) in self.groups:
            raise Exception("BUSYGROUP Consumer Group name already exists")
        self.groups[(name, groupname)] = set()
        self.entries.setdefault(name, [])
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[Any]:
        name = next(iter(streams))
        delivered = self.groups[(name, groupname)]

        def pending() -> list[tuple[str, dict[str, str]]]:
            found: list[tuple[str, dict[str, str]]] = []
            for record_id, fields in self.entries.get(name, []):
                if record_id in delivered:
                    continue
                found.append((record_id, fields))
                if count is not None and len(found) >= count:
                    break
            return found

        # Emulate a blocking read: real XREADGROUP wakes as soon as an entry arrives, so poll
        # briefly instead of sleeping the whole block (which would stall the consumer loop).
        deadline = (block or 0) / 1000
        waited = 0.0
        step = 0.005
        while True:
            found = pending()
            if found:
                delivered.update(record_id for record_id, _ in found)
                return [(name, found)]
            if waited >= deadline:
                return []
            await asyncio.sleep(min(step, deadline - waited))
            waited += step

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        self.acked.update(ids)
        return len(ids)

    async def aclose(self) -> None:
        self.closed = True


__all__ = ["FakePipeline", "FakeRedis"]
