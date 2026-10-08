"""Read replicas and the replication log (M7a).

Replication is **pull-based**: the primary keeps a bounded in-memory backlog of
every mutation (the :class:`ReplicationLog`); a replica connects, pulls a full
snapshot, then polls for records appended since its offset. If the replica falls
too far behind (its offset was evicted from the backlog) it does a full resync.

This trades replication latency (one poll interval) for a small amount of
protocol machinery: no push stream, no separate replication port, no partial
resync handshake.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
from collections import deque
from collections.abc import Callable
from typing import Any

from creek.client import connect
from creek.persist import (
    Snapshot,
    Wal,
    decode_records,
    decode_snapshot,
    delete_record,
    flush_record,
    retain_record,
    set_record,
    tenant_create_record,
    tenant_drop_record,
)
from creek.value import Value

ApplyRecord = Callable[[dict[str, Any]], None]
LoadSnapshot = Callable[[Snapshot], None]


class ReplicationLog:
    """The journal: a bounded backlog for replicas, forwarded to a WAL if set."""

    def __init__(self, capacity: int = 10_000) -> None:
        self._records: deque[tuple[int, dict[str, Any]]] = deque(maxlen=capacity)
        self._sequence = 0
        self._oldest = 1
        self._wal: Wal | None = None

    def set_wal(self, wal: Wal | None) -> None:
        """Attach (or detach) the disk WAL this log forwards to."""
        self._wal = wal

    @property
    def sequence(self) -> int:
        """The replication offset: the number of records appended."""
        return self._sequence

    def backlog(self) -> int:
        """How many records are still available for a partial resync."""
        return len(self._records)

    def since(self, offset: int) -> list[dict[str, Any]] | None:
        """Records after ``offset``, or ``None`` if they were evicted."""
        if offset < self._oldest - 1:
            return None
        return [record for sequence, record in self._records if sequence > offset]

    def _record(self, record: dict[str, Any]) -> None:
        self._sequence += 1
        self._records.append((self._sequence, record))
        self._oldest = self._records[0][0]
        if self._wal is not None:
            self._wal.append(record)

    # --- Journal protocol ---

    def set(self, owner: str, key: str, value: Value, expires_at: float | None) -> None:
        self._record(set_record(owner, key, value, expires_at))

    def delete(self, owner: str, key: str) -> None:
        self._record(delete_record(owner, key))

    def flush(self, owner: str) -> None:
        self._record(flush_record(owner))

    def retain(self, owner: str, channel: str, value: Value) -> None:
        self._record(retain_record(owner, channel, value))

    def tenant_create(
        self, name: str, created_at: float, quota: dict[str, int | float | None]
    ) -> None:
        self._record(tenant_create_record(name, created_at, quota))

    def tenant_drop(self, name: str) -> None:
        self._record(tenant_drop_record(name))


class ReplicationState:
    """Shared, mutable view of this node's replication role."""

    def __init__(self) -> None:
        self.master: tuple[str, int] | None = None
        self.offset = 0
        self.link = "up"
        self.last_error: str | None = None

    @property
    def read_only(self) -> bool:
        """Whether this node is following a primary (and so rejects writes)."""
        return self.master is not None


class Replication:
    """Owns the replication log, role state, and the replica polling task."""

    def __init__(
        self,
        apply_record: ApplyRecord | None = None,
        load_snapshot: LoadSnapshot | None = None,
        *,
        capacity: int = 10_000,
        interval: float = 0.1,
    ) -> None:
        self.log = ReplicationLog(capacity)
        self.state = ReplicationState()
        self._apply_record: ApplyRecord = apply_record or (lambda record: None)
        self._load_snapshot: LoadSnapshot = load_snapshot or (lambda snapshot: None)
        self._interval = interval
        self._token: str | None = None
        self._task: asyncio.Task[None] | None = None

    async def follow(self, host: str, port: int, token: str | None = None) -> None:
        """Start following a primary at ``host:port``."""
        await self.stop()
        self._token = token
        self.state.master = (host, port)
        self.state.link = "connecting"
        self.state.last_error = None
        self._task = asyncio.create_task(self._run(host, port))

    async def promote(self) -> None:
        """Stop following and become a primary."""
        await self.stop()
        self.state.master = None
        self.state.link = "up"

    async def stop(self) -> None:
        """Cancel the replica task, if any."""
        task = self._task
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._task = None

    async def _run(self, host: str, port: int) -> None:
        while True:
            try:
                await self._follow(host, port)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.link = "down"
                self.state.last_error = str(exc)
            await asyncio.sleep(self._interval)

    async def _follow(self, host: str, port: int) -> None:
        client = await connect(f"tcp://{host}:{port}")
        try:
            if self._token is not None:
                await client.execute("AUTH", self._token)
            offset_text, payload = await client.execute("SYNC")
            self._load_snapshot(decode_snapshot(base64.b64decode(payload)))
            self.state.offset = int(offset_text)
            self.state.link = "up"
            self.state.last_error = None
            while True:
                reply = await client.execute("REPLICATE", str(self.state.offset))
                if reply is None:
                    return  # backlog evicted -> full resync on the next pass
                new_offset, records_payload = reply
                for record in decode_records(base64.b64decode(records_payload)):
                    self._apply_record(record)
                self.state.offset = int(new_offset)
                await asyncio.sleep(self._interval)
        finally:
            await client.close()


__all__ = ["Replication", "ReplicationLog", "ReplicationState"]
