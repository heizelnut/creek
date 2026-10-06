"""Persistence: a length+CRC framed write-ahead log and atomic snapshots.

See ``AGENTS.md`` §10. Records are UTF-8 JSON; values are base64 of the
canonical :meth:`creek.value.Value.to_bytes` blob. Blocking disk work (writes,
``fsync``, snapshot serialization) runs off the event loop.

On-disk framing: ``u32 length || u32 crc32 || payload`` (little-endian). A
truncated or corrupt tail is tolerated: recovery stops at the last good record.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import struct
import time
import zlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol

from creek.errors import CreekError, PersistError
from creek.value import Value

SNAPSHOT_VERSION = 1

_FRAME = struct.Struct("<II")
EntryRecord = tuple[str, str, Value, float | None]
RetainedRecord = tuple[str, str, Value]


class Journal(Protocol):
    """What the store and bus call to record mutations."""

    def set(self, owner: str, key: str, value: Value, expires_at: float | None) -> None: ...

    def delete(self, owner: str, key: str) -> None: ...

    def flush(self, owner: str) -> None: ...

    def retain(self, owner: str, channel: str, value: Value) -> None: ...


def encode_record(record: dict[str, Any]) -> bytes:
    """Frame a record as ``length || crc32 || json``."""
    payload = json.dumps(record, separators=(",", ":")).encode("utf-8")
    return _FRAME.pack(len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload


def decode_records(data: bytes) -> list[dict[str, Any]]:
    """Decode framed records, stopping cleanly at a truncated or corrupt tail."""
    records: list[dict[str, Any]] = []
    pos = 0
    while pos + _FRAME.size <= len(data):
        length, crc = _FRAME.unpack_from(data, pos)
        start = pos + _FRAME.size
        end = start + length
        if end > len(data):
            break
        payload = data[start:end]
        if zlib.crc32(payload) & 0xFFFFFFFF != crc:
            break
        try:
            record = json.loads(payload)
        except json.JSONDecodeError:
            break
        if not isinstance(record, dict):
            break
        records.append(record)
        pos = end
    return records


def encode_blob(value: Value) -> str:
    """Base64 of a value's canonical binary encoding."""
    return base64.b64encode(value.to_bytes()).decode("ascii")


def decode_blob(text: str) -> Value:
    """Inverse of :func:`encode_blob`."""
    try:
        return Value.from_bytes(base64.b64decode(text, validate=True))
    except (ValueError, CreekError) as exc:
        raise PersistError(f"invalid value blob: {exc}") from exc


class Wal:
    """Append-only journal with a background writer and configurable ``fsync``."""

    def __init__(self, path: Path, fsync: str = "everysec") -> None:
        self._path = path
        self._fsync = fsync
        self._file = path.open("ab")
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._last_sync = 0.0

    def start(self) -> None:
        """Start the background writer task."""
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            data = await self._queue.get()
            await asyncio.to_thread(self._write, data)
            self._queue.task_done()

    def _write(self, data: bytes) -> None:
        self._file.write(data)
        self._file.flush()
        if self._fsync == "always":
            os.fsync(self._file.fileno())
        elif self._fsync == "everysec":
            now = time.monotonic()
            if now - self._last_sync >= 1.0:
                os.fsync(self._file.fileno())
                self._last_sync = now

    def append(self, record: dict[str, Any]) -> None:
        """Queue a record for writing (never blocks)."""
        self._queue.put_nowait(encode_record(record))

    # --- Journal protocol ---

    def set(self, owner: str, key: str, value: Value, expires_at: float | None) -> None:
        self.append(
            {
                "op": "set",
                "owner": owner,
                "key": key,
                "data": encode_blob(value),
                "expires_at": expires_at,
            }
        )

    def delete(self, owner: str, key: str) -> None:
        self.append({"op": "delete", "owner": owner, "key": key})

    def flush(self, owner: str) -> None:
        self.append({"op": "flush", "owner": owner})

    def retain(self, owner: str, channel: str, value: Value) -> None:
        self.append(
            {"op": "retain", "owner": owner, "channel": channel, "data": encode_blob(value)}
        )

    async def close(self) -> None:
        """Drain the queue, fsync, and close the file."""
        if self._task is not None:
            await self._queue.join()
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await asyncio.to_thread(self._final)

    def _final(self) -> None:
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()


def write_snapshot(
    path: Path,
    entries: Iterable[EntryRecord],
    retained: Iterable[RetainedRecord],
) -> None:
    """Write a versioned snapshot atomically (temp file -> fsync -> rename)."""
    document = {
        "version": SNAPSHOT_VERSION,
        "entries": [
            {"owner": owner, "key": key, "data": encode_blob(value), "expires_at": expires_at}
            for owner, key, value, expires_at in entries
        ],
        "retained": [
            {"owner": owner, "channel": channel, "data": encode_blob(value)}
            for owner, channel, value in retained
        ],
    }
    payload = json.dumps(document).encode("utf-8")
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def read_snapshot(path: Path) -> tuple[list[EntryRecord], list[RetainedRecord]]:
    """Load a snapshot; a missing file yields empty lists."""
    if not path.exists():
        return [], []
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PersistError(f"cannot read snapshot {path}: {exc}") from exc
    if not isinstance(document, dict) or document.get("version") != SNAPSHOT_VERSION:
        version = document.get("version") if isinstance(document, dict) else None
        raise PersistError(f"unsupported snapshot version {version!r}")
    entries: list[EntryRecord] = [
        (entry["owner"], entry["key"], decode_blob(entry["data"]), entry.get("expires_at"))
        for entry in document.get("entries", [])
    ]
    retained: list[RetainedRecord] = [
        (item["owner"], item["channel"], decode_blob(item["data"]))
        for item in document.get("retained", [])
    ]
    return entries, retained


def load(data_dir: Path) -> tuple[list[EntryRecord], list[RetainedRecord], list[dict[str, Any]]]:
    """Read the snapshot and WAL records for recovery."""
    entries, retained = read_snapshot(data_dir / "creek.snapshot")
    wal_path = data_dir / "creek.wal"
    records = decode_records(wal_path.read_bytes()) if wal_path.exists() else []
    return entries, retained, records


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


__all__ = [
    "SNAPSHOT_VERSION",
    "Journal",
    "Wal",
    "decode_blob",
    "decode_records",
    "encode_blob",
    "encode_record",
    "load",
    "read_snapshot",
    "write_snapshot",
]
