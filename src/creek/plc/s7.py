"""Siemens S7 driver built on ``python-snap7`` (``PLC.md`` §5).

Targets S7-300/400, and S7-1200/1500 with PUT/GET enabled and **non-optimized**
DBs (symbolic/optimized access needs S7comm-plus, which is out of scope).

``python-snap7`` is synchronous, so every device call is offloaded with
``asyncio.to_thread`` and serialized by a per-connection lock. Reads are grouped
by ``(area, db)`` into one span request per group.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import struct
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast

from creek.errors import CreekError
from creek.plc.tags import Connection, Tag
from creek.value import Value, ValueType

# S7 is big-endian.
_SIZE = {"bool": 1, "u8": 1, "i8": 1, "u16": 2, "i16": 2, "u32": 4, "i32": 4, "f32": 4, "f64": 8}
_STRUCT = {
    "i8": ">b",
    "u8": ">B",
    "i16": ">h",
    "u16": ">H",
    "i32": ">i",
    "u32": ">I",
    "f32": ">f",
    "f64": ">d",
}
_TYPE = {
    "bool": ValueType.BOOL,
    "u8": ValueType.U8,
    "i8": ValueType.I8,
    "u16": ValueType.U16,
    "i16": ValueType.I16,
    "u32": ValueType.U32,
    "i32": ValueType.I32,
    "f32": ValueType.F32,
    "f64": ValueType.F64,
}

_DB_RE = re.compile(
    r"^DB(?P<db>\d+)\.DB(?P<suffix>[XBWD])(?P<offset>\d+)(?:\.(?P<bit>\d+))?$", re.IGNORECASE
)
_MEM_RE = re.compile(
    r"^(?P<area>[IQM])(?P<suffix>[XBWD]?)(?P<offset>\d+)(?:\.(?P<bit>\d+))?$", re.IGNORECASE
)


class S7Error(CreekError):
    """An S7 protocol, configuration, or connection error."""


@dataclass(frozen=True, slots=True)
class S7Address:
    """A parsed S7 address: area (``DB``/``I``/``Q``/``M``), DB number, byte offset, and bit."""

    area: str
    db: int
    offset: int
    bit: int | None


def parse_address(address: str) -> S7Address:
    """Parse ``DB1.DBX0.0`` / ``DB1.DBD8`` / ``I0.0`` / ``MW10`` / ``QB2``."""
    text = address.strip()
    match = _DB_RE.match(text)
    if match is not None:
        return S7Address(
            "DB",
            int(match.group("db")),
            int(match.group("offset")),
            _bit(match.group("suffix"), match.group("bit")),
        )
    match = _MEM_RE.match(text)
    if match is not None:
        return S7Address(
            match.group("area").upper(),
            0,
            int(match.group("offset")),
            _bit(match.group("suffix"), match.group("bit")),
        )
    raise S7Error(f"invalid S7 address {address!r}")


def _bit(suffix: str | None, bit_text: str | None) -> int | None:
    if bit_text is not None:
        bit = int(bit_text)
    elif suffix is not None and suffix.upper() == "X":
        bit = 0
    else:
        return None
    if not 0 <= bit <= 7:
        raise S7Error(f"invalid S7 bit number {bit}")
    return bit


def decode(data: bytes, type_tag: str, bit: int | None) -> Value:
    """Decode S7 bytes into a typed value."""
    if type_tag == "bool":
        return Value(ValueType.BOOL, bool(data[0] & (1 << (bit or 0))))
    fmt = _STRUCT.get(type_tag)
    if fmt is None:
        raise S7Error(f"unsupported S7 type {type_tag!r}")
    return Value(_TYPE[type_tag], struct.unpack(fmt, data[: _SIZE[type_tag]])[0])


def encode(value: Value, type_tag: str) -> bytes:
    """Encode a typed value into S7 bytes (booleans handled by the caller)."""
    fmt = _STRUCT.get(type_tag)
    if fmt is None:
        raise S7Error(f"unsupported S7 type {type_tag!r}")
    return struct.pack(fmt, cast("int | float", value.data))


@dataclass(frozen=True, slots=True)
class _Planned:
    tag: Tag
    address: S7Address
    size: int


class S7Driver:
    """A Siemens S7 client implementing :class:`creek.plc.driver.PlcDriver`."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._client: Any = None
        self._lock = asyncio.Lock()
        self._rack = int(connection.params.get("rack", 0))
        self._slot = int(connection.params.get("slot", 1))

    async def connect(self) -> None:
        """Open the ISO-on-TCP connection (idempotent)."""
        if self._client is not None and self._client.get_connected():
            return
        client = _new_client()
        await asyncio.to_thread(
            client.connect, self._connection.host, self._rack, self._slot, self._connection.port
        )
        self._client = client

    async def close(self) -> None:
        """Disconnect and release the client."""
        client = self._client
        self._client = None
        if client is not None:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(client.disconnect)
            with contextlib.suppress(Exception):
                await asyncio.to_thread(client.destroy)

    async def read(self, tags: Sequence[Tag]) -> dict[str, Value]:
        """Read the given tags, one span request per ``(area, db)`` group."""
        async with self._lock:
            await self.connect()
            groups: dict[tuple[str, int], list[_Planned]] = {}
            for tag in tags:
                address = parse_address(tag.address)
                size = _SIZE.get(tag.type)
                if size is None:
                    raise S7Error(f"unsupported S7 type {tag.type!r}")
                groups.setdefault((address.area, address.db), []).append(
                    _Planned(tag, address, size)
                )
            result: dict[str, Value] = {}
            for (area, db), planned in groups.items():
                start = min(item.address.offset for item in planned)
                end = max(item.address.offset + item.size for item in planned)
                data = await asyncio.to_thread(self._read_span, area, db, start, end - start)
                for item in planned:
                    offset = item.address.offset - start
                    chunk = data[offset : offset + item.size]
                    result[item.tag.key] = decode(chunk, item.tag.type, item.address.bit)
            return result

    async def write(self, tag: Tag, value: Value) -> None:
        """Write a value to an S7 address (read-modify-write for bits)."""
        async with self._lock:
            await self.connect()
            address = parse_address(tag.address)
            if tag.type == "bool":
                await asyncio.to_thread(self._write_bit, address, bool(value.data))
                return
            data = encode(value, tag.type)
            await asyncio.to_thread(self._write_span, address, data)

    def _read_span(self, area: str, db: int, start: int, size: int) -> bytes:
        return bytes(self._client.read_area(_area(area), db, start, size))

    def _write_span(self, address: S7Address, data: bytes) -> None:
        self._client.write_area(_area(address.area), address.db, address.offset, bytearray(data))

    def _write_bit(self, address: S7Address, value: bool) -> None:
        bit = address.bit or 0
        current = bytearray(
            self._client.read_area(_area(address.area), address.db, address.offset, 1)
        )
        if value:
            current[0] |= 1 << bit
        else:
            current[0] &= ~(1 << bit) & 0xFF
        self._client.write_area(_area(address.area), address.db, address.offset, current)


def _new_client() -> Any:
    try:
        from snap7 import Client
    except Exception as exc:  # pragma: no cover - depends on the native library
        raise S7Error(f"python-snap7 is unavailable: {exc}") from exc
    return Client()


def _area(name: str) -> Any:
    from snap7 import Area

    return {"DB": Area.DB, "I": Area.PE, "Q": Area.PA, "M": Area.MK}[name]


__all__ = ["S7Address", "S7Driver", "S7Error", "decode", "encode", "parse_address"]
