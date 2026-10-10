"""Modbus/TCP driver (``PLC.md`` §4).

Client side of Modbus/TCP: MBAP framing plus the read/write function codes for
coils, discrete inputs, and input/holding registers. Registers are 16-bit
big-endian words; 32-bit values span two registers with a configurable word
order (``params.word_order = "big" | "little"``).
"""

from __future__ import annotations

import asyncio
import contextlib
import struct
from collections.abc import Sequence
from dataclasses import dataclass

from creek.errors import CreekError
from creek.plc.tags import Connection, Tag
from creek.value import Value, ValueType

# MBAP header: transaction id, protocol id (0), length, unit id.
MBAP = struct.Struct(">HHHB")

READ_COILS = 0x01
READ_DISCRETE = 0x02
READ_HOLDING = 0x03
READ_INPUT = 0x04
WRITE_COIL = 0x05
WRITE_REGISTER = 0x06
WRITE_REGISTERS = 0x10

MAX_READ_BITS = 2000
MAX_READ_REGISTERS = 125

_BIT_AREAS = {"coil": READ_COILS, "discrete": READ_DISCRETE}
_REGISTER_AREAS = {"holding": READ_HOLDING, "input": READ_INPUT}
_WRITABLE_AREAS = frozenset({"coil", "holding"})
_WIDTH = {"u16": 1, "i16": 1, "u32": 2, "i32": 2, "f32": 2}
_PREFIXES = (("DI", "discrete"), ("IR", "input"), ("HR", "holding"), ("C", "coil"))


class ModbusError(CreekError):
    """A Modbus/TCP protocol or device error."""


@dataclass(frozen=True, slots=True)
class ModbusAddress:
    """A parsed Modbus address: an area plus a 0-based offset."""

    area: str
    index: int


def parse_address(address: str) -> ModbusAddress:
    """Parse ``C0`` / ``DI3`` / ``IR10`` / ``HR400`` (0-based offsets)."""
    text = address.strip().upper()
    for prefix, area in _PREFIXES:
        if text.startswith(prefix):
            try:
                index = int(text[len(prefix) :], 10)
            except ValueError as exc:
                raise ModbusError(f"invalid Modbus address {address!r}") from exc
            if index < 0:
                raise ModbusError(f"invalid Modbus address {address!r}")
            return ModbusAddress(area, index)
    raise ModbusError(f"invalid Modbus address {address!r}")


def _register_width(type_tag: str) -> int:
    width = _WIDTH.get(type_tag)
    if width is None:
        raise ModbusError(f"unsupported Modbus register type {type_tag!r}")
    return width


def _registers_to_bytes(registers: list[int], word_order: str) -> bytes:
    words = list(reversed(registers)) if word_order == "little" else registers
    return b"".join(struct.pack(">H", word) for word in words)


def _bytes_to_registers(raw: bytes, word_order: str) -> list[int]:
    words = [int(value) for value in struct.unpack(f">{len(raw) // 2}H", raw)]
    if word_order == "little":
        words.reverse()
    return words


def decode_registers(registers: list[int], type_tag: str, word_order: str) -> Value:
    """Decode Modbus registers into a typed value."""
    if type_tag == "u16":
        return Value(ValueType.U16, registers[0])
    if type_tag == "i16":
        return Value(ValueType.I16, struct.unpack(">h", struct.pack(">H", registers[0]))[0])
    raw = _registers_to_bytes(registers, word_order)
    if type_tag == "u32":
        return Value(ValueType.U32, struct.unpack(">I", raw)[0])
    if type_tag == "i32":
        return Value(ValueType.I32, struct.unpack(">i", raw)[0])
    if type_tag == "f32":
        return Value(ValueType.F32, struct.unpack(">f", raw)[0])
    raise ModbusError(f"unsupported Modbus register type {type_tag!r}")


def encode_registers(value: Value, type_tag: str, word_order: str) -> list[int]:
    """Encode a typed value into Modbus registers."""
    if type_tag == "u16":
        return [int(value.data) & 0xFFFF]
    if type_tag == "i16":
        return [int(value.data) & 0xFFFF]
    if type_tag == "u32":
        raw = struct.pack(">I", int(value.data) & 0xFFFFFFFF)
    elif type_tag == "i32":
        raw = struct.pack(">i", int(value.data))
    elif type_tag == "f32":
        raw = struct.pack(">f", float(value.data))
    else:
        raise ModbusError(f"unsupported Modbus register type {type_tag!r}")
    return _bytes_to_registers(raw, word_order)


@dataclass(frozen=True, slots=True)
class _Item:
    tag: Tag
    area: str
    index: int
    width: int


@dataclass(frozen=True, slots=True)
class _Run:
    area: str
    start: int
    count: int
    items: tuple[_Item, ...]


def _plan_reads(tags: Sequence[Tag]) -> list[_Run]:
    """Group tags into contiguous same-area runs, one request each."""
    items: list[_Item] = []
    for tag in tags:
        address = parse_address(tag.address)
        width = 1 if address.area in _BIT_AREAS else _register_width(tag.type)
        items.append(_Item(tag, address.area, address.index, width))
    items.sort(key=lambda item: (item.area, item.width, item.index))

    runs: list[_Run] = []
    for item in items:
        if runs:
            run = runs[-1]
            maximum = MAX_READ_BITS if run.area in _BIT_AREAS else MAX_READ_REGISTERS
            if (
                run.area == item.area
                and run.items[0].width == item.width
                and run.start + run.count == item.index
                and run.count + item.width <= maximum
            ):
                runs[-1] = _Run(run.area, run.start, run.count + item.width, (*run.items, item))
                continue
        runs.append(_Run(item.area, item.index, item.width, (item,)))
    return runs


class ModbusDriver:
    """A Modbus/TCP client implementing :class:`creek.plc.driver.PlcDriver`."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._transaction = 0
        self._lock = asyncio.Lock()
        self._unit = int(connection.params.get("unit", 1))
        self._word_order = str(connection.params.get("word_order", "big"))

    async def connect(self) -> None:
        """Open the TCP connection (idempotent)."""
        if self._writer is not None:
            return
        self._reader, self._writer = await asyncio.open_connection(
            self._connection.host, self._connection.port
        )

    async def close(self) -> None:
        """Close the TCP connection."""
        writer = self._writer
        if writer is not None:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()
        self._reader = None
        self._writer = None

    async def read(self, tags: Sequence[Tag]) -> dict[str, Value]:
        """Read the given tags, batching contiguous addresses."""
        await self.connect()
        result: dict[str, Value] = {}
        for run in _plan_reads(tags):
            if run.area in _BIT_AREAS:
                bits = await self._read_bits(run.area, run.start, run.count)
                for item in run.items:
                    result[item.tag.key] = Value(ValueType.BOOL, bits[item.index - run.start])
            else:
                registers = await self._read_registers(run.area, run.start, run.count)
                for item in run.items:
                    offset = item.index - run.start
                    decoded = decode_registers(
                        registers[offset : offset + item.width], item.tag.type, self._word_order
                    )
                    result[item.tag.key] = decoded
        return result

    async def write(self, tag: Tag, value: Value) -> None:
        """Write a value to a coil or holding register."""
        address = parse_address(tag.address)
        if address.area not in _WRITABLE_AREAS:
            raise ModbusError(f"area {address.area!r} is read-only")
        if address.area == "coil":
            await self._write_coil(address.index, bool(value.data))
            return
        registers = encode_registers(value, tag.type, self._word_order)
        if len(registers) == 1:
            await self._write_register(address.index, registers[0])
        else:
            await self._write_registers(address.index, registers)

    async def _read_bits(self, area: str, start: int, count: int) -> list[bool]:
        body = await self._request(struct.pack(">BHH", _BIT_AREAS[area], start, count))
        data = body[2 : 2 + body[1]]
        return [bool(data[i // 8] & (1 << (i % 8))) for i in range(count)]

    async def _read_registers(self, area: str, start: int, count: int) -> list[int]:
        body = await self._request(struct.pack(">BHH", _REGISTER_AREAS[area], start, count))
        data = body[2 : 2 + body[1]]
        return [int(value) for value in struct.unpack(f">{count}H", data)]

    async def _write_coil(self, index: int, value: bool) -> None:
        await self._request(struct.pack(">BHH", WRITE_COIL, index, 0xFF00 if value else 0x0000))

    async def _write_register(self, index: int, value: int) -> None:
        await self._request(struct.pack(">BHH", WRITE_REGISTER, index, value))

    async def _write_registers(self, start: int, registers: list[int]) -> None:
        data = b"".join(struct.pack(">H", register) for register in registers)
        pdu = struct.pack(">BHHB", WRITE_REGISTERS, start, len(registers), len(data)) + data
        await self._request(pdu)

    async def _request(self, pdu: bytes) -> bytes:
        await self.connect()
        reader, writer = self._reader, self._writer
        if reader is None or writer is None:  # pragma: no cover - connect() guarantees this
            raise ModbusError("not connected")
        async with self._lock:
            self._transaction = (self._transaction + 1) & 0xFFFF
            frame = MBAP.pack(self._transaction, 0, len(pdu) + 1, self._unit) + pdu
            writer.write(frame)
            await writer.drain()
            header = await reader.readexactly(MBAP.size)
            transaction, protocol, length, _unit = MBAP.unpack(header)
            if protocol != 0:
                raise ModbusError("invalid protocol id")
            if transaction != self._transaction:
                raise ModbusError("transaction id mismatch")
            body = await reader.readexactly(length - 1)
        if not body:
            raise ModbusError("empty response")
        if body[0] & 0x80:
            raise ModbusError(f"device exception {body[1]}")
        return body


__all__ = [
    "MBAP",
    "ModbusAddress",
    "ModbusDriver",
    "ModbusError",
    "decode_registers",
    "encode_registers",
    "parse_address",
]
