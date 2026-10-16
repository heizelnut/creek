"""Typed values: parsing, text form, and the binary codec.

See ``AGENTS.md`` §6. A value is a 1-byte type tag followed by a payload
(little-endian for fixed-width numerics). Values are immutable; a mutation
produces a new :class:`Value`.
"""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import cast

from creek.errors import InvalidValue

MAX_STR = 512 * 1024


class ValueType(IntEnum):
    """The type tag; the numeric value is the canonical type byte (``AGENTS.md`` §6)."""

    I4 = 0x01
    U4 = 0x02
    I8 = 0x03
    U8 = 0x04
    I16 = 0x05
    U16 = 0x06
    I32 = 0x07
    U32 = 0x08
    F8 = 0x09
    F16 = 0x0A
    F32 = 0x0B
    F64 = 0x0C
    STR = 0x0D
    BOOL = 0x0E

    @classmethod
    def from_tag(cls, tag: str) -> ValueType:
        """Resolve a wire tag like ``"i32"`` to a type."""
        try:
            return cls[tag.upper()]
        except KeyError as exc:
            raise InvalidValue(f"unknown type '{tag}'") from exc

    @property
    def tag(self) -> str:
        """The lower-case wire tag, e.g. ``"i32"``."""
        return self.name.lower()

    @property
    def is_integer_type(self) -> bool:
        """Whether this type stores an integer."""
        return self in _INT_RANGES


_INT_RANGES: dict[ValueType, tuple[int, int]] = {
    ValueType.I4: (-8, 7),
    ValueType.U4: (0, 15),
    ValueType.I8: (-128, 127),
    ValueType.U8: (0, 255),
    ValueType.I16: (-32768, 32767),
    ValueType.U16: (0, 65535),
    ValueType.I32: (-(2**31), 2**31 - 1),
    ValueType.U32: (0, 2**32 - 1),
}

_STRUCT: dict[ValueType, str] = {
    ValueType.I8: "<b",
    ValueType.U8: "<B",
    ValueType.I16: "<h",
    ValueType.U16: "<H",
    ValueType.I32: "<i",
    ValueType.U32: "<I",
    ValueType.F16: "<e",
    ValueType.F32: "<f",
    ValueType.F64: "<d",
}

_FLOAT_TYPES = frozenset({ValueType.F8, ValueType.F16, ValueType.F32, ValueType.F64})

_FIXED_LENGTHS: dict[ValueType, int] = {
    ValueType.I4: 1,
    ValueType.U4: 1,
    ValueType.I8: 1,
    ValueType.U8: 1,
    ValueType.BOOL: 1,
    ValueType.F8: 1,
    ValueType.I16: 2,
    ValueType.U16: 2,
    ValueType.F16: 2,
    ValueType.I32: 4,
    ValueType.U32: 4,
    ValueType.F32: 4,
    ValueType.F64: 8,
}


@dataclass(frozen=True, slots=True)
class Value:
    """A tagged primitive: its type plus the Python value."""

    type: ValueType
    data: int | float | str | bool

    @classmethod
    def parse(cls, tag: str, raw: str) -> Value:
        """Build a value from its wire text form (``TYPE`` tag + string)."""
        vtype = ValueType.from_tag(tag)
        return cls(vtype, _parse_data(vtype, raw))

    def text(self) -> str:
        """Render the canonical text form used in replies."""
        if self.type is ValueType.STR:
            return cast("str", self.data)
        if self.type is ValueType.BOOL:
            return "true" if self.data else "false"
        if self.type in _FLOAT_TYPES:
            return repr(cast("float", self.data))
        return str(self.data)

    def to_bytes(self) -> bytes:
        """Serialize as ``type byte || payload``."""
        return bytes([self.type]) + _encode_payload(self.type, self.data)

    @classmethod
    def from_bytes(cls, data: bytes) -> Value:
        """Parse a value previously produced by :meth:`to_bytes`."""
        if not data:
            raise InvalidValue("empty value")
        try:
            vtype = ValueType(data[0])
        except ValueError as exc:
            raise InvalidValue(f"unknown value type byte 0x{data[0]:02x}") from exc
        return cls(vtype, _decode_payload(vtype, data[1:]))

    @classmethod
    def from_int(cls, vtype: ValueType, number: int) -> Value:
        """Build a range-checked integer value (used by INCR/DECR)."""
        bounds = _INT_RANGES.get(vtype)
        if bounds is None:
            raise InvalidValue(f"{vtype.tag} is not an integer type")
        low, high = bounds
        if not low <= number <= high:
            raise InvalidValue(f"{vtype.tag} out of range: {number}")
        return cls(vtype, number)


def _parse_data(vtype: ValueType, raw: str) -> int | float | str | bool:
    if vtype is ValueType.STR:
        if len(raw.encode("utf-8")) > MAX_STR:
            raise InvalidValue("string exceeds 512 KiB")
        return raw
    if vtype is ValueType.BOOL:
        lowered = raw.strip().lower()
        if lowered in ("true", "1"):
            return True
        if lowered in ("false", "0"):
            return False
        raise InvalidValue(f"invalid bool {raw!r}")
    if vtype in _INT_RANGES:
        try:
            number = int(raw, 10)
        except ValueError as exc:
            raise InvalidValue(f"invalid integer {raw!r}") from exc
        low, high = _INT_RANGES[vtype]
        if not low <= number <= high:
            raise InvalidValue(f"{vtype.tag} out of range: {raw}")
        return number
    try:
        return float(raw)
    except ValueError as exc:
        raise InvalidValue(f"invalid float {raw!r}") from exc


def _encode_payload(vtype: ValueType, data: int | float | str | bool) -> bytes:
    if vtype is ValueType.STR:
        encoded = cast("str", data).encode("utf-8")
        if len(encoded) > MAX_STR:
            raise InvalidValue("string exceeds 512 KiB")
        return _encode_uvarint(len(encoded)) + encoded
    if vtype is ValueType.BOOL:
        return b"\x01" if data else b"\x00"
    if vtype is ValueType.I4:
        return bytes([cast("int", data) & 0x0F])
    if vtype is ValueType.U4:
        return bytes([cast("int", data) & 0x0F])
    if vtype is ValueType.F8:
        return bytes([_f8_encode(cast("float", data))])
    number = cast("int | float", data)
    if vtype in (ValueType.F16, ValueType.F32):
        try:
            return struct.pack(_STRUCT[vtype], number)
        except OverflowError:  # IEEE overflow rounds to infinity
            return struct.pack(_STRUCT[vtype], math.copysign(math.inf, number))
    return struct.pack(_STRUCT[vtype], number)


def _decode_payload(vtype: ValueType, payload: bytes) -> int | float | str | bool:
    if vtype is ValueType.STR:
        length, pos = _decode_uvarint(payload, 0)
        if length > MAX_STR:
            raise InvalidValue("string exceeds 512 KiB")
        end = pos + length
        if end != len(payload):
            raise InvalidValue("string length does not match payload")
        try:
            return payload[pos:end].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidValue("invalid utf-8") from exc
    expected = _FIXED_LENGTHS[vtype]
    if len(payload) != expected:
        raise InvalidValue(f"{vtype.tag} expects {expected} payload bytes")
    if vtype is ValueType.I4:
        byte = payload[0] & 0x0F
        return byte - 16 if byte & 0x08 else byte
    if vtype is ValueType.U4:
        return payload[0] & 0x0F
    if vtype is ValueType.BOOL:
        return payload[0] != 0
    if vtype is ValueType.F8:
        return _f8_decode(payload[0])
    return cast("int | float", struct.unpack(_STRUCT[vtype], payload)[0])


def _encode_uvarint(number: int) -> bytes:
    out = bytearray()
    while True:
        byte = number & 0x7F
        number >>= 7
        if number:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _decode_uvarint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise InvalidValue("truncated varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise InvalidValue("varint too long")


def _f8_decode(byte: int) -> float:
    """Decode an OCP E4M3 byte (1 sign, 4 exponent bias 7, 3 mantissa)."""
    sign = -1.0 if byte & 0x80 else 1.0
    exponent = (byte >> 3) & 0x0F
    mantissa = byte & 0x07
    if exponent == 0:
        value = (mantissa / 8.0) * 2.0**-6  # subnormal
    elif exponent == 0x0F and mantissa == 0x07:
        return math.nan
    else:
        value = (1.0 + mantissa / 8.0) * 2.0 ** (exponent - 7)
    return sign * value


def _f8_encode(value: float) -> int:
    """Encode to OCP E4M3; overflow saturates to ±448, NaN is ``0x7F``."""
    if math.isnan(value):
        return 0x7F
    sign = 0x80 if math.copysign(1.0, value) < 0 else 0x00
    magnitude = abs(value)
    if math.isinf(magnitude):
        return sign | 0x7E
    if magnitude == 0.0:
        return sign
    _, exponent2 = math.frexp(magnitude)  # magnitude == mantissa * 2**exponent2
    exponent = exponent2 + 6  # biased exponent field
    if exponent <= 0:  # subnormal
        mantissa = round(magnitude * 512.0)
        if mantissa == 0:
            return sign
        if mantissa >= 8:
            return sign | (1 << 3)  # rounds up to the smallest normal
        return sign | mantissa
    if exponent >= 16:  # overflow
        return sign | 0x7E
    mantissa = round((magnitude / 2.0 ** (exponent - 7) - 1.0) * 8.0)
    if mantissa == 8:  # rounded up into the next exponent
        mantissa = 0
        exponent += 1
        if exponent >= 16:
            return sign | 0x7E
    if exponent == 0x0F and mantissa == 0x07:  # reserved NaN code
        return sign | 0x7E
    return sign | (exponent << 3) | mantissa


__all__ = ["MAX_STR", "Value", "ValueType"]
