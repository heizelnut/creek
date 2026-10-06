"""Fuzz-style tests: random input must never crash with an unexpected error.

Deterministic (seeded) rather than a property-testing dependency, so the suite
stays dependency-free. The contracts being checked:

- ``parse`` raises only ``Incomplete`` or ``ProtocolError``.
- ``Value.from_bytes`` raises only ``InvalidValue`` and is canonical (stable).
- ``decode_records`` never raises on arbitrary bytes.
- the client reply parser raises only ``_NeedMore`` / ``ProtocolError`` / ``CommandError``.
"""

from __future__ import annotations

import math
import random

from creek.client import _NeedMore, _parse_reply, _ServerError
from creek.errors import InvalidValue, ProtocolError
from creek.persist import decode_records
from creek.protocol import Command, Incomplete, parse
from creek.value import Value, ValueType

_INT_RANGES = {
    ValueType.I4: (-8, 7),
    ValueType.U4: (0, 15),
    ValueType.I8: (-128, 127),
    ValueType.U8: (0, 255),
    ValueType.I16: (-32768, 32767),
    ValueType.U16: (0, 65535),
    ValueType.I32: (-(2**31), 2**31 - 1),
    ValueType.U32: (0, 2**32 - 1),
}
_FLOATS = (ValueType.F8, ValueType.F16, ValueType.F32, ValueType.F64)


def _random_bytes(rng: random.Random, maximum: int = 40) -> bytes:
    return bytes(rng.randrange(256) for _ in range(rng.randrange(maximum)))


def _random_value(rng: random.Random) -> Value:
    vtype = rng.choice(list(ValueType))
    if vtype is ValueType.STR:
        text = "".join(chr(rng.randrange(32, 127)) for _ in range(rng.randrange(20)))
        return Value(vtype, text)
    if vtype is ValueType.BOOL:
        return Value(vtype, rng.choice([True, False]))
    if vtype in _FLOATS:
        return Value(vtype, rng.choice([rng.uniform(-1000, 1000), 0.0, math.inf, -math.inf]))
    low, high = _INT_RANGES[vtype]
    return Value.from_int(vtype, rng.randint(low, high))


def test_parse_random_bytes_never_crashes() -> None:
    rng = random.Random(1234)
    for _ in range(5000):
        data = _random_bytes(rng)
        try:
            command, used = parse(data)
        except (Incomplete, ProtocolError):
            continue
        assert command is None or isinstance(command, Command)
        assert 0 <= used <= len(data)


def test_parse_random_bytes_final_never_crashes() -> None:
    rng = random.Random(4321)
    for _ in range(5000):
        data = _random_bytes(rng)
        try:
            command, used = parse(data, final=True)
        except (Incomplete, ProtocolError):
            continue
        assert command is None or isinstance(command, Command)
        assert 0 <= used <= len(data)


def test_from_bytes_random_never_crashes() -> None:
    rng = random.Random(11)
    for _ in range(5000):
        data = _random_bytes(rng, maximum=16)
        try:
            Value.from_bytes(data)
        except InvalidValue:
            continue


def test_value_binary_is_canonical() -> None:
    rng = random.Random(7)
    for _ in range(3000):
        value = _random_value(rng)
        once = Value.from_bytes(value.to_bytes())
        twice = Value.from_bytes(once.to_bytes())
        assert once.to_bytes() == twice.to_bytes()
        if not (isinstance(once.data, float) and math.isnan(once.data)):
            assert once == twice


def test_decode_records_never_crashes() -> None:
    rng = random.Random(13)
    for _ in range(3000):
        records = decode_records(_random_bytes(rng, maximum=64))
        assert isinstance(records, list)


def test_client_parse_reply_never_crashes() -> None:
    rng = random.Random(17)
    for _ in range(3000):
        data = _random_bytes(rng, maximum=32)
        try:
            _value, used = _parse_reply(data)
        except (_NeedMore, ProtocolError):
            continue
        assert 0 < used <= len(data)


def test_client_parse_reply_roundtrips_a_frame() -> None:
    from creek.client import encode_command

    # encode a command, then parse a reply frame of the same shape
    assert encode_command("SET", ["k", "v"]) == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
    assert _parse_reply(b"*2\r\n$3\r\nstr\r\n$1\r\nv\r\n") == (["str", "v"], 20)
    assert _parse_reply(b"-ERR nope\r\n") == (_ServerError("ERR", "nope"), 11)
