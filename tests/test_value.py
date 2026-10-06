"""Value model tests: text parsing, range checks, and the binary codec."""

from __future__ import annotations

import math
import random
import struct

import pytest

from creek.errors import InvalidValue
from creek.value import MAX_STR, Value, ValueType

ALL_TAGS = [
    "i4",
    "u4",
    "i8",
    "u8",
    "i16",
    "u16",
    "i32",
    "u32",
    "f8",
    "f16",
    "f32",
    "f64",
    "str",
    "bool",
]


@pytest.mark.parametrize("tag", ALL_TAGS)
def test_tag_roundtrip(tag: str) -> None:
    assert ValueType.from_tag(tag).tag == tag


def test_unknown_tag_is_rejected() -> None:
    with pytest.raises(InvalidValue):
        ValueType.from_tag("nope")


@pytest.mark.parametrize(
    ("tag", "raw", "expected"),
    [
        ("i4", "-8", -8),
        ("i4", "7", 7),
        ("u4", "0", 0),
        ("u4", "15", 15),
        ("i8", "-128", -128),
        ("u8", "255", 255),
        ("i16", "-32768", -32768),
        ("u16", "65535", 65535),
        ("i32", "-2147483648", -(2**31)),
        ("u32", "4294967295", 2**32 - 1),
        ("f32", "0.5", 0.5),
        ("f64", "-2.5", -2.5),
        ("str", "hi", "hi"),
        ("bool", "true", True),
        ("bool", "false", False),
    ],
)
def test_parse(tag: str, raw: str, expected: object) -> None:
    assert Value.parse(tag, raw).data == expected


@pytest.mark.parametrize(
    ("tag", "raw"),
    [
        ("i4", "8"),
        ("i4", "-9"),
        ("u4", "-1"),
        ("u4", "16"),
        ("i8", "128"),
        ("u8", "256"),
        ("i32", "2147483648"),
        ("u32", "-1"),
        ("bool", "maybe"),
        ("i32", "abc"),
        ("f32", "abc"),
        ("str", "x" * (MAX_STR + 1)),
    ],
)
def test_parse_rejects_bad_input(tag: str, raw: str) -> None:
    with pytest.raises(InvalidValue):
        Value.parse(tag, raw)


def test_bool_accepts_one_and_zero() -> None:
    assert Value.parse("bool", "1").data is True
    assert Value.parse("bool", "0").data is False


@pytest.mark.parametrize(
    ("tag", "raw", "expected"),
    [
        ("i4", "-1", b"\x01\x0f"),
        ("u4", "15", b"\x02\x0f"),
        ("i8", "-1", b"\x03\xff"),
        ("u8", "255", b"\x04\xff"),
        ("i16", "-1", b"\x05\xff\xff"),
        ("u16", "65535", b"\x06\xff\xff"),
        ("i32", "-1", b"\x07\xff\xff\xff\xff"),
        ("u32", "1", b"\x08\x01\x00\x00\x00"),
        ("f8", "1.0", b"\x09\x38"),
        ("f16", "1.0", b"\x0a\x00\x3c"),
        ("f32", "1.0", b"\x0b\x00\x00\x80\x3f"),
        ("f64", "1.0", b"\x0c" + struct.pack("<d", 1.0)),
        ("str", "hi", b"\x0d\x02hi"),
        ("bool", "true", b"\x0e\x01"),
    ],
)
def test_binary_golden(tag: str, raw: str, expected: bytes) -> None:
    assert Value.parse(tag, raw).to_bytes() == expected
    assert Value.from_bytes(expected) == Value.parse(tag, raw)


@pytest.mark.parametrize(
    ("tag", "raw"),
    [
        ("i4", "-8"),
        ("u4", "15"),
        ("i8", "-128"),
        ("u8", "255"),
        ("i16", "-32768"),
        ("u16", "65535"),
        ("i32", "-2147483648"),
        ("u32", "4294967295"),
        ("f8", "448.0"),
        ("f16", "0.5"),
        ("f32", "-2.5"),
        ("f64", "3.14159"),
        ("str", "hello, world"),
        ("str", "x" * 300),
        ("bool", "false"),
    ],
)
def test_binary_roundtrip(tag: str, raw: str) -> None:
    value = Value.parse(tag, raw)
    assert Value.from_bytes(value.to_bytes()) == value


def test_string_max_length_boundary() -> None:
    longest = "x" * MAX_STR
    assert Value.from_bytes(Value.parse("str", longest).to_bytes()).data == longest


@pytest.mark.parametrize(
    ("tag", "raw", "text"),
    [
        ("i32", "5", "5"),
        ("bool", "true", "true"),
        ("bool", "false", "false"),
        ("str", "hi", "hi"),
        ("f64", "-2.5", "-2.5"),
        ("f8", "448.0", "448.0"),
    ],
)
def test_text(tag: str, raw: str, text: str) -> None:
    assert Value.parse(tag, raw).text() == text


# --- f8 (OCP E4M3) -------------------------------------------------------


@pytest.mark.parametrize(
    ("byte", "expected"),
    [
        (0x00, 0.0),
        (0x01, 2.0**-9),
        (0x38, 1.0),
        (0x7E, 448.0),
        (0xB8, -1.0),
    ],
)
def test_f8_decode(byte: int, expected: float) -> None:
    assert Value.from_bytes(bytes([0x09, byte])).data == expected


def test_f8_decode_nan() -> None:
    assert math.isnan(Value.from_bytes(b"\x09\x7f").data)


def test_f8_decode_negative_zero() -> None:
    decoded = Value.from_bytes(b"\x09\x80").data
    assert decoded == 0.0
    assert math.copysign(1.0, decoded) == -1.0


@pytest.mark.parametrize(
    ("value", "byte"),
    [
        (0.0, 0x00),
        (1.0, 0x38),
        (448.0, 0x7E),
        (-1.0, 0xB8),
        (1e9, 0x7E),
        (-1e9, 0xFE),
        (float("inf"), 0x7E),
        (float("-inf"), 0xFE),
    ],
)
def test_f8_encode(value: float, byte: int) -> None:
    assert Value(ValueType.F8, value).to_bytes() == bytes([0x09, byte])


def test_f8_encode_nan() -> None:
    assert Value(ValueType.F8, float("nan")).to_bytes() == b"\x09\x7f"


def test_f8_encode_is_nearest() -> None:
    table = [Value.from_bytes(bytes([0x09, b])).data for b in range(256)]
    finite = [value for value in table if not math.isnan(value)]
    random.seed(0)
    for _ in range(2000):
        target = random.uniform(-500.0, 500.0)
        got = table[Value(ValueType.F8, target).to_bytes()[1]]
        nearest = min(finite, key=lambda value: abs(value - target))
        assert abs(got - target) <= abs(nearest - target) + 1e-9


# --- f16 / f32 overflow --------------------------------------------------


@pytest.mark.parametrize("value", [0.0, 1.0, -1.0, 0.5, -2.5, 65504.0, 2.0**-24])
def test_f16_roundtrip(value: float) -> None:
    assert Value.from_bytes(Value(ValueType.F16, value).to_bytes()).data == value


@pytest.mark.parametrize(
    ("tag", "value", "sign"),
    [
        ("f16", 1e10, 1.0),  # f16 max is 65504
        ("f16", -1e10, -1.0),
        ("f32", 1e40, 1.0),  # f32 max is ~3.4e38
        ("f32", -1e40, -1.0),
    ],
)
def test_float_overflow_rounds_to_inf(tag: str, value: float, sign: float) -> None:
    decoded = Value.from_bytes(Value(ValueType.from_tag(tag), value).to_bytes()).data
    assert math.isinf(decoded)
    assert math.copysign(1.0, decoded) == sign


# --- malformed binary ----------------------------------------------------


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"\xff",  # unknown type byte
        b"\x03\x01\x02",  # i8 with too much payload
        b"\x07\x01\x02",  # i32 with too little payload
        b"\x0e",  # bool with no payload
        b"\x0d\x05hi",  # str length says 5, only 2 present
        b"\x0d\x02hi!",  # str with trailing byte
        b"\x0d\x80",  # truncated varint
    ],
)
def test_from_bytes_rejects_malformed(data: bytes) -> None:
    with pytest.raises(InvalidValue):
        Value.from_bytes(data)
