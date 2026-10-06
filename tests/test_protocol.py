"""Codec tests: the parser is a pure function, so no network is needed."""

from __future__ import annotations

import pytest

from creek.errors import ProtocolError
from creek.protocol import Command, Error, Incomplete, SimpleString, encode, encode_message, parse


def test_parse_array() -> None:
    data = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"
    command, used = parse(data)
    assert command == Command("SET", ("a", "b"))
    assert used == len(data)


def test_parse_inline() -> None:
    command, used = parse(b"PING hello\r\n")
    assert command == Command("PING", ("hello",))
    assert used == len(b"PING hello\r\n")


def test_parse_inline_without_newline_when_final() -> None:
    command, _ = parse(b"PING", final=True)
    assert command == Command("PING")


def test_parse_incomplete_without_newline() -> None:
    with pytest.raises(Incomplete):
        parse(b"PING")


def test_parse_array_incomplete() -> None:
    with pytest.raises(Incomplete):
        parse(b"*2\r\n$4\r\nECHO\r\n")


def test_parse_blank_line() -> None:
    command, used = parse(b"\r\n")
    assert command is None
    assert used == 2


def test_parse_rejects_bad_framing() -> None:
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n:1\r\n")


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        (SimpleString("OK"), b"+OK\r\n"),
        (Error("ERR bad"), b"-ERR bad\r\n"),
        (None, b"$-1\r\n"),
        (1, b":1\r\n"),
        ("hi", b"$2\r\nhi\r\n"),
        (["a", "b"], b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"),
    ],
)
def test_encode(reply: object, expected: bytes) -> None:
    assert encode(reply) == expected  # type: ignore[arg-type]


def test_encode_message() -> None:
    assert encode_message("news", "str", "hi") == (
        b"*4\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$3\r\nstr\r\n$2\r\nhi\r\n"
    )
