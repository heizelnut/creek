"""The RESP2 codec.

Parsing is a plain function over bytes, so it is easy to test and is shared by
both transports: TCP feeds it a growing buffer, WebSocket feeds it one frame.

    bytes --parse--> (Command, bytes_used) ... execute ... Reply --encode--> bytes
"""

from __future__ import annotations

from dataclasses import dataclass

from creek.errors import ProtocolError

CRLF = b"\r\n"
MAX_BULK = 512 * 1024


class Incomplete(Exception):
    """Not enough bytes for a full command yet (streaming transports)."""


@dataclass(frozen=True, slots=True)
class Command:
    """A normalized command: an upper-case name plus string arguments."""

    name: str
    args: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SimpleString:
    """A status reply, e.g. ``+OK``."""

    value: str


@dataclass(frozen=True, slots=True)
class Error:
    """An error reply; ``value`` already includes the code, e.g. ``ERR bad``."""

    value: str


type Reply = SimpleString | Error | int | str | list[Reply] | None


def parse(data: bytes, *, final: bool = False) -> tuple[Command | None, int]:
    """Parse one command from the start of ``data``.

    Returns ``(command, bytes_used)``; ``command`` is ``None`` for a blank line.

    Raises:
        Incomplete: ``data`` holds only part of a command and ``final`` is false.
        ProtocolError: the framing is malformed.
    """
    if not data:
        raise Incomplete
    if data[0:1] == b"*":
        return _parse_array(data)
    return _parse_inline(data, final)


def _parse_inline(data: bytes, final: bool) -> tuple[Command | None, int]:
    end = data.find(b"\n")
    if end == -1:
        if not final:
            raise Incomplete
        line, used = data, len(data)
    else:
        line, used = data[:end], end + 1
    parts = line.split()
    if not parts:
        return None, used
    return _command(parts), used


def _parse_array(data: bytes) -> tuple[Command, int]:
    pos = 0
    count, pos = _header(data, pos, b"*")
    parts: list[bytes] = []
    for _ in range(count):
        length, pos = _header(data, pos, b"$")
        if length < 0 or length > MAX_BULK:
            raise ProtocolError("invalid bulk length")
        end = pos + length
        if len(data) < end + 2:
            raise Incomplete
        if data[end : end + 2] != CRLF:
            raise ProtocolError("malformed bulk string")
        parts.append(data[pos:end])
        pos = end + 2
    if not parts:
        raise ProtocolError("empty command")
    return _command(parts), pos


def _header(data: bytes, pos: int, prefix: bytes) -> tuple[int, int]:
    end = data.find(b"\n", pos)
    if end == -1:
        raise Incomplete
    line = data[pos:end].strip()
    if not line.startswith(prefix):
        raise ProtocolError(f"expected '{prefix.decode()}'")
    try:
        return int(line[1:]), end + 1
    except ValueError as exc:
        raise ProtocolError("invalid length") from exc


def _command(parts: list[bytes]) -> Command:
    name = parts[0].decode("utf-8", "replace").upper()
    args = tuple(part.decode("utf-8", "replace") for part in parts[1:])
    return Command(name, args)


def encode(reply: Reply) -> bytes:
    """Encode a reply as RESP2 bytes."""
    if isinstance(reply, SimpleString):
        return b"+" + reply.value.encode() + CRLF
    if isinstance(reply, Error):
        return b"-" + reply.value.encode() + CRLF
    if reply is None:
        return b"$-1\r\n"
    if isinstance(reply, bool):  # bool is an int subclass; check it first
        return b":1\r\n" if reply else b":0\r\n"
    if isinstance(reply, int):
        return b":" + str(reply).encode() + CRLF
    if isinstance(reply, str):
        return _bulk(reply.encode())
    if isinstance(reply, list):
        out = bytearray(b"*" + str(len(reply)).encode() + CRLF)
        for item in reply:
            out += encode(item)
        return bytes(out)
    raise TypeError(f"cannot encode reply of type {type(reply).__name__}")


def encode_message(channel: str, vtype: str, value: str) -> bytes:
    """Encode a pub/sub delivery as a 4-element RESP array."""
    return encode(["message", channel, vtype, value])


def _bulk(data: bytes) -> bytes:
    return b"$" + str(len(data)).encode() + CRLF + data + CRLF
