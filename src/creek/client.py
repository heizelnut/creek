"""An async Python client for Creek.

    import asyncio
    from creek.client import connect

    async def main():
        async with await connect("ws://localhost:7100/ws") as client:
            await client.set("k", "v")
            print(await client.get("k"))   # ['str', 'v']

    asyncio.run(main())

Both transports are supported: ``ws://`` / ``wss://`` and ``tcp://host:port``
(or a bare ``host:port``). Replies are decoded to native Python values; a
server error raises :class:`creek.errors.CommandError`.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterable
from types import TracebackType
from typing import Any, Protocol

from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect

from creek.errors import CommandError, ProtocolError

CRLF = b"\r\n"


class _NeedMore(Exception):
    """The buffer holds only part of a reply."""


def encode_command(name: str, args: Iterable[Any] = ()) -> bytes:
    """Encode a command as a RESP2 array of bulk strings."""
    parts = [name, *(str(arg) for arg in args)]
    out = bytearray(f"*{len(parts)}\r\n".encode())
    for part in parts:
        encoded = part.encode("utf-8")
        out += f"${len(encoded)}\r\n".encode() + encoded + CRLF
    return bytes(out)


def _parse_reply(data: bytes, pos: int = 0) -> tuple[Any, int]:
    if pos >= len(data):
        raise _NeedMore
    end = data.find(CRLF, pos)
    if end == -1:
        raise _NeedMore
    kind = data[pos : pos + 1]
    rest = data[pos + 1 : end]
    if kind == b"+":
        return rest.decode("utf-8", "replace"), end + 2
    if kind == b"-":
        code, _, message = rest.decode("utf-8", "replace").partition(" ")
        raise CommandError(message, code=code or "ERR")
    if kind == b":":
        return int(rest), end + 2
    if kind == b"$":
        length = int(rest)
        if length == -1:
            return None, end + 2
        start = end + 2
        if len(data) < start + length + 2:
            raise _NeedMore
        return data[start : start + length].decode("utf-8", "replace"), start + length + 2
    if kind == b"*":
        items: list[Any] = []
        cursor = end + 2
        for _ in range(int(rest)):
            item, cursor = _parse_reply(data, cursor)
            items.append(item)
        return items, cursor
    raise ProtocolError(f"unknown reply type {kind!r}")


class _Connection(Protocol):
    async def send(self, data: bytes) -> None: ...

    async def receive(self) -> Any: ...

    async def close(self) -> None: ...


class _WsConnection:
    def __init__(self, ws: ClientConnection) -> None:
        self._ws = ws

    @classmethod
    async def connect(cls, uri: str) -> _WsConnection:
        return cls(await ws_connect(uri))

    async def send(self, data: bytes) -> None:
        await self._ws.send(data)

    async def receive(self) -> Any:
        message = await self._ws.recv()
        data = message.encode("utf-8") if isinstance(message, str) else message
        value, used = _parse_reply(data)
        if used != len(data):
            raise ProtocolError("trailing data in frame")
        return value

    async def close(self) -> None:
        await self._ws.close()


class _TcpConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._buffer = b""

    @classmethod
    async def connect(cls, host: str, port: int) -> _TcpConnection:
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    async def send(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def receive(self) -> Any:
        while True:
            try:
                value, used = _parse_reply(self._buffer)
            except _NeedMore:
                chunk = await self._reader.read(65536)
                if not chunk:
                    raise ConnectionError("connection closed") from None
                self._buffer += chunk
                continue
            self._buffer = self._buffer[used:]
            return value

    async def close(self) -> None:
        self._writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await self._writer.wait_closed()


class Client:
    """A Creek client over WebSocket or TCP."""

    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the connection."""
        await self._connection.close()

    async def execute(self, name: str, *args: Any) -> Any:
        """Send a command and return the decoded reply."""
        await self._connection.send(encode_command(name, args))
        return await self._connection.receive()

    # --- convenience wrappers ---

    async def ping(self) -> Any:
        return await self.execute("PING")

    async def set(
        self,
        key: str,
        value: Any,
        *,
        type: str | None = None,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Any:
        """``SET key value [TYPE t] [EX s|PX ms] [NX|XX]``."""
        args: list[Any] = [key, value]
        if type is not None:
            args += ["TYPE", type]
        if ex is not None:
            args += ["EX", ex]
        if px is not None:
            args += ["PX", px]
        if nx:
            args.append("NX")
        if xx:
            args.append("XX")
        return await self.execute("SET", *args)

    async def get(self, key: str) -> Any:
        return await self.execute("GET", key)

    async def delete(self, *keys: str) -> Any:
        return await self.execute("DEL", *keys)

    async def publish(
        self, channel: str, value: Any, *, type: str | None = None, retain: bool = False
    ) -> Any:
        """``PUBLISH channel value [TYPE t] [RETAIN]``."""
        args: list[Any] = [channel, value]
        if type is not None:
            args += ["TYPE", type]
        if retain:
            args.append("RETAIN")
        return await self.execute("PUBLISH", *args)

    async def subscribe(self, *channels: str) -> AsyncIterator[tuple[str, str, str]]:
        """Subscribe and yield ``(channel, type, value)`` for each message.

        A subscribed client should not run other commands concurrently.
        """
        await self.execute("SUBSCRIBE", *channels)
        while True:
            reply = await self._connection.receive()
            if isinstance(reply, list) and len(reply) >= 4 and reply[0] == "message":
                yield (str(reply[1]), str(reply[2]), str(reply[3]))


async def connect(uri: str) -> Client:
    """Connect to ``ws://…`` / ``wss://…`` or ``tcp://host:port`` (or ``host:port``)."""
    if uri.startswith(("ws://", "wss://")):
        return Client(await _WsConnection.connect(uri))
    target = uri.removeprefix("tcp://")
    host, _, port = target.rpartition(":")
    if not port:
        raise ValueError(f"invalid address {uri!r}")
    return Client(await _TcpConnection.connect(host or "127.0.0.1", int(port)))


__all__ = ["Client", "connect", "encode_command"]
