"""The TCP and WebSocket listeners.

Both transports carry the same RESP2 bytes and call the same
:func:`creek.commands.execute`. TCP is a byte stream; WebSocket is one command
per binary frame.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import mimetypes
from dataclasses import dataclass
from pathlib import Path

from websockets.asyncio.server import Server as WsServer
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from creek.auth import ANONYMOUS, Authenticator
from creek.commands import Context, execute
from creek.errors import CommandError, ProtocolError
from creek.protocol import Command, Error, Incomplete, Reply, encode, parse

log = logging.getLogger("creek.server")

_READ_LIMIT = 1024 * 1024
_MAX_MESSAGE = 1024 * 1024


@dataclass(slots=True)
class Config:
    """Server configuration derived from CLI flags."""

    ws_addr: str = ":7100"
    tcp_addr: str = ":7101"
    web_dir: Path | None = None
    data_dir: Path | None = None
    auth_file: Path | None = None
    fsync: str = "everysec"
    snapshot_interval: float = 300.0


def parse_addr(addr: str) -> tuple[str, int]:
    """Split ``":7100"`` / ``"127.0.0.1:7100"`` into ``(host, port)``."""
    host, _, port = addr.rpartition(":")
    if not port:
        raise ValueError(f"invalid address {addr!r}")
    try:
        return host, int(port)
    except ValueError as exc:
        raise ValueError(f"invalid port in address {addr!r}") from exc


class Server:
    """Runs the TCP and WebSocket listeners over one event loop."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.authenticator = Authenticator.from_file(config.auth_file) if config.auth_file else None
        self._tcp: asyncio.Server | None = None
        self._ws: WsServer | None = None
        self._stop = asyncio.Event()
        self.tcp_port = 0
        self.ws_port = 0

    async def start(self) -> None:
        """Bind both listeners; ports are available afterwards."""
        host, port = parse_addr(self.config.tcp_addr)
        self._tcp = await asyncio.start_server(self._handle_tcp, host, port, limit=_READ_LIMIT)
        self.tcp_port = int(self._tcp.sockets[0].getsockname()[1])

        host, port = parse_addr(self.config.ws_addr)
        self._ws = await serve(
            self._handle_ws,
            host,
            port,
            max_size=_MAX_MESSAGE,
            process_request=self._serve_http,
            logger=log,
        )
        self.ws_port = int(self._ws.sockets[0].getsockname()[1])
        log.info(
            "creek listening",
            extra={"tcp_port": self.tcp_port, "ws_port": self.ws_port},
        )

    async def serve_forever(self) -> None:
        """Block until :meth:`request_stop` is called."""
        await self._stop.wait()

    def request_stop(self) -> None:
        """Ask :meth:`serve_forever` to return."""
        self._stop.set()

    async def close(self) -> None:
        """Close both listeners and wait for in-flight handlers."""
        if self._tcp is not None:
            self._tcp.close()
            await self._tcp.wait_closed()
        if self._ws is not None:
            self._ws.close()
            await self._ws.wait_closed()

    def _new_context(self, peer: str) -> Context:
        return Context(
            peer=peer,
            identity=None if self.authenticator is not None else ANONYMOUS,
            authenticator=self.authenticator,
        )

    async def _run(self, command: Command, context: Context) -> Reply:
        try:
            return await execute(command, context)
        except CommandError as exc:
            return Error(str(exc))

    async def _handle_tcp(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = _peer_name(writer.get_extra_info("peername"))
        context = self._new_context(peer)
        buffer = b""
        log.debug("tcp client connected", extra={"peer": peer})
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                buffer += chunk
                while buffer:
                    try:
                        command, used = parse(buffer)
                    except Incomplete:
                        break
                    except ProtocolError as exc:
                        writer.write(encode(Error(f"ERR {exc}")))
                        await writer.drain()
                        return
                    buffer = buffer[used:]
                    if command is None:
                        continue
                    writer.write(encode(await self._run(command, context)))
                    await writer.drain()
                    if context.close_after_reply:
                        return
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()
            log.debug("tcp client disconnected", extra={"peer": peer})

    async def _handle_ws(self, websocket: ServerConnection) -> None:
        peer = _peer_name(websocket.remote_address)
        context = self._new_context(peer)
        log.debug("ws client connected", extra={"peer": peer})
        try:
            async for message in websocket:
                frame = message.encode() if isinstance(message, str) else message
                try:
                    command, used = parse(frame, final=True)
                    if command is None or used != len(frame):
                        raise ProtocolError("invalid frame")
                except ProtocolError as exc:
                    await websocket.send(encode(Error(f"ERR {exc}")))
                    continue
                await websocket.send(encode(await self._run(command, context)))
                if context.close_after_reply:
                    await websocket.close()
                    return
        except ConnectionClosed:
            pass
        finally:
            log.debug("ws client disconnected", extra={"peer": peer})

    async def _serve_http(self, connection: ServerConnection, request: Request) -> Response | None:
        path = request.path.split("?", 1)[0]
        if path == "/ws":
            return None
        web_dir = self.config.web_dir
        if web_dir is None:
            return _response(404, b"not found", "text/plain; charset=utf-8")
        root = web_dir.resolve()
        file = (root / (path.lstrip("/") or "index.html")).resolve()
        if not file.is_file() or root not in file.parents:
            return _response(404, b"not found", "text/plain; charset=utf-8")
        body = await asyncio.to_thread(file.read_bytes)
        content_type = mimetypes.guess_type(file.name)[0] or "application/octet-stream"
        return _response(200, body, content_type)


def _response(status: int, body: bytes, content_type: str) -> Response:
    headers = Headers(
        [
            ("Content-Type", content_type),
            ("Content-Length", str(len(body))),
        ]
    )
    return Response(status, "OK" if status < 400 else "Not Found", headers, body)


def _peer_name(address: object) -> str:
    if isinstance(address, tuple) and len(address) >= 2:
        return f"{address[0]}:{address[1]}"
    return str(address)
