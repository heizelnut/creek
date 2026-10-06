"""The TCP and WebSocket listeners.

Both transports carry the same RESP2 bytes and call the same
:func:`creek.commands.execute`. TCP is a byte stream; WebSocket is one command
per binary frame.

Each connection has one outbound queue (:class:`creek.pubsub.Subscriber`) drained
by a single writer task, so replies and pub/sub messages never interleave.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import mimetypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from websockets.asyncio.server import Server as WsServer
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from creek import persist
from creek.auth import ANONYMOUS, Authenticator
from creek.commands import Context, execute
from creek.errors import CommandError, ProtocolError
from creek.persist import Wal, decode_blob, write_snapshot
from creek.protocol import Command, Error, Incomplete, Reply, encode, parse
from creek.pubsub import Bus, Subscriber
from creek.store import Store

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
        self.store = Store()
        self.bus = Bus()
        self.journal: Wal | None = None
        self._snapshot_task: asyncio.Task[None] | None = None
        if config.data_dir is not None:
            config.data_dir.mkdir(parents=True, exist_ok=True)
            self._recover(config.data_dir)
            self.journal = Wal(config.data_dir / "creek.wal", fsync=config.fsync)
            self.store.journal = self.journal
            self.bus.journal = self.journal
        self._tcp: asyncio.Server | None = None
        self._ws: WsServer | None = None
        self._stop = asyncio.Event()
        self.tcp_port = 0
        self.ws_port = 0

    async def start(self) -> None:
        """Bind both listeners; ports are available afterwards."""
        if self.journal is not None:
            self.journal.start()
            if self.config.snapshot_interval > 0:
                self._snapshot_task = asyncio.create_task(self._snapshot_loop())
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
        """Close both listeners, snapshot, and close the WAL."""
        if self._tcp is not None:
            self._tcp.close()
            await self._tcp.wait_closed()
        if self._ws is not None:
            self._ws.close()
            await self._ws.wait_closed()
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._snapshot_task
            self._snapshot_task = None
        if self.journal is not None:
            await self._snapshot()
            await self.journal.close()

    def _recover(self, data_dir: Path) -> None:
        entries, retained, records = persist.load(data_dir)
        for owner, key, value, expires_at in entries:
            self.store.restore(owner, key, value, expires_at)
        for owner, channel, value in retained:
            self.bus.restore_retained(owner, channel, value)
        for record in records:
            self._apply(record)
        if entries or retained or records:
            log.info(
                "recovered",
                extra={
                    "entries": len(entries),
                    "retained": len(retained),
                    "wal_records": len(records),
                },
            )

    def _apply(self, record: dict[str, Any]) -> None:
        op = record.get("op")
        if op == "set":
            value = decode_blob(record["data"])
            self.store.restore(record["owner"], record["key"], value, record.get("expires_at"))
        elif op == "delete":
            self.store.delete(record["owner"], record["key"])
        elif op == "flush":
            self.store.clear(record["owner"])
        elif op == "retain":
            self.bus.restore_retained(
                record["owner"], record["channel"], decode_blob(record["data"])
            )
        else:
            log.warning("ignoring unknown WAL record", extra={"op": op})

    async def _snapshot(self) -> None:
        data_dir = self.config.data_dir
        if data_dir is None:
            return
        entries = self.store.entries()
        retained = self.bus.retained_entries()
        await asyncio.to_thread(write_snapshot, data_dir / "creek.snapshot", entries, retained)

    async def _snapshot_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.snapshot_interval)
            await self._snapshot()

    def _new_context(self, peer: str) -> Context:
        anonymous = self.authenticator is None or self.authenticator.allow_anonymous
        identity = ANONYMOUS if anonymous else None
        namespace = identity.namespace if identity is not None else "default"
        return Context(
            peer=peer,
            identity=identity,
            authenticator=self.authenticator,
            store=self.store,
            bus=self.bus,
            subscriber=Subscriber(namespace),
        )

    async def _run(self, command: Command, context: Context) -> Reply:
        try:
            return await execute(command, context)
        except CommandError as exc:
            return Error(str(exc))

    async def _dispatch(self, command: Command, context: Context) -> None:
        """Run a command, then enqueue its reply and any deferred messages."""
        reply = await self._run(command, context)
        subscriber = context.subscriber
        await subscriber.send(encode(reply))
        for frame in context.pending:
            subscriber.deliver(frame)
        context.pending.clear()

    async def _handle_tcp(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = _peer_name(writer.get_extra_info("peername"))
        context = self._new_context(peer)
        subscriber = context.subscriber
        pump = asyncio.create_task(_pump_tcp(writer, subscriber))
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
                        await subscriber.send(encode(Error(f"ERR {exc}")))
                        await subscriber.join()
                        return
                    buffer = buffer[used:]
                    if command is None:
                        continue
                    await self._dispatch(command, context)
                    if context.close_after_reply:
                        await subscriber.join()
                        return
        except (ConnectionError, OSError):
            pass
        finally:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump
            self.bus.remove(subscriber)
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()
            log.debug("tcp client disconnected", extra={"peer": peer})

    async def _handle_ws(self, websocket: ServerConnection) -> None:
        peer = _peer_name(websocket.remote_address)
        context = self._new_context(peer)
        subscriber = context.subscriber
        pump = asyncio.create_task(_pump_ws(websocket, subscriber))
        log.debug("ws client connected", extra={"peer": peer})
        try:
            async for message in websocket:
                frame = message.encode() if isinstance(message, str) else message
                try:
                    command, used = parse(frame, final=True)
                    if command is None or used != len(frame):
                        raise ProtocolError("invalid frame")
                except ProtocolError as exc:
                    await subscriber.send(encode(Error(f"ERR {exc}")))
                    continue
                await self._dispatch(command, context)
                if context.close_after_reply:
                    await subscriber.join()
                    await websocket.close()
                    return
        except ConnectionClosed:
            pass
        finally:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump
            self.bus.remove(subscriber)
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


async def _pump_tcp(writer: asyncio.StreamWriter, subscriber: Subscriber) -> None:
    while True:
        frame = await subscriber.next_frame()
        try:
            writer.write(frame)
            await writer.drain()
        except (ConnectionError, OSError):
            pass  # keep draining so a pending join() can complete
        finally:
            subscriber.done()


async def _pump_ws(websocket: ServerConnection, subscriber: Subscriber) -> None:
    while True:
        frame = await subscriber.next_frame()
        try:
            await websocket.send(frame)
        except ConnectionClosed:
            pass
        finally:
            subscriber.done()


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
