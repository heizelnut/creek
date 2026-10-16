"""Consume write commands from a Redis Stream and forward them to PLCs (``PLC.md`` §1).

Redis cannot tell us that a client wrote a key, so writes are explicit: a SCADA client appends an
entry to a command stream and the daemon consumes it::

    XADD plc:commands * key motor.speed value 50

The stream is a consumer group, so entries are acknowledged only after the PLC write succeeds.
Failed commands (unknown tag, read-only tag, driver error) are logged and acknowledged anyway so
one bad entry cannot block the queue; the error is visible on the ``creek.audit`` logger.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from creek.errors import CreekError
from creek.mirror import RedisLike
from creek.plc.manager import PlcManager
from creek.plc.poller import parse_tag_value

log = logging.getLogger("creek.audit")

_MAX_BACKOFF = 30.0


class CommandError(CreekError):
    """A stream entry is malformed."""


@dataclass(frozen=True, slots=True)
class WriteCommand:
    """One parsed stream entry: a tag key and its new value, in engineering units."""

    key: str
    value: str
    id: str


def parse_entry(record_id: Any, fields: Mapping[Any, Any]) -> WriteCommand:
    """Parse a stream entry into a :class:`WriteCommand`."""
    decoded = {_text(k): _text(v) for k, v in fields.items()}
    key = decoded.get("key")
    if not key:
        raise CommandError(f"entry {record_id!r} has no 'key' field")
    if "value" not in decoded:
        raise CommandError(f"entry {record_id!r} has no 'value' field")
    return WriteCommand(key, decoded["value"], _text(record_id))


def _text(raw: Any) -> str:
    return raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)


class CommandConsumer:
    """Reads write commands from a Redis Stream and applies them to PLCs."""

    def __init__(
        self,
        manager: PlcManager,
        redis: RedisLike,
        *,
        stream: str = "plc:commands",
        group: str = "creek-plc",
        consumer: str = "creek-plc",
        batch: int = 32,
        block_ms: int = 5000,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.manager = manager
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.batch = batch
        self.block_ms = block_ms
        self.writes = 0
        self.failures = 0
        self._redis = redis
        self._clock = clock
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Create the consumer group (if needed) and start consuming."""
        try:
            await self._redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as exc:  # redis raises ResponseError("BUSYGROUP ...") when it exists
            if "BUSYGROUP" not in str(exc):
                raise
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop consuming."""
        task = self._task
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._task = None

    async def _run(self) -> None:
        backoff = 0.0
        while True:
            try:
                entries = await self._redis.xreadgroup(
                    self.group,
                    self.consumer,
                    {self.stream: ">"},
                    count=self.batch,
                    block=self.block_ms,
                )
                backoff = 0.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Redis down: back off rather than die
                backoff = self.block_ms / 1000 if backoff == 0.0 else min(backoff * 2, _MAX_BACKOFF)
                log.warning(
                    "command stream read failed",
                    extra={"stream": self.stream, "error": str(exc), "backoff": backoff},
                )
                await asyncio.sleep(backoff)
                continue
            for _stream, records in entries or []:
                for record_id, fields in records:
                    await self.handle(record_id, fields)

    async def handle(self, record_id: Any, fields: Mapping[Any, Any]) -> None:
        """Apply one entry and acknowledge it, logging any failure."""
        try:
            command = parse_entry(record_id, fields)
            tag = self.manager.tag(command.key)
            if tag is None:
                raise CommandError(f"unknown PLC tag {command.key!r}")
            value = parse_tag_value(tag, command.value)
            await self.manager.write(command.key, value)
            self.writes += 1
            log.info(
                "plc write",
                extra={
                    "actor": self.consumer,
                    "key": command.key,
                    "value": command.value,
                    "stream": self.stream,
                    "entry": command.id,
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failures += 1
            log.warning(
                "plc write rejected",
                extra={"entry": _text(record_id), "error": str(exc), "stream": self.stream},
            )
        finally:
            with contextlib.suppress(Exception):
                await self._redis.xack(self.stream, self.group, _text(record_id))


__all__ = ["CommandConsumer", "CommandError", "WriteCommand", "parse_entry"]
