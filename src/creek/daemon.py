"""Wire a PLC config to a Redis instance and run the sync loop (``PLC.md`` §1).

The daemon owns three moving parts: a :class:`~creek.plc.manager.PlcManager` (one driver + poller
per connection), a :class:`~creek.mirror.RedisMirror` (polled values → Redis), and a
:class:`~creek.commands.CommandConsumer` (Redis Stream → PLC writes).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from creek.commands import CommandConsumer
from creek.mirror import MemoryMirror, Mirror, RedisLike, RedisMirror
from creek.plc.manager import DriverFactory, PlcManager
from creek.plc.tags import PlcConfig

log = logging.getLogger("creek")


@dataclass(frozen=True, slots=True)
class RedisConfig:
    """Where and how to talk to Redis."""

    url: str = "redis://127.0.0.1:6379/0"
    key_prefix: str = "plc:"
    command_stream: str = "plc:commands"
    consumer_group: str = "creek"
    timestamps: bool = False


def default_consumer_name() -> str:
    """A consumer name unique per process, so several daemons can share a group."""
    return f"{socket.gethostname()}-{os.getpid()}"


class Daemon:
    """Runs the PLC ↔ Redis sync."""

    def __init__(
        self,
        config: PlcConfig,
        redis_config: RedisConfig | None = None,
        *,
        dry_run: bool = False,
        redis: RedisLike | None = None,
        drivers: dict[str, DriverFactory] | None = None,
        consumer_name: str | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.config = config
        self.redis_config = redis_config or RedisConfig()
        self.dry_run = dry_run
        self.mirror: Mirror | None = None
        self.manager: PlcManager | None = None
        self.consumer: CommandConsumer | None = None
        self._redis = redis
        self._drivers = drivers
        self._consumer_name = consumer_name or default_consumer_name()
        self._clock = clock if clock is not None else time.time
        self._stop = asyncio.Event()

    async def start(self) -> None:
        """Connect to Redis, start polling, and start consuming write commands."""
        if self.dry_run:
            self.mirror = MemoryMirror()
        else:
            self._redis = self._redis or self._connect()
            self.mirror = RedisMirror(
                self._redis,
                prefix=self.redis_config.key_prefix,
                timestamps=self.redis_config.timestamps,
                clock=self._clock,
            )
        self.manager = PlcManager(
            self.config, self.mirror, drivers=self._drivers, clock=self._clock
        )
        self.manager.start()
        if not self.dry_run:
            assert self._redis is not None
            self.consumer = CommandConsumer(
                self.manager,
                self._redis,
                stream=self.redis_config.command_stream,
                group=self.redis_config.consumer_group,
                consumer=self._consumer_name,
                clock=self._clock,
            )
            await self.consumer.start()
        log.info(
            "creek ready",
            extra={
                "connections": len(self.config.connections),
                "tags": len(self.config.tags),
                "dry_run": self.dry_run,
                "redis": None if self.dry_run else self.redis_config.url,
            },
        )

    async def close(self) -> None:
        """Stop consuming and polling, and release the Redis connection."""
        if self.consumer is not None:
            await self.consumer.stop()
            self.consumer = None
        if self.manager is not None:
            await self.manager.close()
            self.manager = None
        if not self.dry_run and self._redis is not None:
            with contextlib.suppress(Exception):
                await self._redis.aclose()
            self._redis = None

    async def serve_forever(self) -> None:
        """Block until :meth:`request_stop` is called."""
        await self._stop.wait()

    def request_stop(self) -> None:
        """Ask :meth:`serve_forever` to return."""
        self._stop.set()

    def _connect(self) -> RedisLike:
        from redis.asyncio import Redis  # imported lazily so --dry-run needs no redis

        client = Redis.from_url(self.redis_config.url, decode_responses=True)
        return cast("RedisLike", client)


__all__ = ["Daemon", "RedisConfig", "default_consumer_name"]
