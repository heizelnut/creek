"""Publish/subscribe: subscription registry, fanout, and retained messages.

Fanout is a linear scan over the subscriber sets (``AGENTS.md`` §11). Delivery
is non-blocking: a subscriber whose outbound buffer is full is dropped from the
bus rather than blocking the publisher.

Each connection owns one :class:`Subscriber`. Its queue carries both command
replies (``send``, which applies backpressure) and pub/sub messages (``deliver``,
which never blocks). A single writer task drains the queue, so frames never
interleave.
"""

from __future__ import annotations

import asyncio

from creek.pattern import match
from creek.protocol import encode_message
from creek.value import Value

_MAX_QUEUE = 256


class Subscriber:
    """One connection's subscriptions plus its outbound frame queue."""

    def __init__(self, owner: str = "default", capacity: int = _MAX_QUEUE) -> None:
        self.owner = owner
        self.channels: set[str] = set()
        self.patterns: set[str] = set()
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=capacity)

    @property
    def count(self) -> int:
        """Total channel + pattern subscriptions."""
        return len(self.channels) + len(self.patterns)

    def deliver(self, frame: bytes) -> bool:
        """Enqueue a pub/sub frame without blocking; ``False`` if the buffer is full."""
        try:
            self._queue.put_nowait(frame)
        except asyncio.QueueFull:
            return False
        return True

    async def send(self, frame: bytes) -> None:
        """Enqueue a reply, waiting for room (backpressure)."""
        await self._queue.put(frame)

    async def next_frame(self) -> bytes:
        """Wait for the next outbound frame."""
        return await self._queue.get()

    def done(self) -> None:
        """Mark the last frame as written."""
        self._queue.task_done()

    async def join(self) -> None:
        """Wait until every queued frame has been written."""
        await self._queue.join()


class Bus:
    """Subscription registry and fanout, scoped by namespace (owner)."""

    def __init__(self) -> None:
        self._channels: dict[tuple[str, str], set[Subscriber]] = {}
        self._patterns: dict[tuple[str, str], set[Subscriber]] = {}
        self._retained: dict[tuple[str, str], Value] = {}

    def subscribe(self, subscriber: Subscriber, channel: str) -> None:
        """Subscribe ``subscriber`` to a literal channel."""
        subscriber.channels.add(channel)
        self._channels.setdefault((subscriber.owner, channel), set()).add(subscriber)

    def unsubscribe(self, subscriber: Subscriber, channel: str) -> bool:
        """Unsubscribe from a channel; returns whether it was subscribed."""
        if channel not in subscriber.channels:
            return False
        subscriber.channels.discard(channel)
        self._drop(self._channels, (subscriber.owner, channel), subscriber)
        return True

    def psubscribe(self, subscriber: Subscriber, pattern: str) -> None:
        """Subscribe ``subscriber`` to a glob pattern."""
        subscriber.patterns.add(pattern)
        self._patterns.setdefault((subscriber.owner, pattern), set()).add(subscriber)

    def punsubscribe(self, subscriber: Subscriber, pattern: str) -> bool:
        """Unsubscribe from a pattern; returns whether it was subscribed."""
        if pattern not in subscriber.patterns:
            return False
        subscriber.patterns.discard(pattern)
        self._drop(self._patterns, (subscriber.owner, pattern), subscriber)
        return True

    def remove(self, subscriber: Subscriber) -> None:
        """Forget a disconnecting subscriber entirely."""
        for channel in list(subscriber.channels):
            self.unsubscribe(subscriber, channel)
        for pattern in list(subscriber.patterns):
            self.punsubscribe(subscriber, pattern)

    def publish(self, owner: str, channel: str, value: Value) -> int:
        """Deliver to channel and pattern subscribers; return the number reached."""
        frame = encode_message(channel, value.type.tag, value.text())
        targets: set[Subscriber] = set(self._channels.get((owner, channel), ()))
        for (pattern_owner, pattern), members in self._patterns.items():
            if pattern_owner == owner and match(pattern, channel):
                targets.update(members)
        delivered = 0
        for subscriber in targets:
            if subscriber.deliver(frame):
                delivered += 1
            else:
                self.remove(subscriber)  # slow or failed subscriber: drop it
        return delivered

    def retain(self, owner: str, channel: str, value: Value) -> None:
        """Store the channel's last value for future subscribers."""
        self._retained[(owner, channel)] = value

    def retained(self, owner: str, channel: str) -> Value | None:
        """The retained value for a channel, if any."""
        return self._retained.get((owner, channel))

    def retained_channels(self, owner: str, pattern: str = "*") -> list[str]:
        """Retained channels matching ``pattern``, sorted."""
        return sorted(
            channel
            for (channel_owner, channel) in self._retained
            if channel_owner == owner and match(pattern, channel)
        )

    def channels(self, owner: str, pattern: str = "*") -> list[str]:
        """Channels with at least one subscriber, sorted."""
        return sorted(
            channel
            for (channel_owner, channel) in self._channels
            if channel_owner == owner and match(pattern, channel)
        )

    def numsub(self, owner: str, channel: str) -> int:
        """Number of direct subscribers to ``channel``."""
        return len(self._channels.get((owner, channel), ()))

    def numpat(self, owner: str) -> int:
        """Number of distinct patterns subscribed to in ``owner``."""
        return sum(1 for (pattern_owner, _) in self._patterns if pattern_owner == owner)

    @staticmethod
    def _drop(
        table: dict[tuple[str, str], set[Subscriber]],
        key: tuple[str, str],
        subscriber: Subscriber,
    ) -> None:
        members = table.get(key)
        if members is None:
            return
        members.discard(subscriber)
        if not members:
            del table[key]


__all__ = ["Bus", "Subscriber"]
