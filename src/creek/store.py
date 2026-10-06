"""In-memory key-value store with TTL and namespace ownership.

One plain dict keyed by ``(owner, key)``. ``owner`` is the client namespace
(``AGENTS.md`` §8), derived from the identity, never from the client. All
methods are synchronous: the event loop serializes access (``AGENTS.md`` §11).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from creek.value import Value


@dataclass(frozen=True, slots=True)
class Entry:
    """A stored value plus its absolute expiry (``None`` means no TTL)."""

    value: Value
    expires_at: float | None


class Store:
    """A single dict of ``(owner, key) -> Entry`` with lazy expiry."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._data: dict[tuple[str, str], Entry] = {}
        self._clock = clock

    def get(self, owner: str, key: str) -> Value | None:
        """Return the live value, or ``None`` if absent or expired."""
        entry = self._live(owner, key)
        return entry.value if entry is not None else None

    def set(
        self,
        owner: str,
        key: str,
        value: Value,
        *,
        ttl: float | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Store ``value``; honor ``NX``/``XX``. Returns whether it was written.

        ``ttl`` is in seconds; ``None`` clears any existing TTL.
        """
        exists = self._live(owner, key) is not None
        if nx and exists:
            return False
        if xx and not exists:
            return False
        self._data[(owner, key)] = Entry(value, self._deadline(ttl))
        return True

    def update(self, owner: str, key: str, value: Value) -> None:
        """Replace the value, keeping any existing TTL (used by INCR/DECR)."""
        entry = self._live(owner, key)
        deadline = entry.expires_at if entry is not None else None
        self._data[(owner, key)] = Entry(value, deadline)

    def delete(self, owner: str, key: str) -> bool:
        """Delete a key; returns whether it existed."""
        if self._live(owner, key) is None:
            return False
        del self._data[(owner, key)]
        return True

    def exists(self, owner: str, key: str) -> bool:
        """Whether a live key exists."""
        return self._live(owner, key) is not None

    def expire(self, owner: str, key: str, ttl: float) -> bool:
        """Set a TTL in seconds; ``ttl <= 0`` deletes. Returns whether it existed."""
        entry = self._live(owner, key)
        if entry is None:
            return False
        if ttl <= 0:
            del self._data[(owner, key)]
        else:
            self._data[(owner, key)] = Entry(entry.value, self._clock() + ttl)
        return True

    def ttl(self, owner: str, key: str) -> int:
        """Remaining seconds, ``-1`` if no TTL, or ``-2`` if the key is gone."""
        entry = self._live(owner, key)
        if entry is None:
            return -2
        if entry.expires_at is None:
            return -1
        return max(0, int(entry.expires_at - self._clock()))

    def keys(self, owner: str) -> list[str]:
        """Live keys for ``owner``, sorted."""
        result: list[str] = []
        for (entry_owner, key), entry in list(self._data.items()):
            if entry_owner != owner:
                continue
            if entry.expires_at is not None and entry.expires_at <= self._clock():
                del self._data[(entry_owner, key)]
                continue
            result.append(key)
        return sorted(result)

    def dbsize(self, owner: str) -> int:
        """Number of live keys for ``owner``."""
        return len(self.keys(owner))

    def _live(self, owner: str, key: str) -> Entry | None:
        entry = self._data.get((owner, key))
        if entry is None:
            return None
        if entry.expires_at is not None and entry.expires_at <= self._clock():
            del self._data[(owner, key)]
            return None
        return entry

    def _deadline(self, ttl: float | None) -> float | None:
        return None if ttl is None else self._clock() + ttl


__all__ = ["Entry", "Store"]
