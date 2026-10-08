"""In-memory key-value store with TTL, namespace ownership, and a journal.

One plain dict keyed by ``(owner, key)``. ``owner`` is the client namespace
(``AGENTS.md`` §8), derived from the identity, never from the client. All
methods are synchronous: the event loop serializes access (``AGENTS.md`` §11).

Every mutation is reported to an optional :class:`creek.persist.Journal` (the
WAL). Recovery uses :meth:`restore` / :meth:`clear`, which do not journal.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from creek.persist import Journal
from creek.value import Value


@dataclass(frozen=True, slots=True)
class Entry:
    """A stored value plus its absolute expiry (``None`` means no TTL)."""

    value: Value
    expires_at: float | None


EntryRecord = tuple[str, str, Value, float | None]


class Store:
    """A single dict of ``(owner, key) -> Entry`` with lazy expiry."""

    def __init__(
        self,
        clock: Callable[[], float] = time.time,
        journal: Journal | None = None,
    ) -> None:
        self._data: dict[tuple[str, str], Entry] = {}
        self._clock = clock
        self.journal = journal

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
        deadline = self._deadline(ttl)
        self._data[(owner, key)] = Entry(value, deadline)
        self._log_set(owner, key, value, deadline)
        return True

    def update(self, owner: str, key: str, value: Value) -> None:
        """Replace the value, keeping any existing TTL (used by INCR/DECR)."""
        entry = self._live(owner, key)
        deadline = entry.expires_at if entry is not None else None
        self._data[(owner, key)] = Entry(value, deadline)
        self._log_set(owner, key, value, deadline)

    def delete(self, owner: str, key: str) -> bool:
        """Delete a key; returns whether it existed."""
        if self._live(owner, key) is None:
            return False
        del self._data[(owner, key)]
        self._log_delete(owner, key)
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
            self._log_delete(owner, key)
        else:
            deadline = self._clock() + ttl
            self._data[(owner, key)] = Entry(entry.value, deadline)
            self._log_set(owner, key, entry.value, deadline)
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

    def usage(self, owner: str) -> tuple[int, int]:
        """Live ``(key_count, approximate_bytes)`` for ``owner``.

        Bytes are ``len(key) + len(value.to_bytes())`` per entry — a rough
        estimate, not allocator overhead.
        """
        count = 0
        total = 0
        now = self._clock()
        for (entry_owner, key), entry in list(self._data.items()):
            if entry_owner != owner:
                continue
            if entry.expires_at is not None and entry.expires_at <= now:
                del self._data[(entry_owner, key)]
                continue
            count += 1
            total += len(key.encode("utf-8")) + len(entry.value.to_bytes())
        return count, total

    def clear(self, owner: str) -> None:
        """Delete every key for ``owner`` (FLUSHALL)."""
        for key in [item for item in self._data if item[0] == owner]:
            del self._data[key]
        if self.journal is not None:
            self.journal.flush(owner)

    def entries(self) -> list[EntryRecord]:
        """Live entries for snapshotting: ``(owner, key, value, expires_at)``."""
        now = self._clock()
        result: list[EntryRecord] = []
        for (owner, key), entry in list(self._data.items()):
            if entry.expires_at is not None and entry.expires_at <= now:
                del self._data[(owner, key)]
                continue
            result.append((owner, key, entry.value, entry.expires_at))
        return result

    def restore(self, owner: str, key: str, value: Value, expires_at: float | None) -> None:
        """Recover an entry with an absolute deadline; skips already-expired ones."""
        if expires_at is not None and expires_at <= self._clock():
            return
        self._data[(owner, key)] = Entry(value, expires_at)

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

    def _log_set(self, owner: str, key: str, value: Value, expires_at: float | None) -> None:
        if self.journal is not None:
            self.journal.set(owner, key, value, expires_at)

    def _log_delete(self, owner: str, key: str) -> None:
        if self.journal is not None:
            self.journal.delete(owner, key)


__all__ = ["Entry", "EntryRecord", "Store"]
