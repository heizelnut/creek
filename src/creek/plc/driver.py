"""The PLC driver interface (``PLC.md`` §3).

A driver speaks one wire protocol and knows nothing about Creek's store, pub/sub, or tenancy: it
maps a :class:`creek.plc.tags.Tag` address to a typed :class:`creek.value.Value`.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from creek.plc.tags import Tag
from creek.value import Value


class PlcDriver(Protocol):
    """Reads and writes PLC points. Implementations are protocol-specific."""

    async def connect(self) -> None:
        """Open the connection (idempotent)."""
        ...

    async def close(self) -> None:
        """Close the connection."""
        ...

    async def read(self, tags: Sequence[Tag]) -> dict[str, Value]:
        """Read the given tags, returning a mapping of tag key to value.

        Tags the device cannot supply are omitted rather than fabricated.
        """
        ...

    async def write(self, tag: Tag, value: Value) -> None:
        """Write ``value`` to ``tag``'s PLC address."""
        ...


__all__ = ["PlcDriver"]
