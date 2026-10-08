"""Hash-slot sharding: slot computation, topology, and redirects (M7b).

Creek uses Redis-compatible hash slots: ``CRC16(key) % 16384``, with the same
hash-tag rule (``{tag}`` pins a key to the slot of ``tag``). A node is
configured with a static topology (``--cluster-config``); a command whose key
belongs to another node gets a ``MOVED`` redirect, and a multi-key command whose
keys span slots gets ``CROSSSLOT``.

Membership is static — there is no gossip, resharding, or automatic migration.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from creek.errors import PersistError

SLOT_COUNT = 16384


def _crc16(data: bytes) -> int:
    """CRC16-CCITT (XMODEM), matching Redis's key hashing."""
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def key_slot(key: str) -> int:
    """Return the hash slot for ``key`` (Redis-compatible)."""
    start = key.find("{")
    if start != -1:
        end = key.find("}", start + 1)
        if end != -1 and end > start + 1:
            key = key[start + 1 : end]
    return _crc16(key.encode("utf-8")) % SLOT_COUNT


@dataclass(frozen=True, slots=True)
class ClusterNode:
    """One node and the inclusive slot ranges it owns."""

    addr: str
    slots: tuple[tuple[int, int], ...]


class Cluster:
    """A static slot map: which node owns each slot."""

    def __init__(self, self_addr: str, nodes: list[ClusterNode]) -> None:
        self.self_addr = self_addr
        self.nodes = nodes
        self._owners: dict[int, ClusterNode] = {}
        for node in nodes:
            for start, end in node.slots:
                for slot in range(start, end + 1):
                    self._owners[slot] = node

    @classmethod
    def from_file(cls, path: Path) -> Cluster:
        """Load a topology from JSON (see ``AGENTS.md`` §17, M7b)."""
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PersistError(f"cannot read cluster config {path}: {exc}") from exc
        if not isinstance(data, dict) or "self" not in data or "nodes" not in data:
            raise PersistError("cluster config needs 'self' and 'nodes'")
        nodes = [
            ClusterNode(str(node["addr"]), tuple((int(r[0]), int(r[1])) for r in node["slots"]))
            for node in data["nodes"]
        ]
        return cls(str(data["self"]), nodes)

    def owner(self, slot: int) -> ClusterNode | None:
        """The node owning ``slot``, or ``None`` if unassigned."""
        return self._owners.get(slot)

    def owns(self, slot: int) -> bool:
        """Whether this node owns ``slot``."""
        node = self._owners.get(slot)
        return node is not None and node.addr == self.self_addr

    def assigned(self) -> int:
        """How many slots are assigned to some node."""
        return len(self._owners)


__all__ = ["SLOT_COUNT", "Cluster", "ClusterNode", "key_slot"]
