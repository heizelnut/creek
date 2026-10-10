"""PLC tag map: connections and tags (``PLC.md`` §2)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from creek.errors import CreekError


class PlcConfigError(CreekError):
    """The PLC config file is missing or malformed."""


@dataclass(frozen=True, slots=True)
class Connection:
    """One PLC connection: how to reach it and where its tags live."""

    name: str
    driver: str
    host: str
    port: int
    namespace: str
    poll_interval: float = 1.0
    params: dict[str, int | str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Tag:
    """One PLC point mapped to a Creek key."""

    key: str
    connection: str
    address: str
    type: str
    writable: bool = False
    scale: float = 1.0
    offset: float = 0.0


@dataclass(frozen=True, slots=True)
class PlcConfig:
    """The full tag map."""

    connections: dict[str, Connection]
    tags: list[Tag]

    @classmethod
    def from_file(cls, path: Path) -> PlcConfig:
        """Load and validate a PLC config file (``PLC.md`` §2)."""
        try:
            raw: Any = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise PlcConfigError(f"cannot read PLC config {path}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise PlcConfigError(f"invalid JSON in PLC config {path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise PlcConfigError("PLC config must contain a JSON object")

        connections: dict[str, Connection] = {}
        for entry in raw.get("connections", []):
            if not isinstance(entry, dict) or "name" not in entry:
                raise PlcConfigError("each connection needs a 'name'")
            name = str(entry["name"])
            connections[name] = Connection(
                name=name,
                driver=str(entry.get("driver", "")),
                host=str(entry.get("host", "127.0.0.1")),
                port=int(entry.get("port", 0)),
                namespace=str(entry.get("namespace", name)),
                poll_interval=float(entry.get("poll_interval", 1.0)),
                params=dict(entry.get("params", {})),
            )

        tags: list[Tag] = []
        for entry in raw.get("tags", []):
            if not isinstance(entry, dict) or "key" not in entry or "address" not in entry:
                raise PlcConfigError("each tag needs 'key' and 'address'")
            connection = str(entry.get("connection", ""))
            if connection not in connections:
                raise PlcConfigError(
                    f"tag {entry['key']!r} references unknown connection {connection!r}"
                )
            tags.append(
                Tag(
                    key=str(entry["key"]),
                    connection=connection,
                    address=str(entry["address"]),
                    type=str(entry.get("type", "u16")),
                    writable=bool(entry.get("writable", False)),
                    scale=float(entry.get("scale", 1.0)),
                    offset=float(entry.get("offset", 0.0)),
                )
            )
        return cls(connections, tags)

    def tags_for(self, connection: str) -> list[Tag]:
        """The tags belonging to one connection."""
        return [tag for tag in self.tags if tag.connection == connection]


__all__ = ["Connection", "PlcConfig", "PlcConfigError", "Tag"]
