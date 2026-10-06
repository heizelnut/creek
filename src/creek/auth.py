"""Client identification: a token resolves to an :class:`Identity`.

Auth recognizes the client and selects its namespace; it is not a full security
boundary in v1 (see ``AGENTS.md`` §8).
"""

from __future__ import annotations

import hmac
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_ALL = frozenset({"all", "*"})


@dataclass(frozen=True, slots=True)
class Identity:
    """An authenticated principal and the namespace it owns."""

    id: str
    namespace: str
    acl: frozenset[str]

    def allows(self, scope: str) -> bool:
        """Return whether this identity may run a command in ``scope``."""
        return bool(self.acl & _ALL) or scope in self.acl


ANONYMOUS = Identity(id="anonymous", namespace="default", acl=frozenset({"all"}))


class AuthConfigError(Exception):
    """The auth file is missing or malformed."""


class Authenticator:
    """Resolves tokens to identities, comparing tokens in constant time."""

    def __init__(self, clients: dict[str, Identity], allow_anonymous: bool) -> None:
        self._clients = clients
        self.allow_anonymous = allow_anonymous

    @classmethod
    def from_file(cls, path: Path) -> Authenticator:
        """Load an auth file (see ``AGENTS.md`` §8 for the schema)."""
        try:
            raw: Any = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise AuthConfigError(f"cannot read auth file {path}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise AuthConfigError(f"invalid JSON in auth file {path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise AuthConfigError("auth file must contain a JSON object")
        clients_raw = raw.get("clients", [])
        if not isinstance(clients_raw, list):
            raise AuthConfigError("'clients' must be an array")
        clients: dict[str, Identity] = {}
        for entry in clients_raw:
            if not isinstance(entry, dict):
                raise AuthConfigError("each client must be an object")
            token = entry.get("token")
            if not isinstance(token, str) or not token:
                raise AuthConfigError("each client needs a non-empty 'token'")
            clients[token] = Identity(
                id=str(entry.get("id", token)),
                namespace=str(entry.get("namespace", "default")),
                acl=frozenset(str(item) for item in entry.get("acl", ["all"])),
            )
        return cls(clients, bool(raw.get("allow_anonymous", False)))

    def authenticate(self, token: str) -> Identity | None:
        """Return the identity for ``token``, or ``None`` if unknown."""
        match: Identity | None = None
        for stored, identity in self._clients.items():
            if hmac.compare_digest(stored, token):
                match = identity
        return match
