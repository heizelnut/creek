"""Client identification: a token resolves to an :class:`Identity`.

Tokens may be stored plaintext (development) or hashed with scrypt. Auth
recognizes the client and selects its namespace; it is not a full security
boundary in v1 (see ``AGENTS.md`` §8).

Hashed tokens are compared by trying every stored verifier (so the number of
``scrypt`` runs is the number of hashed clients). That keeps the comparison
constant-time across clients; it is fine for the small client counts v1 targets.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_ALL = frozenset({"all", "*"})

_SCRYPT_N = 2**14
_SCRYPT_R = 8
_SCRYPT_P = 1
_SCRYPT_LEN = 32


class AuthConfigError(Exception):
    """The auth file is missing or malformed."""


@dataclass(frozen=True, slots=True)
class ScryptHash:
    """A salted scrypt verifier, serialized as ``scrypt$n$r$p$salt$hash``."""

    salt: bytes
    digest: bytes
    n: int = _SCRYPT_N
    r: int = _SCRYPT_R
    p: int = _SCRYPT_P

    def verify(self, token: str) -> bool:
        """Constant-time check of ``token`` against this verifier."""
        candidate = hashlib.scrypt(
            token.encode("utf-8"),
            salt=self.salt,
            n=self.n,
            r=self.r,
            p=self.p,
            dklen=len(self.digest),
        )
        return hmac.compare_digest(candidate, self.digest)

    def to_string(self) -> str:
        salt = base64.b64encode(self.salt).decode("ascii")
        digest = base64.b64encode(self.digest).decode("ascii")
        return f"scrypt${self.n}${self.r}${self.p}${salt}${digest}"

    @classmethod
    def parse(cls, text: str) -> ScryptHash:
        try:
            scheme, n, r, p, salt, digest = text.split("$")
            if scheme != "scrypt":
                raise ValueError(scheme)
            return cls(
                salt=base64.b64decode(salt, validate=True),
                digest=base64.b64decode(digest, validate=True),
                n=int(n),
                r=int(r),
                p=int(p),
            )
        except (ValueError, TypeError) as exc:
            raise AuthConfigError(f"invalid scrypt token: {text!r}") from exc


def hash_token(token: str, *, n: int = _SCRYPT_N, r: int = _SCRYPT_R, p: int = _SCRYPT_P) -> str:
    """Hash a token for storage in the auth file."""
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(token.encode("utf-8"), salt=salt, n=n, r=r, p=p, dklen=_SCRYPT_LEN)
    return ScryptHash(salt, digest, n, r, p).to_string()


Verifier = str | ScryptHash


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


class Authenticator:
    """Resolves tokens to identities, comparing tokens in constant time."""

    def __init__(self, entries: list[tuple[Verifier, Identity]], allow_anonymous: bool) -> None:
        self._entries = entries
        self.allow_anonymous = allow_anonymous

    @classmethod
    def from_tokens(cls, tokens: dict[str, Identity], allow_anonymous: bool) -> Authenticator:
        """Build from plaintext tokens (tests and simple setups)."""
        return cls(list(tokens.items()), allow_anonymous)

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
        entries: list[tuple[Verifier, Identity]] = []
        for entry in clients_raw:
            if not isinstance(entry, dict):
                raise AuthConfigError("each client must be an object")
            token = entry.get("token")
            if not isinstance(token, str) or not token:
                raise AuthConfigError("each client needs a non-empty 'token'")
            verifier: Verifier = ScryptHash.parse(token) if token.startswith("scrypt$") else token
            entries.append(
                (
                    verifier,
                    Identity(
                        id=str(entry.get("id", "client")),
                        namespace=str(entry.get("namespace", "default")),
                        acl=frozenset(str(item) for item in entry.get("acl", ["all"])),
                    ),
                )
            )
        return cls(entries, bool(raw.get("allow_anonymous", False)))

    def authenticate(self, token: str) -> Identity | None:
        """Return the identity for ``token``, or ``None`` if unknown."""
        match: Identity | None = None
        for verifier, identity in self._entries:
            if isinstance(verifier, str):
                if hmac.compare_digest(verifier, token):
                    match = identity
            elif verifier.verify(token):
                match = identity
        return match


__all__ = [
    "ANONYMOUS",
    "AuthConfigError",
    "Authenticator",
    "Identity",
    "ScryptHash",
    "hash_token",
]
