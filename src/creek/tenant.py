"""Tenants: explicit lifecycle and quotas for namespaces (M6).

A tenant is a namespace with metadata and optional limits. Tenants are
auto-provisioned when a client authenticates into one; the admin API
(`TENANT …`) manages them explicitly.

Dropping a tenant clears its data and forgets the record; it can be
re-provisioned by authenticating again (DROP is "forget", not a permanent ban).

Memory is an **approximation**: ``len(key) + len(value.to_bytes())`` for each
key plus retained channel values. It is not allocator overhead.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from creek.persist import Journal, TenantRecord

_QUOTA_FIELDS = ("max_keys", "max_memory", "max_connections", "max_subscriptions", "max_rate")


def _optional_int(value: int | float | None) -> int | None:
    return None if value is None else int(value)


@dataclass(frozen=True, slots=True)
class Quota:
    """Per-tenant limits; ``None`` means unlimited."""

    max_keys: int | None = None
    max_memory: int | None = None
    max_connections: int | None = None
    max_subscriptions: int | None = None
    max_rate: float | None = None

    def to_dict(self) -> dict[str, int | float | None]:
        return {name: getattr(self, name) for name in _QUOTA_FIELDS}

    @classmethod
    def from_dict(cls, data: dict[str, int | float | None]) -> Quota:
        rate = data.get("max_rate")
        return cls(
            max_keys=_optional_int(data.get("max_keys")),
            max_memory=_optional_int(data.get("max_memory")),
            max_connections=_optional_int(data.get("max_connections")),
            max_subscriptions=_optional_int(data.get("max_subscriptions")),
            max_rate=None if rate is None else float(rate),
        )


@dataclass(slots=True)
class Tenant:
    """A tenant: a namespace, its metadata, limits, and live counters."""

    name: str
    created_at: float
    quota: Quota = field(default_factory=Quota)
    active_connections: int = 0
    _tokens: float = 0.0
    _rate_last: float = -1.0

    def check_rate(self, now: float) -> bool:
        """Token-bucket rate limit; ``True`` if a command may proceed."""
        rate = self.quota.max_rate
        if rate is None:
            return True
        if self._rate_last < 0.0:
            self._rate_last = now
            self._tokens = float(rate)
        elapsed = max(0.0, now - self._rate_last)
        self._tokens = min(float(rate), self._tokens + elapsed * rate)
        self._rate_last = now
        if self._tokens < 1.0:
            return False
        self._tokens -= 1.0
        return True


class TenantRegistry:
    """The set of tenants, shared across connections like the store and bus."""

    def __init__(
        self,
        clock: Callable[[], float] = time.time,
        journal: Journal | None = None,
    ) -> None:
        self._tenants: dict[str, Tenant] = {}
        self._clock = clock
        self.journal = journal

    def get(self, name: str) -> Tenant | None:
        """Return the tenant, or ``None`` if it does not exist."""
        return self._tenants.get(name)

    def ensure(self, name: str) -> Tenant:
        """Return the tenant, creating it (and journaling) if needed."""
        tenant = self._tenants.get(name)
        return tenant if tenant is not None else self.create(name)

    def create(self, name: str, quota: Quota | None = None) -> Tenant:
        """Create a tenant (or update its quota); idempotent."""
        tenant = self._tenants.get(name)
        if tenant is None:
            tenant = Tenant(name, self._clock(), quota or Quota())
            self._tenants[name] = tenant
        elif quota is not None:
            tenant.quota = quota
        self._log(tenant)
        return tenant

    def set_quota(self, name: str, quota: Quota) -> bool:
        """Replace a tenant's quota; ``False`` if the tenant does not exist."""
        tenant = self._tenants.get(name)
        if tenant is None:
            return False
        tenant.quota = quota
        self._log(tenant)
        return True

    def drop(self, name: str) -> bool:
        """Remove a tenant record; returns whether it existed."""
        if self._tenants.pop(name, None) is None:
            return False
        if self.journal is not None:
            self.journal.tenant_drop(name)
        return True

    def names(self) -> list[str]:
        """Tenant names, sorted."""
        return sorted(self._tenants)

    def allow(self, name: str) -> bool:
        """Rate-limit check for one command."""
        tenant = self._tenants.get(name)
        return True if tenant is None else tenant.check_rate(self._clock())

    def acquire(self, name: str) -> bool:
        """Take a connection slot; ``False`` if the tenant is at its limit."""
        tenant = self._tenants.get(name)
        if tenant is None:
            return True
        limit = tenant.quota.max_connections
        if limit is not None and tenant.active_connections >= limit:
            return False
        tenant.active_connections += 1
        return True

    def release(self, name: str) -> None:
        """Release a connection slot."""
        tenant = self._tenants.get(name)
        if tenant is not None and tenant.active_connections > 0:
            tenant.active_connections -= 1

    def entries(self) -> list[TenantRecord]:
        """All tenants for snapshotting: ``(name, created_at, quota)``."""
        return [
            (tenant.name, tenant.created_at, tenant.quota.to_dict())
            for tenant in self._tenants.values()
        ]

    def restore(self, name: str, created_at: float, quota: dict[str, int | float | None]) -> None:
        """Recover a tenant without journaling."""
        self._tenants[name] = Tenant(name, created_at, Quota.from_dict(quota))

    def _log(self, tenant: Tenant) -> None:
        if self.journal is not None:
            self.journal.tenant_create(tenant.name, tenant.created_at, tenant.quota.to_dict())


__all__ = ["Quota", "Tenant", "TenantRecord", "TenantRegistry"]
