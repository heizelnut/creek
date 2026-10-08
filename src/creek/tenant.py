"""Tenants: explicit lifecycle for namespaces (M6a).

A tenant is a namespace with metadata (and, from M6b, limits). Tenants are
**auto-provisioned** when a client authenticates into one, so existing auth
files keep working; the admin API (`TENANT …`) manages them explicitly.

Dropping a tenant clears its keys, channels, and retained values. Because a
namespace can always be re-provisioned by a client that authenticates into it,
`TENANT DROP` is "delete the data and forget the record", not a permanent ban.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from creek.persist import Journal

TenantRecord = tuple[str, float]


@dataclass(slots=True)
class Tenant:
    """A tenant: a namespace plus its metadata."""

    name: str
    created_at: float


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

    def create(self, name: str) -> Tenant:
        """Create a tenant; idempotent if it already exists."""
        tenant = self._tenants.get(name)
        if tenant is not None:
            return tenant
        tenant = Tenant(name, self._clock())
        self._tenants[name] = tenant
        if self.journal is not None:
            self.journal.tenant_create(name, tenant.created_at)
        return tenant

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

    def entries(self) -> list[TenantRecord]:
        """All tenants for snapshotting: ``(name, created_at)``."""
        return [(tenant.name, tenant.created_at) for tenant in self._tenants.values()]

    def restore(self, name: str, created_at: float) -> None:
        """Recover a tenant without journaling."""
        self._tenants[name] = Tenant(name, created_at)


__all__ = ["Tenant", "TenantRecord", "TenantRegistry"]
