"""M6c hardening: length limits, per-tenant counters, and audit logging."""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest

from creek.auth import Identity
from creek.commands import MAX_CHANNEL_BYTES, MAX_KEY_BYTES, Context, execute
from creek.errors import CommandError
from creek.protocol import Command, SimpleString
from creek.pubsub import Bus, Subscriber
from creek.store import Store
from creek.tenant import TenantRegistry

Tenant = Callable[..., Context]


@pytest.fixture
def world() -> Tenant:
    store = Store()
    bus = Bus()
    tenants = TenantRegistry()

    def make(namespace: str, acl: tuple[str, ...] = ("all",)) -> Context:
        return Context(
            identity=Identity(namespace, namespace, frozenset(acl)),
            store=store,
            bus=bus,
            tenants=tenants,
            subscriber=Subscriber(namespace),
        )

    return make


# --- length limits --------------------------------------------------------


async def test_key_too_long(world: Tenant) -> None:
    context = world("acme")
    with pytest.raises(CommandError) as info:
        await execute(Command("SET", ("k" * (MAX_KEY_BYTES + 1), "v")), context)
    assert "too long" in str(info.value)
    # the boundary itself is allowed
    assert await execute(Command("SET", ("k" * MAX_KEY_BYTES, "v")), context) == SimpleString("OK")


async def test_multi_key_commands_are_checked(world: Tenant) -> None:
    context = world("acme")
    long_key = "k" * (MAX_KEY_BYTES + 1)
    with pytest.raises(CommandError):
        await execute(Command("MGET", ("ok", long_key)), context)
    with pytest.raises(CommandError):
        await execute(Command("DEL", ("ok", long_key)), context)
    with pytest.raises(CommandError):
        await execute(Command("EXISTS", (long_key,)), context)


async def test_channel_too_long(world: Tenant) -> None:
    context = world("acme")
    long_channel = "c" * (MAX_CHANNEL_BYTES + 1)
    with pytest.raises(CommandError):
        await execute(Command("SUBSCRIBE", (long_channel,)), context)
    with pytest.raises(CommandError):
        await execute(Command("PUBLISH", (long_channel, "v")), context)


# --- counters -------------------------------------------------------------


async def test_counters_are_reported(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme", "MAXKEYS", "1")), admin)
    await execute(Command("SET", ("a", "1")), tenant)
    with pytest.raises(CommandError):
        await execute(Command("SET", ("b", "1")), tenant)  # key quota
    info = await execute(Command("TENANT", ("INFO", "acme")), admin)
    assert info[info.index("commands") + 1] >= 2
    assert info[info.index("rejected") + 1] == 1


async def test_connections_opened_counter(world: Tenant) -> None:
    admin = world("admin")
    registry = admin.tenants
    registry.create("acme")
    assert registry.acquire("acme") is True
    registry.release("acme")
    tenant = registry.get("acme")
    assert tenant is not None
    assert tenant.connections_opened == 1
    assert tenant.active_connections == 0


async def test_info_tenant_report(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme")), admin)
    await execute(Command("SET", ("k", "v")), tenant)
    report = await execute(Command("INFO", ("tenant",)), admin)
    assert "# Tenants" in report
    assert "acme" in report
    assert "keys=1" in report


# --- audit ----------------------------------------------------------------


async def test_admin_commands_are_audited(world: Tenant, caplog) -> None:
    admin = world("admin")
    with caplog.at_level(logging.INFO, logger="creek.audit"):
        await execute(Command("TENANT", ("LIST",)), admin)
    records = [record for record in caplog.records if record.name == "creek.audit"]
    assert records
    assert records[-1].command == "TENANT"
    assert records[-1].actor == "admin"


async def test_non_admin_commands_are_not_audited(world: Tenant, caplog) -> None:
    context = world("acme")
    with caplog.at_level(logging.INFO, logger="creek.audit"):
        await execute(Command("SET", ("k", "v")), context)
    assert [record for record in caplog.records if record.name == "creek.audit"] == []
