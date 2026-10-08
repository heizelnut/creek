"""Per-tenant quotas (M6b): keys, memory, connections, subscriptions, rate."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path

import pytest

from creek.auth import Identity
from creek.client import connect
from creek.commands import Context, execute
from creek.errors import CommandError
from creek.protocol import Command, SimpleString
from creek.pubsub import Bus, Subscriber
from creek.server import Config, Server
from creek.store import Store
from creek.tenant import Quota, TenantRegistry

Tenant = Callable[..., Context]


@pytest.fixture
def world(clock) -> Tenant:
    store = Store()
    bus = Bus()
    tenants = TenantRegistry(clock=clock)

    def make(namespace: str, acl: tuple[str, ...] = ("all",)) -> Context:
        return Context(
            identity=Identity(namespace, namespace, frozenset(acl)),
            store=store,
            bus=bus,
            tenants=tenants,
            subscriber=Subscriber(namespace),
        )

    return make


async def test_unlimited_by_default(world: Tenant) -> None:
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme")), world("admin"))
    for index in range(20):
        assert await execute(Command("SET", (f"k{index}", "v")), tenant) == SimpleString("OK")


async def test_max_keys(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme", "MAXKEYS", "2")), admin)
    await execute(Command("SET", ("a", "1")), tenant)
    await execute(Command("SET", ("b", "1")), tenant)
    with pytest.raises(CommandError) as info:
        await execute(Command("SET", ("c", "1")), tenant)
    assert info.value.code == "QUOTA"
    # overwriting an existing key does not consume a new slot
    assert await execute(Command("SET", ("a", "2")), tenant) == SimpleString("OK")


async def test_max_memory(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    # "k" (1 byte) + str "v" blob (type byte + length + 1 byte = 3) = 4
    await execute(Command("TENANT", ("CREATE", "acme", "MAXMEMORY", "4")), admin)
    assert await execute(Command("SET", ("k", "v")), tenant) == SimpleString("OK")
    with pytest.raises(CommandError) as info:
        await execute(Command("SET", ("k2", "v")), tenant)
    assert info.value.code == "QUOTA"


async def test_retained_counts_toward_memory(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme", "MAXMEMORY", "10")), admin)
    assert await execute(Command("PUBLISH", ("chan", "v", "RETAIN")), tenant) == 0
    with pytest.raises(CommandError):
        await execute(Command("PUBLISH", ("chan2", "v", "RETAIN")), tenant)


async def test_max_subscriptions(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme", "MAXSUBSCRIPTIONS", "1")), admin)
    await execute(Command("SUBSCRIBE", ("a",)), tenant)
    with pytest.raises(CommandError) as info:
        await execute(Command("SUBSCRIBE", ("b",)), tenant)
    assert info.value.code == "QUOTA"
    # re-subscribing to the same channel is not a new subscription
    assert await execute(Command("SUBSCRIBE", ("a",)), tenant) == [["subscribe", "a", 1]]


async def test_max_rate(world: Tenant, clock) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme", "MAXRATE", "2")), admin)
    await execute(Command("PING"), tenant)
    await execute(Command("PING"), tenant)
    with pytest.raises(CommandError) as info:
        await execute(Command("PING"), tenant)
    assert info.value.code == "QUOTA"
    clock.advance(1)  # the bucket refills
    assert await execute(Command("PING"), tenant) == SimpleString("PONG")


def test_max_connections(clock) -> None:
    registry = TenantRegistry(clock=clock)
    registry.create("acme", Quota(max_connections=1))
    assert registry.acquire("acme") is True
    assert registry.acquire("acme") is False
    registry.release("acme")
    assert registry.acquire("acme") is True


async def test_tenant_set_updates_a_limit(world: Tenant) -> None:
    admin = world("admin")
    await execute(Command("TENANT", ("CREATE", "acme", "MAXKEYS", "1")), admin)
    await execute(Command("TENANT", ("SET", "acme", "MAXKEYS", "5")), admin)
    info = await execute(Command("TENANT", ("INFO", "acme")), admin)
    assert info[info.index("max_keys") + 1] == 5


async def test_info_reports_usage_and_limits(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme", "MAXKEYS", "9")), admin)
    await execute(Command("SET", ("k", "v")), tenant)
    info = await execute(Command("TENANT", ("INFO", "acme")), admin)
    assert info[info.index("keys") + 1] == 1
    assert info[info.index("memory") + 1] == 4
    assert info[info.index("max_keys") + 1] == 9
    assert info[info.index("max_rate") + 1] == -1  # unlimited


def _config(tmp_path: Path) -> Config:
    return Config(
        ws_addr="127.0.0.1:0",
        tcp_addr="127.0.0.1:0",
        data_dir=tmp_path,
        snapshot_interval=0,
    )


async def test_quota_survives_restart(tmp_path: Path) -> None:
    first = Server(_config(tmp_path))
    await first.start()
    first.tenants.create("acme", Quota(max_keys=3, max_memory=1024))
    await first.close()

    second = Server(_config(tmp_path))
    await second.start()
    try:
        tenant = second.tenants.get("acme")
        assert tenant is not None
        assert tenant.quota.max_keys == 3
        assert tenant.quota.max_memory == 1024
    finally:
        await second.close()


async def test_connection_quota_end_to_end(tmp_path: Path) -> None:
    auth = tmp_path / "auth.json"
    auth.write_text(
        json.dumps(
            {
                "allow_anonymous": False,
                "clients": [{"id": "a", "namespace": "acme", "token": "ta", "acl": ["all"]}],
            }
        ),
        encoding="utf-8",
    )
    server = Server(Config(ws_addr="127.0.0.1:0", tcp_addr="127.0.0.1:0", auth_file=auth))
    await server.start()
    server.tenants.create("acme", Quota(max_connections=1))
    url = f"ws://127.0.0.1:{server.ws_port}/ws"
    first = await connect(url)
    second = await connect(url)
    try:
        assert await first.execute("AUTH", "ta") == "OK"
        with pytest.raises(CommandError) as info:
            await second.execute("AUTH", "ta")
        assert info.value.code == "QUOTA"
        await first.close()
        await asyncio.sleep(0.05)  # let the server release the slot
        assert await second.execute("AUTH", "ta") == "OK"
    finally:
        await second.close()
        await server.close()
