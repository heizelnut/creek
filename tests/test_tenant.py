"""Tenant lifecycle, admin API, and persistence (M6a)."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from creek.auth import Identity
from creek.client import connect
from creek.commands import Context, execute
from creek.errors import CommandError
from creek.persist import encode_record
from creek.protocol import Command, SimpleString
from creek.pubsub import Bus, Subscriber
from creek.server import Config, Server
from creek.store import Store
from creek.tenant import TenantRegistry

Tenant = Callable[..., Context]


class FakeJournal:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def tenant_create(self, name, created_at):  # type: ignore[no-untyped-def]
        self.calls.append(("create", name, created_at))

    def tenant_drop(self, name):  # type: ignore[no-untyped-def]
        self.calls.append(("drop", name))


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


# --- registry -------------------------------------------------------------


def test_ensure_creates_once(clock) -> None:
    registry = TenantRegistry(clock=clock)
    first = registry.ensure("acme")
    clock.advance(5)
    second = registry.ensure("acme")
    assert first is second
    assert first.created_at == 1000.0
    assert registry.names() == ["acme"]


def test_create_is_idempotent(clock) -> None:
    registry = TenantRegistry(clock=clock)
    registry.create("acme")
    clock.advance(5)
    registry.create("acme")
    assert registry.entries() == [("acme", 1000.0)]


def test_drop(clock) -> None:
    registry = TenantRegistry(clock=clock)
    registry.create("acme")
    assert registry.drop("acme") is True
    assert registry.drop("acme") is False
    assert registry.names() == []


def test_registry_journals(clock) -> None:
    journal = FakeJournal()
    registry = TenantRegistry(clock=clock, journal=journal)
    registry.create("acme")
    registry.drop("acme")
    assert journal.calls == [("create", "acme", 1000.0), ("drop", "acme")]


# --- admin commands -------------------------------------------------------


async def test_create_list_info(world: Tenant) -> None:
    admin = world("admin")
    assert await execute(Command("TENANT", ("CREATE", "acme")), admin) == SimpleString("OK")
    assert await execute(Command("TENANT", ("LIST",)), admin) == ["acme"]
    info = await execute(Command("TENANT", ("INFO", "acme")), admin)
    assert info[0] == "name"
    assert info[1] == "acme"
    assert "created_at" in info


async def test_info_for_missing_tenant(world: Tenant) -> None:
    assert await execute(Command("TENANT", ("INFO", "nope")), world("admin")) is None


async def test_tenant_requires_admin(world: Tenant) -> None:
    reader = world("ns", acl=("read",))
    with pytest.raises(CommandError) as info:
        await execute(Command("TENANT", ("LIST",)), reader)
    assert info.value.code == "NOPERM"


async def test_drop_clears_keys_channels_and_retained(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme")), admin)
    await execute(Command("SET", ("k", "v")), tenant)
    await execute(Command("SUBSCRIBE", ("news",)), tenant)
    await execute(Command("PUBLISH", ("news", "x", "RETAIN")), tenant)

    assert await execute(Command("TENANT", ("DROP", "acme")), admin) == 1
    assert await execute(Command("GET", ("k",)), tenant) is None
    assert await execute(Command("DBSIZE", ()), tenant) == 0
    assert tenant.bus.retained("acme", "news") is None
    assert tenant.bus.numsub("acme", "news") == 0
    assert await execute(Command("TENANT", ("DROP", "acme")), admin) == 0


async def test_info_reports_counts(world: Tenant) -> None:
    admin = world("admin")
    tenant = world("acme")
    await execute(Command("TENANT", ("CREATE", "acme")), admin)
    await execute(Command("SET", ("a", "1")), tenant)
    await execute(Command("SET", ("b", "1")), tenant)
    await execute(Command("SUBSCRIBE", ("news",)), tenant)
    info = await execute(Command("TENANT", ("INFO", "acme")), admin)
    assert info[info.index("keys") + 1] == 2
    assert info[info.index("channels") + 1] == 1


# --- persistence ----------------------------------------------------------


def _config(data_dir: Path) -> Config:
    return Config(
        ws_addr="127.0.0.1:0",
        tcp_addr="127.0.0.1:0",
        data_dir=data_dir,
        snapshot_interval=0,
    )


async def test_tenants_survive_restart(tmp_path: Path) -> None:
    first = Server(_config(tmp_path))
    await first.start()
    first.tenants.create("acme")
    first.tenants.create("globex")
    first.tenants.drop("globex")
    await first.close()

    second = Server(_config(tmp_path))
    await second.start()
    try:
        assert second.tenants.names() == ["acme"]
    finally:
        await second.close()


async def test_tenant_recovered_from_wal(tmp_path: Path) -> None:
    (tmp_path / "creek.wal").write_bytes(
        encode_record({"op": "tenant_create", "name": "acme", "created_at": 123.0})
    )
    server = Server(_config(tmp_path))
    await server.start()
    try:
        tenant = server.tenants.get("acme")
        assert tenant is not None
        assert tenant.created_at == 123.0
    finally:
        await server.close()


async def test_auth_provisions_the_tenant(tmp_path: Path) -> None:
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
    client = await connect(f"ws://127.0.0.1:{server.ws_port}/ws")
    try:
        assert server.tenants.get("acme") is None
        await client.execute("AUTH", "ta")
        assert server.tenants.get("acme") is not None
    finally:
        await client.close()
        await server.close()
