"""Read-replica tests (M7a)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

import pytest

from creek.client import connect
from creek.commands import Context, execute
from creek.errors import CommandError
from creek.protocol import Command, SimpleString
from creek.replication import ReplicationLog
from creek.server import Config, Server
from creek.value import Value


async def _wait_until(predicate: Callable[[], bool], timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return False


def _config(**kwargs: object) -> Config:
    return Config(ws_addr="127.0.0.1:0", tcp_addr="127.0.0.1:0", **kwargs)  # type: ignore[arg-type]


async def _pair() -> tuple[Server, Server]:
    primary = Server(_config())
    await primary.start()
    replica = Server(_config(replicaof=("127.0.0.1", primary.tcp_port, None)))
    await replica.start()
    return primary, replica


# --- the log --------------------------------------------------------------


def test_log_sequence_and_since() -> None:
    log = ReplicationLog(capacity=8)
    assert log.sequence == 0
    log.set("ns", "k", Value.parse("str", "v"), None)
    assert log.sequence == 1
    assert log.since(0) == [
        {"op": "set", "owner": "ns", "key": "k", "data": "DQF2", "expires_at": None}
    ]
    assert log.since(1) == []


def test_log_evicts_and_reports_gap() -> None:
    log = ReplicationLog(capacity=2)
    for index in range(5):
        log.set("ns", f"k{index}", Value.parse("str", "v"), None)
    assert log.sequence == 5
    assert log.backlog() == 2
    assert log.since(0) is None  # too far behind -> full resync
    assert log.since(3) is not None  # records 4 and 5 are still available


# --- replication ----------------------------------------------------------


async def test_replica_follows_writes() -> None:
    primary, replica = await _pair()
    try:
        primary.store.set("default", "k", Value.parse("str", "v"))
        assert await _wait_until(
            lambda: replica.store.get("default", "k") == Value.parse("str", "v")
        )
    finally:
        await replica.close()
        await primary.close()


async def test_replica_follows_deletes_and_retained() -> None:
    primary, replica = await _pair()
    try:
        primary.store.set("default", "k", Value.parse("str", "v"))
        primary.bus.retain("default", "news", Value.parse("str", "last"))
        assert await _wait_until(lambda: replica.store.get("default", "k") is not None)
        primary.store.delete("default", "k")
        assert await _wait_until(lambda: replica.store.get("default", "k") is None)
        assert await _wait_until(
            lambda: replica.bus.retained("default", "news") == Value.parse("str", "last")
        )
    finally:
        await replica.close()
        await primary.close()


async def test_replica_follows_tenants() -> None:
    primary, replica = await _pair()
    try:
        primary.tenants.create("acme")
        assert await _wait_until(lambda: replica.tenants.get("acme") is not None)
    finally:
        await replica.close()
        await primary.close()


async def test_replica_is_read_only() -> None:
    primary, replica = await _pair()
    client = await connect(f"tcp://127.0.0.1:{replica.tcp_port}")
    try:
        with pytest.raises(CommandError) as info:
            await client.set("k", "v")
        assert info.value.code == "READONLY"
        assert await client.ping() == "PONG"  # reads still work
    finally:
        await client.close()
        await replica.close()
        await primary.close()


async def test_role_and_info() -> None:
    primary, replica = await _pair()
    try:
        primary_client = await connect(f"tcp://127.0.0.1:{primary.tcp_port}")
        replica_client = await connect(f"tcp://127.0.0.1:{replica.tcp_port}")
        try:
            assert (await primary_client.execute("ROLE"))[0] == "master"
            assert (await replica_client.execute("ROLE"))[0] == "replica"
            assert "role:master" in await primary_client.execute("INFO", "replication")
            replica_info = await replica_client.execute("INFO", "replication")
            assert "role:replica" in replica_info
            assert f"master_port:{primary.tcp_port}" in replica_info
        finally:
            await primary_client.close()
            await replica_client.close()
    finally:
        await replica.close()
        await primary.close()


async def test_promotion_allows_writes() -> None:
    primary, replica = await _pair()
    try:
        context = Context(replication=replica.replication)
        with pytest.raises(CommandError):
            await execute(Command("SET", ("k", "v")), context)
        await execute(Command("REPLICAOF", ("NO", "ONE")), context)
        assert replica.replication.state.read_only is False
        assert await execute(Command("SET", ("k", "v")), context) == SimpleString("OK")
    finally:
        await replica.close()
        await primary.close()


async def test_full_resync_replaces_state() -> None:
    primary, replica = await _pair()
    try:
        # A key written before the replica connects is delivered by the snapshot.
        replica.store.set("default", "stale", Value.parse("str", "old"))
        primary.store.set("default", "fresh", Value.parse("str", "new"))
        # Force a resync by pointing the replica at the primary again.
        await replica.replication.follow("127.0.0.1", primary.tcp_port)
        assert await _wait_until(lambda: replica.store.get("default", "fresh") is not None)
        assert await _wait_until(lambda: replica.store.get("default", "stale") is None)
    finally:
        await replica.close()
        await primary.close()
