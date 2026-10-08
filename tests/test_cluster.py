"""Sharding tests (M7b): slots, topology, MOVED/CROSSSLOT, and client redirects."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from creek.auth import Identity
from creek.client import connect
from creek.cluster import SLOT_COUNT, Cluster, ClusterNode, key_slot
from creek.commands import Context, execute
from creek.errors import CommandError, PersistError
from creek.protocol import Command, SimpleString
from creek.server import Config, Server
from creek.value import Value


def _key_in_slot_range(low: int, high: int) -> str:
    for index in range(1_000_000):
        key = f"key:{index}"
        if low <= key_slot(key) <= high:
            return key
    raise AssertionError("no key found")


# --- slots ----------------------------------------------------------------


@pytest.mark.parametrize(("key", "slot"), [("foo", 12182), ("bar", 5061), ("hello", 866)])
def test_key_slot_matches_redis(key: str, slot: int) -> None:
    assert key_slot(key) == slot


def test_hash_tags_pin_a_slot() -> None:
    assert key_slot("{user1000}.following") == key_slot("{user1000}.followers")
    assert key_slot("foo{bar}baz") == key_slot("bar")


def test_cluster_from_file(tmp_path: Path) -> None:
    path = tmp_path / "cluster.json"
    path.write_text(
        json.dumps(
            {
                "self": "127.0.0.1:7001",
                "nodes": [
                    {"addr": "127.0.0.1:7001", "slots": [[0, 8191]]},
                    {"addr": "127.0.0.1:7002", "slots": [[8192, 16383]]},
                ],
            }
        ),
        encoding="utf-8",
    )
    cluster = Cluster.from_file(path)
    assert cluster.self_addr == "127.0.0.1:7001"
    assert cluster.owns(0) and cluster.owns(8191)
    assert not cluster.owns(8192)
    assert cluster.owner(9000) is not None
    assert cluster.owner(9000).addr == "127.0.0.1:7002"
    assert cluster.assigned() == SLOT_COUNT


def test_cluster_rejects_bad_config(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(PersistError):
        Cluster.from_file(path)


# --- commands -------------------------------------------------------------


def _cluster() -> Cluster:
    return Cluster(
        "127.0.0.1:7001",
        [
            ClusterNode("127.0.0.1:7001", ((0, 8191),)),
            ClusterNode("127.0.0.1:7002", ((8192, 16383),)),
        ],
    )


@pytest.fixture
def clustered() -> Context:
    return Context(
        identity=Identity("a", "ns", frozenset({"all"})),
        cluster=_cluster(),
    )


async def test_keyslot_command(clustered: Context) -> None:
    assert await execute(Command("KEYSLOT", ("foo",)), clustered) == 12182


async def test_cluster_info(clustered: Context) -> None:
    report = await execute(Command("CLUSTER", ("INFO",)), clustered)
    assert "cluster_enabled:1" in report
    assert "cluster_known_nodes:2" in report


async def test_cluster_slots(clustered: Context) -> None:
    slots = await execute(Command("CLUSTER", ("SLOTS",)), clustered)
    assert slots[0] == [0, 8191, ["127.0.0.1", 7001, "127.0.0.1:7001"]]
    assert slots[1] == [8192, 16383, ["127.0.0.1", 7002, "127.0.0.1:7002"]]


async def test_local_key_is_served(clustered: Context) -> None:
    key = _key_in_slot_range(0, 8191)
    assert await execute(Command("SET", (key, "v")), clustered) == SimpleString("OK")


async def test_remote_key_redirects(clustered: Context) -> None:
    key = _key_in_slot_range(8192, 16383)
    with pytest.raises(CommandError) as info:
        await execute(Command("SET", (key, "v")), clustered)
    assert info.value.code == "MOVED"
    assert "127.0.0.1:7002" in str(info.value)


async def test_cross_slot_is_rejected(clustered: Context) -> None:
    first = _key_in_slot_range(0, 8191)
    second = _key_in_slot_range(8192, 16383)
    with pytest.raises(CommandError) as info:
        await execute(Command("MGET", (first, second)), clustered)
    assert info.value.code == "CROSSSLOT"


async def test_unassigned_slot_is_clusterdown() -> None:
    context = Context(cluster=Cluster("127.0.0.1:7001", [ClusterNode("127.0.0.1:7001", ((0, 0),))]))
    key = _key_in_slot_range(1, SLOT_COUNT - 1)
    with pytest.raises(CommandError) as info:
        await execute(Command("GET", (key,)), context)
    assert info.value.code == "CLUSTERDOWN"


# --- end to end -----------------------------------------------------------


async def test_client_follows_moved() -> None:
    first = Server(Config(ws_addr="127.0.0.1:0", tcp_addr="127.0.0.1:0"))
    second = Server(Config(ws_addr="127.0.0.1:0", tcp_addr="127.0.0.1:0"))
    await first.start()
    await second.start()
    try:
        first_addr = f"127.0.0.1:{first.tcp_port}"
        second_addr = f"127.0.0.1:{second.tcp_port}"
        nodes = [
            ClusterNode(first_addr, ((0, 8191),)),
            ClusterNode(second_addr, ((8192, 16383),)),
        ]
        first.cluster = Cluster(first_addr, nodes)
        second.cluster = Cluster(second_addr, nodes)

        key = _key_in_slot_range(8192, 16383)
        client = await connect(f"tcp://{first_addr}")
        try:
            assert await client.set(key, "v") == "OK"  # redirected transparently
            assert second.store.get("default", key) == Value.parse("str", "v")
            assert first.store.get("default", key) is None
            assert await client.get(key) == ["str", "v"]
        finally:
            await client.close()
    finally:
        await second.close()
        await first.close()
