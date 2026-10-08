"""Isolation guarantees for namespaces-as-tenants (the safety net for M6).

These tests pin down what "a tenant" means **today**: a namespace string that
scopes keys, channels, enumeration, flushing, and persisted records. They do
*not* claim hard isolation — one process, one store, one WAL, no quotas; that is
what M6 will add (``AGENTS.md`` §8, §17).

If any of these break, either the change is a deliberate semantic shift (update
``AGENTS.md``) or it is a regression.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import pytest

from creek.auth import Authenticator, Identity
from creek.client import connect
from creek.commands import Context, execute
from creek.errors import CommandError
from creek.protocol import Command, SimpleString, encode_message
from creek.pubsub import Bus, Subscriber
from creek.server import Config, Server
from creek.store import Store

Tenant = Callable[..., Context]


@pytest.fixture
def world() -> Tenant:
    """A factory that builds contexts sharing one store and one bus."""
    store = Store()
    bus = Bus()

    def make(namespace: str, acl: tuple[str, ...] = ("all",)) -> Context:
        return Context(
            identity=Identity(namespace, namespace, frozenset(acl)),
            store=store,
            bus=bus,
            subscriber=Subscriber(namespace),
        )

    return make


async def _expect_no_frame(subscriber: Subscriber, timeout: float = 0.02) -> None:
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(subscriber.next_frame(), timeout)


# --- keys -----------------------------------------------------------------


async def test_keys_are_isolated(world: Tenant) -> None:
    a, b = world("ns-a"), world("ns-b")
    await execute(Command("SET", ("k", "a-value")), a)
    await execute(Command("SET", ("k", "b-value")), b)
    assert await execute(Command("GET", ("k",)), a) == ["str", "a-value"]
    assert await execute(Command("GET", ("k",)), b) == ["str", "b-value"]


async def test_missing_key_is_per_namespace(world: Tenant) -> None:
    a, b = world("ns-a"), world("ns-b")
    await execute(Command("SET", ("k", "v")), a)
    assert await execute(Command("GET", ("k",)), b) is None
    assert await execute(Command("EXISTS", ("k",)), b) == 0
    assert await execute(Command("TYPE", ("k",)), b) == SimpleString("none")


async def test_enumeration_is_scoped(world: Tenant) -> None:
    a, b = world("ns-a"), world("ns-b")
    for key in ("x", "y"):
        await execute(Command("SET", (key, "1")), a)
    await execute(Command("SET", ("z", "1")), b)
    assert await execute(Command("KEYS", ("*",)), a) == ["x", "y"]
    assert await execute(Command("KEYS", ("*",)), b) == ["z"]
    assert await execute(Command("DBSIZE", ()), a) == 2
    assert await execute(Command("DBSIZE", ()), b) == 1
    _, batch = await execute(Command("SCAN", ("0",)), a)
    assert batch == ["x", "y"]


async def test_delete_and_flush_are_scoped(world: Tenant) -> None:
    a, b = world("ns-a"), world("ns-b")
    await execute(Command("SET", ("k", "v")), a)
    await execute(Command("SET", ("k", "v")), b)
    assert await execute(Command("DEL", ("k",)), a) == 1
    assert await execute(Command("GET", ("k",)), b) == ["str", "v"]

    await execute(Command("SET", ("k2", "v")), a)
    assert await execute(Command("FLUSHALL", ()), a) == SimpleString("OK")
    assert await execute(Command("DBSIZE", ()), a) == 0
    assert await execute(Command("GET", ("k",)), b) == ["str", "v"]


async def test_counters_are_independent(world: Tenant) -> None:
    a, b = world("ns-a"), world("ns-b")
    assert await execute(Command("INCR", ("n",)), a) == 1
    assert await execute(Command("INCR", ("n",)), b) == 1
    assert await execute(Command("INCR", ("n",)), a) == 2
    assert await execute(Command("GET", ("n",)), b) == ["i32", "1"]


async def test_namespace_is_not_part_of_the_key(world: Tenant) -> None:
    # A key literally named "ns-b:k" is still owned by the caller's namespace:
    # ownership is orthogonal to the key string.
    a, b = world("ns-a"), world("ns-b")
    await execute(Command("SET", ("ns-b:k", "v")), a)
    assert await execute(Command("GET", ("ns-b:k",)), a) == ["str", "v"]
    assert await execute(Command("GET", ("ns-b:k",)), b) is None


# --- channels -------------------------------------------------------------


async def test_channels_are_isolated(world: Tenant) -> None:
    a, b = world("ns-a"), world("ns-b")
    await execute(Command("SUBSCRIBE", ("news",)), a)
    await execute(Command("SUBSCRIBE", ("news",)), b)
    assert await execute(Command("PUBLISH", ("news", "hello")), b) == 1
    assert await b.subscriber.next_frame() == encode_message("news", "str", "hello")
    await _expect_no_frame(a.subscriber)


async def test_retained_is_isolated(world: Tenant) -> None:
    a, b = world("ns-a"), world("ns-b")
    await execute(Command("PUBLISH", ("news", "last", "RETAIN")), b)
    await execute(Command("SUBSCRIBE", ("news",)), a)
    assert a.pending == []  # no cross-namespace replay
    await execute(Command("SUBSCRIBE", ("news",)), b)
    assert b.pending == [encode_message("news", "str", "last")]


async def test_pubsub_introspection_is_scoped(world: Tenant) -> None:
    a, b = world("ns-a"), world("ns-b")
    await execute(Command("SUBSCRIBE", ("a-chan",)), a)
    await execute(Command("SUBSCRIBE", ("b-chan",)), b)
    assert await execute(Command("PUBSUB", ("CHANNELS",)), a) == ["a-chan"]
    assert await execute(Command("PUBSUB", ("CHANNELS",)), b) == ["b-chan"]
    assert await execute(Command("PUBSUB", ("NUMSUB", "a-chan", "b-chan")), a) == [
        "a-chan",
        1,
        "b-chan",
        0,
    ]


# --- identity / ACL -------------------------------------------------------


async def test_acl_is_per_identity_not_per_namespace(world: Tenant) -> None:
    writer = world("ns-a")
    reader = world("ns-a", acl=("read",))
    await execute(Command("SET", ("k", "v")), writer)
    assert await execute(Command("GET", ("k",)), reader) == ["str", "v"]
    with pytest.raises(CommandError) as info:
        await execute(Command("SET", ("k", "other")), reader)
    assert info.value.code == "NOPERM"


async def test_reauthentication_switches_namespace() -> None:
    auth = Authenticator.from_tokens(
        {
            "ta": Identity("a", "ns-a", frozenset({"all"})),
            "tb": Identity("b", "ns-b", frozenset({"all"})),
        },
        allow_anonymous=False,
    )
    context = Context(
        authenticator=auth,
        store=Store(),
        bus=Bus(),
        subscriber=Subscriber("default"),
    )
    await execute(Command("AUTH", ("ta",)), context)
    await execute(Command("SET", ("k", "a")), context)
    await execute(Command("AUTH", ("tb",)), context)
    assert await execute(Command("GET", ("k",)), context) is None
    await execute(Command("SET", ("k", "b")), context)
    await execute(Command("AUTH", ("ta",)), context)
    assert await execute(Command("GET", ("k",)), context) == ["str", "a"]


async def test_reauthentication_drops_old_subscriptions() -> None:
    auth = Authenticator.from_tokens(
        {
            "ta": Identity("a", "ns-a", frozenset({"all"})),
            "tb": Identity("b", "ns-b", frozenset({"all"})),
        },
        allow_anonymous=False,
    )
    bus = Bus()
    context = Context(
        authenticator=auth,
        store=Store(),
        bus=bus,
        subscriber=Subscriber("default"),
    )
    await execute(Command("AUTH", ("ta",)), context)
    await execute(Command("SUBSCRIBE", ("news",)), context)
    assert bus.numsub("ns-a", "news") == 1
    await execute(Command("AUTH", ("tb",)), context)
    assert bus.numsub("ns-a", "news") == 0


# --- end to end (token -> namespace, through the real server) -------------

AUTH_FILE = {
    "allow_anonymous": False,
    "clients": [
        {"id": "a1", "namespace": "ns-a", "token": "ta", "acl": ["all"]},
        {"id": "b1", "namespace": "ns-b", "token": "tb", "acl": ["all"]},
        {"id": "a2", "namespace": "ns-a", "token": "ta2", "acl": ["read"]},
    ],
}


def _write_auth(tmp_path: Path) -> Path:
    path = tmp_path / "auth.json"
    path.write_text(json.dumps(AUTH_FILE), encoding="utf-8")
    return path


@pytest.fixture
async def server(tmp_path: Path) -> AsyncIterator[Server]:
    instance = Server(
        Config(ws_addr="127.0.0.1:0", tcp_addr="127.0.0.1:0", auth_file=_write_auth(tmp_path))
    )
    await instance.start()
    try:
        yield instance
    finally:
        await instance.close()


async def test_namespace_comes_from_the_token(server: Server) -> None:
    url = f"ws://127.0.0.1:{server.ws_port}/ws"
    a = await connect(url)
    b = await connect(url)
    try:
        await a.execute("AUTH", "ta")
        await b.execute("AUTH", "tb")
        await a.set("k", "a-value")
        await b.set("k", "b-value")
        assert await a.get("k") == ["str", "a-value"]
        assert await b.get("k") == ["str", "b-value"]
    finally:
        await a.close()
        await b.close()


async def test_pubsub_after_auth(server: Server) -> None:
    # Regression: the subscriber's namespace must follow the authenticated identity.
    url = f"ws://127.0.0.1:{server.ws_port}/ws"
    subscriber = await connect(url)
    publisher = await connect(url)
    try:
        await subscriber.execute("AUTH", "ta")
        await publisher.execute("AUTH", "ta")
        received: list[tuple[str, str, str]] = []

        async def collect() -> None:
            async for message in subscriber.subscribe("news"):
                received.append(message)
                return

        task = asyncio.create_task(collect())
        await asyncio.sleep(0.05)  # let the subscription register
        assert await publisher.publish("news", "hello") == 1
        await asyncio.wait_for(task, 1.0)
        assert received == [("news", "str", "hello")]
    finally:
        await subscriber.close()
        await publisher.close()


async def test_pubsub_isolation_end_to_end(server: Server) -> None:
    url = f"ws://127.0.0.1:{server.ws_port}/ws"
    subscriber = await connect(url)
    other = await connect(url)
    same = await connect(url)
    try:
        await subscriber.execute("AUTH", "ta")
        await other.execute("AUTH", "tb")
        await same.execute("AUTH", "ta")
        await subscriber.execute("SUBSCRIBE", "news")
        assert await other.publish("news", "x") == 0  # different namespace
        assert await same.publish("news", "x") == 1  # same namespace
    finally:
        await subscriber.close()
        await other.close()
        await same.close()


async def test_persistence_preserves_ownership(tmp_path: Path) -> None:
    auth = _write_auth(tmp_path)
    data = tmp_path / "data"

    def make() -> Server:
        return Server(
            Config(
                ws_addr="127.0.0.1:0",
                tcp_addr="127.0.0.1:0",
                auth_file=auth,
                data_dir=data,
                snapshot_interval=0,
            )
        )

    first = make()
    await first.start()
    url = f"ws://127.0.0.1:{first.ws_port}/ws"
    a = await connect(url)
    b = await connect(url)
    await a.execute("AUTH", "ta")
    await b.execute("AUTH", "tb")
    await a.set("k", "a-value")
    await b.set("k", "b-value")
    await a.execute("FLUSHALL")  # clears only ns-a
    await a.close()
    await b.close()
    await first.close()

    second = make()
    await second.start()
    url2 = f"ws://127.0.0.1:{second.ws_port}/ws"
    a2 = await connect(url2)
    b2 = await connect(url2)
    try:
        await a2.execute("AUTH", "ta")
        await b2.execute("AUTH", "tb")
        assert await a2.get("k") is None  # flushed before restart
        assert await b2.get("k") == ["str", "b-value"]  # untouched
    finally:
        await a2.close()
        await b2.close()
        await second.close()
