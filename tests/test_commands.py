"""Command dispatch, ACL, and key-value command tests."""

from __future__ import annotations

import pytest

from creek.auth import Authenticator, Identity
from creek.commands import Context, execute
from creek.errors import CommandError
from creek.protocol import Command, SimpleString
from creek.store import Store

# --- dispatch / auth -----------------------------------------------------


async def test_ping_defaults_to_pong() -> None:
    assert await execute(Command("PING"), Context()) == SimpleString("PONG")


async def test_ping_echoes_argument() -> None:
    assert await execute(Command("PING", ("hi",)), Context()) == "hi"


async def test_hello() -> None:
    assert await execute(Command("HELLO"), Context()) == SimpleString("HI")


async def test_unknown_command() -> None:
    with pytest.raises(CommandError) as info:
        await execute(Command("NOPE"), Context())
    assert str(info.value) == "ERR unknown command 'nope'"


async def test_wrong_arity() -> None:
    with pytest.raises(CommandError) as info:
        await execute(Command("ECHO"), Context())
    assert str(info.value) == "ERR wrong number of arguments for 'echo'"


async def test_quit_requests_close() -> None:
    context = Context()
    assert await execute(Command("QUIT"), context) == SimpleString("OK")
    assert context.close_after_reply


async def test_auth_required_when_anonymous_disabled() -> None:
    context = Context(authenticator=Authenticator({}, allow_anonymous=False))
    with pytest.raises(CommandError) as info:
        await execute(Command("INFO"), context)
    assert info.value.code == "NOAUTH"


async def test_set_requires_auth_when_anonymous_disabled() -> None:
    context = Context(authenticator=Authenticator({}, allow_anonymous=False))
    with pytest.raises(CommandError) as info:
        await execute(Command("SET", ("k", "v")), context)
    assert info.value.code == "NOAUTH"


async def test_auth_selects_identity() -> None:
    identity = Identity("sensor-a", "sensors", frozenset({"all"}))
    authenticator = Authenticator({"t1": identity}, allow_anonymous=False)
    context = Context(authenticator=authenticator)
    assert await execute(Command("AUTH", ("t1",)), context) == SimpleString("OK")
    assert context.identity is identity


async def test_auth_rejects_bad_token() -> None:
    context = Context(authenticator=Authenticator({}, allow_anonymous=False))
    with pytest.raises(CommandError) as info:
        await execute(Command("AUTH", ("bad",)), context)
    assert info.value.code == "WRONGPASS"


async def test_acl_denies_scope() -> None:
    identity = Identity("reader", "ns", frozenset({"read"}))
    context = Context(identity=identity)
    with pytest.raises(CommandError) as info:
        await execute(Command("SET", ("k", "v")), context)
    assert info.value.code == "NOPERM"


# --- key / value ---------------------------------------------------------


async def test_set_get_default_str(context) -> None:
    assert await execute(Command("SET", ("k", "hello")), context) == SimpleString("OK")
    assert await execute(Command("GET", ("k",)), context) == ["str", "hello"]


async def test_get_missing(context) -> None:
    assert await execute(Command("GET", ("nope",)), context) is None


async def test_set_typed(context) -> None:
    await execute(Command("SET", ("k", "21.5", "TYPE", "f32")), context)
    assert await execute(Command("GET", ("k",)), context) == ["f32", "21.5"]


async def test_set_rejects_bad_type(context) -> None:
    with pytest.raises(CommandError):
        await execute(Command("SET", ("k", "1", "TYPE", "i64")), context)


async def test_set_rejects_out_of_range(context) -> None:
    with pytest.raises(CommandError):
        await execute(Command("SET", ("k", "999", "TYPE", "u8")), context)


async def test_set_nx_xx(context) -> None:
    assert await execute(Command("SET", ("k", "a", "NX")), context) == SimpleString("OK")
    assert await execute(Command("SET", ("k", "b", "NX")), context) is None
    assert await execute(Command("GET", ("k",)), context) == ["str", "a"]
    assert await execute(Command("SET", ("k", "c", "XX")), context) == SimpleString("OK")
    assert await execute(Command("SET", ("missing", "c", "XX")), context) is None


async def test_set_ex_sets_ttl(context, clock) -> None:
    await execute(Command("SET", ("k", "v", "EX", "10")), context)
    assert await execute(Command("TTL", ("k",)), context) == 10
    clock.advance(11)
    assert await execute(Command("GET", ("k",)), context) is None


async def test_set_px_sets_ttl(context) -> None:
    await execute(Command("SET", ("k", "v", "PX", "1500")), context)
    assert await execute(Command("TTL", ("k",)), context) == 1


async def test_mget(context) -> None:
    await execute(Command("SET", ("a", "1", "TYPE", "i32")), context)
    await execute(Command("SET", ("b", "hi")), context)
    assert await execute(Command("MGET", ("a", "b", "missing")), context) == [
        ["i32", "1"],
        ["str", "hi"],
        None,
    ]


async def test_del_exists_dbsize(context) -> None:
    await execute(Command("SET", ("a", "1")), context)
    await execute(Command("SET", ("b", "1")), context)
    assert await execute(Command("EXISTS", ("a", "b", "c")), context) == 2
    assert await execute(Command("DBSIZE", ()), context) == 2
    assert await execute(Command("DEL", ("a", "b", "c")), context) == 2
    assert await execute(Command("DBSIZE", ()), context) == 0


async def test_type(context) -> None:
    assert await execute(Command("TYPE", ("missing",)), context) == SimpleString("none")
    await execute(Command("SET", ("k", "1", "TYPE", "u4")), context)
    assert await execute(Command("TYPE", ("k",)), context) == SimpleString("u4")


async def test_expire_ttl(context, clock) -> None:
    await execute(Command("SET", ("k", "v")), context)
    assert await execute(Command("TTL", ("k",)), context) == -1
    assert await execute(Command("EXPIRE", ("k", "5")), context) == 1
    assert await execute(Command("TTL", ("k",)), context) == 5
    clock.advance(6)
    assert await execute(Command("TTL", ("k",)), context) == -2
    assert await execute(Command("EXPIRE", ("k", "5")), context) == 0


async def test_keys_pattern(context) -> None:
    await execute(Command("SET", ("sensor.r1.temp", "1")), context)
    await execute(Command("SET", ("sensor.r2.temp", "1")), context)
    await execute(Command("SET", ("other", "1")), context)
    assert await execute(Command("KEYS", ("sensor.*",)), context) == [
        "sensor.r1.temp",
        "sensor.r2.temp",
    ]


async def test_scan_pages(context) -> None:
    for index in range(25):
        await execute(Command("SET", (f"key:{index:02d}", "1")), context)
    cursor, batch = await execute(Command("SCAN", ("0", "COUNT", "10")), context)
    assert (cursor, len(batch)) == ("10", 10)
    cursor, batch = await execute(Command("SCAN", (cursor, "COUNT", "10")), context)
    assert (cursor, len(batch)) == ("20", 10)
    cursor, batch = await execute(Command("SCAN", (cursor, "COUNT", "10")), context)
    assert (cursor, len(batch)) == ("0", 5)


async def test_scan_match(context) -> None:
    await execute(Command("SET", ("a:1", "1")), context)
    await execute(Command("SET", ("b:1", "1")), context)
    assert await execute(Command("SCAN", ("0", "MATCH", "a:*")), context) == ["0", ["a:1"]]


async def test_incr_decr(context) -> None:
    assert await execute(Command("INCR", ("n",)), context) == 1
    assert await execute(Command("INCR", ("n", "BY", "5")), context) == 6
    assert await execute(Command("DECR", ("n",)), context) == 5
    assert await execute(Command("TYPE", ("n",)), context) == SimpleString("i32")


async def test_incr_preserves_type(context) -> None:
    await execute(Command("SET", ("n", "1", "TYPE", "u8")), context)
    assert await execute(Command("INCR", ("n",)), context) == 2
    assert await execute(Command("TYPE", ("n",)), context) == SimpleString("u8")


async def test_incr_overflow(context) -> None:
    await execute(Command("SET", ("n", "127", "TYPE", "i8")), context)
    with pytest.raises(CommandError):
        await execute(Command("INCR", ("n",)), context)


async def test_incr_rejects_non_integer(context) -> None:
    await execute(Command("SET", ("s", "hi")), context)
    with pytest.raises(CommandError):
        await execute(Command("INCR", ("s",)), context)


async def test_namespaces_are_isolated() -> None:
    store = Store()
    alice = Context(identity=Identity("a", "ns-a", frozenset({"all"})), store=store)
    bob = Context(identity=Identity("b", "ns-b", frozenset({"all"})), store=store)
    await execute(Command("SET", ("k", "alice-value")), alice)
    assert await execute(Command("GET", ("k",)), bob) is None
    assert await execute(Command("GET", ("k",)), alice) == ["str", "alice-value"]
