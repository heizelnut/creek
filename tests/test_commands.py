"""Command dispatch, arity, and ACL tests."""

from __future__ import annotations

import pytest

from creek.auth import Authenticator, Identity
from creek.commands import Context, execute
from creek.errors import CommandError
from creek.protocol import Command, SimpleString


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
    authenticator = Authenticator({}, allow_anonymous=False)
    context = Context(authenticator=authenticator)
    with pytest.raises(CommandError) as info:
        await execute(Command("INFO"), context)
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
        await execute(Command("INFO"), context)
    assert info.value.code == "NOPERM"
