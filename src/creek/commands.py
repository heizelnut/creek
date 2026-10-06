"""Command registry, dispatch, and the built-in commands.

Add a command by writing an ``async def`` handler and calling :func:`register`.
Nothing else needs to change; both transports pick it up automatically.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from creek import __version__
from creek.auth import Authenticator, Identity
from creek.errors import CommandError, InvalidValue
from creek.pattern import match
from creek.protocol import Command, Reply, SimpleString, encode_message
from creek.pubsub import Bus, Subscriber
from creek.store import Store
from creek.value import Value, ValueType


@dataclass(slots=True)
class Context:
    """State for one client connection.

    ``store`` and ``authenticator`` are shared services injected by the server.
    The store defaults to a fresh instance so tests can use ``Context()`` alone.
    """

    peer: str = ""
    identity: Identity | None = None
    authenticator: Authenticator | None = None
    store: Store = field(default_factory=Store)
    bus: Bus = field(default_factory=Bus)
    subscriber: Subscriber = field(default_factory=Subscriber)
    pending: list[bytes] = field(default_factory=list)
    close_after_reply: bool = False


Handler = Callable[[Command, Context], Awaitable[Reply]]


@dataclass(frozen=True, slots=True)
class Spec:
    """A registered command."""

    handler: Handler
    min_args: int
    max_args: int | None
    scope: str


REGISTRY: dict[str, Spec] = {}


def register(
    name: str,
    handler: Handler,
    *,
    min_args: int = 0,
    max_args: int | None = 0,
    scope: str = "read",
) -> None:
    """Add a command. ``scope`` is public, read, write, subscribe, or admin."""
    REGISTRY[name.upper()] = Spec(handler, min_args, max_args, scope)


async def execute(command: Command, context: Context) -> Reply:
    """Find and run the handler for ``command``."""
    spec = REGISTRY.get(command.name)
    if spec is None:
        raise CommandError(f"unknown command '{command.name.lower()}'")
    count = len(command.args)
    if count < spec.min_args or (spec.max_args is not None and count > spec.max_args):
        raise CommandError(f"wrong number of arguments for '{command.name.lower()}'")
    _check_access(spec.scope, context)
    return await spec.handler(command, context)


def _check_access(scope: str, context: Context) -> None:
    if scope == "public":
        return
    identity = context.identity
    if identity is None:
        authenticator = context.authenticator
        if authenticator is not None and not authenticator.allow_anonymous:
            raise CommandError("authentication required", code="NOAUTH")
    elif not identity.allows(scope):
        raise CommandError("no permission for this command", code="NOPERM")


# --- connection / auth ---------------------------------------------------


async def cmd_ping(command: Command, context: Context) -> Reply:
    return command.args[0] if command.args else SimpleString("PONG")


async def cmd_echo(command: Command, context: Context) -> Reply:
    return command.args[0]


async def cmd_quit(command: Command, context: Context) -> Reply:
    context.close_after_reply = True
    return SimpleString("OK")


async def cmd_auth(command: Command, context: Context) -> Reply:
    if context.authenticator is None:
        raise CommandError("AUTH is not enabled")
    identity = context.authenticator.authenticate(command.args[0])
    if identity is None:
        raise CommandError("invalid password", code="WRONGPASS")
    context.identity = identity
    return SimpleString("OK")


async def cmd_command(command: Command, context: Context) -> Reply:
    if command.args:
        if command.args[0].upper() != "COUNT":
            raise CommandError(f"unknown subcommand '{command.args[0]}'")
        return len(REGISTRY)
    names: list[Reply] = [name for name in sorted(REGISTRY)]
    return names


async def cmd_info(command: Command, context: Context) -> Reply:
    return "\r\n".join(
        [
            "# Server",
            f"creek_version:{__version__}",
            f"process_id:{os.getpid()}",
            "transport:websocket,tcp",
        ]
    )


async def cmd_hello(command: Command, context: Context) -> Reply:
    return SimpleString("HI")


# --- key / value ---------------------------------------------------------


def _owner(context: Context) -> str:
    """The namespace to scope keys by (never client-supplied)."""
    identity = context.identity
    return identity.namespace if identity is not None else "default"


def _pair(value: Value) -> list[Reply]:
    pair: list[Reply] = [value.type.tag, value.text()]
    return pair


def _parse_value(tag: str, raw: str) -> Value:
    try:
        return Value.parse(tag, raw)
    except InvalidValue as exc:
        raise CommandError(str(exc)) from exc


async def cmd_set(command: Command, context: Context) -> Reply:
    key = command.args[0]
    tag = "str"
    ttl: float | None = None
    nx = False
    xx = False
    options = command.args[2:]
    index = 0
    while index < len(options):
        option = options[index].upper()
        if option == "TYPE" and index + 1 < len(options):
            tag = options[index + 1]
            index += 2
        elif option in ("EX", "PX") and index + 1 < len(options):
            try:
                amount = int(options[index + 1], 10)
            except ValueError as exc:
                raise CommandError("value is not an integer or out of range") from exc
            if amount <= 0:
                raise CommandError("invalid expire time")
            ttl = float(amount) if option == "EX" else amount / 1000.0
            index += 2
        elif option == "NX":
            nx = True
            index += 1
        elif option == "XX":
            xx = True
            index += 1
        else:
            raise CommandError(f"syntax error near '{options[index]}'")
    if nx and xx:
        raise CommandError("NX and XX are mutually exclusive")
    value = _parse_value(tag, command.args[1])
    if not context.store.set(_owner(context), key, value, ttl=ttl, nx=nx, xx=xx):
        return None
    return SimpleString("OK")


async def cmd_get(command: Command, context: Context) -> Reply:
    value = context.store.get(_owner(context), command.args[0])
    return _pair(value) if value is not None else None


async def cmd_mget(command: Command, context: Context) -> Reply:
    owner = _owner(context)
    entries: list[Reply] = []
    for key in command.args:
        value = context.store.get(owner, key)
        entries.append(_pair(value) if value is not None else None)
    return entries


async def cmd_del(command: Command, context: Context) -> Reply:
    owner = _owner(context)
    return sum(1 for key in command.args if context.store.delete(owner, key))


async def cmd_exists(command: Command, context: Context) -> Reply:
    owner = _owner(context)
    return sum(1 for key in command.args if context.store.exists(owner, key))


async def cmd_type(command: Command, context: Context) -> Reply:
    value = context.store.get(_owner(context), command.args[0])
    return SimpleString(value.type.tag if value is not None else "none")


async def cmd_expire(command: Command, context: Context) -> Reply:
    try:
        seconds = int(command.args[1], 10)
    except ValueError as exc:
        raise CommandError("value is not an integer or out of range") from exc
    existed = context.store.expire(_owner(context), command.args[0], float(seconds))
    return 1 if existed else 0


async def cmd_ttl(command: Command, context: Context) -> Reply:
    return context.store.ttl(_owner(context), command.args[0])


async def cmd_keys(command: Command, context: Context) -> Reply:
    pattern = command.args[0]
    keys: list[Reply] = [key for key in context.store.keys(_owner(context)) if match(pattern, key)]
    return keys


async def cmd_scan(command: Command, context: Context) -> Reply:
    try:
        start = int(command.args[0], 10)
    except ValueError as exc:
        raise CommandError("invalid cursor") from exc
    if start < 0:
        raise CommandError("invalid cursor")
    pattern = "*"
    count = 10
    options = command.args[1:]
    index = 0
    while index < len(options):
        option = options[index].upper()
        if option == "MATCH" and index + 1 < len(options):
            pattern = options[index + 1]
            index += 2
        elif option == "COUNT" and index + 1 < len(options):
            try:
                count = int(options[index + 1], 10)
            except ValueError as exc:
                raise CommandError("value is not an integer or out of range") from exc
            if count <= 0:
                raise CommandError("COUNT must be positive")
            index += 2
        else:
            raise CommandError(f"syntax error near '{options[index]}'")
    keys = context.store.keys(_owner(context))
    end = min(start + count, len(keys))
    batch: list[Reply] = [key for key in keys[start:end] if match(pattern, key)]
    result: list[Reply] = ["0" if end >= len(keys) else str(end), batch]
    return result


async def cmd_dbsize(command: Command, context: Context) -> Reply:
    return context.store.dbsize(_owner(context))


async def _adjust(command: Command, context: Context, sign: int) -> Reply:
    key = command.args[0]
    amount = 1
    if len(command.args) > 1:
        if len(command.args) != 3 or command.args[1].upper() != "BY":
            raise CommandError("syntax error")
        try:
            amount = int(command.args[2], 10)
        except ValueError as exc:
            raise CommandError("value is not an integer or out of range") from exc
    owner = _owner(context)
    existing = context.store.get(owner, key)
    if existing is None:
        vtype = ValueType.I32
        current = 0
    elif existing.type.is_integer_type:
        vtype = existing.type
        current = int(existing.data)
    else:
        raise CommandError("value is not an integer or out of range")
    updated = current + sign * amount
    try:
        value = Value.from_int(vtype, updated)
    except InvalidValue as exc:
        raise CommandError("increment or decrement would overflow") from exc
    context.store.update(owner, key, value)
    return updated


async def cmd_incr(command: Command, context: Context) -> Reply:
    return await _adjust(command, context, 1)


async def cmd_decr(command: Command, context: Context) -> Reply:
    return await _adjust(command, context, -1)


# --- pub/sub -------------------------------------------------------------


def _deliver_retained(context: Context, channel: str) -> None:
    value = context.bus.retained(context.subscriber.owner, channel)
    if value is not None:
        frame = encode_message(channel, value.type.tag, value.text())
        context.pending.append(frame)


async def cmd_subscribe(command: Command, context: Context) -> Reply:
    subscriber = context.subscriber
    replies: list[Reply] = []
    for channel in command.args:
        context.bus.subscribe(subscriber, channel)
        entry: list[Reply] = ["subscribe", channel, subscriber.count]
        replies.append(entry)
        _deliver_retained(context, channel)
    return replies


async def cmd_psubscribe(command: Command, context: Context) -> Reply:
    subscriber = context.subscriber
    replies: list[Reply] = []
    for pattern in command.args:
        context.bus.psubscribe(subscriber, pattern)
        entry: list[Reply] = ["psubscribe", pattern, subscriber.count]
        replies.append(entry)
        for channel in context.bus.retained_channels(subscriber.owner, pattern):
            _deliver_retained(context, channel)
    return replies


async def cmd_unsubscribe(command: Command, context: Context) -> Reply:
    subscriber = context.subscriber
    targets = list(command.args) if command.args else list(subscriber.channels)
    return sum(1 for channel in targets if context.bus.unsubscribe(subscriber, channel))


async def cmd_punsubscribe(command: Command, context: Context) -> Reply:
    subscriber = context.subscriber
    targets = list(command.args) if command.args else list(subscriber.patterns)
    return sum(1 for pattern in targets if context.bus.punsubscribe(subscriber, pattern))


async def cmd_publish(command: Command, context: Context) -> Reply:
    channel = command.args[0]
    tag = "str"
    retain = False
    options = command.args[2:]
    index = 0
    while index < len(options):
        option = options[index].upper()
        if option == "TYPE" and index + 1 < len(options):
            tag = options[index + 1]
            index += 2
        elif option == "RETAIN":
            retain = True
            index += 1
        else:
            raise CommandError(f"syntax error near '{options[index]}'")
    value = _parse_value(tag, command.args[1])
    owner = _owner(context)
    if retain:
        context.bus.retain(owner, channel, value)
    return context.bus.publish(owner, channel, value)


async def cmd_pubsub(command: Command, context: Context) -> Reply:
    subcommand = command.args[0].upper()
    owner = _owner(context)
    if subcommand == "CHANNELS":
        pattern = command.args[1] if len(command.args) > 1 else "*"
        channels: list[Reply] = list(context.bus.channels(owner, pattern))
        return channels
    if subcommand == "NUMSUB":
        counts: list[Reply] = []
        for channel in command.args[1:]:
            counts.append(channel)
            counts.append(context.bus.numsub(owner, channel))
        return counts
    if subcommand == "NUMPAT":
        return context.bus.numpat(owner)
    raise CommandError(f"unknown subcommand '{command.args[0]}'")


register("PING", cmd_ping, max_args=1, scope="public")
register("ECHO", cmd_echo, min_args=1, max_args=1, scope="public")
register("QUIT", cmd_quit, scope="public")
register("AUTH", cmd_auth, min_args=1, max_args=1, scope="public")
register("COMMAND", cmd_command, max_args=2, scope="admin")
register("INFO", cmd_info, max_args=1, scope="admin")
register("HELLO", cmd_hello, scope="public")
register("SET", cmd_set, min_args=2, max_args=None, scope="write")
register("GET", cmd_get, min_args=1, max_args=1, scope="read")
register("MGET", cmd_mget, min_args=1, max_args=None, scope="read")
register("DEL", cmd_del, min_args=1, max_args=None, scope="write")
register("EXISTS", cmd_exists, min_args=1, max_args=None, scope="read")
register("TYPE", cmd_type, min_args=1, max_args=1, scope="read")
register("EXPIRE", cmd_expire, min_args=2, max_args=2, scope="write")
register("TTL", cmd_ttl, min_args=1, max_args=1, scope="read")
register("KEYS", cmd_keys, min_args=1, max_args=1, scope="read")
register("SCAN", cmd_scan, min_args=1, max_args=None, scope="read")
register("DBSIZE", cmd_dbsize, scope="read")
register("INCR", cmd_incr, min_args=1, max_args=3, scope="write")
register("DECR", cmd_decr, min_args=1, max_args=3, scope="write")
register("PUBLISH", cmd_publish, min_args=2, max_args=None, scope="subscribe")
register("SUBSCRIBE", cmd_subscribe, min_args=1, max_args=None, scope="subscribe")
register("PSUBSCRIBE", cmd_psubscribe, min_args=1, max_args=None, scope="subscribe")
register("UNSUBSCRIBE", cmd_unsubscribe, max_args=None, scope="subscribe")
register("PUNSUBSCRIBE", cmd_punsubscribe, max_args=None, scope="subscribe")
register("PUBSUB", cmd_pubsub, min_args=1, max_args=None, scope="read")
