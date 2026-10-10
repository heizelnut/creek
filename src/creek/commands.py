"""Command registry, dispatch, and the built-in commands.

Add a command by writing an ``async def`` handler and calling :func:`register`.
Nothing else needs to change; both transports pick it up automatically.
"""

from __future__ import annotations

import base64
import logging
import os
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

from creek import __version__
from creek.auth import Authenticator, Identity
from creek.cluster import Cluster, key_slot
from creek.errors import CommandError, CreekError, InvalidValue
from creek.pattern import match
from creek.persist import Snapshot, encode_records, encode_snapshot
from creek.plc.manager import PlcError, PlcManager
from creek.plc.poller import parse_tag_value
from creek.protocol import Command, Reply, SimpleString, encode_message
from creek.pubsub import Bus, Subscriber
from creek.replication import Replication
from creek.store import Store
from creek.tenant import Quota, Tenant, TenantRegistry
from creek.value import Value, ValueType

log = logging.getLogger("creek.audit")


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
    tenants: TenantRegistry = field(default_factory=TenantRegistry)
    subscriber: Subscriber = field(default_factory=Subscriber)
    pending: list[bytes] = field(default_factory=list)
    quota_tenant: str | None = None
    replication: Replication = field(default_factory=Replication)
    cluster: Cluster | None = None
    plc: PlcManager | None = None
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
    _check_lengths(command)
    _check_cluster(command, context)
    _check_rate(context)
    if spec.scope == "admin":
        _audit(command, context)
    return await spec.handler(command, context)


def _check_access(scope: str, context: Context) -> None:
    if scope == "public":
        return
    if scope == "write" and context.replication.state.read_only:
        raise CommandError("replica is read-only", code="READONLY")
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
    # AUTH selects the namespace, so the pub/sub subscriber moves with it.
    # Drop any subscriptions registered under the previous namespace.
    context.bus.remove(context.subscriber)
    context.identity = identity
    context.subscriber.owner = identity.namespace
    context.tenants.ensure(identity.namespace)
    previous = context.quota_tenant
    if previous != identity.namespace and not context.tenants.acquire(identity.namespace):
        raise _quota(
            context.tenants.get(identity.namespace),
            f"connection quota exceeded for tenant '{identity.namespace}'",
        )
    if previous is not None and previous != identity.namespace:
        context.tenants.release(previous)
    context.quota_tenant = identity.namespace
    return SimpleString("OK")


async def cmd_command(command: Command, context: Context) -> Reply:
    if command.args:
        if command.args[0].upper() != "COUNT":
            raise CommandError(f"unknown subcommand '{command.args[0]}'")
        return len(REGISTRY)
    names: list[Reply] = [name for name in sorted(REGISTRY)]
    return names


async def cmd_info(command: Command, context: Context) -> Reply:
    section = command.args[0].lower() if command.args else "all"
    if section in ("tenant", "tenants"):
        return _tenant_report(context)
    if section == "replication":
        return _replication_report(context)
    if section not in ("all", "default", "server"):
        return ""
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


# --- quotas --------------------------------------------------------------

_LIMIT_FIELDS = {
    "MAXKEYS": "max_keys",
    "MAXMEMORY": "max_memory",
    "MAXCONNECTIONS": "max_connections",
    "MAXSUBSCRIPTIONS": "max_subscriptions",
    "MAXRATE": "max_rate",
}


def _quota(tenant: Tenant | None, message: str) -> CommandError:
    if tenant is not None:
        tenant.quota_rejections += 1
    return CommandError(message, code="QUOTA")


def _limit(value: int | float | None) -> int:
    return -1 if value is None else int(value)


# --- length limits + audit ----------------------------------------------

MAX_KEY_BYTES = 1024
MAX_CHANNEL_BYTES = 1024

_KEY_COMMANDS = frozenset(
    {"SET", "GET", "MGET", "DEL", "EXISTS", "TYPE", "EXPIRE", "TTL", "INCR", "DECR"}
)
_MULTI_KEY_COMMANDS = frozenset({"MGET", "DEL", "EXISTS"})
_CHANNEL_COMMANDS = frozenset({"PUBLISH", "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE"})
_MULTI_CHANNEL_COMMANDS = frozenset({"SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE"})


def _check_key(key: str) -> None:
    if len(key.encode("utf-8")) > MAX_KEY_BYTES:
        raise CommandError(f"key too long (max {MAX_KEY_BYTES} bytes)")


def _check_channel(channel: str) -> None:
    if len(channel.encode("utf-8")) > MAX_CHANNEL_BYTES:
        raise CommandError(f"channel too long (max {MAX_CHANNEL_BYTES} bytes)")


def _check_lengths(command: Command) -> None:
    if command.name in _KEY_COMMANDS:
        keys = command.args if command.name in _MULTI_KEY_COMMANDS else command.args[:1]
        for key in keys:
            _check_key(key)
    elif command.name in _CHANNEL_COMMANDS:
        channels = command.args if command.name in _MULTI_CHANNEL_COMMANDS else command.args[:1]
        for channel in channels:
            _check_channel(channel)


def _check_cluster(command: Command, context: Context) -> None:
    cluster = context.cluster
    if cluster is None or command.name not in _KEY_COMMANDS:
        return
    keys = command.args if command.name in _MULTI_KEY_COMMANDS else command.args[:1]
    if not keys:
        return
    slots = {key_slot(key) for key in keys}
    if len(slots) > 1:
        raise CommandError("keys in request don't hash to the same slot", code="CROSSSLOT")
    slot = next(iter(slots))
    node = cluster.owner(slot)
    if node is None:
        raise CommandError("hash slot not served", code="CLUSTERDOWN")
    if node.addr != cluster.self_addr:
        raise CommandError(f"{slot} {node.addr}", code="MOVED")


def _audit(command: Command, context: Context) -> None:
    identity = context.identity
    actor = identity.id if identity is not None else "anonymous"
    log.info(
        "admin command %s by %s",
        command.name,
        actor,
        extra={
            "actor": actor,
            "namespace": identity.namespace if identity is not None else "default",
            "command": command.name,
            "command_args": list(command.args),
        },
    )


def _tenant_report(context: Context) -> str:
    lines = ["# Tenants"]
    for name in context.tenants.names():
        tenant = context.tenants.get(name)
        count, store_bytes = context.store.usage(name)
        channels, patterns = context.bus.owner_stats(name)
        lines.append(
            f"{name} keys={count} memory={store_bytes + context.bus.usage(name)} "
            f"connections={tenant.active_connections if tenant else 0} "
            f"subscriptions={context.bus.subscription_count(name)} "
            f"channels={channels} patterns={patterns} "
            f"commands={tenant.commands if tenant else 0} "
            f"rejected={tenant.quota_rejections if tenant else 0}"
        )
    return "\r\n".join(lines)


def _entry_size(key: str, value: Value) -> int:
    return len(key.encode("utf-8")) + len(value.to_bytes())


def _parse_limits(args: Sequence[str]) -> dict[str, int | float | None]:
    values: dict[str, int | float | None] = {}
    index = 0
    while index < len(args):
        field = _LIMIT_FIELDS.get(args[index].upper())
        if field is None or index + 1 >= len(args):
            raise CommandError(f"syntax error near '{args[index]}'")
        try:
            number = int(args[index + 1], 10)
        except ValueError as exc:
            raise CommandError("value is not an integer or out of range") from exc
        if number < 0:
            raise CommandError("quota must be non-negative")
        values[field] = number
        index += 2
    return values


def _check_rate(context: Context) -> None:
    owner = _owner(context)
    tenant = context.tenants.get(owner)
    if tenant is None:
        return
    tenant.commands += 1
    if not context.tenants.allow(owner):
        raise _quota(tenant, f"rate quota exceeded for tenant '{owner}'")


def _check_write_quota(
    context: Context, owner: str, key: str, value: Value, existing: Value | None
) -> None:
    tenant = context.tenants.get(owner)
    if tenant is None:
        return
    quota = tenant.quota
    if quota.max_keys is None and quota.max_memory is None:
        return
    count, store_bytes = context.store.usage(owner)
    if quota.max_keys is not None and existing is None and count >= quota.max_keys:
        raise _quota(tenant, f"key quota exceeded for tenant '{owner}'")
    if quota.max_memory is not None:
        total = store_bytes + context.bus.usage(owner)
        delta = _entry_size(key, value)
        if existing is not None:
            delta -= _entry_size(key, existing)
        if total + delta > quota.max_memory:
            raise _quota(tenant, f"memory quota exceeded for tenant '{owner}'")


def _check_retain_quota(context: Context, owner: str, channel: str, value: Value) -> None:
    tenant = context.tenants.get(owner)
    if tenant is None or tenant.quota.max_memory is None:
        return
    total = context.store.usage(owner)[1] + context.bus.usage(owner)
    delta = _entry_size(channel, value)
    previous = context.bus.retained(owner, channel)
    if previous is not None:
        delta -= _entry_size(channel, previous)
    if total + delta > tenant.quota.max_memory:
        raise _quota(tenant, f"memory quota exceeded for tenant '{owner}'")


def _check_subscription_quota(context: Context, owner: str, adding: int) -> None:
    tenant = context.tenants.get(owner)
    if tenant is None or tenant.quota.max_subscriptions is None:
        return
    current = context.bus.subscription_count(owner)
    if current + adding > tenant.quota.max_subscriptions:
        raise _quota(tenant, f"subscription quota exceeded for tenant '{owner}'")


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
    owner = _owner(context)
    existing = context.store.get(owner, key)
    if (not nx or existing is None) and (not xx or existing is not None):
        _check_write_quota(context, owner, key, value, existing)
    if not context.store.set(owner, key, value, ttl=ttl, nx=nx, xx=xx):
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


async def cmd_flushall(command: Command, context: Context) -> Reply:
    context.store.clear(_owner(context))
    return SimpleString("OK")


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
    _check_write_quota(context, owner, key, value, existing)
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
        if channel not in subscriber.channels:
            _check_subscription_quota(context, subscriber.owner, 1)
        context.bus.subscribe(subscriber, channel)
        entry: list[Reply] = ["subscribe", channel, subscriber.count]
        replies.append(entry)
        _deliver_retained(context, channel)
    return replies


async def cmd_psubscribe(command: Command, context: Context) -> Reply:
    subscriber = context.subscriber
    replies: list[Reply] = []
    for pattern in command.args:
        if pattern not in subscriber.patterns:
            _check_subscription_quota(context, subscriber.owner, 1)
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
        _check_retain_quota(context, owner, channel, value)
        context.bus.retain(owner, channel, value)
    return context.bus.publish(owner, channel, value)


async def cmd_tenant(command: Command, context: Context) -> Reply:
    subcommand = command.args[0].upper()
    registry = context.tenants
    if subcommand == "CREATE":
        if len(command.args) < 2:
            raise CommandError("syntax error")
        registry.create(command.args[1], Quota.from_dict(_parse_limits(command.args[2:])))
        return SimpleString("OK")
    if subcommand == "SET":
        if len(command.args) != 4:
            raise CommandError("syntax error")
        tenant = registry.get(command.args[1])
        if tenant is None:
            raise CommandError(f"no such tenant '{command.args[1]}'")
        limits = _parse_limits(command.args[2:])
        registry.set_quota(tenant.name, Quota.from_dict({**tenant.quota.to_dict(), **limits}))
        return SimpleString("OK")
    if subcommand == "LIST":
        names: list[Reply] = list(registry.names())
        return names
    if subcommand == "DROP":
        if len(command.args) != 2:
            raise CommandError("syntax error")
        name = command.args[1]
        context.store.clear(name)
        context.bus.drop_owner(name)
        return 1 if registry.drop(name) else 0
    if subcommand == "INFO":
        if len(command.args) != 2:
            raise CommandError("syntax error")
        tenant = registry.get(command.args[1])
        if tenant is None:
            return None
        count, store_bytes = context.store.usage(tenant.name)
        info: list[Reply] = [
            "name",
            tenant.name,
            "created_at",
            str(tenant.created_at),
            "keys",
            count,
            "memory",
            store_bytes + context.bus.usage(tenant.name),
            "connections",
            tenant.active_connections,
            "subscriptions",
            context.bus.subscription_count(tenant.name),
            "commands",
            tenant.commands,
            "rejected",
            tenant.quota_rejections,
            "connections_opened",
            tenant.connections_opened,
            "max_keys",
            _limit(tenant.quota.max_keys),
            "max_memory",
            _limit(tenant.quota.max_memory),
            "max_connections",
            _limit(tenant.quota.max_connections),
            "max_subscriptions",
            _limit(tenant.quota.max_subscriptions),
            "max_rate",
            _limit(tenant.quota.max_rate),
        ]
        return info
    raise CommandError(f"unknown subcommand '{command.args[0]}'")


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


# --- replication ---------------------------------------------------------


def _replication_report(context: Context) -> str:
    state = context.replication.state
    log = context.replication.log
    replica = state.master is not None
    lines = ["# Replication", f"role:{'replica' if replica else 'master'}"]
    if state.master is None:
        lines.append(f"offset:{log.sequence}")
        lines.append(f"backlog:{log.backlog()}")
    else:
        lines.append(f"master_host:{state.master[0]}")
        lines.append(f"master_port:{state.master[1]}")
        lines.append(f"offset:{state.offset}")
        lines.append(f"link:{state.link}")
        if state.down_since is not None:
            lines.append(f"master_link_down_since:{state.down_since:.3f}")
        if state.last_error is not None:
            lines.append(f"last_error:{state.last_error}")
    return "\r\n".join(lines)


async def cmd_sync(command: Command, context: Context) -> Reply:
    snapshot = Snapshot(
        entries=context.store.entries(),
        retained=context.bus.retained_entries(),
        tenants=context.tenants.entries(),
    )
    payload = base64.b64encode(encode_snapshot(snapshot)).decode("ascii")
    return [str(context.replication.log.sequence), payload]


async def cmd_replicate(command: Command, context: Context) -> Reply:
    try:
        offset = int(command.args[0], 10)
    except ValueError as exc:
        raise CommandError("invalid offset") from exc
    records = context.replication.log.since(offset)
    if records is None:
        return None
    payload = base64.b64encode(encode_records(records)).decode("ascii")
    return [str(context.replication.log.sequence), payload]


async def cmd_role(command: Command, context: Context) -> Reply:
    state = context.replication.state
    if state.master is None:
        return ["master", context.replication.log.sequence]
    host, port = state.master
    return ["replica", host, port, state.offset, state.link]


async def cmd_replicaof(command: Command, context: Context) -> Reply:
    if (
        len(command.args) == 2
        and command.args[0].upper() == "NO"
        and command.args[1].upper() == "ONE"
    ):
        await context.replication.promote()
        return SimpleString("OK")
    if len(command.args) not in (2, 3):
        raise CommandError("syntax error")
    try:
        port = int(command.args[1], 10)
    except ValueError as exc:
        raise CommandError("invalid port") from exc
    token = command.args[2] if len(command.args) == 3 else None
    await context.replication.follow(command.args[0], port, token)
    return SimpleString("OK")


async def cmd_keyslot(command: Command, context: Context) -> Reply:
    return key_slot(command.args[0])


def _cluster_report(cluster: Cluster | None) -> str:
    lines = ["# Cluster", f"cluster_enabled:{1 if cluster is not None else 0}"]
    if cluster is not None:
        lines.append(f"cluster_known_nodes:{len(cluster.nodes)}")
        lines.append(f"cluster_self:{cluster.self_addr}")
        lines.append(f"cluster_slots:{cluster.assigned()}")
    return "\r\n".join(lines)


async def cmd_cluster(command: Command, context: Context) -> Reply:
    subcommand = command.args[0].upper()
    cluster = context.cluster
    if subcommand == "INFO":
        return _cluster_report(cluster)
    if subcommand == "MYID":
        return cluster.self_addr if cluster is not None else ""
    if subcommand == "SLOTS":
        if cluster is None:
            return []
        slots: list[Reply] = []
        for node in cluster.nodes:
            ip, _, port = node.addr.rpartition(":")
            for start, end in node.slots:
                slots.append([start, end, [ip, int(port), node.addr]])
        return slots
    raise CommandError(f"unknown subcommand '{command.args[0]}'")


def _plc_report(manager: PlcManager, connection: str | None) -> str:
    lines = ["# PLC"]
    for entry in manager.connections.values():
        if connection is not None and entry.connection.name != connection:
            continue
        poller = entry.poller
        last = f"{poller.last_success:.3f}" if poller.last_success is not None else "-"
        lines.append(
            f"{entry.connection.name} driver={entry.connection.driver} "
            f"addr={entry.connection.host}:{entry.connection.port} link={poller.link} "
            f"tags={len(entry.tags)} polls={poller.polls} errors={poller.errors} "
            f"last_success={last}"
        )
    return "\r\n".join(lines)


async def cmd_plc(command: Command, context: Context) -> Reply:
    manager = context.plc
    if manager is None:
        raise CommandError("PLC support is not configured")
    subcommand = command.args[0].upper()
    if subcommand == "LIST":
        names: list[Reply] = list(manager.connections)
        return names
    if subcommand == "TAGS":
        if len(command.args) > 2:
            raise CommandError("syntax error")
        connection = command.args[1] if len(command.args) == 2 else None
        keys: list[Reply] = [
            tag.key for tag in manager.tags() if connection is None or tag.connection == connection
        ]
        return keys
    if subcommand == "INFO":
        if len(command.args) > 2:
            raise CommandError("syntax error")
        return _plc_report(manager, command.args[1] if len(command.args) == 2 else None)
    if subcommand == "WRITE":
        if len(command.args) != 3:
            raise CommandError("syntax error")
        tag = manager.tag(command.args[1])
        if tag is None:
            raise CommandError(f"unknown PLC tag '{command.args[1]}'")
        try:
            value = parse_tag_value(tag, command.args[2])
        except (ValueError, CreekError) as exc:
            raise CommandError(str(exc)) from exc
        try:
            await manager.write(tag.key, value)
        except PlcError as exc:
            raise CommandError(str(exc)) from exc
        _audit_plc_write(context, tag.key, value)
        return SimpleString("OK")
    raise CommandError(f"unknown subcommand '{command.args[0]}'")


def _audit_plc_write(context: Context, key: str, value: Value) -> None:
    identity = context.identity
    actor = identity.id if identity is not None else "anonymous"
    log.info(
        "plc write %s = %s by %s",
        key,
        value.text(),
        actor,
        extra={
            "actor": actor,
            "namespace": identity.namespace if identity is not None else "default",
            "command": "PLC WRITE",
            "tag": key,
            "value": value.text(),
        },
    )


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
register("FLUSHALL", cmd_flushall, scope="admin")
register("INCR", cmd_incr, min_args=1, max_args=3, scope="write")
register("DECR", cmd_decr, min_args=1, max_args=3, scope="write")
register("PUBLISH", cmd_publish, min_args=2, max_args=None, scope="subscribe")
register("SUBSCRIBE", cmd_subscribe, min_args=1, max_args=None, scope="subscribe")
register("PSUBSCRIBE", cmd_psubscribe, min_args=1, max_args=None, scope="subscribe")
register("UNSUBSCRIBE", cmd_unsubscribe, max_args=None, scope="subscribe")
register("PUNSUBSCRIBE", cmd_punsubscribe, max_args=None, scope="subscribe")
register("PUBSUB", cmd_pubsub, min_args=1, max_args=None, scope="read")
register("TENANT", cmd_tenant, min_args=1, max_args=None, scope="admin")
register("SYNC", cmd_sync, scope="admin")
register("REPLICATE", cmd_replicate, min_args=1, max_args=1, scope="admin")
register("ROLE", cmd_role, scope="read")
register("REPLICAOF", cmd_replicaof, min_args=1, max_args=3, scope="admin")
register("KEYSLOT", cmd_keyslot, min_args=1, max_args=1, scope="read")
register("CLUSTER", cmd_cluster, min_args=1, max_args=None, scope="admin")
register("PLC", cmd_plc, min_args=1, max_args=3, scope="write")
