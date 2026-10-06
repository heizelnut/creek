"""Command registry, dispatch, and the built-in commands.

Add a command by writing an ``async def`` handler and calling :func:`register`.
Nothing else needs to change; both transports pick it up automatically.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from creek import __version__
from creek.auth import Authenticator, Identity
from creek.errors import CommandError
from creek.protocol import Command, Reply, SimpleString


@dataclass(slots=True)
class Context:
    """State for one client connection."""

    peer: str = ""
    identity: Identity | None = None
    authenticator: Authenticator | None = None
    close_after_reply: bool = False


Handler = Callable[[Command, Context], Awaitable[Reply]]


@dataclass(frozen=True, slots=True)
class Spec:
    """A registered command."""

    handler: Handler
    min_args: int
    max_args: int
    scope: str


REGISTRY: dict[str, Spec] = {}


def register(
    name: str,
    handler: Handler,
    *,
    min_args: int = 0,
    max_args: int = 0,
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
    if count < spec.min_args or count > spec.max_args:
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


register("PING", cmd_ping, max_args=1, scope="public")
register("ECHO", cmd_echo, min_args=1, max_args=1, scope="public")
register("QUIT", cmd_quit, scope="public")
register("AUTH", cmd_auth, min_args=1, max_args=1, scope="public")
register("COMMAND", cmd_command, max_args=2, scope="admin")
register("INFO", cmd_info, max_args=1, scope="admin")
