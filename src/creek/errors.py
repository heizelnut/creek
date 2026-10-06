"""Exception hierarchy for Creek.

Library code raises these; only ``creek.cli`` decides process exit codes.
"""

from __future__ import annotations


class CreekError(Exception):
    """Base class for all Creek errors."""


class ProtocolError(CreekError):
    """A malformed frame or command was received from a peer."""


class InvalidValue(CreekError):
    """A value could not be parsed, encoded, or is out of range."""


class CommandError(CreekError):
    """A command failed; carries a RESP error code such as ``ERR``."""

    def __init__(self, message: str, code: str = "ERR") -> None:
        super().__init__(message)
        self.code = code

    def __str__(self) -> str:
        return f"{self.code} {super().__str__()}"
