"""Exception hierarchy for creek.

Library code raises these; only :mod:`creek.cli` decides process exit codes.
"""

from __future__ import annotations


class CreekError(Exception):
    """Base class for all creek errors."""


class InvalidValue(CreekError):
    """A value could not be parsed, encoded, or is out of range."""


__all__ = ["CreekError", "InvalidValue"]
