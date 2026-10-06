"""Glob matching for ``KEYS``, ``PUBSUB``, and ``PSUBSCRIBE``.

``*`` matches any run of characters (including delimiters), ``?`` matches one,
and ``[...]`` matches a set — the same semantics as Redis. Backed by the
standard library's :func:`fnmatch.fnmatchcase`.
"""

from __future__ import annotations

from fnmatch import fnmatchcase


def match(pattern: str, name: str) -> bool:
    """Return whether ``name`` matches the glob ``pattern``."""
    return fnmatchcase(name, pattern)


__all__ = ["match"]
