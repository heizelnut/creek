#!/usr/bin/env python3
"""Hash a token for the Creek auth file.

uv run python scripts/hash_token.py 'my-secret-token'
"""

from __future__ import annotations

import sys

from creek.auth import hash_token


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: hash_token.py <token>", file=sys.stderr)
        return 2
    print(hash_token(argv[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
