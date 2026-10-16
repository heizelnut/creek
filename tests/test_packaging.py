"""Packaging guard: the pip extra and the uv group must stay in sync."""

from __future__ import annotations

import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def test_dev_dependency_lists_match() -> None:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    extra = data["project"]["optional-dependencies"]["dev"]
    group = data["dependency-groups"]["dev"]
    assert sorted(extra) == sorted(group)


def test_entry_point_is_creek_plc() -> None:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    assert data["project"]["scripts"] == {"creek-plc": "creek.cli:main"}
