"""Guard against drift between the two dev-dependency declarations.

``pip`` reads ``[project.optional-dependencies].dev``; ``uv`` reads
``[dependency-groups].dev``. They must stay identical.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def test_dev_dependencies_are_in_sync() -> None:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    extras = data["project"]["optional-dependencies"]["dev"]
    groups = data["dependency-groups"]["dev"]
    assert extras == groups
