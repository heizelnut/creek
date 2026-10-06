"""Duration parsing and address splitting."""

from __future__ import annotations

import pytest

from creek.cli import parse_duration
from creek.server import parse_addr


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("300", 300.0), ("5m", 300.0), ("500ms", 0.5), ("2h", 7200.0), ("1.5s", 1.5)],
)
def test_parse_duration(text: str, seconds: float) -> None:
    assert parse_duration(text) == seconds


def test_parse_duration_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        parse_duration("soon")


@pytest.mark.parametrize(
    ("addr", "expected"),
    [(":7100", ("", 7100)), ("127.0.0.1:0", ("127.0.0.1", 0))],
)
def test_parse_addr(addr: str, expected: tuple[str, int]) -> None:
    assert parse_addr(addr) == expected
