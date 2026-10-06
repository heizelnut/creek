"""Shared test fixtures."""

from __future__ import annotations

import pytest

from creek.commands import Context
from creek.store import Store


class Clock:
    """A manually advanced clock for deterministic TTL tests."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(clock: Clock) -> Store:
    return Store(clock=clock)


@pytest.fixture
def context(store: Store) -> Context:
    return Context(store=store)
