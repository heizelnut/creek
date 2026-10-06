"""Bus tests: delivery, patterns, namespaces, retained, and backpressure."""

from __future__ import annotations

from creek.protocol import encode_message
from creek.pubsub import Bus
from creek.value import Value


class FakeSubscriber:
    """A stand-in for a connection that records frames instead of queuing them."""

    def __init__(self, owner: str = "default") -> None:
        self.owner = owner
        self.channels: set[str] = set()
        self.patterns: set[str] = set()
        self.frames: list[bytes] = []
        self.full = False

    def deliver(self, frame: bytes) -> bool:
        if self.full:
            return False
        self.frames.append(frame)
        return True


def _value(raw: str = "hi") -> Value:
    return Value.parse("str", raw)


def test_direct_delivery() -> None:
    bus = Bus()
    subscriber = FakeSubscriber()
    bus.subscribe(subscriber, "news")
    assert bus.publish("default", "news", _value()) == 1
    assert subscriber.frames == [encode_message("news", "str", "hi")]


def test_pattern_delivery() -> None:
    bus = Bus()
    subscriber = FakeSubscriber()
    bus.psubscribe(subscriber, "news.*")
    assert bus.publish("default", "news.tech", _value()) == 1
    assert subscriber.frames == [encode_message("news.tech", "str", "hi")]


def test_namespaces_are_isolated() -> None:
    bus = Bus()
    subscriber = FakeSubscriber(owner="a")
    bus.subscribe(subscriber, "news")
    assert bus.publish("b", "news", _value()) == 0
    assert subscriber.frames == []


def test_unsubscribe_stops_delivery() -> None:
    bus = Bus()
    subscriber = FakeSubscriber()
    bus.subscribe(subscriber, "news")
    assert bus.unsubscribe(subscriber, "news") is True
    assert bus.unsubscribe(subscriber, "news") is False
    assert bus.publish("default", "news", _value()) == 0
    assert subscriber.frames == []


def test_slow_subscriber_is_dropped() -> None:
    bus = Bus()
    subscriber = FakeSubscriber()
    bus.subscribe(subscriber, "news")
    subscriber.full = True
    assert bus.publish("default", "news", _value()) == 0
    assert bus.numsub("default", "news") == 0


def test_publish_dedups_a_subscriber() -> None:
    bus = Bus()
    subscriber = FakeSubscriber()
    bus.subscribe(subscriber, "news")
    bus.psubscribe(subscriber, "news.*")
    assert bus.publish("default", "news", _value()) == 1
    assert len(subscriber.frames) == 1


def test_remove_clears_subscriptions() -> None:
    bus = Bus()
    subscriber = FakeSubscriber()
    bus.subscribe(subscriber, "a")
    bus.psubscribe(subscriber, "b.*")
    bus.remove(subscriber)
    assert bus.channels("default") == []
    assert bus.numpat("default") == 0


def test_retained() -> None:
    bus = Bus()
    bus.retain("default", "news", _value("last"))
    assert bus.retained("default", "news") == _value("last")
    assert bus.retained_channels("default", "n*") == ["news"]
    assert bus.retained("other", "news") is None


def test_introspection() -> None:
    bus = Bus()
    first = FakeSubscriber()
    second = FakeSubscriber()
    bus.subscribe(first, "b")
    bus.subscribe(first, "a")
    bus.subscribe(second, "a")
    bus.psubscribe(first, "x.*")
    assert bus.channels("default") == ["a", "b"]
    assert bus.channels("default", "a*") == ["a"]
    assert bus.numsub("default", "a") == 2
    assert bus.numsub("default", "z") == 0
    assert bus.numpat("default") == 1
