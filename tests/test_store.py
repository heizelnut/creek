"""Store tests: namespacing, NX/XX, and TTL, with a fake clock."""

from __future__ import annotations

from creek.value import Value


def _int(number: int) -> Value:
    return Value.parse("i32", str(number))


def test_set_get(store) -> None:
    assert store.get("ns", "k") is None
    assert store.set("ns", "k", _int(1)) is True
    assert store.get("ns", "k") == _int(1)


def test_owners_are_isolated(store) -> None:
    store.set("a", "k", _int(1))
    assert store.get("b", "k") is None
    assert store.get("a", "k") == _int(1)


def test_nx_and_xx(store) -> None:
    assert store.set("ns", "k", _int(1), nx=True) is True
    assert store.set("ns", "k", _int(2), nx=True) is False
    assert store.get("ns", "k") == _int(1)
    assert store.set("ns", "missing", _int(1), xx=True) is False
    assert store.set("ns", "k", _int(3), xx=True) is True


def test_delete(store) -> None:
    store.set("ns", "k", _int(1))
    assert store.delete("ns", "k") is True
    assert store.delete("ns", "k") is False
    assert store.get("ns", "k") is None


def test_exists(store) -> None:
    assert store.exists("ns", "k") is False
    store.set("ns", "k", _int(1))
    assert store.exists("ns", "k") is True


def test_expire_and_ttl(store, clock) -> None:
    store.set("ns", "k", _int(1))
    assert store.ttl("ns", "k") == -1
    assert store.expire("ns", "k", 10) is True
    assert store.ttl("ns", "k") == 10
    clock.advance(4)
    assert store.ttl("ns", "k") == 6


def test_expiry_is_purged(store, clock) -> None:
    store.set("ns", "k", _int(1), ttl=5)
    clock.advance(6)
    assert store.get("ns", "k") is None
    assert store.ttl("ns", "k") == -2
    assert store.dbsize("ns") == 0


def test_expire_non_positive_deletes(store) -> None:
    store.set("ns", "k", _int(1))
    assert store.expire("ns", "k", 0) is True
    assert store.get("ns", "k") is None
    assert store.expire("ns", "k", 5) is False


def test_set_clears_ttl(store) -> None:
    store.set("ns", "k", _int(1), ttl=10)
    store.set("ns", "k", _int(2))
    assert store.ttl("ns", "k") == -1


def test_update_keeps_ttl(store) -> None:
    store.set("ns", "k", _int(1), ttl=10)
    store.update("ns", "k", _int(2))
    assert store.ttl("ns", "k") == 10
    assert store.get("ns", "k") == _int(2)


def test_keys_are_sorted_and_scoped(store) -> None:
    for key in ("b", "a", "c"):
        store.set("ns", key, _int(1))
    store.set("other", "z", _int(1))
    assert store.keys("ns") == ["a", "b", "c"]
    assert store.dbsize("ns") == 3
