"""Persistence tests: framing, snapshots, journaling, and recovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from creek.errors import PersistError
from creek.persist import (
    Snapshot,
    decode_records,
    encode_blob,
    encode_record,
    read_snapshot,
    write_snapshot,
)
from creek.pubsub import Bus
from creek.server import Config, Server
from creek.store import Store
from creek.value import Value


class FakeJournal:
    """Records journal calls instead of writing them."""

    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def set(self, owner, key, value, expires_at):
        self.calls.append(("set", owner, key, value, expires_at))

    def delete(self, owner, key):
        self.calls.append(("delete", owner, key))

    def flush(self, owner):
        self.calls.append(("flush", owner))

    def retain(self, owner, channel, value):
        self.calls.append(("retain", owner, channel, value))


def _value(raw: str = "hi") -> Value:
    return Value.parse("str", raw)


# --- record framing ------------------------------------------------------


def test_record_roundtrip() -> None:
    record = {"op": "set", "owner": "ns", "key": "k", "data": "AA==", "expires_at": None}
    assert decode_records(encode_record(record)) == [record]


def test_decode_tolerates_truncated_tail() -> None:
    good = encode_record({"op": "delete", "owner": "ns", "key": "a"})
    assert decode_records(good + good[:5]) == [{"op": "delete", "owner": "ns", "key": "a"}]


def test_decode_stops_at_bad_crc() -> None:
    good = encode_record({"op": "delete", "owner": "ns", "key": "a"})
    corrupted = bytearray(good)
    corrupted[4] ^= 0xFF
    assert decode_records(bytes(corrupted)) == []


# --- snapshots -----------------------------------------------------------


def test_snapshot_roundtrip(tmp_path: Path) -> None:
    snapshot = Snapshot(
        entries=[("ns", "k", Value.parse("i32", "42"), None)],
        retained=[("ns", "ch", _value("last"))],
        tenants=[("ns", 1000.0)],
    )
    path = tmp_path / "creek.snapshot"
    write_snapshot(path, snapshot)
    assert read_snapshot(path) == snapshot


def test_missing_snapshot_is_empty(tmp_path: Path) -> None:
    assert read_snapshot(tmp_path / "nope") == Snapshot([], [], [])


def test_unsupported_snapshot_version(tmp_path: Path) -> None:
    path = tmp_path / "creek.snapshot"
    path.write_text('{"version": 99}')
    with pytest.raises(PersistError):
        read_snapshot(path)


def test_decode_blob_rejects_garbage() -> None:
    from creek.persist import decode_blob

    with pytest.raises(PersistError):
        decode_blob("not base64!!")


# --- journaling ----------------------------------------------------------


def test_store_journals_set(clock) -> None:
    journal = FakeJournal()
    store = Store(clock=clock, journal=journal)
    value = Value.parse("i32", "1")
    store.set("ns", "k", value, ttl=10)
    assert journal.calls == [("set", "ns", "k", value, 1010.0)]


def test_store_journals_expire(clock) -> None:
    journal = FakeJournal()
    store = Store(clock=clock, journal=journal)
    value = Value.parse("i32", "1")
    store.set("ns", "k", value)
    journal.calls.clear()
    store.expire("ns", "k", 5)
    assert journal.calls == [("set", "ns", "k", value, 1005.0)]


def test_store_journals_delete_and_flush(clock) -> None:
    journal = FakeJournal()
    store = Store(clock=clock, journal=journal)
    store.set("ns", "k", Value.parse("i32", "1"))
    journal.calls.clear()
    store.delete("ns", "k")
    store.clear("ns")
    assert journal.calls == [("delete", "ns", "k"), ("flush", "ns")]


def test_bus_journals_retain() -> None:
    journal = FakeJournal()
    bus = Bus(journal=journal)
    value = _value("last")
    bus.retain("ns", "ch", value)
    assert journal.calls == [("retain", "ns", "ch", value)]


def test_restore_skips_expired(clock) -> None:
    store = Store(clock=clock)
    store.restore("ns", "k", _value("v"), 999.0)  # clock is 1000
    assert store.get("ns", "k") is None
    store.restore("ns", "k", _value("v"), 2000.0)
    assert store.get("ns", "k") == _value("v")


# --- end-to-end recovery -------------------------------------------------


def _config(tmp_path: Path) -> Config:
    return Config(
        ws_addr="127.0.0.1:0",
        tcp_addr="127.0.0.1:0",
        data_dir=tmp_path,
        snapshot_interval=0,
    )


async def test_survives_restart(tmp_path: Path) -> None:
    first = Server(_config(tmp_path))
    await first.start()
    first.store.set("default", "k", _value("v"), ttl=3600)
    first.bus.retain("default", "ch", _value("last"))
    await first.close()

    second = Server(_config(tmp_path))
    await second.start()
    try:
        assert second.store.get("default", "k") == _value("v")
        assert second.store.ttl("default", "k") > 0
        assert second.bus.retained("default", "ch") == _value("last")
    finally:
        await second.close()


async def test_snapshot_compacts_the_wal(tmp_path: Path) -> None:
    first = Server(_config(tmp_path))
    await first.start()
    first.store.set("default", "k", _value("v"))
    await first.close()
    # Nothing raced the shutdown snapshot, so the WAL was truncated.
    assert (tmp_path / "creek.wal").read_bytes() == b""

    second = Server(_config(tmp_path))
    await second.start()
    try:
        assert second.store.get("default", "k") == _value("v")
    finally:
        await second.close()


async def test_recovers_from_wal_only(tmp_path: Path) -> None:
    records = [
        encode_record(
            {
                "op": "set",
                "owner": "default",
                "key": "k",
                "data": encode_blob(_value("from-wal")),
                "expires_at": None,
            }
        ),
        encode_record(
            {
                "op": "retain",
                "owner": "default",
                "channel": "ch",
                "data": encode_blob(_value("retained")),
            }
        ),
    ]
    (tmp_path / "creek.wal").write_bytes(b"".join(records))

    server = Server(_config(tmp_path))
    await server.start()
    try:
        assert server.store.get("default", "k") == _value("from-wal")
        assert server.bus.retained("default", "ch") == _value("retained")
    finally:
        await server.close()
