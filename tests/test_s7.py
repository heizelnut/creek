"""Siemens S7 driver tests (M9c), against a python-snap7 server.

``python-snap7`` needs the native snap7 library; the module is skipped if it is
unavailable.
"""

from __future__ import annotations

import socket
import struct
from collections.abc import Iterator
from types import SimpleNamespace

import pytest

pytest.importorskip("snap7")

from snap7.server import Server
from snap7.type import SrvArea

from creek.plc import Connection, S7Driver, S7Error, Tag, s7
from creek.value import Value


def _connection(port: int) -> Connection:
    return Connection(
        name="s7",
        driver="s7",
        host="127.0.0.1",
        port=port,
        params={"rack": 0, "slot": 1},
    )


def _tag(key: str, address: str, type: str) -> Tag:
    return Tag(key=key, connection="s7", address=address, type=type)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def s7_server() -> Iterator[SimpleNamespace]:
    db, markers, inputs = bytearray(64), bytearray(64), bytearray(64)
    server = Server(log=False)
    server.register_area(SrvArea.DB, 1, db)
    server.register_area(SrvArea.MK, 0, markers)
    server.register_area(SrvArea.PE, 0, inputs)
    port = _free_port()
    server.start(tcp_port=port)
    try:
        yield SimpleNamespace(db=db, markers=markers, inputs=inputs, port=port)
    finally:
        server.stop()


# --- addressing -----------------------------------------------------------


@pytest.mark.parametrize(
    ("address", "area", "db", "offset", "bit"),
    [
        ("DB1.DBX0.0", "DB", 1, 0, 0),
        ("DB1.DBX5.3", "DB", 1, 5, 3),
        ("DB1.DBB4", "DB", 1, 4, None),
        ("DB1.DBW8", "DB", 1, 8, None),
        ("DB1.DBD12", "DB", 1, 12, None),
        ("I0.0", "I", 0, 0, 0),
        ("IB2", "I", 0, 2, None),
        ("MW10", "M", 0, 10, None),
        ("Q0.7", "Q", 0, 0, 7),
    ],
)
def test_parse_address(address: str, area: str, db: int, offset: int, bit: int | None) -> None:
    parsed = s7.parse_address(address)
    assert (parsed.area, parsed.db, parsed.offset, parsed.bit) == (area, db, offset, bit)


@pytest.mark.parametrize("address", ["", "nope", "DB1.X0", "DBX0.0", "I0.9", "DB1.DBX0.8"])
def test_parse_address_rejects_bad(address: str) -> None:
    with pytest.raises(S7Error):
        s7.parse_address(address)


# --- codec ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("type_tag", "raw"),
    [
        ("u8", "200"),
        ("i8", "-5"),
        ("u16", "4660"),
        ("i16", "-1234"),
        ("u32", "4000000000"),
        ("i32", "-9"),
        ("f32", "21.5"),
        ("f64", "3.14159"),
    ],
)
def test_codec_roundtrip(type_tag: str, raw: str) -> None:
    value = Value.parse(type_tag, raw)
    assert s7.decode(s7.encode(value, type_tag), type_tag, None) == value


def test_decode_bool() -> None:
    assert s7.decode(bytes([0b00000100]), "bool", 2) == Value.parse("bool", "true")
    assert s7.decode(bytes([0b00000100]), "bool", 3) == Value.parse("bool", "false")


# --- driver ---------------------------------------------------------------


async def test_read_and_write_db(s7_server: SimpleNamespace) -> None:
    driver = S7Driver(_connection(s7_server.port))
    try:
        s7_server.db[0:2] = b"\x12\x34"
        values = await driver.read([_tag("word", "DB1.DBW0", "u16")])
        assert values["word"] == Value.parse("u16", "4660")

        await driver.write(_tag("out", "DB1.DBW2", "u16"), Value.parse("u16", "43981"))
        assert s7_server.db[2:4] == b"\xab\xcd"
    finally:
        await driver.close()


async def test_bit_read_and_write(s7_server: SimpleNamespace) -> None:
    driver = S7Driver(_connection(s7_server.port))
    try:
        s7_server.db[5] = 0b00000100
        values = await driver.read(
            [_tag("set", "DB1.DBX5.2", "bool"), _tag("clear", "DB1.DBX5.3", "bool")]
        )
        assert values["set"] == Value.parse("bool", "true")
        assert values["clear"] == Value.parse("bool", "false")

        await driver.write(_tag("clear", "DB1.DBX5.3", "bool"), Value.parse("bool", "true"))
        assert s7_server.db[5] & 0x08  # other bits preserved
    finally:
        await driver.close()


async def test_float(s7_server: SimpleNamespace) -> None:
    driver = S7Driver(_connection(s7_server.port))
    try:
        struct.pack_into(">f", s7_server.db, 8, 21.5)
        values = await driver.read([_tag("temp", "DB1.DBD8", "f32")])
        assert values["temp"].data == pytest.approx(21.5)
    finally:
        await driver.close()


async def test_marker_area(s7_server: SimpleNamespace) -> None:
    driver = S7Driver(_connection(s7_server.port))
    try:
        s7_server.markers[0] = 0b00000101
        values = await driver.read([_tag("byte", "MB0", "u8"), _tag("bit", "M0.2", "bool")])
        assert values["byte"] == Value.parse("u8", "5")
        assert values["bit"] == Value.parse("bool", "true")
    finally:
        await driver.close()
