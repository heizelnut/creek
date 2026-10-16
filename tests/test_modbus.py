"""Modbus/TCP driver tests (M9b): addressing, codec, and a simulator round-trip."""

from __future__ import annotations

import asyncio
import struct
from collections.abc import AsyncIterator

import pytest

from creek.plc import (
    Connection,
    ModbusDriver,
    ModbusError,
    ModbusSimulator,
    Tag,
    decode_registers,
    encode_registers,
    parse_address,
)
from creek.value import Value


def _tag(key: str, address: str, type: str = "u16") -> Tag:
    return Tag(key=key, connection="plc", address=address, type=type)


def _connection(port: int, **params: int | str) -> Connection:
    return Connection(
        name="plc",
        driver="modbus",
        host="127.0.0.1",
        port=port,
        params={"unit": 1, **params},
    )


@pytest.fixture
async def simulator() -> AsyncIterator[ModbusSimulator]:
    instance = ModbusSimulator()
    await instance.start()
    try:
        yield instance
    finally:
        await instance.close()


@pytest.fixture
async def driver(simulator: ModbusSimulator) -> AsyncIterator[ModbusDriver]:
    instance = ModbusDriver(_connection(simulator.port))
    try:
        yield instance
    finally:
        await instance.close()


# --- addressing -----------------------------------------------------------


@pytest.mark.parametrize(
    ("address", "area", "index"),
    [("C0", "coil", 0), ("DI3", "discrete", 3), ("IR10", "input", 10), ("HR400", "holding", 400)],
)
def test_parse_address(address: str, area: str, index: int) -> None:
    parsed = parse_address(address)
    assert (parsed.area, parsed.index) == (area, index)


@pytest.mark.parametrize("address", ["", "X0", "HR", "HRabc", "C-1"])
def test_parse_address_rejects_bad(address: str) -> None:
    with pytest.raises(ModbusError):
        parse_address(address)


# --- codec ----------------------------------------------------------------


def test_codec_roundtrip() -> None:
    for type_tag, raw in [("u16", "65535"), ("i16", "-1234"), ("u32", "4000000000"), ("i32", "-5")]:
        value = Value.parse(type_tag, raw)
        registers = encode_registers(value, type_tag, "big")
        assert decode_registers(registers, type_tag, "big") == value


def test_f32_roundtrip() -> None:
    value = Value.parse("f32", "21.5")
    registers = encode_registers(value, "f32", "big")
    assert len(registers) == 2
    assert decode_registers(registers, "f32", "big").data == pytest.approx(21.5)


def test_word_order() -> None:
    value = Value.parse("u32", "305419896")  # 0x12345678
    assert encode_registers(value, "u32", "big") == [0x1234, 0x5678]
    assert encode_registers(value, "u32", "little") == [0x5678, 0x1234]


# --- reads ----------------------------------------------------------------


async def test_read_holding_registers(driver: ModbusDriver, simulator: ModbusSimulator) -> None:
    simulator.holding[0] = 1500
    simulator.holding[1] = 42
    values = await driver.read([_tag("speed", "HR0"), _tag("count", "HR1")])
    assert values["speed"] == Value.parse("u16", "1500")
    assert values["count"] == Value.parse("u16", "42")


async def test_read_coils(driver: ModbusDriver, simulator: ModbusSimulator) -> None:
    simulator.coils[0] = True
    simulator.coils[2] = True
    values = await driver.read(
        [_tag("a", "C0", "bool"), _tag("b", "C1", "bool"), _tag("c", "C2", "bool")]
    )
    assert values == {
        "a": Value.parse("bool", "true"),
        "b": Value.parse("bool", "false"),
        "c": Value.parse("bool", "true"),
    }


async def test_contiguous_registers_are_batched(
    driver: ModbusDriver, simulator: ModbusSimulator
) -> None:
    tags = [_tag(f"r{i}", f"HR{i}") for i in range(3)]
    await driver.read(tags)
    assert simulator.requests == 1  # one request for the contiguous run


async def test_non_contiguous_registers_are_separate(
    driver: ModbusDriver, simulator: ModbusSimulator
) -> None:
    await driver.read([_tag("a", "HR0"), _tag("b", "HR10")])
    assert simulator.requests == 2


async def test_read_32bit_spanning_two_registers(
    simulator: ModbusSimulator,
) -> None:
    simulator.holding[0] = 0x1234
    simulator.holding[1] = 0x5678
    driver = ModbusDriver(_connection(simulator.port))
    try:
        values = await driver.read([_tag("big", "HR0", "u32")])
        assert values["big"] == Value.parse("u32", "305419896")
    finally:
        await driver.close()


# --- writes ---------------------------------------------------------------


async def test_write_register(driver: ModbusDriver, simulator: ModbusSimulator) -> None:
    await driver.write(_tag("setpoint", "HR5", "u16"), Value.parse("u16", "1234"))
    assert simulator.holding[5] == 1234


async def test_write_coil(driver: ModbusDriver, simulator: ModbusSimulator) -> None:
    await driver.write(_tag("run", "C3", "bool"), Value.parse("bool", "true"))
    assert simulator.coils[3] is True


async def test_write_32bit_register_pair(driver: ModbusDriver, simulator: ModbusSimulator) -> None:
    await driver.write(_tag("setpoint", "HR0", "u32"), Value.parse("u32", "305419896"))
    assert simulator.holding[0] == 0x1234
    assert simulator.holding[1] == 0x5678


async def test_write_rejects_read_only_area(driver: ModbusDriver) -> None:
    with pytest.raises(ModbusError):
        await driver.write(_tag("di", "DI0", "bool"), Value.parse("bool", "true"))


# --- errors ---------------------------------------------------------------


async def test_device_exception_is_raised() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        header = await reader.readexactly(7)
        transaction, _, length, unit = struct.unpack(">HHHB", header)
        await reader.readexactly(length - 1)
        pdu = bytes([0x83, 0x02])  # exception for function 0x03 (illegal data address)
        writer.write(struct.pack(">HHHB", transaction, 0, len(pdu) + 1, unit) + pdu)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = int(server.sockets[0].getsockname()[1])
    driver = ModbusDriver(_connection(port))
    try:
        with pytest.raises(ModbusError, match="exception 2"):
            await driver.read([_tag("x", "HR0")])
    finally:
        await driver.close()
        server.close()
        await server.wait_closed()
