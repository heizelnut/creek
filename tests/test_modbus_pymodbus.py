"""Cross-check the Modbus driver against pymodbus, an independent implementation.

Our in-tree simulator shares our assumptions, so it could agree with a framing
bug. This test drives a real ``pymodbus`` server instead. ``pymodbus`` is a dev
dependency; the test is skipped when it is unavailable.
"""

from __future__ import annotations

import asyncio
import socket
import warnings
from collections.abc import AsyncIterator

import pytest

pytest.importorskip("pymodbus")

from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
)
from pymodbus.server import ModbusTcpServer

from creek.plc import Connection, ModbusDriver, Tag
from creek.value import Value


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
async def pymodbus_server() -> AsyncIterator[int]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        device = ModbusDeviceContext(
            co=ModbusSequentialDataBlock(1, [0] * 64),
            di=ModbusSequentialDataBlock(1, [0] * 64),
            ir=ModbusSequentialDataBlock(1, [0] * 64),
            hr=ModbusSequentialDataBlock(1, [0] * 64),
        )
        context = ModbusServerContext(devices=device, single=True)
        port = _free_port()
        server = ModbusTcpServer(context, address=("127.0.0.1", port))
    await server.serve_forever(background=True)
    await asyncio.sleep(0.1)
    try:
        yield port
    finally:
        await server.shutdown()
        await asyncio.sleep(0.05)


def _tag(key: str, address: str, type: str) -> Tag:
    return Tag(key=key, connection="plc", address=address, type=type)


async def test_driver_reads_and_writes_against_pymodbus(pymodbus_server: int) -> None:
    connection = Connection(name="plc", driver="modbus", host="127.0.0.1", port=pymodbus_server)
    driver = ModbusDriver(connection)
    try:
        await driver.write(_tag("sp", "HR0", "u16"), Value.parse("u16", "1234"))
        await driver.write(_tag("run", "C0", "bool"), Value.parse("bool", "true"))
        values = await driver.read(
            [
                _tag("sp", "HR0", "u16"),
                _tag("run", "C0", "bool"),
                _tag("stopped", "C1", "bool"),
            ]
        )
        assert values["sp"] == Value.parse("u16", "1234")
        assert values["run"] == Value.parse("bool", "true")
        assert values["stopped"] == Value.parse("bool", "false")
    finally:
        await driver.close()
