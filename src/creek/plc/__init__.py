"""PLC connectivity: drivers, tag map, and polling (see ``PLC.md``)."""

from creek.plc.driver import PlcDriver
from creek.plc.fake import FakeDriver
from creek.plc.modbus import (
    ModbusAddress,
    ModbusDriver,
    ModbusError,
    decode_registers,
    encode_registers,
    parse_address,
)
from creek.plc.modbus_server import ModbusSimulator
from creek.plc.poller import Poller, apply_scale
from creek.plc.tags import Connection, PlcConfig, PlcConfigError, Tag

__all__ = [
    "Connection",
    "FakeDriver",
    "ModbusAddress",
    "ModbusDriver",
    "ModbusError",
    "ModbusSimulator",
    "PlcConfig",
    "PlcConfigError",
    "PlcDriver",
    "Poller",
    "Tag",
    "apply_scale",
    "decode_registers",
    "encode_registers",
    "parse_address",
]
