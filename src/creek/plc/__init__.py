"""PLC connectivity: drivers, tag map, and polling (see ``PLC.md``)."""

from creek.plc.driver import PlcDriver
from creek.plc.fake import FakeDriver
from creek.plc.manager import PlcConnection, PlcError, PlcManager
from creek.plc.modbus import (
    ModbusAddress,
    ModbusDriver,
    ModbusError,
    decode_registers,
    encode_registers,
    parse_address,
)
from creek.plc.modbus_server import ModbusSimulator
from creek.plc.poller import Poller, apply_scale, parse_tag_value
from creek.plc.s7 import S7Address, S7Driver, S7Error
from creek.plc.s7 import parse_address as parse_s7_address
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
    "PlcConnection",
    "PlcDriver",
    "PlcError",
    "PlcManager",
    "Poller",
    "S7Address",
    "S7Driver",
    "S7Error",
    "Tag",
    "apply_scale",
    "decode_registers",
    "encode_registers",
    "parse_address",
    "parse_s7_address",
    "parse_tag_value",
]
