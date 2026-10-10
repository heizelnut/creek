"""A minimal Modbus/TCP server for tests and dry runs (``PLC.md`` §4).

This is **not** a PLC: it stores coils and registers in lists, answers the
read/write function codes the driver uses, and nothing else. It exists so the
driver can be exercised deterministically without hardware.
"""

from __future__ import annotations

import asyncio
import contextlib
import struct

from creek.plc.modbus import (
    MBAP,
    READ_COILS,
    READ_DISCRETE,
    READ_HOLDING,
    READ_INPUT,
    WRITE_COIL,
    WRITE_REGISTER,
    WRITE_REGISTERS,
)


class ModbusSimulator:
    """A tiny Modbus/TCP slave backed by coils and registers."""

    def __init__(
        self,
        *,
        coils: int = 64,
        discrete: int = 64,
        input_registers: int = 64,
        holding: int = 64,
    ) -> None:
        self.coils = [False] * coils
        self.discrete = [False] * discrete
        self.input = [0] * input_registers
        self.holding = [0] * holding
        self.requests = 0
        self.port = 0
        self._server: asyncio.Server | None = None

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> int:
        """Bind and serve; returns the actual port (useful when ``port`` is 0)."""
        self._server = await asyncio.start_server(self._handle, host, port)
        self.port = int(self._server.sockets[0].getsockname()[1])
        return self.port

    async def close(self) -> None:
        """Stop serving."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                header = await reader.readexactly(MBAP.size)
                transaction, _protocol, length, unit = MBAP.unpack(header)
                pdu = await reader.readexactly(length - 1)
                self.requests += 1
                response = self._dispatch(pdu)
                writer.write(MBAP.pack(transaction, 0, len(response) + 1, unit) + response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()

    def _dispatch(self, pdu: bytes) -> bytes:
        function = pdu[0]
        if function in (READ_COILS, READ_DISCRETE):
            start, count = struct.unpack(">HH", pdu[1:5])
            bits = self.coils if function == READ_COILS else self.discrete
            data = bytearray((count + 7) // 8)
            for offset in range(count):
                if bits[start + offset]:
                    data[offset // 8] |= 1 << (offset % 8)
            return struct.pack(">BB", function, len(data)) + bytes(data)
        if function in (READ_HOLDING, READ_INPUT):
            start, count = struct.unpack(">HH", pdu[1:5])
            registers = self.holding if function == READ_HOLDING else self.input
            raw = b"".join(struct.pack(">H", registers[start + offset]) for offset in range(count))
            return struct.pack(">BB", function, len(raw)) + raw
        if function == WRITE_COIL:
            index, payload = struct.unpack(">HH", pdu[1:5])
            self.coils[index] = payload == 0xFF00
            return pdu[:5]
        if function == WRITE_REGISTER:
            index, value = struct.unpack(">HH", pdu[1:5])
            self.holding[index] = value
            return pdu[:5]
        if function == WRITE_REGISTERS:
            start, count, byte_count = struct.unpack(">HHB", pdu[1:6])
            values = struct.unpack(f">{count}H", pdu[6 : 6 + byte_count])
            for offset, value in enumerate(values):
                self.holding[start + offset] = value
            return struct.pack(">BHH", function, start, count)
        return bytes([function | 0x80, 0x01])  # illegal function


__all__ = ["ModbusSimulator"]
