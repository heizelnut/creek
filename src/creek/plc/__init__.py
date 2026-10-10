"""PLC connectivity: drivers, tag map, and polling (see ``PLC.md``)."""

from creek.plc.driver import PlcDriver
from creek.plc.fake import FakeDriver
from creek.plc.poller import Poller, apply_scale
from creek.plc.tags import Connection, PlcConfig, PlcConfigError, Tag

__all__ = [
    "Connection",
    "FakeDriver",
    "PlcConfig",
    "PlcConfigError",
    "PlcDriver",
    "Poller",
    "Tag",
    "apply_scale",
]
