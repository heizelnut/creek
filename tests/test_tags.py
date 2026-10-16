"""Tag map and configuration tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from creek.plc import Connection, PlcConfig, PlcConfigError, Tag


def _write(tmp_path: Path, document: object) -> Path:
    path = tmp_path / "plc.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_config_from_file(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {
            "connections": [
                {
                    "name": "line1",
                    "driver": "modbus",
                    "host": "10.0.0.10",
                    "port": 502,
                    "poll_interval": 0.5,
                    "params": {"unit": 1},
                }
            ],
            "tags": [
                {
                    "key": "speed",
                    "connection": "line1",
                    "address": "HR0",
                    "type": "u16",
                    "scale": 0.1,
                },
                {
                    "key": "run",
                    "connection": "line1",
                    "address": "C0",
                    "type": "bool",
                    "writable": True,
                },
            ],
        },
    )
    config = PlcConfig.from_file(path)
    assert config.connections["line1"].port == 502
    assert config.connections["line1"].params == {"unit": 1}
    assert config.tags[0].scale == 0.1
    assert config.tags[1].writable is True
    assert [tag.key for tag in config.tags_for("line1")] == ["speed", "run"]


def test_config_ignores_legacy_namespace(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {
            "connections": [
                {"name": "line1", "driver": "modbus", "host": "h", "port": 502, "namespace": "x"}
            ],
            "tags": [{"key": "speed", "connection": "line1", "address": "HR0", "type": "u16"}],
        },
    )
    config = PlcConfig.from_file(path)
    assert not hasattr(config.connections["line1"], "namespace")


def test_config_defaults(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {
            "connections": [{"name": "line1", "driver": "modbus"}],
            "tags": [{"key": "speed", "connection": "line1", "address": "HR0"}],
        },
    )
    config = PlcConfig.from_file(path)
    connection = config.connections["line1"]
    assert (connection.host, connection.port, connection.poll_interval) == ("127.0.0.1", 0, 1.0)
    assert config.tags[0].type == "u16"
    assert config.tags[0].writable is False


def test_config_rejects_unknown_connection(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {"connections": [], "tags": [{"key": "x", "connection": "nope", "address": "HR0"}]},
    )
    with pytest.raises(PlcConfigError):
        PlcConfig.from_file(path)


def test_config_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(PlcConfigError):
        PlcConfig.from_file(tmp_path / "missing.json")


def test_config_rejects_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "plc.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(PlcConfigError):
        PlcConfig.from_file(path)


def test_tags_for_filters_by_connection() -> None:
    config = PlcConfig(
        connections={
            "a": Connection("a", "modbus", "h", 502),
            "b": Connection("b", "modbus", "h", 502),
        },
        tags=[Tag("x", "a", "HR0", "u16"), Tag("y", "b", "HR0", "u16")],
    )
    assert [tag.key for tag in config.tags_for("a")] == ["x"]
