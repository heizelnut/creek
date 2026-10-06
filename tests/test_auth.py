"""Auth file loading and ACL tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from creek.auth import AuthConfigError, Authenticator, Identity


def _write(tmp_path: Path, data: object) -> Path:
    path = tmp_path / "auth.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_load_file(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        {
            "allow_anonymous": False,
            "clients": [
                {"id": "sensor-a", "namespace": "sensors", "token": "t1", "acl": ["read"]},
                {"id": "admin", "token": "t2", "acl": ["all"]},
            ],
        },
    )
    auth = Authenticator.from_file(path)
    assert auth.allow_anonymous is False
    sensor = auth.authenticate("t1")
    assert sensor == Identity("sensor-a", "sensors", frozenset({"read"}))
    assert sensor is not None and sensor.allows("read") and not sensor.allows("admin")
    assert auth.authenticate("nope") is None


def test_load_file_defaults_namespace_and_acl(tmp_path: Path) -> None:
    auth = Authenticator.from_file(_write(tmp_path, {"clients": [{"token": "t"}]}))
    identity = auth.authenticate("t")
    assert identity is not None
    assert identity.namespace == "default"
    assert identity.allows("admin")


def test_load_file_rejects_missing_client_token(tmp_path: Path) -> None:
    with pytest.raises(AuthConfigError):
        Authenticator.from_file(_write(tmp_path, {"clients": [{"id": "x"}]}))


def test_load_file_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(AuthConfigError):
        Authenticator.from_file(tmp_path / "missing.json")
