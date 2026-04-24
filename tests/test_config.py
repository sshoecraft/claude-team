import json
import os
from pathlib import Path
from unittest import mock

from claude_team import config


def test_defaults() -> None:
    with mock.patch.dict(os.environ, {}, clear=True):
        cfg = config.load(project_root=None)
    assert cfg.nats_url == config.DEFAULT_NATS_URL
    assert cfg.discovery_port == config.DEFAULT_DISCOVERY_PORT


def test_env_overrides(tmp_path: Path) -> None:
    with mock.patch.dict(
        os.environ,
        {
            "CLAUDE_TEAM_NATS_URL": "nats://other:4222",
            "CLAUDE_TEAM_DISCOVERY_PORT": "9999",
            "CLAUDE_TEAM_SHARED": "true",
        },
        clear=True,
    ):
        cfg = config.load(project_root=tmp_path)
    assert cfg.nats_url == "nats://other:4222"
    assert cfg.discovery_port == 9999
    assert cfg.shared_filesystem is True


def test_project_file_overrides_user_file(tmp_path: Path) -> None:
    project_cfg = tmp_path / ".claude-team"
    project_cfg.mkdir()
    (project_cfg / "config.json").write_text(json.dumps({"nats_url": "nats://project:4222"}))

    # Point user-home at an empty dir for isolation.
    with mock.patch.dict(os.environ, {"HOME": str(tmp_path / "fake-home")}, clear=True):
        (tmp_path / "fake-home").mkdir(exist_ok=True)
        cfg = config.load(project_root=tmp_path)
    assert cfg.nats_url == "nats://project:4222"


def test_explicit_overrides_win(tmp_path: Path) -> None:
    with mock.patch.dict(
        os.environ, {"CLAUDE_TEAM_NATS_URL": "nats://env:4222"}, clear=True,
    ):
        cfg = config.load(project_root=tmp_path, nats_url="nats://explicit:4222")
    assert cfg.nats_url == "nats://explicit:4222"
