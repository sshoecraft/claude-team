"""Configuration loading for claude-team.

Precedence, highest first:

1. Values passed explicitly to `load()`.
2. Environment variables (``CLAUDE_TEAM_*``).
3. Project-level ``{project_root}/.claude-team/config.json``.
4. User-level ``~/.claude-team/config.json``.
5. Built-in defaults below.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any


log = logging.getLogger(__name__)


DEFAULT_NATS_URL = "nats://localhost:4222"
DEFAULT_DISCOVERY_PORT = 7500
DEFAULT_CLAIM_TIMEOUT_MS = 30_000
DEFAULT_MAX_DIFF_SIZE = 1_048_576  # 1 MB


@dataclass
class Config:
    nats_url: str = DEFAULT_NATS_URL
    discovery_port: int = DEFAULT_DISCOVERY_PORT
    claim_timeout_ms: int = DEFAULT_CLAIM_TIMEOUT_MS
    max_diff_size: int = DEFAULT_MAX_DIFF_SIZE
    cluster_name_override: str | None = None
    shared_filesystem: bool = False
    log_level: str = "INFO"


_INT_FIELDS = {"discovery_port", "claim_timeout_ms", "max_diff_size"}
_BOOL_FIELDS = {"shared_filesystem"}
_ENV_MAP = {
    "CLAUDE_TEAM_NATS_URL": "nats_url",
    "CLAUDE_TEAM_DISCOVERY_PORT": "discovery_port",
    "CLAUDE_TEAM_CLAIM_TIMEOUT_MS": "claim_timeout_ms",
    "CLAUDE_TEAM_MAX_DIFF_SIZE": "max_diff_size",
    "CLAUDE_TEAM_CLUSTER": "cluster_name_override",
    "CLAUDE_TEAM_SHARED": "shared_filesystem",
    "CLAUDE_TEAM_LOG_LEVEL": "log_level",
}


def _coerce(field_name: str, raw: Any) -> Any:
    if field_name in _INT_FIELDS:
        return int(raw)
    if field_name in _BOOL_FIELDS:
        if isinstance(raw, bool):
            return raw
        return str(raw).lower() in {"1", "true", "yes", "on"}
    return raw


def _load_file(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        log.warning("Config file %s is not valid JSON: %s", path, exc)
        return {}
    except OSError as exc:
        log.warning("Could not read config file %s: %s", path, exc)
        return {}


def load(project_root: Path | None = None, **overrides: Any) -> Config:
    """Assemble a `Config` from files, environment, and explicit overrides."""
    known = {f.name for f in fields(Config)}
    data: dict[str, Any] = {}

    user_file = Path.home() / ".claude-team" / "config.json"
    for key, value in _load_file(user_file).items():
        if key in known:
            data[key] = _coerce(key, value)

    if project_root is not None:
        project_file = project_root / ".claude-team" / "config.json"
        for key, value in _load_file(project_file).items():
            if key in known:
                data[key] = _coerce(key, value)

    for env_key, field_name in _ENV_MAP.items():
        if env_key in os.environ:
            data[field_name] = _coerce(field_name, os.environ[env_key])

    for key, value in overrides.items():
        if key in known:
            data[key] = _coerce(key, value)

    return Config(**data)
