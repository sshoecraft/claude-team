"""Cluster and node identity.

Cluster ID: sha1 of the project root basename (first 16 hex chars = 64
bits of entropy, plenty for collision avoidance across developer
machines). May be overridden via config.

Node ID: per-process identifier, stable for the life of the Claude Code
session: {hostname}-{pid}-{rand4}.
"""
from __future__ import annotations

import hashlib
import os
import secrets
import socket
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Identity:
    cluster_id: str
    node_id: str
    hostname: str
    pid: int
    project_root: Path


def cluster_id_from_root(root: Path, override: str | None = None) -> str:
    """Return the cluster ID for a project root.

    Source is either the override (if supplied) or the basename of the
    resolved path. Result is sha1(source)[:16] — 16 hex chars, case-
    insensitive, stable across hosts when the basename matches.
    """
    source = override if override is not None else root.resolve().name
    return hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]


def make_node_id() -> str:
    """Generate a unique node ID for this process.

    Format: `{shorthost}-{pid}-{hex4}`. The random suffix disambiguates
    in case PIDs recycle within a single host lifetime.
    """
    shorthost = socket.gethostname().split(".", 1)[0]
    return f"{shorthost}-{os.getpid()}-{secrets.token_hex(2)}"


def make_identity(root: Path, cluster_override: str | None = None) -> Identity:
    """Bundle identity for a Claude Code session in one object."""
    return Identity(
        cluster_id=cluster_id_from_root(root, cluster_override),
        node_id=make_node_id(),
        hostname=socket.gethostname(),
        pid=os.getpid(),
        project_root=root.resolve(),
    )
