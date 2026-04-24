"""Project root detection by walking up from cwd.

This module does NOT read git contents. `.git/` is listed as one of several
convenience markers for locating the project root, but only its presence —
never its contents — is inspected.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path


log = logging.getLogger(__name__)

# Priority order. The first marker found walking up wins.
MARKERS: tuple[str, ...] = (
    ".claude-team",
    "pyproject.toml",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "Makefile",
    ".git",
)


@dataclass(frozen=True)
class ProjectRoot:
    path: Path
    marker: str | None


def find(start: Path | None = None) -> ProjectRoot:
    """Walk up from start (default: cwd) and return the first directory
    containing one of MARKERS.

    If nothing matches before reaching the filesystem root, returns the
    starting directory with marker=None and logs a warning.
    """
    start_path = (start or Path.cwd()).resolve()
    current = start_path
    while True:
        for marker in MARKERS:
            if (current / marker).exists():
                return ProjectRoot(path=current, marker=marker)
        parent = current.parent
        if parent == current:
            break
        current = parent
    log.warning("No project root marker found walking up from %s; using cwd", start_path)
    return ProjectRoot(path=start_path, marker=None)
