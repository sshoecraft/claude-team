"""File manifest computation.

A manifest is a mapping ``{relative_path: sha256_hex}`` of all tracked
files under the project root. "Tracked" means the file does not match any
pattern in ``{root}/.claude-team/ignore`` or the built-in `DEFAULT_IGNORE`.

The manifest hash (used as the ``checkpoint_id`` component) is sha256
over the JSON-serialized, key-sorted manifest.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


DEFAULT_IGNORE: tuple[str, ...] = (
    ".git",
    ".hg",
    ".svn",
    ".claude-team",
    "__pycache__",
    "node_modules",
    ".venv",
    "venv",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "*.pyc",
    "*.pyo",
    "*.so",
    "*.o",
    "*.a",
    "*.class",
    "dist",
    "build",
    "target",
    "*.egg-info",
    ".DS_Store",
    ".idea",
    ".vscode",
)


@dataclass(frozen=True)
class Manifest:
    root: Path
    entries: dict[str, str]

    @property
    def hash(self) -> str:
        serialized = json.dumps(self.entries, sort_keys=True).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()

    def to_json(self) -> str:
        return json.dumps(self.entries, sort_keys=True, indent=2)


def load_ignore(root: Path) -> tuple[str, ...]:
    """Combine built-in defaults with any patterns from `.claude-team/ignore`."""
    patterns = list(DEFAULT_IGNORE)
    ignore_file = root / ".claude-team" / "ignore"
    if ignore_file.is_file():
        for line in ignore_file.read_text().splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                patterns.append(stripped)
    return tuple(patterns)


def is_ignored(rel_path: str, patterns: tuple[str, ...]) -> bool:
    """Return True if `rel_path` matches any ignore pattern.

    Matching runs against both the full relative path (for patterns like
    ``docs/*.tmp``) and each individual path component (so ``node_modules``
    excludes anything beneath it).
    """
    parts = rel_path.split("/")
    for pattern in patterns:
        if fnmatch.fnmatch(rel_path, pattern):
            return True
        for part in parts:
            if fnmatch.fnmatch(part, pattern):
                return True
    return False


def sha256_file(path: Path, chunk_size: int = 65536) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def compute(root: Path, patterns: tuple[str, ...] | None = None) -> Manifest:
    """Walk `root`, hash every non-ignored regular file, return a Manifest.

    Symlinks are skipped (their targets may be outside the tree or
    irrelevant to replication).
    """
    root = root.resolve()
    active_patterns = patterns if patterns is not None else load_ignore(root)
    entries: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if is_ignored(rel, active_patterns):
            continue
        entries[rel] = sha256_file(path)
    return Manifest(root=root, entries=entries)


def diff_manifests(
    local: Manifest,
    remote: Manifest,
) -> dict[str, tuple[str | None, str | None]]:
    """Return `{path: (local_hash, remote_hash)}` for every differing path.

    Either side may be `None` when the path is missing on that side.
    """
    all_paths = set(local.entries) | set(remote.entries)
    result: dict[str, tuple[str | None, str | None]] = {}
    for path in sorted(all_paths):
        left = local.entries.get(path)
        right = remote.entries.get(path)
        if left != right:
            result[path] = (left, right)
    return result
