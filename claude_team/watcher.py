"""Filesystem watcher.

Runs in the background for the life of the MCP server, watching the
project root via ``watchfiles`` (inotify on Linux, FSEvents on macOS).
When a file changes outside of a claude-team-coordinated edit — for
example, ``sed -i``, a shell redirect, a ``git pull``, or a developer's
editor — the watcher hashes the file and publishes the appropriate
overlay event (``DIFF`` / ``CREATE`` / ``DELETE``) so peers stay in
sync.

Dedup with the hook path: ``Overlay`` keeps a ``last_known_hashes``
cache. When the hook publishes a diff after Claude's Edit/Write, it
records the post-hash. The watcher wakes up on the same file change,
computes the hash, finds it equal to the cached value, and skips
publishing. This prevents duplicate events for Claude's own edits.

Out of scope: the watcher does not take DLM claims. A Bash-driven edit
is still not mutually-exclusive with another peer's concurrent edit —
that's a documented gap. The watcher only handles replication.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Awaitable, Callable

from watchfiles import Change, awatch

from . import manifest as manifest_mod
from . import overlay as overlay_mod


log = logging.getLogger(__name__)


class FsWatcher:
    """Background filesystem watcher that publishes overlay events."""

    def __init__(
        self,
        overlay: overlay_mod.Overlay,
        root: Path,
        ignore_patterns: tuple[str, ...],
    ) -> None:
        self.overlay = overlay
        self.root = root.resolve()
        self.ignore_patterns = ignore_patterns
        self.task: asyncio.Task | None = None
        self.stop_event: asyncio.Event = asyncio.Event()

    def filter(self, change: Change, path_str: str) -> bool:
        try:
            rel = Path(path_str).resolve().relative_to(self.root).as_posix()
        except ValueError:
            return False
        return not manifest_mod.is_ignored(rel, self.ignore_patterns)

    async def start(self) -> None:
        self.stop_event.clear()
        self.task = asyncio.create_task(self.run())
        log.info("filesystem watcher started on %s", self.root)

    async def stop(self) -> None:
        self.stop_event.set()
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        log.info("filesystem watcher stopped")

    async def run(self) -> None:
        try:
            async for changes in awatch(
                str(self.root),
                watch_filter=self.filter,
                stop_event=self.stop_event,
                recursive=True,
                rust_timeout=1_000,
            ):
                await self.handle_batch(changes)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("watcher loop died; restart required")

    async def handle_batch(self, changes: set[tuple[Change, str]]) -> None:
        # Collapse (change, path) → latest-state-per-path. watchfiles
        # already coalesces within a short window, but if a file is
        # created then modified we only need to act on the final state.
        paths: dict[str, Change] = {}
        for change, raw_path in changes:
            try:
                rel = Path(raw_path).resolve().relative_to(self.root).as_posix()
            except ValueError:
                continue
            if manifest_mod.is_ignored(rel, self.ignore_patterns):
                continue
            paths[rel] = change

        for rel, change in paths.items():
            try:
                await self.handle_one(rel, change)
            except Exception:
                log.exception("watcher failed to handle %s", rel)

    async def handle_one(self, rel: str, change: Change) -> None:
        full = self.root / rel
        cached = self.overlay.cached_hash(rel)

        if change == Change.deleted or not full.exists():
            if cached is None:
                return  # Already untracked.
            env = overlay_mod.EventEnvelope(
                type=overlay_mod.EventType.DELETE.value,
                path=rel,
                sender=self.overlay.node_id,
                payload={"pre_sha256": cached},
            )
            await self.overlay.publish_event(env)
            self.overlay.record_hash(rel, None)
            log.debug("watcher: published DELETE for %s", rel)
            return

        if full.is_symlink() or not full.is_file():
            return

        size = full.stat().st_size
        if size <= self.overlay.max_diff_size:
            content = full.read_bytes()
            new_sha = overlay_mod.sha256_bytes(content)
        else:
            content = None
            new_sha = overlay_mod.sha256_file(full)

        if new_sha == cached:
            return  # Matches Claude's hook publish or a benign touch.

        if cached is None:
            # File is new (to us). Publish a CREATE event.
            await self.overlay.publish_create(rel, new_sha, content)
            self.overlay.record_hash(rel, new_sha)
            log.debug("watcher: published CREATE for %s", rel)
            return

        # Existing file with a different hash — treat as a diff driven
        # outside the hook. Build a synthetic pre-snapshot from the
        # previous known contents (if we have them cached in an active
        # snapshot) or fall back to BINARY_INVAL (full replacement).
        pre_snap = self.overlay.snapshots.get(rel)
        if pre_snap is None:
            # No pre-content in memory; ship full new content.
            inline = content is not None and not overlay_mod.looks_binary(content)
            fmt = (
                overlay_mod.DiffFormat.REPLACED
                if inline
                else overlay_mod.DiffFormat.BINARY_INVAL
            )
            payload_text = content.decode("utf-8", errors="replace") if inline else ""
            env = overlay_mod.EventEnvelope(
                type=overlay_mod.EventType.DIFF.value,
                path=rel,
                sender=self.overlay.node_id,
                payload={
                    "pre_sha256": cached,
                    "post_sha256": new_sha,
                    "format": fmt.value,
                    "payload": payload_text,
                },
            )
            await self.overlay.publish_event(env)
            if fmt == overlay_mod.DiffFormat.BINARY_INVAL and content is not None:
                await self.overlay.resources.objects.put(new_sha, content)
        else:
            # We have an in-memory pre-snapshot (typical for the hook
            # path). Compute a proper diff.
            fmt, payload_text = overlay_mod.compute_diff_payload(
                pre_snap, content, new_sha, self.overlay.max_diff_size,
            )
            env = overlay_mod.EventEnvelope(
                type=overlay_mod.EventType.DIFF.value,
                path=rel,
                sender=self.overlay.node_id,
                payload={
                    "pre_sha256": pre_snap.sha256,
                    "post_sha256": new_sha,
                    "format": fmt.value,
                    "payload": payload_text,
                },
            )
            await self.overlay.publish_event(env)
            if fmt == overlay_mod.DiffFormat.BINARY_INVAL and content is not None:
                await self.overlay.resources.objects.put(new_sha, content)

        self.overlay.record_hash(rel, new_sha)
        log.debug("watcher: published DIFF for %s", rel)
