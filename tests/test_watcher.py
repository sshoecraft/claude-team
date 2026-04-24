"""Lightweight tests for watcher filtering and lifecycle.

End-to-end event propagation is covered by the NATS integration tests.
Here we verify the pure-logic filter and that ``start``/``stop`` are
idempotent and non-leaky (no dangling tasks).
"""
import asyncio
from pathlib import Path
from unittest import mock

import pytest
from watchfiles import Change

from claude_team import manifest, overlay, watcher


def make_watcher(root: Path) -> watcher.FsWatcher:
    ov = mock.MagicMock(spec=overlay.Overlay)
    ov.cached_hash = mock.MagicMock(return_value=None)
    ov.record_hash = mock.MagicMock()
    ov.snapshots = {}
    ov.max_diff_size = 1_000_000
    ov.node_id = "nodeA"
    ov.publish_event = mock.AsyncMock()
    ov.publish_create = mock.AsyncMock()
    ov.resources = mock.MagicMock()
    ov.resources.objects.put = mock.AsyncMock()
    patterns = manifest.load_ignore(root)
    return watcher.FsWatcher(overlay=ov, root=root, ignore_patterns=patterns)


def test_filter_excludes_ignored_paths(tmp_path: Path) -> None:
    (tmp_path / "node_modules").mkdir()
    w = make_watcher(tmp_path)

    allowed_path = str(tmp_path / "src" / "foo.py")
    ignored_path = str(tmp_path / "node_modules" / "junk.js")
    pyc_path = str(tmp_path / "src" / "foo.cpython-311.pyc")

    # These don't need to exist; the filter is pattern-based.
    assert w.filter(Change.modified, allowed_path) is True
    assert w.filter(Change.modified, ignored_path) is False
    assert w.filter(Change.modified, pyc_path) is False


def test_filter_rejects_paths_outside_root(tmp_path: Path) -> None:
    w = make_watcher(tmp_path)
    assert w.filter(Change.modified, "/etc/passwd") is False


@pytest.mark.asyncio
async def test_start_stop_clean(tmp_path: Path) -> None:
    w = make_watcher(tmp_path)
    await w.start()
    # Let the watcher run briefly so awatch() enters its loop.
    await asyncio.sleep(0.2)
    await w.stop()
    assert w.task is None


@pytest.mark.asyncio
async def test_handle_deletion_publishes_delete(tmp_path: Path) -> None:
    w = make_watcher(tmp_path)
    # Simulate a previously-known file.
    w.overlay.cached_hash = mock.MagicMock(return_value="deadbeef" * 8)
    (tmp_path / "gone.py").touch()
    (tmp_path / "gone.py").unlink()

    await w.handle_one("gone.py", Change.deleted)
    w.overlay.publish_event.assert_awaited_once()
    env = w.overlay.publish_event.await_args.args[0]
    assert env.type == overlay.EventType.DELETE.value
    assert env.path == "gone.py"
    w.overlay.record_hash.assert_called_once_with("gone.py", None)


@pytest.mark.asyncio
async def test_handle_new_file_publishes_create(tmp_path: Path) -> None:
    w = make_watcher(tmp_path)
    w.overlay.cached_hash = mock.MagicMock(return_value=None)
    (tmp_path / "new.py").write_text("hello\n")

    await w.handle_one("new.py", Change.added)
    w.overlay.publish_create.assert_awaited_once()
    args = w.overlay.publish_create.await_args.args
    assert args[0] == "new.py"


@pytest.mark.asyncio
async def test_cached_hash_match_skips_publish(tmp_path: Path) -> None:
    w = make_watcher(tmp_path)
    (tmp_path / "x.py").write_text("hello\n")
    expected = overlay.sha256_bytes(b"hello\n")
    w.overlay.cached_hash = mock.MagicMock(return_value=expected)

    await w.handle_one("x.py", Change.modified)
    w.overlay.publish_event.assert_not_awaited()
    w.overlay.publish_create.assert_not_awaited()
