"""End-to-end integration tests against a live NATS server.

These tests are skipped unless ``CLAUDE_TEAM_NATS_URL`` points at a
reachable nats-server with JetStream enabled. Run them locally with:

    nats-server -js &
    CLAUDE_TEAM_NATS_URL=nats://localhost:4222 pytest tests/test_integration_nats.py
"""
from __future__ import annotations

import asyncio
import os
import secrets
import time
from pathlib import Path

import pytest

from claude_team import checkpoint, cluster, config, dlm, manifest, nats_client, node, overlay
from claude_team.dlm import LockMode


NATS_URL = os.environ.get("CLAUDE_TEAM_NATS_URL")
pytestmark = pytest.mark.skipif(NATS_URL is None, reason="CLAUDE_TEAM_NATS_URL not set")


def fresh_cluster_id() -> str:
    return secrets.token_hex(4)


@pytest.mark.asyncio
async def test_claim_and_release(tmp_path: Path) -> None:
    cluster_id = fresh_cluster_id()
    res = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeA")
    try:
        d = dlm.DLM(res.locks, "nodeA", 1, "hostA")
        result = await d.claim("src/x.py", LockMode.EXCLUSIVE, timeout_ms=1_000)
        assert result.granted

        state = await d.held_by("src/x.py")
        assert state is not None
        assert any(h.node == "nodeA" for h in state.holders)

        assert await d.release("src/x.py")
        assert await d.held_by("src/x.py") is None
    finally:
        await nats_client.close(res)


@pytest.mark.asyncio
async def test_exclusive_blocks_then_succeeds() -> None:
    cluster_id = fresh_cluster_id()
    a = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeA")
    b = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeB")
    try:
        d_a = dlm.DLM(a.locks, "nodeA", 1, "hA")
        d_b = dlm.DLM(b.locks, "nodeB", 2, "hB")

        assert (await d_a.claim("p.py", LockMode.EXCLUSIVE, 500)).granted

        async def release_after(delay: float) -> None:
            await asyncio.sleep(delay)
            await d_a.release("p.py")

        release_task = asyncio.create_task(release_after(0.4))
        start = time.monotonic()
        result = await d_b.claim("p.py", LockMode.EXCLUSIVE, timeout_ms=5_000)
        elapsed = time.monotonic() - start
        await release_task

        assert result.granted
        assert 0.3 < elapsed < 3.0, f"B should have woken around A's release; elapsed={elapsed}"
    finally:
        await nats_client.close(a)
        await nats_client.close(b)


@pytest.mark.asyncio
async def test_shared_locks_coexist() -> None:
    cluster_id = fresh_cluster_id()
    a = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeA")
    b = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeB")
    try:
        d_a = dlm.DLM(a.locks, "nodeA", 1, "hA")
        d_b = dlm.DLM(b.locks, "nodeB", 2, "hB")

        assert (await d_a.claim("x", LockMode.SHARED, 500)).granted
        assert (await d_b.claim("x", LockMode.SHARED, 500)).granted

        state = await d_a.held_by("x")
        assert state is not None
        assert len(state.holders) == 2

        # An EX claim from a third node should time out.
        c = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeC")
        try:
            d_c = dlm.DLM(c.locks, "nodeC", 3, "hC")
            result = await d_c.claim("x", LockMode.EXCLUSIVE, timeout_ms=500)
            assert not result.granted
            assert result.timed_out
        finally:
            await nats_client.close(c)
    finally:
        await nats_client.close(a)
        await nats_client.close(b)


@pytest.mark.asyncio
async def test_force_claim_preempts_holder() -> None:
    cluster_id = fresh_cluster_id()
    a = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeA")
    b = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeB")
    try:
        d_a = dlm.DLM(a.locks, "nodeA", 1, "hA")
        d_b = dlm.DLM(b.locks, "nodeB", 2, "hB")

        assert (await d_a.claim("stuck.py", LockMode.EXCLUSIVE, 500)).granted

        # B preempts.
        result, prior = await d_b.force_claim("stuck.py", LockMode.EXCLUSIVE)
        assert result.granted
        assert any(h.node == "nodeA" for h in prior)

        state = await d_b.held_by("stuck.py")
        assert state is not None
        assert [h.node for h in state.holders] == ["nodeB"]
    finally:
        await nats_client.close(a)
        await nats_client.close(b)


@pytest.mark.asyncio
async def test_resync_fetches_from_object_store(tmp_path: Path) -> None:
    cluster_id = fresh_cluster_id()
    res = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeA")
    try:
        root = tmp_path / "p"
        root.mkdir()
        original = b"genuine content line 1\ngenuine content line 2\n"
        (root / "foo.txt").write_bytes(original)
        ov = overlay.Overlay(res, root, "nodeA", max_diff_size=1_048_576)

        # Simulate prior publish: put content in object store and record hash.
        expected_sha = overlay.sha256_bytes(original)
        await res.objects.put(expected_sha, original)
        ov.record_hash("foo.txt", expected_sha)

        # Corrupt the local file.
        (root / "foo.txt").write_bytes(b"CORRUPTED\n")

        # Simulate resync by calling fetch_object + writing.
        content = await ov.fetch_object(expected_sha)
        assert content == original
        (root / "foo.txt").write_bytes(content)
        assert (root / "foo.txt").read_bytes() == original
    finally:
        await nats_client.close(res)


@pytest.mark.asyncio
async def test_diff_publish_and_apply(tmp_path: Path) -> None:
    cluster_id = fresh_cluster_id()
    # Two separate filesystems for the two peers.
    root_a = tmp_path / "peer_a"
    root_b = tmp_path / "peer_b"
    root_a.mkdir()
    root_b.mkdir()
    (root_a / "file.txt").write_text("line1\nline2\nline3\n")
    (root_b / "file.txt").write_text("line1\nline2\nline3\n")

    a = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeA")
    b = await nats_client.bootstrap(NATS_URL, cluster_id, "nodeB")
    try:
        ov_a = overlay.Overlay(a, root_a, "nodeA", max_diff_size=1_048_576)
        ov_b = overlay.Overlay(b, root_b, "nodeB", max_diff_size=1_048_576)

        # A snapshots, edits, publishes.
        ov_a.snapshot("file.txt")
        (root_a / "file.txt").write_text("line1\nline2 CHANGED\nline3\nline4\n")
        env = await ov_a.publish_on_release("file.txt")
        assert env is not None
        assert env.type == overlay.EventType.DIFF.value

        # B applies the same event.
        applied, reason = await ov_b.apply_event(env, apply_files=True)
        assert applied, f"apply failed: {reason}"
        assert (root_b / "file.txt").read_text() == "line1\nline2 CHANGED\nline3\nline4\n"
    finally:
        await nats_client.close(a)
        await nats_client.close(b)
