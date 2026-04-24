import asyncio
import time

import pytest

from claude_team import discovery


@pytest.mark.asyncio
async def test_two_peers_find_each_other() -> None:
    port = 37500
    a = discovery.Discovery(
        cluster_id="cl", node_id="A", hostname="hA", pid=1,
        nats_url="nats://a", port=port,
        announce_interval_s=0.25,
    )
    b = discovery.Discovery(
        cluster_id="cl", node_id="B", hostname="hB", pid=2,
        nats_url="nats://b", port=port,
        announce_interval_s=0.25,
    )
    await a.start()
    await b.start()
    try:
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if "B" in a.peers and "A" in b.peers:
                break
            await asyncio.sleep(0.1)
        assert "B" in a.peers
        assert "A" in b.peers
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_cluster_id_filter() -> None:
    port = 37501
    a = discovery.Discovery(
        cluster_id="clX", node_id="A", hostname="h", pid=1,
        nats_url="nats://a", port=port,
        announce_interval_s=0.25,
    )
    b = discovery.Discovery(
        cluster_id="clY", node_id="B", hostname="h", pid=2,
        nats_url="nats://b", port=port,
        announce_interval_s=0.25,
    )
    await a.start()
    await b.start()
    try:
        await asyncio.sleep(1.0)
        assert a.peers == {}
        assert b.peers == {}
    finally:
        await a.stop()
        await b.stop()


@pytest.mark.asyncio
async def test_self_echo_ignored() -> None:
    port = 37502
    a = discovery.Discovery(
        cluster_id="cl", node_id="A", hostname="h", pid=1,
        nats_url="nats://a", port=port,
        announce_interval_s=0.25,
    )
    await a.start()
    try:
        await asyncio.sleep(1.0)
        assert a.peers == {}
    finally:
        await a.stop()
