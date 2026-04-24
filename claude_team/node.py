"""Composition root — wires identity, NATS, discovery, DLM, and overlay
into one object the MCP server and CLI shim both drive.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import checkpoint as checkpoint_mod
from . import cluster as cluster_mod
from . import config as config_mod
from . import dlm as dlm_mod
from . import discovery as discovery_mod
from . import manifest as manifest_mod
from . import nats_client
from . import overlay as overlay_mod
from . import project_root as project_root_mod
from . import watcher as watcher_mod


log = logging.getLogger(__name__)


class StartupError(Exception):
    pass


@dataclass
class StartupDivergence(StartupError):
    """Raised when local state does not match the cluster checkpoint."""
    reason: str
    cluster_checkpoint_number: int | None = None
    local_checkpoint_number: int | None = None
    divergent_paths: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        return self.reason


@dataclass
class PeerInfo:
    node_id: str
    hostname: str
    pid: int
    nats_url: str
    addr: str
    first_seen: float
    last_seen: float


class Node:
    """Represents one Claude Code session's participation in a cluster."""

    def __init__(
        self,
        cfg: config_mod.Config,
        root: Path,
        identity: cluster_mod.Identity,
    ) -> None:
        self.cfg = cfg
        self.root = root
        self.identity = identity
        self.resources: nats_client.NatsResources | None = None
        self.discovery: discovery_mod.Discovery | None = None
        self.dlm: dlm_mod.DLM | None = None
        self.overlay: overlay_mod.Overlay | None = None
        self.watcher: watcher_mod.FsWatcher | None = None
        self.checkpoint_meta: checkpoint_mod.CheckpointMeta | None = None
        self.started_at: float | None = None
        self.consumer_task: asyncio.Task | None = None
        self.running: bool = False

    async def start(
        self,
        accept_cluster: bool = False,
        new_cluster: bool = False,
        bump_cluster: bool = False,
    ) -> None:
        """Connect, reconcile, and join the cluster.

        Raises ``StartupDivergence`` if the local state does not match the
        cluster's checkpoint and no escape hatch flag is set.
        """
        self.resources = await nats_client.bootstrap(
            self.cfg.nats_url, self.identity.cluster_id, self.identity.node_id,
        )
        self.dlm = dlm_mod.DLM(
            self.resources.locks,
            self.identity.node_id,
            self.identity.pid,
            self.identity.hostname,
        )
        self.overlay = overlay_mod.Overlay(
            self.resources, self.root, self.identity.node_id, self.cfg.max_diff_size,
        )

        await self.reconcile_checkpoint(
            accept_cluster=accept_cluster,
            new_cluster=new_cluster,
            bump_cluster=bump_cluster,
        )

        # Seed the overlay's dedup cache with the current manifest so the
        # watcher doesn't misinterpret pre-existing files as brand-new
        # changes when it boots.
        live_mf = manifest_mod.compute(self.root)
        self.overlay.seed_hashes(live_mf.entries)

        ignore_patterns = manifest_mod.load_ignore(self.root)
        self.watcher = watcher_mod.FsWatcher(
            overlay=self.overlay,
            root=self.root,
            ignore_patterns=ignore_patterns,
        )
        await self.watcher.start()

        self.discovery = discovery_mod.Discovery(
            cluster_id=self.identity.cluster_id,
            node_id=self.identity.node_id,
            hostname=self.identity.hostname,
            pid=self.identity.pid,
            nats_url=self.cfg.nats_url,
            port=self.cfg.discovery_port,
        )
        self.discovery.on_peer_event(self.on_peer_event)
        await self.discovery.start()

        self.running = True
        self.consumer_task = asyncio.create_task(self.consume_events())
        self.started_at = time.time()
        log.info(
            "node started: cluster=%s node=%s root=%s",
            self.identity.cluster_id, self.identity.node_id, self.root,
        )

    async def stop(self) -> None:
        self.running = False
        if self.consumer_task is not None:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
            self.consumer_task = None
        if self.discovery is not None:
            await self.discovery.stop()
        if self.watcher is not None:
            await self.watcher.stop()
        if self.resources is not None:
            await nats_client.close(self.resources)
        log.info("node stopped")

    async def consume_events(self) -> None:
        """Subscribe to the overlay stream and apply events to the filesystem.

        Runs for the life of the node. Self-sent events are filtered out
        by ``overlay.apply_event``. In SHARED cluster mode, events are
        recorded but not applied to disk (peers share a filesystem).
        """
        assert self.resources is not None
        assert self.overlay is not None
        apply_files = not self.cfg.shared_filesystem
        sub = await self.resources.js.subscribe(
            subject=nats_client.event_subjects(self.identity.cluster_id),
            stream=self.resources.events_stream_name,
            ordered_consumer=True,
        )
        try:
            while self.running:
                try:
                    msg = await asyncio.wait_for(sub.next_msg(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    log.exception("event fetch error")
                    continue
                # The CHECKPOINT record at seq 1 has its own schema and is
                # consumed during startup via reconcile_checkpoint; skip it
                # here rather than failing to parse as an EventEnvelope.
                if msg.subject.endswith(".checkpoint"):
                    continue
                try:
                    env = overlay_mod.EventEnvelope.from_bytes(msg.data)
                except Exception:
                    log.exception("malformed event on %s; skipping", msg.subject)
                    continue
                applied, reason = await self.overlay.apply_event(env, apply_files)
                if applied:
                    log.info(
                        "applied %s for %s from %s (%s)",
                        env.type, env.path, env.sender, reason,
                    )
                elif reason not in {"self-event", "metadata-only", "already-applied"}:
                    log.debug(
                        "skipped %s for %s: %s",
                        env.type, env.path, reason,
                    )
        except asyncio.CancelledError:
            raise
        finally:
            try:
                await sub.unsubscribe()
            except Exception:
                pass

    async def reconcile_checkpoint(
        self,
        accept_cluster: bool,
        new_cluster: bool,
        bump_cluster: bool,
    ) -> None:
        """Compare local checkpoint+manifest with cluster's; act accordingly."""
        assert self.resources is not None
        local = checkpoint_mod.read(self.root)
        cluster_record = await checkpoint_mod.read_checkpoint_record(
            self.resources.js,
            self.resources.events_stream_name,
            self.identity.cluster_id,
        )

        if cluster_record is None:
            # Empty stream → we are (or become) the founder.
            await self.found_new_cluster(local)
            return

        cluster_num = cluster_record["checkpoint_number"]
        cluster_id = cluster_record["checkpoint_id"]

        if local is None:
            # No local checkpoint file, but our filesystem might already
            # match what the cluster has. If so, backfill the checkpoint
            # locally and join — equivalent to the user having received
            # the checkpoint file alongside the files themselves.
            live_mf = manifest_mod.compute(self.root)
            if live_mf.hash == cluster_id:
                adopted = checkpoint_mod.CheckpointMeta(
                    cluster_id=self.identity.cluster_id,
                    checkpoint_number=cluster_num,
                    checkpoint_id=cluster_id,
                    created_at=cluster_record.get("created_at", time.time()),
                    created_by=cluster_record.get("created_by", "unknown"),
                    message=cluster_record.get("message", ""),
                )
                checkpoint_mod.write(self.root, adopted, live_mf)
                self.checkpoint_meta = adopted
                log.info(
                    "adopted cluster checkpoint #%d (%s) — local files matched",
                    cluster_num, cluster_id[:16],
                )
                return
            raise StartupDivergence(
                reason=(
                    f"Cluster is at checkpoint #{cluster_num} but no local "
                    "`.claude-team/checkpoint.json` exists and the local "
                    "filesystem does not match the cluster checkpoint either. "
                    "Fetch the checkpoint from another peer (e.g., `git pull`) "
                    "or start a new cluster with --new-cluster."
                ),
                cluster_checkpoint_number=cluster_num,
                local_checkpoint_number=None,
            )

        local_meta, local_mf = local

        if new_cluster:
            log.warning(
                "--new-cluster specified; purging existing cluster state and taking checkpoint",
            )
            await self.rotate_to_local_checkpoint(local_meta, local_mf)
            return

        if local_meta.checkpoint_number > cluster_num:
            if not bump_cluster:
                raise StartupDivergence(
                    reason=(
                        f"Local checkpoint #{local_meta.checkpoint_number} is "
                        f"newer than cluster checkpoint #{cluster_num}. "
                        "Re-run with --bump-cluster to publish yours as the new "
                        "cluster state."
                    ),
                    cluster_checkpoint_number=cluster_num,
                    local_checkpoint_number=local_meta.checkpoint_number,
                )
            await self.rotate_to_local_checkpoint(local_meta, local_mf)
            return

        if local_meta.checkpoint_number < cluster_num:
            raise StartupDivergence(
                reason=(
                    f"Cluster is at checkpoint #{cluster_num}, you have "
                    f"checkpoint #{local_meta.checkpoint_number}. Fetch the "
                    "newer checkpoint (e.g., `git pull`) and retry."
                ),
                cluster_checkpoint_number=cluster_num,
                local_checkpoint_number=local_meta.checkpoint_number,
            )

        # Same checkpoint number.
        if local_meta.checkpoint_id != cluster_id:
            raise StartupDivergence(
                reason=(
                    f"Cluster and local are both at checkpoint "
                    f"#{local_meta.checkpoint_number}, but their checkpoint IDs "
                    f"differ ({cluster_id[:16]} vs {local_meta.checkpoint_id[:16]}). "
                    "Someone bumped the checkpoint without syncing. Reconcile "
                    "the checkpoint files first."
                ),
                cluster_checkpoint_number=local_meta.checkpoint_number,
                local_checkpoint_number=local_meta.checkpoint_number,
            )

        # Checkpoint matches; verify the local filesystem still matches the manifest.
        live_mf = manifest_mod.compute(self.root)
        verify = checkpoint_mod.verify_against(live_mf, local_mf)
        if not verify.matches:
            if accept_cluster:
                log.warning(
                    "--accept-cluster specified; local files will be overwritten on replay",
                )
            else:
                raise StartupDivergence(
                    reason=(
                        f"Local files diverge from the checkpoint manifest. "
                        f"Divergent paths: {verify.all_divergent_paths[:10]}"
                        f"{'...' if len(verify.all_divergent_paths) > 10 else ''}. "
                        "Re-run with --accept-cluster (destructive) or "
                        "--new-cluster, or reconcile manually."
                    ),
                    cluster_checkpoint_number=local_meta.checkpoint_number,
                    local_checkpoint_number=local_meta.checkpoint_number,
                    divergent_paths=verify.all_divergent_paths,
                )

        self.checkpoint_meta = local_meta
        log.info(
            "joined cluster at checkpoint #%d (%s)",
            local_meta.checkpoint_number, local_meta.checkpoint_id[:16],
        )

    async def found_new_cluster(
        self,
        local: tuple[checkpoint_mod.CheckpointMeta, manifest_mod.Manifest] | None,
    ) -> None:
        """We are first in. Establish the cluster from local state."""
        assert self.resources is not None
        if local is None:
            meta, mf = checkpoint_mod.take(
                self.root,
                cluster_id=self.identity.cluster_id,
                created_by=self.identity.node_id,
                previous=None,
                message="auto-founded",
            )
            checkpoint_mod.write(self.root, meta, mf)
            log.info(
                "no local checkpoint; founded cluster with checkpoint #1 (%s)",
                meta.checkpoint_id[:16],
            )
        else:
            meta, _ = local

        await checkpoint_mod.publish_checkpoint_record(
            self.resources.js,
            self.identity.cluster_id,
            meta,
            nats_client.objects_bucket_name(self.identity.cluster_id),
        )
        self.checkpoint_meta = meta
        log.info(
            "founded cluster at checkpoint #%d (%s)",
            meta.checkpoint_number, meta.checkpoint_id[:16],
        )

    async def rotate_to_local_checkpoint(
        self,
        meta: checkpoint_mod.CheckpointMeta,
        mf: manifest_mod.Manifest,
    ) -> None:
        """Overwrite the cluster's checkpoint with local state."""
        assert self.resources is not None
        await checkpoint_mod.rotate_stream(
            self.resources.js,
            self.resources.events_stream_name,
            self.identity.cluster_id,
            meta,
            nats_client.objects_bucket_name(self.identity.cluster_id),
        )
        self.checkpoint_meta = meta

    def verify(self) -> dict:
        """Rehash every tracked file and report drift against the expected
        state (overlay cache = baseline manifest + applied events).

        Returns ``{drift: [{path, local, expected}], missing, extra}``.
        """
        assert self.overlay is not None
        ignore_patterns = manifest_mod.load_ignore(self.root)
        live = manifest_mod.compute(self.root, ignore_patterns)
        expected = self.overlay.last_known_hashes

        drift = []
        for path, local_hash in live.entries.items():
            exp = expected.get(path)
            if exp is None:
                # File is in live but claude-team doesn't know about it.
                drift.append({"path": path, "local": local_hash, "expected": None, "kind": "extra"})
            elif exp != local_hash:
                drift.append({"path": path, "local": local_hash, "expected": exp, "kind": "differs"})
        for path, exp in expected.items():
            if path not in live.entries:
                drift.append({"path": path, "local": None, "expected": exp, "kind": "missing"})

        return {
            "drift": drift,
            "tracked_count": len(live.entries),
            "expected_count": len(expected),
        }

    async def resync(self, rel_path: str) -> dict:
        """Overwrite the local file with the expected authoritative content.

        Pulls by hash from the NATS object store — any peer's most recent
        publish of that content (from CREATE or any DIFF) put it there.
        Returns ``{ok, path, reason, bytes_written}``.
        """
        assert self.overlay is not None
        assert self.resources is not None
        expected = self.overlay.last_known_hashes.get(rel_path)
        if expected is None:
            return {
                "ok": False,
                "path": rel_path,
                "reason": "no expected hash (untracked path)",
                "bytes_written": 0,
            }
        content = await self.overlay.fetch_object(expected)
        if content is None:
            return {
                "ok": False,
                "path": rel_path,
                "reason": f"expected hash {expected[:12]} not in object store",
                "bytes_written": 0,
            }
        full = self.root / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_bytes(content)
        return {
            "ok": True,
            "path": rel_path,
            "reason": "resynced from object store",
            "bytes_written": len(content),
        }

    async def force_claim(
        self, rel_path: str, reason: str,
    ) -> dict:
        """Preempt any current holder and take the lock for ourselves.

        Publishes a ``claim`` event marked ``forced: true`` with the list
        of preempted holders so peers have an audit trail.
        """
        assert self.dlm is not None
        assert self.overlay is not None
        result, prior = await self.dlm.force_claim(
            rel_path, dlm_mod.LockMode.EXCLUSIVE,
        )
        if result.granted:
            env = overlay_mod.EventEnvelope(
                type=overlay_mod.EventType.CLAIM.value,
                path=rel_path,
                sender=self.identity.node_id,
                payload={
                    "mode": dlm_mod.LockMode.EXCLUSIVE.value,
                    "forced": True,
                    "reason": reason,
                    "prior_holders": [
                        {"node": h.node, "mode": h.mode, "hostname": h.hostname, "pid": h.pid}
                        for h in prior
                    ],
                },
            )
            await self.overlay.publish_event(env)
        return {
            "granted": result.granted,
            "elapsed_ms": result.elapsed_ms,
            "preempted": [
                {"node": h.node, "mode": h.mode, "hostname": h.hostname, "pid": h.pid}
                for h in prior
            ],
        }

    async def take_checkpoint(self, message: str = "") -> checkpoint_mod.CheckpointMeta:
        """Advance the checkpoint to current filesystem state."""
        assert self.resources is not None
        meta, mf = checkpoint_mod.take(
            self.root,
            cluster_id=self.identity.cluster_id,
            created_by=self.identity.node_id,
            previous=self.checkpoint_meta,
            message=message,
        )
        checkpoint_mod.write(self.root, meta, mf)
        await checkpoint_mod.rotate_stream(
            self.resources.js,
            self.resources.events_stream_name,
            self.identity.cluster_id,
            meta,
            nats_client.objects_bucket_name(self.identity.cluster_id),
        )
        self.checkpoint_meta = meta
        log.info(
            "checkpoint advanced to #%d (%s)",
            meta.checkpoint_number, meta.checkpoint_id[:16],
        )
        return meta

    def peers(self) -> list[PeerInfo]:
        if self.discovery is None:
            return []
        return [
            PeerInfo(
                node_id=p.announce.node_id,
                hostname=p.announce.hostname,
                pid=p.announce.pid,
                nats_url=p.announce.nats_url,
                addr=p.addr[0],
                first_seen=p.first_seen,
                last_seen=p.last_seen,
            )
            for p in self.discovery.peers.values()
        ]

    async def on_peer_event(self, peer: discovery_mod.Peer, event: str) -> None:
        # Purge orphaned locks when a peer times out.
        if event == "leave" and self.dlm is not None:
            try:
                count = await self.dlm.purge_node(peer.announce.node_id)
                if count:
                    log.info("purged %d stale locks for departed node %s", count, peer.announce.node_id)
            except Exception:
                log.exception("purge on leave failed for %s", peer.announce.node_id)


def bootstrap_from_cwd(
    cwd: Path | None = None,
    cluster_override: str | None = None,
    **cfg_overrides,
) -> Node:
    """Convenience constructor: detect project root, build identity, load config."""
    pr = project_root_mod.find(cwd)
    cfg = config_mod.load(pr.path, **cfg_overrides)
    identity = cluster_mod.make_identity(
        pr.path, cluster_override or cfg.cluster_name_override,
    )
    return Node(cfg=cfg, root=pr.path, identity=identity)
