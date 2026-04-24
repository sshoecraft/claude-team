"""MCP server entry point.

Runs over stdio (the transport Claude Code uses). In addition to the MCP
stdio loop, this process also listens on a per-project Unix socket so
the CLI shim (invoked by Claude Code hooks) can issue commands directly
to the running session.

Tools exposed to Claude:

- ``peers`` — list active peers for this cluster
- ``claim`` — take a lock on one or more paths, blocking until granted
- ``release`` — drop a lock
- ``status`` — what I hold, cluster checkpoint, peer count
- ``recent_changes`` — pull diffs from the overlay (filtered)
- ``checkpoint`` — advance the checkpoint to current filesystem state
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from . import dlm as dlm_mod
from . import local_ipc
from . import node as node_mod
from . import overlay as overlay_mod


log = logging.getLogger(__name__)


MCP_INSTRUCTIONS = """\
claude-team coordinates file edits across multiple Claude Code instances \
working on the same project.

Before editing a file, call `claim` with the file path. If another peer \
holds it exclusively, the call blocks until released (default 120s) and \
then returns. On timeout the call returns a structured error naming the \
current holder — surface that to the user and ask whether to wait, \
force, or skip.

After an edit completes, call `release` so the peer can take its turn \
and so the change propagates via the overlay.

Use `peers` to see who else is connected, `status` for your own state, \
and `recent_changes` to see what others have edited.
"""


def build_mcp(node: node_mod.Node) -> FastMCP:
    mcp = FastMCP(name="claude-team", instructions=MCP_INSTRUCTIONS)

    @mcp.tool()
    async def peers() -> dict:
        """List active peers for this cluster."""
        return {
            "cluster_id": node.identity.cluster_id,
            "me": {
                "node_id": node.identity.node_id,
                "hostname": node.identity.hostname,
                "pid": node.identity.pid,
                "root": str(node.root),
            },
            "peers": [
                {
                    "node_id": p.node_id,
                    "hostname": p.hostname,
                    "pid": p.pid,
                    "addr": p.addr,
                    "nats_url": p.nats_url,
                    "first_seen": p.first_seen,
                    "last_seen": p.last_seen,
                }
                for p in node.peers()
            ],
        }

    @mcp.tool()
    async def claim(
        paths: list[str],
        mode: str = "exclusive",
        timeout_ms: int | None = None,
    ) -> dict:
        """Block-claim one or more paths.

        `mode` is "exclusive" or "shared". `timeout_ms` defaults to the
        configured claim timeout. Returns `granted: true` on success;
        on timeout returns `granted: false, timed_out: true` with the
        current holders named so the caller can ask the user what to do.
        """
        if node.dlm is None or node.overlay is None:
            return {"granted": False, "error": "node not started"}
        lock_mode = dlm_mod.LockMode.EXCLUSIVE if mode.lower().startswith("ex") else dlm_mod.LockMode.SHARED
        effective_timeout = timeout_ms if timeout_ms is not None else node.cfg.claim_timeout_ms
        results = []
        for path in paths:
            snap = node.overlay.snapshot(path) if lock_mode == dlm_mod.LockMode.EXCLUSIVE else None
            result = await node.dlm.claim(path, lock_mode, effective_timeout)
            results.append({
                "path": path,
                "granted": result.granted,
                "timed_out": result.timed_out,
                "elapsed_ms": result.elapsed_ms,
                "holders": [
                    {"node": h.node, "mode": h.mode, "hostname": h.hostname, "pid": h.pid}
                    for h in result.current_holders
                ] if not result.granted else [],
            })
            if result.granted:
                await node.overlay.publish_claim(path, lock_mode.value)
            elif snap is not None:
                node.overlay.drop_snapshot(path)
        granted_all = all(r["granted"] for r in results)
        return {"granted": granted_all, "results": results}

    @mcp.tool()
    async def release(paths: list[str]) -> dict:
        """Release one or more held paths; publishes the diff on EX releases."""
        if node.dlm is None or node.overlay is None:
            return {"released": False, "error": "node not started"}
        results = []
        for path in paths:
            env = await node.overlay.publish_on_release(path)
            ok = await node.dlm.release(path)
            await node.overlay.publish_release(path)
            results.append({
                "path": path,
                "released": ok,
                "event": env.type if env is not None else None,
            })
        return {"released": all(r["released"] for r in results), "results": results}

    @mcp.tool()
    async def status() -> dict:
        """Return self, cluster, and checkpoint state."""
        cp = node.checkpoint_meta
        return {
            "cluster_id": node.identity.cluster_id,
            "node_id": node.identity.node_id,
            "root": str(node.root),
            "peers": len(node.peers()),
            "checkpoint": {
                "number": cp.checkpoint_number if cp else None,
                "id": cp.checkpoint_id if cp else None,
                "created_by": cp.created_by if cp else None,
                "message": cp.message if cp else None,
            },
            "shared_filesystem": node.cfg.shared_filesystem,
            "nats_url": node.cfg.nats_url,
        }

    @mcp.tool()
    async def recent_changes(
        path: str | None = None,
        limit: int = 50,
    ) -> dict:
        """Fetch recent overlay events (diffs, creates, deletes, renames).

        If `path` is provided, returns only events affecting that path.
        """
        if node.resources is None:
            return {"events": [], "error": "node not started"}

        js = node.resources.js
        stream = node.resources.events_stream_name
        try:
            info = await js.stream_info(stream)
            last_seq = info.state.last_seq
        except Exception as exc:
            return {"events": [], "error": f"stream info failed: {exc}"}

        events: list[dict[str, Any]] = []
        # Walk backwards from the most recent message up to `limit` entries.
        seq = last_seq
        while seq > 0 and len(events) < limit:
            try:
                msg = await js.get_msg(stream, seq=seq)
            except Exception:
                break
            try:
                env = overlay_mod.EventEnvelope.from_bytes(msg.data)
            except Exception:
                seq -= 1
                continue
            if env.type in {"checkpoint", "claim", "release"}:
                seq -= 1
                continue
            if path is not None and env.path != path:
                seq -= 1
                continue
            events.append({
                "seq": seq,
                "type": env.type,
                "path": env.path,
                "sender": env.sender,
                **env.payload,
            })
            seq -= 1
        return {"events": events}

    @mcp.tool()
    async def force_claim(paths: list[str], reason: str = "") -> dict:
        """Preempt the current holder on one or more paths and take the lock.

        Publishes a `claim` event tagged `forced=true` with the preempted
        holders listed, so peers see the audit trail. Intended for stuck
        holders or explicit override; should be surfaced to the user
        before invoking.
        """
        if node.dlm is None:
            return {"granted": False, "error": "node not started"}
        results = []
        for path in paths:
            r = await node.force_claim(path, reason)
            r["path"] = path
            results.append(r)
        return {"granted": all(r["granted"] for r in results), "results": results}

    @mcp.tool()
    async def verify() -> dict:
        """Rehash local tracked files and report drift against the expected
        state (baseline manifest + overlay events).

        Returns a list of paths that differ, are missing locally, or
        exist locally but aren't tracked. Useful for diagnosing
        out-of-coordination edits (e.g., editor used outside Claude).
        """
        if node.overlay is None:
            return {"error": "node not started"}
        return node.verify()

    @mcp.tool()
    async def resync(path: str) -> dict:
        """Overwrite a local file with the cluster's authoritative content.

        Fetches the expected content by hash from the object store and
        writes it to `path`. Use when `verify` reports drift for a file
        you want to reset to the cluster's view.
        """
        if node.overlay is None:
            return {"error": "node not started"}
        return await node.resync(path)

    @mcp.tool()
    async def checkpoint(message: str = "") -> dict:
        """Advance the cluster checkpoint to current filesystem state.

        Overwrites `.claude-team/checkpoint.json` and `manifest.json`,
        purges the overlay stream, and republishes a fresh CHECKPOINT
        record. Remember to commit the updated files to your VCS so
        peers receive them.
        """
        if node.resources is None:
            return {"ok": False, "error": "node not started"}
        meta = await node.take_checkpoint(message=message)
        return {
            "ok": True,
            "checkpoint_number": meta.checkpoint_number,
            "checkpoint_id": meta.checkpoint_id,
            "created_at": meta.created_at,
            "message": meta.message,
            "reminder": "Commit .claude-team/checkpoint.json and .claude-team/manifest.json to share this checkpoint with peers.",
        }

    return mcp


def build_ipc_handler(node: node_mod.Node) -> local_ipc.Handler:
    async def handle(req: local_ipc.Request) -> local_ipc.Response:
        cmd = req.command
        args = req.args or {}

        if cmd == "claim":
            if node.dlm is None or node.overlay is None:
                return local_ipc.Response(ok=False, data={}, error="node not started")
            path = args["path"]
            mode_str = str(args.get("mode", "exclusive")).lower()
            mode = dlm_mod.LockMode.EXCLUSIVE if mode_str.startswith("ex") else dlm_mod.LockMode.SHARED
            timeout_ms = int(args.get("timeout_ms", node.cfg.claim_timeout_ms))
            snap = node.overlay.snapshot(path) if mode == dlm_mod.LockMode.EXCLUSIVE else None
            result = await node.dlm.claim(path, mode, timeout_ms)
            if result.granted:
                await node.overlay.publish_claim(path, mode.value)
            elif snap is not None:
                node.overlay.drop_snapshot(path)
            return local_ipc.Response(
                ok=result.granted,
                data={
                    "granted": result.granted,
                    "timed_out": result.timed_out,
                    "elapsed_ms": result.elapsed_ms,
                    "holders": [
                        {"node": h.node, "mode": h.mode, "hostname": h.hostname, "pid": h.pid}
                        for h in result.current_holders
                    ],
                },
                error=None if result.granted else (
                    f"timed out waiting for {path}" if result.timed_out else "not granted"
                ),
            )

        if cmd == "release":
            if node.dlm is None or node.overlay is None:
                return local_ipc.Response(ok=False, data={}, error="node not started")
            path = args["path"]
            env = await node.overlay.publish_on_release(path)
            ok = await node.dlm.release(path)
            await node.overlay.publish_release(path)
            return local_ipc.Response(
                ok=ok,
                data={"released": ok, "event": env.type if env is not None else None},
            )

        if cmd == "status":
            cp = node.checkpoint_meta
            return local_ipc.Response(
                ok=True,
                data={
                    "cluster_id": node.identity.cluster_id,
                    "node_id": node.identity.node_id,
                    "root": str(node.root),
                    "peers": len(node.peers()),
                    "checkpoint_number": cp.checkpoint_number if cp else None,
                    "checkpoint_id": cp.checkpoint_id if cp else None,
                },
            )

        if cmd == "peers":
            return local_ipc.Response(
                ok=True,
                data={"peers": [
                    {
                        "node_id": p.node_id,
                        "hostname": p.hostname,
                        "pid": p.pid,
                        "addr": p.addr,
                    } for p in node.peers()
                ]},
            )

        if cmd == "force_claim":
            result = await node.force_claim(args["path"], args.get("reason", ""))
            return local_ipc.Response(ok=result["granted"], data=result)

        if cmd == "verify":
            return local_ipc.Response(ok=True, data=node.verify())

        if cmd == "resync":
            result = await node.resync(args["path"])
            return local_ipc.Response(ok=result["ok"], data=result)

        return local_ipc.Response(ok=False, data={}, error=f"unknown command: {cmd}")

    return handle


async def run(
    cwd: Path | None = None,
    accept_cluster: bool = False,
    new_cluster: bool = False,
    bump_cluster: bool = False,
) -> None:
    node = node_mod.bootstrap_from_cwd(cwd)
    try:
        await node.start(
            accept_cluster=accept_cluster,
            new_cluster=new_cluster,
            bump_cluster=bump_cluster,
        )
    except node_mod.StartupDivergence as exc:
        print(f"claude-team startup: {exc}", file=sys.stderr)
        sys.exit(2)
    except Exception as exc:
        print(f"claude-team startup failed: {exc}", file=sys.stderr)
        raise

    ipc_path = local_ipc.socket_path_for(node.root)
    ipc = local_ipc.Server(ipc_path, build_ipc_handler(node))
    await ipc.start()

    mcp = build_mcp(node)
    try:
        await mcp.run_stdio_async()
    finally:
        await ipc.stop()
        await node.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="claude-team MCP server")
    parser.add_argument("--cwd", type=Path, default=None, help="project root override")
    parser.add_argument("--accept-cluster", action="store_true")
    parser.add_argument("--new-cluster", action="store_true")
    parser.add_argument("--bump-cluster", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    asyncio.run(run(
        cwd=args.cwd,
        accept_cluster=args.accept_cluster,
        new_cluster=args.new_cluster,
        bump_cluster=args.bump_cluster,
    ))


if __name__ == "__main__":
    main()
