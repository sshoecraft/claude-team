"""Command-line shim used by Claude Code hooks and for manual operation.

Hook mode: invoked by PreToolUse/PostToolUse with a JSON payload on
stdin describing the tool call. The shim extracts the path, talks to
the running MCP server over its Unix socket, and exits with an
appropriate code (0 = proceed, 2 = block the tool call).

Manual mode: subcommands like ``claude-team claim foo.py``,
``claude-team status``, ``claude-team peers`` — useful for debugging
and direct operation without a hook.

If no MCP server is running for the current project (no socket), the
hook mode exits 0 silently so that missing-coordinator never blocks
edits. Manual mode prints a clear error.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from . import local_ipc
from . import project_root as project_root_mod


def load_hook_payload() -> dict | None:
    """Parse a hook JSON payload from stdin. Returns None if stdin is a TTY."""
    if sys.stdin.isatty():
        return None
    data = sys.stdin.read()
    if not data.strip():
        return None
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return None


def extract_paths_from_tool_input(tool_name: str, tool_input: dict) -> list[str]:
    """Map Claude Code tool_input shapes to the files they would modify."""
    if tool_name in {"Edit", "Write", "NotebookEdit"}:
        p = tool_input.get("file_path")
        return [p] if p else []
    if tool_name == "MultiEdit":
        p = tool_input.get("file_path")
        return [p] if p else []
    return []


HOOK_CLAIM_TIMEOUT_MS = 5_000


def format_block_message(path: str, data: dict) -> str:
    holders = data.get("holders") or []
    if holders:
        h = holders[0]
        holder_desc = f"{h.get('node')} on {h.get('hostname')} (pid {h.get('pid')}, mode {h.get('mode')})"
    else:
        holder_desc = "another peer"
    return (
        f"claude-team: {path} is held by {holder_desc}. "
        f"Their edit is still in progress after a {HOOK_CLAIM_TIMEOUT_MS // 1000}s grace.\n"
        "Surface this to the user. Options:\n"
        f"  - Wait longer: call mcp__claude-team__claim with paths=['{path}'] and timeout_ms=30000\n"
        "  - Check peers: call mcp__claude-team__peers to see who is connected\n"
        f"  - Preempt: call mcp__claude-team__force_claim with paths=['{path}'] and a reason\n"
        "  - Skip: don't edit this file for now\n"
    )


async def hook_pretooluse(payload: dict, socket_path: Path) -> int:
    tool_name = payload.get("tool_name", "")
    tool_input = payload.get("tool_input", {}) or {}
    paths = extract_paths_from_tool_input(tool_name, tool_input)
    if not paths:
        return 0

    if not socket_path.exists():
        return 0  # No coordinator; don't block edits.

    for path in paths:
        rel = to_relative(path)
        try:
            resp = await local_ipc.call(
                socket_path, "claim",
                path=rel, mode="exclusive", timeout_ms=HOOK_CLAIM_TIMEOUT_MS,
            )
        except (FileNotFoundError, ConnectionRefusedError):
            return 0
        if not resp.ok:
            sys.stderr.write(format_block_message(rel, resp.data))
            return 2
    return 0


async def hook_posttooluse(payload: dict, socket_path: Path) -> int:
    tool_name = payload.get("tool_name", "")
    tool_input = payload.get("tool_input", {}) or {}
    paths = extract_paths_from_tool_input(tool_name, tool_input)
    if not paths or not socket_path.exists():
        return 0

    for path in paths:
        rel = to_relative(path)
        try:
            await local_ipc.call(socket_path, "release", path=rel)
        except (FileNotFoundError, ConnectionRefusedError):
            pass
    return 0


def to_relative(path: str) -> str:
    """Normalize a path (possibly absolute) to a project-root-relative form."""
    root = project_root_mod.find().path
    p = Path(path).resolve()
    try:
        return p.relative_to(root).as_posix()
    except ValueError:
        return p.as_posix()


async def cmd_claim(args: argparse.Namespace) -> int:
    root = project_root_mod.find().path
    socket_path = local_ipc.socket_path_for(root)
    rel = to_relative(args.path)
    try:
        resp = await local_ipc.call(
            socket_path, "claim",
            path=rel,
            mode=args.mode,
            timeout_ms=args.timeout_ms,
        )
    except (FileNotFoundError, ConnectionRefusedError):
        print(f"No claude-team server running for {root}", file=sys.stderr)
        return 1
    print(json.dumps(resp.data, indent=2))
    return 0 if resp.ok else 1


async def cmd_release(args: argparse.Namespace) -> int:
    root = project_root_mod.find().path
    socket_path = local_ipc.socket_path_for(root)
    rel = to_relative(args.path)
    try:
        resp = await local_ipc.call(socket_path, "release", path=rel)
    except (FileNotFoundError, ConnectionRefusedError):
        print(f"No claude-team server running for {root}", file=sys.stderr)
        return 1
    print(json.dumps(resp.data, indent=2))
    return 0 if resp.ok else 1


async def cmd_status(args: argparse.Namespace) -> int:
    root = project_root_mod.find().path
    socket_path = local_ipc.socket_path_for(root)
    try:
        resp = await local_ipc.call(socket_path, "status")
    except (FileNotFoundError, ConnectionRefusedError):
        print(f"No claude-team server running for {root}", file=sys.stderr)
        return 1
    print(json.dumps(resp.data, indent=2))
    return 0 if resp.ok else 1


async def cmd_peers(args: argparse.Namespace) -> int:
    root = project_root_mod.find().path
    socket_path = local_ipc.socket_path_for(root)
    try:
        resp = await local_ipc.call(socket_path, "peers")
    except (FileNotFoundError, ConnectionRefusedError):
        print(f"No claude-team server running for {root}", file=sys.stderr)
        return 1
    print(json.dumps(resp.data, indent=2))
    return 0 if resp.ok else 1


async def cmd_force_claim(args: argparse.Namespace) -> int:
    root = project_root_mod.find().path
    socket_path = local_ipc.socket_path_for(root)
    rel = to_relative(args.path)
    try:
        resp = await local_ipc.call(
            socket_path, "force_claim", path=rel, reason=args.reason,
        )
    except (FileNotFoundError, ConnectionRefusedError):
        print(f"No claude-team server running for {root}", file=sys.stderr)
        return 1
    print(json.dumps(resp.data, indent=2))
    return 0 if resp.ok else 1


async def cmd_verify(args: argparse.Namespace) -> int:
    root = project_root_mod.find().path
    socket_path = local_ipc.socket_path_for(root)
    try:
        resp = await local_ipc.call(socket_path, "verify")
    except (FileNotFoundError, ConnectionRefusedError):
        print(f"No claude-team server running for {root}", file=sys.stderr)
        return 1
    print(json.dumps(resp.data, indent=2))
    return 0 if resp.ok else 1


async def cmd_resync(args: argparse.Namespace) -> int:
    root = project_root_mod.find().path
    socket_path = local_ipc.socket_path_for(root)
    rel = to_relative(args.path)
    try:
        resp = await local_ipc.call(socket_path, "resync", path=rel)
    except (FileNotFoundError, ConnectionRefusedError):
        print(f"No claude-team server running for {root}", file=sys.stderr)
        return 1
    print(json.dumps(resp.data, indent=2))
    return 0 if resp.ok else 1


async def cmd_hook(args: argparse.Namespace) -> int:
    payload = load_hook_payload()
    if payload is None:
        print("hook mode requires a JSON payload on stdin", file=sys.stderr)
        return 1
    root = project_root_mod.find().path
    socket_path = local_ipc.socket_path_for(root)
    if args.phase == "pre":
        return await hook_pretooluse(payload, socket_path)
    return await hook_posttooluse(payload, socket_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="claude-team")
    sub = parser.add_subparsers(dest="command", required=True)

    p_claim = sub.add_parser("claim", help="claim a path")
    p_claim.add_argument("path")
    p_claim.add_argument("--mode", default="exclusive", choices=["exclusive", "shared"])
    p_claim.add_argument("--timeout-ms", type=int, default=120_000)

    p_release = sub.add_parser("release", help="release a path")
    p_release.add_argument("path")

    sub.add_parser("status", help="show cluster/checkpoint status")
    sub.add_parser("peers", help="list active peers")

    p_force = sub.add_parser("force-claim", help="preempt a held lock")
    p_force.add_argument("path")
    p_force.add_argument("--reason", default="manual override")

    sub.add_parser("verify", help="rehash local files and report drift")

    p_resync = sub.add_parser("resync", help="overwrite a local file with cluster's authoritative content")
    p_resync.add_argument("path")

    p_hook = sub.add_parser("hook", help="invoked by Claude Code hooks (reads stdin JSON)")
    p_hook.add_argument("phase", choices=["pre", "post"])

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    handlers = {
        "claim": cmd_claim,
        "release": cmd_release,
        "status": cmd_status,
        "peers": cmd_peers,
        "force-claim": cmd_force_claim,
        "verify": cmd_verify,
        "resync": cmd_resync,
        "hook": cmd_hook,
    }
    handler = handlers[args.command]
    sys.exit(asyncio.run(handler(args)))


if __name__ == "__main__":
    main()
