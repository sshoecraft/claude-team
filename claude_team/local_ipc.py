"""Unix-socket IPC between the CLI shim (hook) and the running MCP server.

The MCP server listens on a per-project Unix socket. The CLI shim
(invoked by Claude Code hooks) discovers the same socket path, connects,
sends a line-delimited JSON request, reads one line back, and exits.

Socket path: ``~/.claude-team/runtime/<sha256(cwd)[:16]>.sock``. Both
sides derive it from their cwd, so there is no ambient configuration.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable


log = logging.getLogger(__name__)


def runtime_dir() -> Path:
    base = Path.home() / ".claude-team" / "runtime"
    base.mkdir(parents=True, exist_ok=True)
    return base


def socket_path_for(project_root: Path) -> Path:
    tag = hashlib.sha256(str(project_root.resolve()).encode("utf-8")).hexdigest()[:16]
    return runtime_dir() / f"{tag}.sock"


@dataclass
class Request:
    command: str
    args: dict[str, Any]


@dataclass
class Response:
    ok: bool
    data: dict[str, Any]
    error: str | None = None

    def to_json(self) -> str:
        return json.dumps({"ok": self.ok, "data": self.data, "error": self.error})


Handler = Callable[[Request], Awaitable[Response]]


class Server:
    """Accepts line-delimited JSON requests on a Unix socket."""

    def __init__(self, socket_path: Path, handler: Handler) -> None:
        self.socket_path = socket_path
        self.handler = handler
        self.server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        if self.socket_path.exists():
            try:
                self.socket_path.unlink()
            except OSError as exc:
                raise RuntimeError(
                    f"Could not remove stale socket {self.socket_path}: {exc}"
                ) from exc
        self.server = await asyncio.start_unix_server(
            self.handle_client, path=str(self.socket_path),
        )
        os.chmod(self.socket_path, 0o600)
        log.info("IPC socket listening at %s", self.socket_path)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass
        log.info("IPC socket stopped")

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            try:
                obj = json.loads(line.decode("utf-8"))
                req = Request(command=obj["command"], args=obj.get("args", {}))
            except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as exc:
                resp = Response(ok=False, data={}, error=f"bad request: {exc}")
            else:
                try:
                    resp = await self.handler(req)
                except Exception as exc:
                    log.exception("handler error for %s", req.command)
                    resp = Response(ok=False, data={}, error=str(exc))
            writer.write((resp.to_json() + "\n").encode("utf-8"))
            await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


async def call(socket_path: Path, command: str, **args: Any) -> Response:
    """Client helper: send one request, read one response, return it."""
    reader, writer = await asyncio.open_unix_connection(str(socket_path))
    try:
        payload = json.dumps({"command": command, "args": args}) + "\n"
        writer.write(payload.encode("utf-8"))
        await writer.drain()
        line = await reader.readline()
        if not line:
            return Response(ok=False, data={}, error="empty response")
        obj = json.loads(line.decode("utf-8"))
        return Response(
            ok=bool(obj.get("ok")),
            data=obj.get("data") or {},
            error=obj.get("error"),
        )
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
