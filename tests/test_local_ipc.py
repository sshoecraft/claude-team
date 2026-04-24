import asyncio
import os
import tempfile
from pathlib import Path

import pytest

from claude_team import local_ipc


@pytest.mark.asyncio
async def test_call_roundtrip() -> None:
    # AF_UNIX paths are limited to ~104 chars on macOS; pytest's tmp_path is
    # longer than that. Using /tmp directly keeps us under the limit.
    sock_path = Path(tempfile.mkdtemp(prefix="ct-ipc-", dir="/tmp")) / "t.sock"

    async def handler(req: local_ipc.Request) -> local_ipc.Response:
        if req.command == "ping":
            return local_ipc.Response(ok=True, data={"echo": req.args.get("msg", "")})
        return local_ipc.Response(ok=False, data={}, error="unknown")

    server = local_ipc.Server(sock_path, handler)
    await server.start()
    try:
        resp = await local_ipc.call(sock_path, "ping", msg="hi")
        assert resp.ok
        assert resp.data == {"echo": "hi"}

        bad = await local_ipc.call(sock_path, "nope")
        assert not bad.ok
        assert bad.error == "unknown"
    finally:
        await server.stop()
