"""Checkpoint file handling.

A checkpoint is a labeled snapshot of the project's tracked-file state.
It lives in two plain JSON files under ``.claude-team/`` at the project
root:

- ``checkpoint.json`` — metadata: cluster id, checkpoint number,
  checkpoint id (hash of manifest), creator, timestamp, optional message.
- ``manifest.json`` — the full ``{relative_path: sha256}`` manifest.

Users commit these files to git (or otherwise distribute them) so
checkpoints travel to other developer machines. Claude-team itself does
not touch git.

A new checkpoint is taken explicitly via the ``checkpoint`` MCP tool: the
current filesystem is re-manifested, the checkpoint number is bumped,
the files on disk are overwritten, and the JetStream overlay is purged
and restarted with a fresh ``CHECKPOINT`` record at seq 0.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from nats.js import JetStreamContext

from . import manifest as manifest_mod
from . import nats_client


log = logging.getLogger(__name__)

CHECKPOINT_FILE = "checkpoint.json"
MANIFEST_FILE = "manifest.json"


@dataclass(frozen=True)
class CheckpointMeta:
    cluster_id: str
    checkpoint_number: int
    checkpoint_id: str
    created_at: float
    created_by: str
    message: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, indent=2)

    @classmethod
    def from_json(cls, text: str) -> "CheckpointMeta":
        obj = json.loads(text)
        return cls(
            cluster_id=obj["cluster_id"],
            checkpoint_number=obj["checkpoint_number"],
            checkpoint_id=obj["checkpoint_id"],
            created_at=obj["created_at"],
            created_by=obj["created_by"],
            message=obj.get("message", ""),
        )


def checkpoint_dir(root: Path) -> Path:
    return root / ".claude-team"


def read(root: Path) -> tuple[CheckpointMeta, manifest_mod.Manifest] | None:
    """Load the checkpoint + manifest from disk. Returns None if absent."""
    base = checkpoint_dir(root)
    cp_path = base / CHECKPOINT_FILE
    mf_path = base / MANIFEST_FILE
    if not cp_path.is_file() or not mf_path.is_file():
        return None

    meta = CheckpointMeta.from_json(cp_path.read_text())
    entries: dict[str, str] = json.loads(mf_path.read_text())
    mf = manifest_mod.Manifest(root=root.resolve(), entries=entries)

    if mf.hash != meta.checkpoint_id:
        log.warning(
            "checkpoint.json and manifest.json disagree: meta id=%s manifest hash=%s",
            meta.checkpoint_id, mf.hash,
        )
    return meta, mf


def write(root: Path, meta: CheckpointMeta, mf: manifest_mod.Manifest) -> None:
    base = checkpoint_dir(root)
    base.mkdir(parents=True, exist_ok=True)
    (base / CHECKPOINT_FILE).write_text(meta.to_json() + "\n")
    (base / MANIFEST_FILE).write_text(
        json.dumps(mf.entries, sort_keys=True, indent=2) + "\n"
    )


def take(
    root: Path,
    cluster_id: str,
    created_by: str,
    previous: CheckpointMeta | None,
    message: str = "",
) -> tuple[CheckpointMeta, manifest_mod.Manifest]:
    """Compute a new manifest from the filesystem and build a new meta.

    Caller is responsible for writing to disk (``write``) and rotating
    the JetStream overlay (``rotate_stream``).
    """
    mf = manifest_mod.compute(root)
    number = 1 if previous is None else previous.checkpoint_number + 1
    meta = CheckpointMeta(
        cluster_id=cluster_id,
        checkpoint_number=number,
        checkpoint_id=mf.hash,
        created_at=time.time(),
        created_by=created_by,
        message=message,
    )
    return meta, mf


@dataclass
class VerifyResult:
    matches: bool
    missing_local: list[str]
    extra_local: list[str]
    differing: list[str]

    @property
    def all_divergent_paths(self) -> list[str]:
        return sorted(set(self.missing_local) | set(self.extra_local) | set(self.differing))


def verify_against(local: manifest_mod.Manifest, remote: manifest_mod.Manifest) -> VerifyResult:
    """Compare two manifests and categorize divergences."""
    missing_local = [p for p in remote.entries if p not in local.entries]
    extra_local = [p for p in local.entries if p not in remote.entries]
    differing = [
        p for p in local.entries
        if p in remote.entries and local.entries[p] != remote.entries[p]
    ]
    matches = not (missing_local or extra_local or differing)
    return VerifyResult(
        matches=matches,
        missing_local=sorted(missing_local),
        extra_local=sorted(extra_local),
        differing=sorted(differing),
    )


def checkpoint_record_subject(cluster_id: str) -> str:
    return f"{nats_client.subject_prefix(cluster_id)}.checkpoint"


async def publish_checkpoint_record(
    js: JetStreamContext,
    cluster_id: str,
    meta: CheckpointMeta,
    object_store_bucket: str,
) -> None:
    """Publish the CHECKPOINT record as the first message in the stream."""
    payload = {
        "type": "checkpoint",
        "cluster_id": meta.cluster_id,
        "checkpoint_number": meta.checkpoint_number,
        "checkpoint_id": meta.checkpoint_id,
        "created_at": meta.created_at,
        "created_by": meta.created_by,
        "message": meta.message,
        "object_store_bucket": object_store_bucket,
    }
    await js.publish(
        checkpoint_record_subject(cluster_id),
        nats_client.encode_json(payload),
    )


async def rotate_stream(
    js: JetStreamContext,
    stream_name: str,
    cluster_id: str,
    meta: CheckpointMeta,
    object_store_bucket: str,
) -> None:
    """Purge the overlay stream and republish a fresh CHECKPOINT record.

    Called when a new checkpoint is taken, establishing a clean slate.
    """
    try:
        await js.purge_stream(stream_name)
    except Exception:
        log.exception("purge_stream(%s) failed", stream_name)
    await publish_checkpoint_record(js, cluster_id, meta, object_store_bucket)
    log.info(
        "stream %s rotated to checkpoint #%d (%s)",
        stream_name, meta.checkpoint_number, meta.checkpoint_id[:16],
    )


async def read_checkpoint_record(
    js: JetStreamContext,
    stream_name: str,
    cluster_id: str,
) -> dict[str, Any] | None:
    """Read the CHECKPOINT record (seq 0) from the overlay stream.

    Returns the record payload or ``None`` if the stream is empty.
    """
    try:
        msg = await js.get_msg(stream_name, seq=1)
    except Exception:
        return None
    try:
        return nats_client.decode_json(msg.data)
    except Exception:
        log.exception("malformed checkpoint record at seq 1")
        return None
