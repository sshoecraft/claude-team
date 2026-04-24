"""Overlay events: snapshot, diff, publish, apply, replay.

The overlay is the append-only event log (a JetStream stream) describing
every filesystem change since the current checkpoint. Events are keyed
by a hash of the relative path so peers can optionally filter by file.

Event flow on the editing peer:

1. ``snapshot(path)`` is called when a lock is claimed (file read into
   memory for files <= ``max_diff_size``, otherwise hash-only).
2. Claude performs its Edit/Write.
3. ``publish_on_release(path)`` computes the diff against the snapshot
   and publishes a ``DIFF`` / ``CREATE`` / ``DELETE`` event.

Event flow on the receiving peer:

1. Subscribes to ``ct.{cluster}.>`` with ``DeliverAll``.
2. For each event, calls ``apply_event`` which translates the event to
   a filesystem mutation, after checking the pre-hash matches the
   receiver's local state. Mismatches are logged and the file is
   marked out-of-sync (peer can later call ``resync``).

For sync-on-join, ``replay(js, stream)`` walks the full stream from seq 1
and applies every event in order.
"""
from __future__ import annotations

import difflib
import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from . import nats_client


log = logging.getLogger(__name__)


class EventType(str, Enum):
    CHECKPOINT = "checkpoint"
    CLAIM = "claim"
    RELEASE = "release"
    DIFF = "diff"
    CREATE = "create"
    DELETE = "delete"
    RENAME = "rename"


class DiffFormat(str, Enum):
    UNIFIED = "unified"
    REPLACED = "replaced"
    BINARY_INVAL = "binary_inval"


@dataclass
class Snapshot:
    """Pre-edit state captured on claim."""
    path: str
    exists: bool
    sha256: str | None
    content: bytes | None  # None means either !exists or too large


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, chunk: int = 65536) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            data = f.read(chunk)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


def snapshot_path(root: Path, rel: str, max_size: int) -> Snapshot:
    full = root / rel
    if not full.exists():
        return Snapshot(path=rel, exists=False, sha256=None, content=None)
    if full.is_symlink() or not full.is_file():
        return Snapshot(path=rel, exists=False, sha256=None, content=None)
    size = full.stat().st_size
    if size <= max_size:
        content = full.read_bytes()
        return Snapshot(path=rel, exists=True, sha256=sha256_bytes(content), content=content)
    return Snapshot(path=rel, exists=True, sha256=sha256_file(full), content=None)


def looks_binary(data: bytes) -> bool:
    """Cheap heuristic: presence of NUL bytes in the first 8 KB."""
    return b"\x00" in data[:8192]


def compute_diff_payload(
    pre: Snapshot,
    post_content: bytes | None,
    post_sha256: str,
    max_size: int,
) -> tuple[DiffFormat, str]:
    """Build a diff payload and return ``(format, payload_text)``.

    ``payload_text`` is a string (unified diff or literal contents) or
    the empty string for ``binary_inval``.
    """
    if post_content is None or pre.content is None:
        return DiffFormat.BINARY_INVAL, ""
    if looks_binary(pre.content) or looks_binary(post_content):
        return DiffFormat.BINARY_INVAL, ""

    try:
        pre_text = pre.content.decode("utf-8")
        post_text = post_content.decode("utf-8")
    except UnicodeDecodeError:
        return DiffFormat.BINARY_INVAL, ""

    diff_lines = list(difflib.unified_diff(
        pre_text.splitlines(keepends=True),
        post_text.splitlines(keepends=True),
        fromfile=pre.path,
        tofile=pre.path,
    ))
    diff_text = "".join(diff_lines)

    # If the diff is larger than the file itself (or exceeds budget),
    # ship the full post-image instead.
    if len(diff_text.encode("utf-8")) > max_size or len(diff_text) > len(post_text) * 1.5:
        return DiffFormat.REPLACED, post_text

    return DiffFormat.UNIFIED, diff_text


def apply_diff_to_text(pre_text: str, diff_text: str) -> str | None:
    """Apply a unified-diff patch without external tools.

    We can't fall back to a generic ``patch`` utility, so the diff must
    be the one we generated (which always has intact hunks and no fuzz).
    Returns the patched text or ``None`` if application fails.
    """
    lines = diff_text.splitlines(keepends=True)
    pre_lines = pre_text.splitlines(keepends=True)
    out: list[str] = []
    i = 0  # index into pre_lines

    idx = 0
    # Skip file headers
    while idx < len(lines) and not lines[idx].startswith("@@"):
        idx += 1

    while idx < len(lines):
        header = lines[idx]
        if not header.startswith("@@"):
            return None
        # Parse @@ -a,b +c,d @@
        try:
            range_part = header.split("@@")[1].strip()
            minus, plus = range_part.split(" ")
            pre_start_str = minus.split(",")[0].lstrip("-")
            pre_start = int(pre_start_str) - 1 if pre_start_str else 0
        except (IndexError, ValueError):
            return None
        # Copy unchanged lines before this hunk
        while i < pre_start:
            out.append(pre_lines[i])
            i += 1
        idx += 1
        while idx < len(lines) and not lines[idx].startswith("@@"):
            line = lines[idx]
            if line.startswith("+"):
                out.append(line[1:])
            elif line.startswith("-"):
                i += 1
            elif line.startswith(" "):
                if i < len(pre_lines):
                    out.append(pre_lines[i])
                i += 1
            elif line.startswith("\\"):
                pass
            idx += 1

    while i < len(pre_lines):
        out.append(pre_lines[i])
        i += 1

    return "".join(out)


@dataclass
class EventEnvelope:
    """Shape every event shares on the wire."""
    type: str
    path: str
    sender: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_bytes(self) -> bytes:
        return nats_client.encode_json({
            "type": self.type,
            "path": self.path,
            "sender": self.sender,
            **self.payload,
        })

    @classmethod
    def from_bytes(cls, data: bytes) -> "EventEnvelope":
        obj = nats_client.decode_json(data)
        return cls(
            type=obj["type"],
            path=obj["path"],
            sender=obj["sender"],
            payload={k: v for k, v in obj.items() if k not in {"type", "path", "sender"}},
        )


def path_hash(path: str) -> str:
    return hashlib.sha256(path.encode("utf-8")).hexdigest()[:16]


def subject_for(cluster_id: str, event_type: EventType, path: str | None) -> str:
    """Build the publish subject for an event."""
    base = f"{nats_client.subject_prefix(cluster_id)}.{event_type.value}"
    if path is None:
        return base
    return f"{base}.{path_hash(path)}"


class Overlay:
    """Producer + applier + replayer for the overlay event stream."""

    def __init__(
        self,
        resources: nats_client.NatsResources,
        project_root: Path,
        node_id: str,
        max_diff_size: int,
    ) -> None:
        self.resources = resources
        self.root = project_root.resolve()
        self.node_id = node_id
        self.max_diff_size = max_diff_size
        self.snapshots: dict[str, Snapshot] = {}
        # Shared dedup cache: last known sha256 per relative path. Both the
        # hook-driven publish path and the filesystem watcher consult this
        # cache before publishing, so Claude's own edits don't get
        # re-published by the watcher after the hook already sent them.
        self.last_known_hashes: dict[str, str] = {}

    def seed_hashes(self, entries: dict[str, str]) -> None:
        self.last_known_hashes = dict(entries)

    def record_hash(self, rel_path: str, sha: str | None) -> None:
        if sha is None:
            self.last_known_hashes.pop(rel_path, None)
        else:
            self.last_known_hashes[rel_path] = sha

    def cached_hash(self, rel_path: str) -> str | None:
        return self.last_known_hashes.get(rel_path)

    def snapshot(self, rel_path: str) -> Snapshot:
        snap = snapshot_path(self.root, rel_path, self.max_diff_size)
        self.snapshots[rel_path] = snap
        return snap

    def drop_snapshot(self, rel_path: str) -> Snapshot | None:
        return self.snapshots.pop(rel_path, None)

    async def publish_event(self, envelope: EventEnvelope) -> None:
        subject = subject_for(
            self.resources.cluster_id,
            EventType(envelope.type),
            envelope.path or None,
        )
        await self.resources.js.publish(subject, envelope.to_bytes())

    async def publish_on_release(self, rel_path: str) -> EventEnvelope | None:
        """Compute diff against snapshot, publish event, clear snapshot.

        Returns the envelope that was published, or ``None`` if nothing
        changed (no publish).
        """
        snap = self.snapshots.pop(rel_path, None)
        if snap is None:
            return None

        full = self.root / rel_path
        if not full.exists():
            if snap.exists:
                env = EventEnvelope(
                    type=EventType.DELETE.value,
                    path=rel_path,
                    sender=self.node_id,
                    payload={"pre_sha256": snap.sha256},
                )
                await self.publish_event(env)
                self.record_hash(rel_path, None)
                return env
            return None

        post_size = full.stat().st_size
        if post_size <= self.max_diff_size:
            post_content = full.read_bytes()
            post_sha = sha256_bytes(post_content)
        else:
            post_content = None
            post_sha = sha256_file(full)

        if not snap.exists:
            env = await self.publish_create(rel_path, post_sha, post_content)
            self.record_hash(rel_path, post_sha)
            return env

        if snap.sha256 == post_sha:
            return None  # no-op edit

        fmt, payload_text = compute_diff_payload(
            snap, post_content, post_sha, self.max_diff_size,
        )
        env = EventEnvelope(
            type=EventType.DIFF.value,
            path=rel_path,
            sender=self.node_id,
            payload={
                "pre_sha256": snap.sha256,
                "post_sha256": post_sha,
                "format": fmt.value,
                "payload": payload_text,
            },
        )
        await self.publish_event(env)
        self.record_hash(rel_path, post_sha)

        # Stash post-content in the object store keyed by hash so the
        # `resync` tool can recover the authoritative version regardless
        # of diff format. The object store is content-addressable, so
        # identical contents don't duplicate.
        if post_content is not None:
            try:
                await self.resources.objects.put(post_sha, post_content)
            except Exception:
                log.debug("object store put failed for %s", post_sha[:12])
        return env

    async def publish_create(
        self, rel_path: str, sha256: str, content: bytes | None,
    ) -> EventEnvelope:
        if content is not None and len(content) <= self.max_diff_size:
            payload_text = content.decode("utf-8", errors="replace") if not looks_binary(content) else ""
            inline = not looks_binary(content)
        else:
            payload_text = ""
            inline = False

        # Always stash in the object store so resync can recover by hash.
        if content is not None:
            try:
                await self.resources.objects.put(sha256, content)
            except Exception:
                log.debug("object store put failed for %s", sha256[:12])

        env = EventEnvelope(
            type=EventType.CREATE.value,
            path=rel_path,
            sender=self.node_id,
            payload={
                "sha256": sha256,
                "inline": inline,
                "content": payload_text if inline else "",
            },
        )
        await self.publish_event(env)
        return env

    async def publish_claim(self, rel_path: str, mode: str) -> EventEnvelope:
        env = EventEnvelope(
            type=EventType.CLAIM.value,
            path=rel_path,
            sender=self.node_id,
            payload={"mode": mode},
        )
        await self.publish_event(env)
        return env

    async def publish_release(self, rel_path: str) -> EventEnvelope:
        env = EventEnvelope(
            type=EventType.RELEASE.value,
            path=rel_path,
            sender=self.node_id,
            payload={},
        )
        await self.publish_event(env)
        return env

    async def apply_event(
        self, envelope: EventEnvelope, apply_files: bool,
    ) -> tuple[bool, str]:
        """Mutate the local filesystem to match the event.

        Returns ``(applied, reason)``. If ``apply_files`` is False, we
        only track the event for metadata purposes (SHARED cluster mode).
        CLAIM and RELEASE events are always metadata-only.
        """
        if envelope.sender == self.node_id:
            return False, "self-event"

        t = envelope.type
        if t in (EventType.CLAIM.value, EventType.RELEASE.value):
            return False, "metadata-only"

        if not apply_files:
            return False, "shared-fs-no-apply"

        if t == EventType.DIFF.value:
            return await self.apply_diff(envelope)
        if t == EventType.CREATE.value:
            return await self.apply_create(envelope)
        if t == EventType.DELETE.value:
            return await self.apply_delete(envelope)
        if t == EventType.RENAME.value:
            return await self.apply_rename(envelope)
        return False, f"unknown-type:{t}"

    async def apply_diff(self, env: EventEnvelope) -> tuple[bool, str]:
        full = self.root / env.path
        pre_sha = env.payload.get("pre_sha256")
        post_sha = env.payload.get("post_sha256")
        fmt = env.payload.get("format")

        if not full.exists():
            return False, "missing-local"

        local_sha = sha256_file(full)
        if local_sha == post_sha:
            return False, "already-applied"
        if local_sha != pre_sha:
            return False, f"pre-hash-mismatch:{local_sha[:8]}!={pre_sha[:8] if pre_sha else None}"

        if fmt == DiffFormat.REPLACED.value:
            new_text = env.payload.get("payload", "")
            full.write_text(new_text)
            return True, "replaced"

        if fmt == DiffFormat.UNIFIED.value:
            diff_text = env.payload.get("payload", "")
            pre_text = full.read_text()
            patched = apply_diff_to_text(pre_text, diff_text)
            if patched is None:
                return False, "patch-failed"
            full.write_text(patched)
            new_sha = sha256_file(full)
            if new_sha != post_sha:
                return False, f"post-hash-mismatch:{new_sha[:8]}!={post_sha[:8]}"
            return True, "unified-applied"

        if fmt == DiffFormat.BINARY_INVAL.value:
            content = await self.fetch_object(post_sha)
            if content is None:
                return False, "object-store-miss"
            full.write_bytes(content)
            return True, "binary-replaced"

        return False, f"unknown-format:{fmt}"

    async def apply_create(self, env: EventEnvelope) -> tuple[bool, str]:
        full = self.root / env.path
        if full.exists():
            local_sha = sha256_file(full)
            if local_sha == env.payload.get("sha256"):
                return False, "already-exists-match"
            return False, "already-exists-diff"
        full.parent.mkdir(parents=True, exist_ok=True)
        if env.payload.get("inline"):
            full.write_text(env.payload.get("content", ""))
        else:
            content = await self.fetch_object(env.payload.get("sha256"))
            if content is None:
                return False, "object-store-miss"
            full.write_bytes(content)
        return True, "created"

    async def apply_delete(self, env: EventEnvelope) -> tuple[bool, str]:
        full = self.root / env.path
        if not full.exists():
            return False, "already-absent"
        pre_sha = env.payload.get("pre_sha256")
        if pre_sha is not None and sha256_file(full) != pre_sha:
            return False, "pre-hash-mismatch"
        full.unlink()
        return True, "deleted"

    async def apply_rename(self, env: EventEnvelope) -> tuple[bool, str]:
        old = self.root / env.payload.get("old", "")
        new = self.root / env.payload.get("new", "")
        if not old.exists():
            return False, "source-missing"
        if new.exists():
            return False, "dest-exists"
        new.parent.mkdir(parents=True, exist_ok=True)
        old.rename(new)
        return True, "renamed"

    async def fetch_object(self, sha256: str | None) -> bytes | None:
        if sha256 is None:
            return None
        try:
            obj = await self.resources.objects.get(sha256)
            return obj.data
        except Exception:
            log.exception("fetch object %s failed", sha256)
            return None
