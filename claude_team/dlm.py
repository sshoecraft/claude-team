"""Distributed lock manager backed by a NATS KV bucket.

Each lock corresponds to one KV entry keyed by ``lock.<sha256(path)[:16]>``.
The value is a JSON document containing the canonical path and an array
of current holders. Multi-holder arrays let us support SHARED locks
without a second bucket:

    {
      "path": "src/foo.py",
      "holders": [
        {"node": ..., "mode": "EX" | "SHARED",
         "epoch": N, "acquired_at": ts, "pid": int, "hostname": str}
      ]
    }

Concurrency is managed via KV optimistic-CAS (``update(key, value, last=rev)``)
so two peers claiming the same path at once serialize cleanly: one wins,
the other's CAS fails and it retries after observing the new state.

Blocking claims use ``kv.watch(key)``; when a holder releases (or the
state changes) the watcher wakes and the claimant retries.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from nats.js.errors import (
    APIError,
    KeyDeletedError,
    KeyNotFoundError,
    KeyWrongLastSequenceError,
)
from nats.js.kv import KeyValue

from . import nats_client


log = logging.getLogger(__name__)


class LockMode(str, Enum):
    EXCLUSIVE = "EX"
    SHARED = "SHARED"


@dataclass
class Holder:
    node: str
    mode: str
    epoch: int
    acquired_at: float
    pid: int
    hostname: str


@dataclass
class LockState:
    path: str
    holders: list[Holder] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "holders": [asdict(h) for h in self.holders]}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "LockState":
        return cls(
            path=d["path"],
            holders=[Holder(**h) for h in d.get("holders", [])],
        )

    def has_exclusive(self) -> bool:
        return any(h.mode == LockMode.EXCLUSIVE.value for h in self.holders)

    def held_by_other(self, my_node: str) -> list[Holder]:
        return [h for h in self.holders if h.node != my_node]


@dataclass
class ClaimResult:
    granted: bool
    timed_out: bool = False
    current_holders: list[Holder] = field(default_factory=list)
    elapsed_ms: int = 0


def path_key(path: str) -> str:
    """Stable KV key for a given canonical relative path."""
    return "lock." + hashlib.sha256(path.encode("utf-8")).hexdigest()[:16]


def compatible(existing: LockState, my_node: str, my_mode: LockMode) -> bool:
    """Can a holder of ``my_mode`` from ``my_node`` coexist with ``existing``?"""
    others = existing.held_by_other(my_node)
    if my_mode == LockMode.SHARED:
        # Any EXCLUSIVE holder from another node blocks us.
        return not any(h.mode == LockMode.EXCLUSIVE.value for h in others)
    # EXCLUSIVE requires no other holders at all.
    return not others


def serialize(state: LockState) -> bytes:
    return nats_client.encode_json(state.to_dict())


def deserialize(data: bytes) -> LockState:
    return LockState.from_dict(nats_client.decode_json(data))


class DLM:
    """Claim / release operations on top of a KV bucket."""

    def __init__(self, kv: KeyValue, node_id: str, pid: int, hostname: str) -> None:
        self.kv = kv
        self.node_id = node_id
        self.pid = pid
        self.hostname = hostname
        self.epoch_counter = 0

    def next_epoch(self) -> int:
        self.epoch_counter += 1
        return self.epoch_counter

    def me(self, mode: LockMode) -> Holder:
        return Holder(
            node=self.node_id,
            mode=mode.value,
            epoch=self.next_epoch(),
            acquired_at=time.time(),
            pid=self.pid,
            hostname=self.hostname,
        )

    async def try_claim_once(
        self, path: str, mode: LockMode,
    ) -> tuple[bool, LockState]:
        """One CAS attempt. Returns (granted, observed_state)."""
        key = path_key(path)
        try:
            entry = await self.kv.get(key)
        except (KeyNotFoundError, KeyDeletedError):
            entry = None

        if entry is None:
            state = LockState(path=path, holders=[self.me(mode)])
            try:
                await self.kv.create(key, serialize(state))
                return True, state
            except (APIError, KeyWrongLastSequenceError):
                return False, LockState(path=path)

        current = deserialize(entry.value)
        if not compatible(current, self.node_id, mode):
            return False, current

        new_holders = [h for h in current.holders if h.node != self.node_id]
        new_holders.append(self.me(mode))
        new_state = LockState(path=path, holders=new_holders)
        try:
            await self.kv.update(key, serialize(new_state), last=entry.revision)
            return True, new_state
        except (APIError, KeyWrongLastSequenceError):
            return False, current

    async def claim(
        self, path: str, mode: LockMode, timeout_ms: int,
    ) -> ClaimResult:
        """Blocking claim with millisecond timeout."""
        start = time.monotonic()
        deadline = start + (timeout_ms / 1000.0)
        key = path_key(path)
        last_seen: LockState = LockState(path=path)

        while True:
            granted, state = await self.try_claim_once(path, mode)
            if granted:
                return ClaimResult(
                    granted=True,
                    elapsed_ms=int((time.monotonic() - start) * 1000),
                )
            last_seen = state

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return ClaimResult(
                    granted=False,
                    timed_out=True,
                    current_holders=last_seen.holders,
                    elapsed_ms=int((time.monotonic() - start) * 1000),
                )

            await self.wait_for_change(key, remaining)

    async def wait_for_change(self, key: str, timeout_s: float) -> None:
        """Block until the KV key changes or until `timeout_s` elapses."""
        try:
            watcher = await self.kv.watch(key, include_history=False, ignore_deletes=False)
        except Exception:
            log.exception("watch failed for %s; polling fallback", key)
            await asyncio.sleep(min(1.0, timeout_s))
            return

        deadline = time.monotonic() + timeout_s
        # The watcher yields one initial entry (current state) and then real updates.
        # Discard the initial snapshot; wake on the next genuine change.
        got_initial = False
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                try:
                    update = await watcher.updates(timeout=min(1.0, remaining))
                except asyncio.TimeoutError:
                    continue
                if update is None:
                    if got_initial:
                        return
                    got_initial = True
                    continue
                if got_initial:
                    return
                got_initial = True
        finally:
            await watcher.stop()

    async def release(self, path: str) -> bool:
        """Remove self from the lock's holder list, purging if empty."""
        key = path_key(path)
        for _ in range(5):
            try:
                entry = await self.kv.get(key)
            except (KeyNotFoundError, KeyDeletedError):
                return True

            state = deserialize(entry.value)
            new_holders = [h for h in state.holders if h.node != self.node_id]

            if not new_holders:
                try:
                    await self.kv.purge(key)
                    return True
                except (APIError, KeyWrongLastSequenceError):
                    continue

            new_state = LockState(path=path, holders=new_holders)
            try:
                await self.kv.update(key, serialize(new_state), last=entry.revision)
                return True
            except (APIError, KeyWrongLastSequenceError):
                continue

        log.warning("release failed after retries for %s", path)
        return False

    async def held_by(self, path: str) -> LockState | None:
        """Read current lock state for a path, or None if unheld."""
        try:
            entry = await self.kv.get(path_key(path))
        except (KeyNotFoundError, KeyDeletedError):
            return None
        return deserialize(entry.value)

    async def force_claim(
        self, path: str, mode: LockMode,
    ) -> tuple[ClaimResult, list[Holder]]:
        """Forcibly take a lock regardless of current holders.

        Returns ``(result, prior_holders)`` — the prior_holders list is
        intended for an audit log / CLAIM event payload so peers can
        see who got preempted and why.
        """
        key = path_key(path)
        prior: list[Holder] = []
        start = time.monotonic()
        for _ in range(5):
            try:
                entry = await self.kv.get(key)
            except (KeyNotFoundError, KeyDeletedError):
                entry = None

            if entry is None:
                # Nothing to preempt; normal create path.
                state = LockState(path=path, holders=[self.me(mode)])
                try:
                    await self.kv.create(key, serialize(state))
                    return (
                        ClaimResult(
                            granted=True,
                            elapsed_ms=int((time.monotonic() - start) * 1000),
                        ),
                        prior,
                    )
                except (APIError, KeyWrongLastSequenceError):
                    continue

            current = deserialize(entry.value)
            prior = list(current.holders)
            new_state = LockState(path=path, holders=[self.me(mode)])
            try:
                await self.kv.update(key, serialize(new_state), last=entry.revision)
                return (
                    ClaimResult(
                        granted=True,
                        elapsed_ms=int((time.monotonic() - start) * 1000),
                    ),
                    prior,
                )
            except (APIError, KeyWrongLastSequenceError):
                continue

        return (
            ClaimResult(
                granted=False,
                elapsed_ms=int((time.monotonic() - start) * 1000),
            ),
            prior,
        )

    async def purge_node(self, node: str) -> int:
        """Remove all holds by `node` across every lock in the bucket.

        Used on peer timeout to clean up after a crashed Claude Code.
        Returns the number of locks mutated.
        """
        count = 0
        try:
            keys = await self.kv.keys()
        except Exception:
            log.exception("purge_node: listing keys failed")
            return 0

        for key in keys:
            try:
                entry = await self.kv.get(key)
            except (KeyNotFoundError, KeyDeletedError):
                continue
            state = deserialize(entry.value)
            new_holders = [h for h in state.holders if h.node != node]
            if len(new_holders) == len(state.holders):
                continue
            if not new_holders:
                try:
                    await self.kv.purge(key)
                    count += 1
                except (APIError, KeyWrongLastSequenceError):
                    pass
                continue
            new_state = LockState(path=state.path, holders=new_holders)
            try:
                await self.kv.update(key, serialize(new_state), last=entry.revision)
                count += 1
            except (APIError, KeyWrongLastSequenceError):
                pass
        return count
