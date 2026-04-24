"""Thin wrapper around the NATS client for claude-team.

Centralizes connection setup and the three JetStream primitives we rely
on: a KV bucket (lock table), an event stream (overlay), and an object-
store bucket (file contents). Other modules (``dlm``, ``overlay``,
``checkpoint``) consume this wrapper instead of talking to ``nats-py``
directly.

Subject and bucket names are derived from ``cluster_id`` via the helpers
at the bottom of this module so that every other module uses the same
strings.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import nats
from nats.aio.client import Client
from nats.js import JetStreamContext
from nats.js.api import (
    DiscardPolicy,
    KeyValueConfig,
    ObjectStoreConfig,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import BucketNotFoundError, NotFoundError
from nats.js.kv import KeyValue
from nats.js.object_store import ObjectStore


log = logging.getLogger(__name__)


def locks_bucket_name(cluster_id: str) -> str:
    return f"claude-team-{cluster_id}-locks"


def events_stream_name(cluster_id: str) -> str:
    return f"claude-team-{cluster_id}-events"


def objects_bucket_name(cluster_id: str) -> str:
    return f"claude-team-{cluster_id}-files"


def subject_prefix(cluster_id: str) -> str:
    return f"ct.{cluster_id}"


def event_subjects(cluster_id: str) -> str:
    """Wildcard covering every event type for one cluster."""
    return f"{subject_prefix(cluster_id)}.>"


def bast_subject(cluster_id: str, holder_node: str) -> str:
    return f"{subject_prefix(cluster_id)}.bast.{holder_node}"


@dataclass
class NatsResources:
    nc: Client
    js: JetStreamContext
    locks: KeyValue
    events_stream_name: str
    objects: ObjectStore
    cluster_id: str


async def connect(nats_url: str, name: str) -> Client:
    """Open a NATS connection, failing fast if the server is unreachable.

    nats-py retries the initial connect ``max_reconnect_attempts`` times
    by default; we want startup to error immediately so the operator
    gets a useful message. We wrap the call in our own short timeout and
    let the caller surface the failure.
    """
    try:
        nc = await asyncio.wait_for(
            nats.connect(
                nats_url,
                name=name,
                connect_timeout=2,
                max_reconnect_attempts=0,
                allow_reconnect=True,
                reconnect_time_wait=2,
            ),
            timeout=4.0,
        )
    except asyncio.TimeoutError:
        raise ConnectionError(f"timed out connecting to NATS at {nats_url}") from None
    log.info("connected to NATS at %s as %s", nats_url, name)
    return nc


async def ensure_kv(js: JetStreamContext, bucket: str) -> KeyValue:
    try:
        return await js.key_value(bucket)
    except (BucketNotFoundError, NotFoundError):
        log.info("creating KV bucket %s", bucket)
        return await js.create_key_value(
            bucket=bucket,
            history=1,
            storage=StorageType.FILE,
            description="claude-team lock table",
        )


async def ensure_stream(js: JetStreamContext, stream: str, subjects: list[str]) -> None:
    try:
        await js.stream_info(stream)
        return
    except NotFoundError:
        log.info("creating JetStream stream %s", stream)
        await js.add_stream(
            config=StreamConfig(
                name=stream,
                subjects=subjects,
                storage=StorageType.FILE,
                retention=RetentionPolicy.LIMITS,
                discard=DiscardPolicy.OLD,
                description="claude-team overlay events",
            )
        )


async def ensure_object_store(js: JetStreamContext, bucket: str) -> ObjectStore:
    try:
        return await js.object_store(bucket)
    except (BucketNotFoundError, NotFoundError):
        log.info("creating object store %s", bucket)
        # nats-py overwrites config.bucket with the positional arg, so
        # we pass bucket both ways to avoid a None overwrite.
        return await js.create_object_store(
            bucket,
            config=ObjectStoreConfig(
                bucket=bucket,
                storage=StorageType.FILE,
                description="claude-team file contents",
            ),
        )


async def bootstrap(nats_url: str, cluster_id: str, node_id: str) -> NatsResources:
    """Connect and ensure all cluster-scoped resources exist.

    Idempotent: if resources already exist (another peer created them),
    this simply opens handles to them.
    """
    nc = await connect(nats_url, name=f"claude-team-{node_id}")
    js = nc.jetstream()

    stream = events_stream_name(cluster_id)
    subjects = [f"{subject_prefix(cluster_id)}.>"]
    await ensure_stream(js, stream, subjects)

    locks = await ensure_kv(js, locks_bucket_name(cluster_id))
    objects = await ensure_object_store(js, objects_bucket_name(cluster_id))

    return NatsResources(
        nc=nc,
        js=js,
        locks=locks,
        events_stream_name=stream,
        objects=objects,
        cluster_id=cluster_id,
    )


async def close(resources: NatsResources) -> None:
    await resources.nc.drain()


def encode_json(obj: Any) -> bytes:
    import json

    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def decode_json(data: bytes) -> Any:
    import json

    return json.loads(data.decode("utf-8"))
