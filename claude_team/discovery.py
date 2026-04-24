"""Peer discovery over UDP multicast.

Each node periodically publishes an ``ANNOUNCE`` packet to a well-known
multicast group and port. Receivers in the same cluster (matching
``cluster_id``) pick it up, track the peer, and age it out on missed
announces.

Multicast is used instead of broadcast because:

- It works on loopback (two Claude Code sessions on the same host find
  each other), whereas ``255.255.255.255`` does not reliably deliver to
  loopback on macOS.
- It crosses LAN boundaries with TTL 1 set, and LANs rarely block
  ``239.0.0.0/8`` organization-local traffic.

Wire format is JSON for readability and ease of debugging; packets are
tiny and this is not a hot path.
"""
from __future__ import annotations

import asyncio
import json
import logging
import socket
import struct
import time
from dataclasses import dataclass
from typing import Awaitable, Callable


log = logging.getLogger(__name__)


MAGIC = "CLTM"
VERSION = 1
MULTICAST_GROUP = "239.66.83.2"
ANNOUNCE_INTERVAL_S = 2.0
PEER_TIMEOUT_S = 6.0


@dataclass(frozen=True)
class Announce:
    cluster_id: str
    node_id: str
    hostname: str
    pid: int
    nats_url: str
    sent_at: float

    def to_bytes(self) -> bytes:
        return json.dumps({
            "magic": MAGIC,
            "version": VERSION,
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "hostname": self.hostname,
            "pid": self.pid,
            "nats_url": self.nats_url,
            "sent_at": self.sent_at,
        }).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Announce | None":
        try:
            obj = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        if obj.get("magic") != MAGIC or obj.get("version") != VERSION:
            return None
        try:
            return cls(
                cluster_id=str(obj["cluster_id"]),
                node_id=str(obj["node_id"]),
                hostname=str(obj["hostname"]),
                pid=int(obj["pid"]),
                nats_url=str(obj["nats_url"]),
                sent_at=float(obj["sent_at"]),
            )
        except (KeyError, TypeError, ValueError):
            return None


@dataclass
class Peer:
    announce: Announce
    addr: tuple[str, int]
    last_seen: float
    first_seen: float


# Event strings: "join", "update", "leave".
PeerEventHandler = Callable[[Peer, str], Awaitable[None]]


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, on_announce: Callable[[Announce, tuple[str, int]], None]) -> None:
        self.on_announce = on_announce

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        ann = Announce.from_bytes(data)
        if ann is not None:
            self.on_announce(ann, addr)

    def error_received(self, exc: Exception) -> None:
        log.debug("discovery recv error: %s", exc)


class Discovery:
    """Runs the announce sender and peer tracker.

    Self-echoes (packets with our own ``node_id``) are ignored, which is
    necessary because ``IP_MULTICAST_LOOP`` is enabled so we can reach
    peers on loopback.
    """

    def __init__(
        self,
        cluster_id: str,
        node_id: str,
        hostname: str,
        pid: int,
        nats_url: str,
        port: int = 7500,
        multicast_group: str = MULTICAST_GROUP,
        announce_interval_s: float = ANNOUNCE_INTERVAL_S,
        peer_timeout_s: float = PEER_TIMEOUT_S,
    ) -> None:
        self.cluster_id = cluster_id
        self.node_id = node_id
        self.hostname = hostname
        self.pid = pid
        self.nats_url = nats_url
        self.port = port
        self.multicast_group = multicast_group
        self.announce_interval_s = announce_interval_s
        self.peer_timeout_s = peer_timeout_s

        self.peers: dict[str, Peer] = {}
        self.running = False

        self.transport: asyncio.DatagramTransport | None = None
        self.send_sock: socket.socket | None = None
        self.announce_task: asyncio.Task | None = None
        self.reap_task: asyncio.Task | None = None
        self.peer_handlers: list[PeerEventHandler] = []

    def on_peer_event(self, handler: PeerEventHandler) -> None:
        self.peer_handlers.append(handler)

    async def start(self) -> None:
        loop = asyncio.get_running_loop()

        recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        recv_sock.bind(("", self.port))
        mreq = struct.pack(
            "=4s4s",
            socket.inet_aton(self.multicast_group),
            socket.inet_aton("0.0.0.0"),
        )
        recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        recv_sock.setblocking(False)

        self.transport, _ = await loop.create_datagram_endpoint(
            lambda: _Protocol(self.handle_announce),
            sock=recv_sock,
        )

        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", 1))
        self.send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, struct.pack("b", 1))
        self.send_sock.setblocking(False)

        self.running = True
        self.announce_task = asyncio.create_task(self.announce_loop())
        self.reap_task = asyncio.create_task(self.reap_loop())
        log.info(
            "discovery started on %s:%d (cluster=%s node=%s)",
            self.multicast_group, self.port, self.cluster_id, self.node_id,
        )

    async def stop(self) -> None:
        self.running = False
        for task in (self.announce_task, self.reap_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self.transport is not None:
            self.transport.close()
            self.transport = None
        if self.send_sock is not None:
            self.send_sock.close()
            self.send_sock = None
        log.info("discovery stopped")

    def handle_announce(self, ann: Announce, addr: tuple[str, int]) -> None:
        if ann.cluster_id != self.cluster_id:
            return
        if ann.node_id == self.node_id:
            return
        now = time.monotonic()
        existing = self.peers.get(ann.node_id)
        if existing is None:
            peer = Peer(announce=ann, addr=addr, last_seen=now, first_seen=now)
            self.peers[ann.node_id] = peer
            log.info(
                "peer joined: %s (%s) at %s via %s",
                ann.node_id, ann.hostname, addr[0], ann.nats_url,
            )
            asyncio.create_task(self.fire(peer, "join"))
        else:
            existing.announce = ann
            existing.addr = addr
            existing.last_seen = now

    async def fire(self, peer: Peer, event: str) -> None:
        for handler in self.peer_handlers:
            try:
                await handler(peer, event)
            except Exception:
                log.exception("peer handler failed for event=%s", event)

    def build_announce(self) -> Announce:
        return Announce(
            cluster_id=self.cluster_id,
            node_id=self.node_id,
            hostname=self.hostname,
            pid=self.pid,
            nats_url=self.nats_url,
            sent_at=time.time(),
        )

    async def announce_loop(self) -> None:
        loop = asyncio.get_running_loop()
        assert self.send_sock is not None
        while self.running:
            data = self.build_announce().to_bytes()
            try:
                await loop.sock_sendto(
                    self.send_sock, data, (self.multicast_group, self.port),
                )
            except OSError as exc:
                log.warning("discovery send failed: %s", exc)
            await asyncio.sleep(self.announce_interval_s)

    async def reap_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.announce_interval_s)
            now = time.monotonic()
            stale = [
                node_id for node_id, peer in self.peers.items()
                if now - peer.last_seen > self.peer_timeout_s
            ]
            for node_id in stale:
                peer = self.peers.pop(node_id)
                log.info("peer left (timeout): %s", node_id)
                asyncio.create_task(self.fire(peer, "leave"))
