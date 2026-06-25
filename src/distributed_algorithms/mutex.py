from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import StrEnum
from time import time
from uuid import uuid4

from distributed_algorithms.lamport import LamportClock
from distributed_algorithms.transport import Transport


class MutexState(StrEnum):
    RELEASED = "released"
    WANTED = "wanted"
    HELD = "held"


@dataclass
class MutexEvent:
    kind: str
    details: str
    wall_time: float = field(default_factory=time)


class RicartAgrawalaMutex:
    def __init__(self, node_id: str, clock: LamportClock, transport: Transport) -> None:
        self.node_id = node_id
        self._clock = clock
        self._transport = transport
        self._lock = asyncio.Lock()
        self._all_replies_received = asyncio.Event()
        self._state = MutexState.RELEASED
        self._request_timestamp: int | None = None
        self._request_id: str | None = None
        self._pending_replies: set[str] = set()
        self._deferred_replies: dict[str, str] = {}
        self._history: list[MutexEvent] = []
        self._logger = logging.getLogger("mc714.mutex")

    @property
    def status(self) -> dict[str, object]:
        return {
            "state": self._state,
            "request_timestamp": self._request_timestamp,
            "request_id": self._request_id,
            "pending_replies": sorted(self._pending_replies, key=int),
            "deferred_replies": sorted(self._deferred_replies.keys(), key=int),
            "history": [
                {"kind": event.kind, "details": event.details, "wall_time": event.wall_time}
                for event in self._history[-30:]
            ],
        }

    async def acquire(self, timeout_seconds: float) -> bool:
        request_id = str(uuid4())
        timestamp = await self._clock.send_event(f"mutex request {request_id}")
        peer_ids = self._transport.peer_ids

        async with self._lock:
            if self._state != MutexState.RELEASED:
                self._record("reject-local", "node already wants or holds the critical section")
                return False
            self._state = MutexState.WANTED
            self._request_timestamp = timestamp
            self._request_id = request_id
            self._pending_replies = set(peer_ids)
            self._all_replies_received.clear()
            if not self._pending_replies:
                self._all_replies_received.set()
            self._record(
                "request",
                f"request_id={request_id} timestamp={timestamp} waiting_for={sorted(peer_ids, key=int)}",
            )

        payload = {"sender_id": self.node_id, "timestamp": timestamp, "request_id": request_id}
        self._record("broadcast", f"request_id={request_id} peers={peer_ids}")
        results = await self._transport.broadcast("/mutex/internal/request", payload)
        failed = [result.peer_id for result in results if not result.ok]
        if failed:
            self._record("request-failed", f"request_id={request_id} failed_peers={failed}")

        try:
            await asyncio.wait_for(self._all_replies_received.wait(), timeout=timeout_seconds)
        except TimeoutError:
            async with self._lock:
                pending = sorted(self._pending_replies, key=int)
                self._state = MutexState.RELEASED
                self._request_timestamp = None
                self._request_id = None
                self._pending_replies.clear()
                self._record("timeout", f"request_id={request_id} pending_replies={pending}")
            return False

        async with self._lock:
            self._state = MutexState.HELD
            self._record("enter", f"request_id={request_id}")
        await self._clock.local_event(f"entered critical section {request_id}")
        return True

    async def release(self) -> None:
        async with self._lock:
            if self._state == MutexState.RELEASED:
                return
            request_id = self._request_id
            deferred = sorted(self._deferred_replies.items(), key=lambda item: int(item[0]))
            deferred_peers = [peer_id for peer_id, _ in deferred]
            self._state = MutexState.RELEASED
            self._request_timestamp = None
            self._request_id = None
            self._pending_replies.clear()
            self._deferred_replies.clear()
            self._record("release", f"request_id={request_id} replying_to_deferred={deferred_peers}")

        await self._clock.local_event(f"released critical section {request_id}")
        await asyncio.gather(*(
            self._send_reply(peer_id, deferred_request_id)
            for peer_id, deferred_request_id in deferred
        ))

    async def handle_request(self, sender_id: str, timestamp: int, request_id: str) -> dict[str, object]:
        await self._clock.receive_event(timestamp, f"mutex request from node {sender_id}")
        should_reply = False

        async with self._lock:
            local_priority = (
                self._request_timestamp is not None
                and (self._request_timestamp, int(self.node_id)) < (timestamp, int(sender_id))
            )
            if self._state == MutexState.HELD or (self._state == MutexState.WANTED and local_priority):
                self._deferred_replies[sender_id] = request_id
                self._record(
                    "defer",
                    f"reply to node {sender_id} request_id={request_id}; local request has priority",
                )
            else:
                should_reply = True
                self._record("reply", f"immediate reply to node {sender_id} request_id={request_id}")

        if should_reply:
            await self._send_reply(sender_id, request_id)

        return {"accepted": True, "reply_deferred": not should_reply}

    async def handle_reply(self, sender_id: str, timestamp: int, request_id: str) -> dict[str, object]:
        await self._clock.receive_event(timestamp, f"mutex reply from node {sender_id}")
        async with self._lock:
            if request_id == self._request_id and sender_id in self._pending_replies:
                self._pending_replies.remove(sender_id)
                self._record(
                    "reply-received",
                    f"from node {sender_id} request_id={request_id} pending={sorted(self._pending_replies, key=int)}",
                )
                if not self._pending_replies:
                    self._record("all-replies", f"request_id={request_id}; node may enter critical section")
                    self._all_replies_received.set()
            else:
                self._record("reply-ignored", f"from node {sender_id} request_id={request_id}")
        return {"accepted": True}

    async def _send_reply(self, peer_id: str, request_id: str) -> None:
        timestamp = await self._clock.send_event(f"mutex reply to node {peer_id}")
        payload = {"sender_id": self.node_id, "timestamp": timestamp, "request_id": request_id}
        result = await self._transport.post(peer_id, "/mutex/internal/reply", payload)
        if result.ok:
            self._record("reply-sent", f"to node {peer_id} request_id={request_id} timestamp={timestamp}")
        else:
            self._record("reply-failed", f"to node {peer_id} request_id={request_id} error={result.error}")

    def _record(self, kind: str, details: str) -> None:
        self._history.append(MutexEvent(kind, details))
        self._logger.info("MUTEX   | %-14s | %s", kind, details)
