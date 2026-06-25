from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from time import time

from distributed_algorithms.transport import Transport


@dataclass
class ElectionEvent:
    kind: str
    details: str
    wall_time: float = field(default_factory=time)


class BullyElection:
    def __init__(self, node_id: str, transport: Transport, all_node_ids: list[str]) -> None:
        self.node_id = node_id
        self._transport = transport
        self._all_node_ids = sorted(all_node_ids, key=int)
        self._lock = asyncio.Lock()
        self._coordinator_received = asyncio.Event()
        self._leader_id = max(self._all_node_ids, key=int)
        self._election_in_progress = False
        self._history: list[ElectionEvent] = []
        self._logger = logging.getLogger("mc714.election")

    @property
    def status(self) -> dict[str, object]:
        return {
            "leader_id": self._leader_id,
            "election_in_progress": self._election_in_progress,
            "history": [
                {"kind": event.kind, "details": event.details, "wall_time": event.wall_time}
                for event in self._history[-30:]
            ],
        }

    async def start_election(self, reason: str = "manual") -> dict[str, object]:
        async with self._lock:
            if self._election_in_progress:
                return {"started": False, "leader_id": self._leader_id, "reason": "already in progress"}
            self._election_in_progress = True
            self._coordinator_received.clear()
            self._record("start", reason)

        higher_peer_ids = [peer_id for peer_id in self._transport.peer_ids if int(peer_id) > int(self.node_id)]
        payload = {"sender_id": self.node_id, "reason": reason}
        results = await self._transport.broadcast("/election/internal/election", payload, higher_peer_ids)
        ok_received = any(result.ok and result.data and result.data.get("ok") for result in results)
        answered = [result.peer_id for result in results if result.ok and result.data and result.data.get("ok")]
        failed = [result.peer_id for result in results if not result.ok]
        self._record(
            "higher-nodes",
            f"asked={higher_peer_ids} answered_ok={answered} failed_or_down={failed}",
        )

        if not ok_received:
            await self._become_leader("no higher node answered")
            return {"started": True, "leader_id": self._leader_id, "higher_nodes_answered": False}

        async with self._lock:
            self._record("wait-coordinator", "higher node answered OK")

        try:
            await asyncio.wait_for(self._coordinator_received.wait(), timeout=6.0)
        except TimeoutError:
            await self._become_leader("higher nodes answered but no coordinator arrived")

        return {"started": True, "leader_id": self._leader_id, "higher_nodes_answered": True}

    async def handle_election_message(self, sender_id: str, reason: str) -> dict[str, object]:
        async with self._lock:
            self._record("receive-election", f"from node {sender_id}: {reason}")
        if int(sender_id) < int(self.node_id):
            self._record("answer-ok", f"answering node {sender_id}; my id is higher")
            asyncio.create_task(self.start_election(f"node {sender_id} requested election"))
            return {"ok": True, "sender_id": self.node_id}
        self._record("answer-no", f"ignoring node {sender_id}; sender id is not lower")
        return {"ok": False, "sender_id": self.node_id}

    async def handle_coordinator_message(self, sender_id: str, reason: str) -> dict[str, object]:
        async with self._lock:
            self._leader_id = sender_id
            self._election_in_progress = False
            self._record("coordinator", f"leader is node {sender_id}: {reason}")
            self._coordinator_received.set()
        return {"accepted": True, "leader_id": self._leader_id}

    async def _become_leader(self, reason: str) -> None:
        async with self._lock:
            self._leader_id = self.node_id
            self._election_in_progress = False
            self._record("become-leader", reason)
            self._coordinator_received.set()
        payload = {"sender_id": self.node_id, "reason": reason}
        lower_peer_ids = [peer_id for peer_id in self._transport.peer_ids if int(peer_id) < int(self.node_id)]
        results = await self._transport.broadcast("/election/internal/coordinator", payload, lower_peer_ids)
        reached = [result.peer_id for result in results if result.ok]
        failed = [result.peer_id for result in results if not result.ok]
        self._record("announce", f"leader={self.node_id} reached={reached} failed_or_down={failed}")

    def _record(self, kind: str, details: str) -> None:
        self._history.append(ElectionEvent(kind, details))
        self._logger.info("ELECTION| %-18s | %s", kind, details)
