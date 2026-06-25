from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from distributed_algorithms.config import Peer


@dataclass
class SendResult:
    peer_id: str
    ok: bool
    status_code: int | None = None
    data: dict[str, Any] | None = None
    error: str | None = None


class Transport:
    def __init__(self, peers: dict[str, Peer], timeout_seconds: float = 3.0) -> None:
        self._peers = peers
        self._timeout_seconds = timeout_seconds

    @property
    def peer_ids(self) -> list[str]:
        return sorted(self._peers.keys(), key=int)

    async def post(self, peer_id: str, path: str, payload: dict[str, Any]) -> SendResult:
        peer = self._peers[peer_id]
        url = f"{peer.url}{path}"
        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.post(url, json=payload)
            data = response.json() if response.content else {}
            return SendResult(peer_id=peer_id, ok=response.is_success, status_code=response.status_code, data=data)
        except (httpx.HTTPError, ValueError) as exc:
            return SendResult(peer_id=peer_id, ok=False, error=str(exc))

    async def broadcast(self, path: str, payload: dict[str, Any], peer_ids: list[str] | None = None) -> list[SendResult]:
        targets = peer_ids if peer_ids is not None else self.peer_ids
        return await asyncio.gather(*(self.post(peer_id, path, payload) for peer_id in targets))

