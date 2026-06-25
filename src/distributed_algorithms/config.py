from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Peer:
    node_id: str
    url: str


@dataclass(frozen=True)
class Settings:
    node_id: str
    peers: dict[str, Peer]

    @property
    def other_peers(self) -> dict[str, Peer]:
        return {node_id: peer for node_id, peer in self.peers.items() if node_id != self.node_id}


def load_settings() -> Settings:
    node_id = os.getenv("NODE_ID", "1")
    raw_peers = os.getenv("PEERS", "1=http://localhost:8000")
    peers: dict[str, Peer] = {}

    for item in raw_peers.split(","):
        item = item.strip()
        if not item:
            continue
        peer_id, peer_url = item.split("=", 1)
        peers[peer_id] = Peer(node_id=peer_id, url=peer_url.rstrip("/"))

    if node_id not in peers:
        peers[node_id] = Peer(node_id=node_id, url="http://localhost:8000")

    return Settings(node_id=node_id, peers=peers)

