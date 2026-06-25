from __future__ import annotations

import asyncio

from fastapi import FastAPI

from distributed_algorithms.config import load_settings
from distributed_algorithms.election import BullyElection
from distributed_algorithms.lamport import LamportClock
from distributed_algorithms.models import (
    CriticalSectionRequest,
    ElectionMessage,
    InternalMessage,
    LamportMessage,
    LamportPayload,
    MutexReply,
    MutexRequest,
)
from distributed_algorithms.mutex import RicartAgrawalaMutex
from distributed_algorithms.transport import Transport

settings = load_settings()
clock = LamportClock()
transport = Transport(settings.other_peers)
mutex = RicartAgrawalaMutex(settings.node_id, clock, transport)
election = BullyElection(settings.node_id, transport, list(settings.peers.keys()))

app = FastAPI(title=f"MC714 node {settings.node_id}")


@app.get("/health")
async def health() -> dict[str, object]:
    return {"ok": True, "node_id": settings.node_id}


@app.get("/status")
async def status() -> dict[str, object]:
    return {
        "node_id": settings.node_id,
        "peers": {node_id: peer.url for node_id, peer in settings.other_peers.items()},
        "clock": clock.value,
        "lamport_history": clock.history,
        "mutex": mutex.status,
        "election": election.status,
    }


@app.post("/lamport/local")
async def lamport_local(payload: LamportPayload) -> dict[str, object]:
    timestamp = await clock.local_event(payload.description)
    return {"node_id": settings.node_id, "timestamp": timestamp}


@app.post("/lamport/send/{peer_id}")
async def lamport_send(peer_id: str, payload: LamportPayload) -> dict[str, object]:
    timestamp = await clock.send_event(f"send to node {peer_id}: {payload.description}")
    message = {"sender_id": settings.node_id, "timestamp": timestamp, "description": payload.description}
    result = await transport.post(peer_id, "/lamport/internal/message", message)
    return {
        "node_id": settings.node_id,
        "timestamp": timestamp,
        "sent_to": peer_id,
        "delivered": result.ok,
        "response": result.data,
        "error": result.error,
    }


@app.post("/lamport/internal/message")
async def lamport_receive(message: LamportMessage) -> dict[str, object]:
    timestamp = await clock.receive_event(
        message.timestamp,
        f"message from node {message.sender_id}: {message.description}",
    )
    return {"node_id": settings.node_id, "timestamp": timestamp}


@app.post("/mutex/request")
async def mutex_request(payload: CriticalSectionRequest) -> dict[str, object]:
    acquired = await mutex.acquire(timeout_seconds=payload.timeout_seconds)
    if not acquired:
        return {"node_id": settings.node_id, "entered": False, "status": mutex.status}

    await asyncio.sleep(payload.hold_seconds)
    await mutex.release()
    return {"node_id": settings.node_id, "entered": True, "status": mutex.status}


@app.post("/mutex/internal/request")
async def mutex_internal_request(message: MutexRequest) -> dict[str, object]:
    return await mutex.handle_request(message.sender_id, message.timestamp, message.request_id)


@app.post("/mutex/internal/reply")
async def mutex_internal_reply(message: MutexReply) -> dict[str, object]:
    return await mutex.handle_reply(message.sender_id, message.timestamp, message.request_id)


@app.post("/election/start")
async def election_start(message: ElectionMessage | None = None) -> dict[str, object]:
    reason = message.reason if message else "manual"
    return await election.start_election(reason)


@app.post("/election/internal/election")
async def election_internal_election(message: ElectionMessage) -> dict[str, object]:
    return await election.handle_election_message(message.sender_id, message.reason)


@app.post("/election/internal/coordinator")
async def election_internal_coordinator(message: ElectionMessage) -> dict[str, object]:
    return await election.handle_coordinator_message(message.sender_id, message.reason)

