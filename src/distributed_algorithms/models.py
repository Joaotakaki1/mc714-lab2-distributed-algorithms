from __future__ import annotations

from pydantic import BaseModel, Field


class InternalMessage(BaseModel):
    sender_id: str
    timestamp: int


class LamportPayload(BaseModel):
    description: str = Field(default="application message")


class LamportMessage(InternalMessage):
    description: str


class MutexRequest(InternalMessage):
    request_id: str


class MutexReply(InternalMessage):
    request_id: str


class CriticalSectionRequest(BaseModel):
    hold_seconds: float = Field(default=2.0, ge=0.0, le=20.0)
    timeout_seconds: float = Field(default=20.0, ge=1.0, le=60.0)


class ElectionMessage(BaseModel):
    sender_id: str
    reason: str = "manual"

