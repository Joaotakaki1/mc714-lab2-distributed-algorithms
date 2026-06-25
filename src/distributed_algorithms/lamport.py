from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from time import time


@dataclass
class ClockEvent:
    kind: str
    clock: int
    description: str
    wall_time: float = field(default_factory=time)


class LamportClock:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self._value = 0
        self._lock = asyncio.Lock()
        self._history: list[ClockEvent] = []
        self._logger = logging.getLogger("mc714.lamport")

    @property
    def value(self) -> int:
        return self._value

    @property
    def history(self) -> list[dict[str, object]]:
        return [
            {
                "kind": event.kind,
                "clock": event.clock,
                "description": event.description,
                "wall_time": event.wall_time,
            }
            for event in self._history[-50:]
        ]

    async def local_event(self, description: str) -> int:
        async with self._lock:
            self._value += 1
            self._record("local", description)
            return self._value

    async def send_event(self, description: str) -> int:
        async with self._lock:
            self._value += 1
            self._record("send", description)
            return self._value

    async def receive_event(self, received_clock: int, description: str) -> int:
        async with self._lock:
            old_value = self._value
            self._value = max(self._value, received_clock) + 1
            self._record("receive", description)
            self._logger.info(
                "LAMPORT | receive rule max(local=%s, received=%s) + 1 -> %s",
                old_value,
                received_clock,
                self._value,
            )
            return self._value

    def _record(self, kind: str, description: str) -> None:
        self._history.append(ClockEvent(kind=kind, clock=self._value, description=description))
        self._logger.info("LAMPORT | %-7s | L=%s | %s", kind, self._value, description)
