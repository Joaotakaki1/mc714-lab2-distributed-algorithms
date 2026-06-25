from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)


NODE_PORTS = {
    "1": 8001,
    "2": 8002,
    "3": 8003,
    "4": 8004,
    "5": 8005,
}


def node_url(node_id: str, path: str) -> str:
    return f"http://localhost:{NODE_PORTS[node_id]}{path}"


def post(node_id: str, path: str, payload: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    body = json.dumps(payload or {}).encode("utf-8")
    request = urllib.request.Request(
        node_url(node_id, path),
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def get(node_id: str, path: str, timeout: int = 10) -> dict[str, Any]:
    with urllib.request.urlopen(node_url(node_id, path), timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_nodes(node_ids: list[str] | None = None, attempts: int = 30) -> None:
    targets = node_ids or list(NODE_PORTS)
    for _ in range(attempts):
        ready = True
        for node_id in targets:
            try:
                get(node_id, "/health", timeout=2)
            except (urllib.error.URLError, TimeoutError, OSError):
                ready = False
                break
        if ready:
            return
        time.sleep(1)
    raise RuntimeError(f"nodes not ready: {targets}")


def print_status(node_ids: list[str]) -> None:
    for node_id in node_ids:
        status = get(node_id, "/status")
        print(
            f"node {node_id}: clock={status['clock']} "
            f"mutex={status['mutex']['state']} leader={status['election']['leader_id']}"
        )


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True)
