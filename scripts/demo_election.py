from __future__ import annotations

import time

from common import compose, get, post, print_status, wait_for_nodes


def main() -> None:
    wait_for_nodes()

    print("== Initial leader: highest node id, node 5 ==")
    print_status(["1", "2", "3", "4", "5"])

    print("\n== Stopping node 5 to simulate leader failure ==")
    compose("stop", "node5")
    time.sleep(2)

    print("\n== Node 2 starts the Bully election ==")
    result = post("2", "/election/start", {"sender_id": "2", "reason": "leader node 5 stopped"})
    print(result)
    time.sleep(2)

    print("\n== Election status after node 5 failure ==")
    print_status(["1", "2", "3", "4"])

    print("\n== Election histories ==")
    for node_id in ["2", "3", "4"]:
        status = get(node_id, "/status")
        print(f"\nnode {node_id}")
        for event in status["election"]["history"]:
            print(f"  {event['kind']:<18} {event['details']}")

    print("\n== Starting node 5 again ==")
    compose("start", "node5")
    wait_for_nodes(["5"])

    print("\n== Node 5 announces recovery and becomes leader again ==")
    print(post("5", "/election/start", {"sender_id": "5", "reason": "node 5 recovered"}))
    time.sleep(1)
    print_status(["1", "2", "3", "4", "5"])


if __name__ == "__main__":
    main()
