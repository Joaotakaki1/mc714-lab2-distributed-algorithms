from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from common import get, post, print_status, wait_for_nodes


def request_critical_section(node_id: str) -> dict[str, object]:
    return post(
        node_id,
        "/mutex/request",
        {"hold_seconds": 3.0, "timeout_seconds": 25.0},
        timeout=40,
    )


def main() -> None:
    wait_for_nodes()

    print("== Before concurrent critical-section requests ==")
    print_status(["2", "3"])

    print("\n== Nodes 2 and 3 request the critical section at nearly the same time ==")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            "2": executor.submit(request_critical_section, "2"),
            "3": executor.submit(request_critical_section, "3"),
        }
        for node_id, future in futures.items():
            result = future.result()
            print(f"node {node_id}: entered={result['entered']}")

    print("\n== After requests ==")
    print_status(["2", "3"])

    print("\n== Mutex histories ==")
    for node_id in ["2", "3"]:
        status = get(node_id, "/status")
        print(f"\nnode {node_id}")
        for event in status["mutex"]["history"]:
            print(f"  {event['kind']:<14} {event['details']}")


if __name__ == "__main__":
    main()

