from common import get, post, print_status, wait_for_nodes


def main() -> None:
    wait_for_nodes()

    print("== Initial clocks ==")
    print_status(["1", "2", "3"])

    print("\n== Local event on node 1 ==")
    print(post("1", "/lamport/local", {"description": "local computation before sending"}))

    print("\n== Node 1 sends a message to node 2 ==")
    print(post("1", "/lamport/send/2", {"description": "hello from node 1"}))

    print("\n== Node 2 sends a message to node 3 ==")
    print(post("2", "/lamport/send/3", {"description": "causal message forwarded by node 2"}))

    print("\n== Final clocks ==")
    print_status(["1", "2", "3"])

    print("\n== Recent Lamport histories ==")
    for node_id in ["1", "2", "3"]:
        status = get(node_id, "/status")
        print(f"\nnode {node_id}")
        for event in status["lamport_history"]:
            print(f"  L={event['clock']:>2} {event['kind']:<7} {event['description']}")


if __name__ == "__main__":
    main()

