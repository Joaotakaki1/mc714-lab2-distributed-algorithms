from common import print_status, wait_for_nodes


def main() -> None:
    wait_for_nodes()
    print_status(["1", "2", "3", "4", "5"])


if __name__ == "__main__":
    main()

