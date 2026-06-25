from common import get, note, post, print_status, step, title, wait_for_nodes


def main() -> None:
    wait_for_nodes()

    title("CENA 1 - Relogio logico de Lamport")
    note("Objetivo: mostrar que envio e recebimento preservam a ordem causal.")

    step("Estado inicial dos relogios")
    print_status(["1", "2", "3"])

    step("No 1 executa um evento local")
    result = post("1", "/lamport/local", {"description": "local computation before sending"})
    note(f"node 1 incrementa seu relogio para L={result['timestamp']}")

    step("No 1 envia mensagem para o no 2")
    result = post("1", "/lamport/send/2", {"description": "hello from node 1"})
    note(f"node 1 envia com timestamp L={result['timestamp']}")
    note(f"node 2 recebe e ajusta para L={result['response']['timestamp']}")

    step("No 2 encaminha uma mensagem causal para o no 3")
    result = post("2", "/lamport/send/3", {"description": "causal message forwarded by node 2"})
    note(f"node 2 envia com timestamp L={result['timestamp']}")
    note(f"node 3 recebe e ajusta para L={result['response']['timestamp']}")

    step("Estado final dos relogios")
    print_status(["1", "2", "3"])

    step("Historico resumido por no")
    for node_id in ["1", "2", "3"]:
        status = get(node_id, "/status")
        print(f"\nnode {node_id}")
        for event in status["lamport_history"]:
            print(f"  L={event['clock']:>2} {event['kind']:<7} {event['description']}")

    note("Observe que cada recebimento fica com timestamp maior que o envio correspondente.")


if __name__ == "__main__":
    main()
