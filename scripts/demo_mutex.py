from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from common import get, note, post, print_status, step, title, wait_for_nodes


def request_critical_section(node_id: str) -> dict[str, object]:
    return post(
        node_id,
        "/mutex/request",
        {"hold_seconds": 3.0, "timeout_seconds": 25.0},
        timeout=40,
    )


def main() -> None:
    wait_for_nodes()

    title("CENA 2 - Exclusao mutua com Ricart-Agrawala")
    note("Objetivo: dois nos pedem a secao critica quase ao mesmo tempo.")
    note("A prioridade e definida por (timestamp de Lamport, node_id).")

    step("Estado antes dos pedidos concorrentes")
    print_status(["2", "3"])

    step("Nos 2 e 3 solicitam a secao critica em paralelo")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            "2": executor.submit(request_critical_section, "2"),
            "3": executor.submit(request_critical_section, "3"),
        }
        for node_id, future in futures.items():
            result = future.result()
            note(f"node {node_id}: entrou_na_secao_critica={result['entered']}")

    step("Estado depois dos pedidos")
    print_status(["2", "3"])

    step("Historico do protocolo")
    for node_id in ["2", "3"]:
        status = get(node_id, "/status")
        print(f"\nnode {node_id}")
        for event in status["mutex"]["history"]:
            print(f"  {event['kind']:<14} {event['details']}")

    note("Se os timestamps empatam, o menor node_id tem prioridade. O outro aguarda a resposta adiada.")


if __name__ == "__main__":
    main()
