from __future__ import annotations

import time

from common import compose, get, note, post, print_status, step, title, wait_for_nodes


def main() -> None:
    wait_for_nodes()

    title("CENA 3 - Eleicao de lider com Bully Algorithm")
    note("Objetivo: simular falha do lider e eleger o maior no ainda ativo.")

    step("Estado inicial: maior identificador e o lider")
    print_status(["1", "2", "3", "4", "5"])

    step("Parando o no 5 para simular falha do lider")
    compose("stop", "node5")
    time.sleep(2)

    step("No 2 inicia a eleicao")
    result = post("2", "/election/start", {"sender_id": "2", "reason": "leader node 5 stopped"})
    note(f"resultado: lider_atual={result['leader_id']} higher_nodes_answered={result['higher_nodes_answered']}")
    time.sleep(2)

    step("Estado apos a falha do no 5")
    print_status(["1", "2", "3", "4"])

    step("Historico da eleicao")
    for node_id in ["2", "3", "4"]:
        status = get(node_id, "/status")
        print(f"\nnode {node_id}")
        for event in status["election"]["history"]:
            print(f"  {event['kind']:<18} {event['details']}")

    step("Subindo o no 5 novamente")
    compose("start", "node5")
    wait_for_nodes(["5"])

    step("No 5 anuncia recuperacao e reassume por ser o maior identificador")
    result = post("5", "/election/start", {"sender_id": "5", "reason": "node 5 recovered"})
    note(f"resultado: lider_atual={result['leader_id']} higher_nodes_answered={result['higher_nodes_answered']}")
    time.sleep(1)
    print_status(["1", "2", "3", "4", "5"])

    note("No Bully, o maior identificador ativo vence. Ao recuperar, o no 5 pode anunciar nova coordenacao.")


if __name__ == "__main__":
    main()
