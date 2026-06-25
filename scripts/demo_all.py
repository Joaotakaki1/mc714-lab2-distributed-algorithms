from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from common import compose_quiet, note, step, title, wait_for_nodes


SCRIPT_DIR = Path(__file__).resolve().parent


def reset_environment(scene: str) -> None:
    step(f"Preparando ambiente limpo para: {scene}")
    note("docker compose down/up sera executado sem mostrar o ruido do build.")
    compose_quiet("down")
    compose_quiet("up", "-d", "--build")
    wait_for_nodes()
    note("todos os 5 nos estao prontos.")


def run_demo(script_name: str) -> None:
    subprocess.run([sys.executable, str(SCRIPT_DIR / script_name)], check=True)


def maybe_pause(enabled: bool) -> None:
    if enabled:
        input("\nPressione Enter para continuar para a proxima cena...")


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa todas as demos do trabalho MC714.")
    parser.add_argument(
        "--pause",
        action="store_true",
        help="pausa entre as cenas; util para gravar o video explicando com calma",
    )
    args = parser.parse_args()

    title("DEMO COMPLETA - MC714 Lab 2")
    note("Este roteiro executa Lamport, exclusao mutua e eleicao com estado limpo entre as cenas.")

    reset_environment("Relogio logico de Lamport")
    run_demo("demo_lamport.py")
    maybe_pause(args.pause)

    reset_environment("Exclusao mutua com Ricart-Agrawala")
    run_demo("demo_mutex.py")
    maybe_pause(args.pause)

    reset_environment("Eleicao de lider com Bully Algorithm")
    run_demo("demo_election.py")

    title("FIM DA DEMO")
    note("Para inspecionar logs internos dos containers, execute: make logs")
    note("Para parar o ambiente, execute: make down")


if __name__ == "__main__":
    main()
