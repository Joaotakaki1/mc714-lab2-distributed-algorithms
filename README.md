# MC714 Lab 2 - Distributed Algorithms

Implementacao dos algoritmos pedidos no 2o trabalho de MC714, usando FastAPI para comunicacao HTTP entre processos e Docker Compose para executar varios nos localmente.

## Algoritmos implementados

- Relogio logico de Lamport.
- Exclusao mutua distribuida com Ricart-Agrawala.
- Eleicao de lider com Bully Algorithm.

Cada container executa o mesmo servico FastAPI. O comportamento de cada no muda apenas pelas variaveis de ambiente `NODE_ID` e `PEERS`.

## Requisitos

- Docker
- Docker Compose
- Python 3 no host, apenas para rodar os scripts de demonstracao

## Como executar

```bash
make up
make status
```

Os nos ficam disponiveis no host em:

- node1: <http://localhost:8001>
- node2: <http://localhost:8002>
- node3: <http://localhost:8003>
- node4: <http://localhost:8004>
- node5: <http://localhost:8005>

Para parar o ambiente:

```bash
make down
```

Para limpar o estado em memoria dos nos entre demonstracoes:

```bash
make reset
```

## Demonstracoes

Relogio de Lamport:

```bash
make demo-lamport
```

Exclusao mutua:

```bash
make demo-mutex
```

Eleicao de lider:

```bash
make demo-election
```

O demo de eleicao para o container `node5`, inicia uma eleicao pelo `node2`, mostra que o `node4` vira lider, sobe o `node5` novamente e faz o `node5` anunciar recuperacao.

## Endpoints principais

- `GET /status`: mostra estado do no, relogio, exclusao mutua e eleicao.
- `POST /lamport/local`: registra um evento local.
- `POST /lamport/send/{peer_id}`: envia mensagem de aplicacao para outro no.
- `POST /mutex/request`: solicita entrada na secao critica.
- `POST /election/start`: inicia uma eleicao Bully.

## Organizacao do codigo

- `src/distributed_algorithms/main.py`: API FastAPI e injecao dos componentes.
- `src/distributed_algorithms/lamport.py`: relogio logico.
- `src/distributed_algorithms/mutex.py`: Ricart-Agrawala.
- `src/distributed_algorithms/election.py`: Bully Algorithm.
- `src/distributed_algorithms/transport.py`: cliente HTTP usado para troca de mensagens.
- `scripts/`: roteiros executaveis para demonstracao.
- `docs/relatorio.md`: base para o relatorio final.

## Sugestao de roteiro para o video

1. Mostrar `docker-compose.yml` e explicar que cada container representa um no.
2. Rodar `make demo-lamport` e explicar incremento no envio e atualizacao no recebimento.
3. Rodar `make demo-mutex` e explicar como os pedidos concorrentes sao ordenados por `(timestamp, node_id)`.
4. Rodar `make demo-election` e explicar a falha do lider e a eleicao do maior id ativo.
5. Mostrar rapidamente os arquivos dos algoritmos e os logs/status dos nos.
