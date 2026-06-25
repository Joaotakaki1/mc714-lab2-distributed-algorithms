# Relatorio - MC714 Lab 2

## Participantes

- Nome:
- RA:
- Nome:
- RA:

## Problema

Este trabalho implementa algoritmos classicos de sistemas distribuidos usando processos independentes que se comunicam por mensagens HTTP. Cada processo representa um no do sistema e executa em um container Docker.

## Ambiente de execucao

- Linguagem: Python
- API/comunicacao: FastAPI + HTTP
- Execucao local: Docker Compose
- Numero de nos: 5

## Algoritmos

### Relogio logico de Lamport

Cada no mantem um contador local. Em eventos locais e envios de mensagens, o contador e incrementado. No recebimento de mensagem, o contador passa a ser `max(relogio_local, timestamp_recebido) + 1`.

### Exclusao mutua - Ricart-Agrawala

Quando um no quer entrar na secao critica, ele envia uma requisicao para todos os demais nos com seu timestamp de Lamport. Um no responde imediatamente se nao estiver na secao critica e se nao tiver prioridade sobre o pedido recebido. Caso contrario, adia a resposta ate sair da secao critica. O solicitante so entra quando recebe resposta de todos os demais nos.

### Eleicao de lider - Bully Algorithm

Quando um no detecta a necessidade de eleicao, ele envia mensagens para os nos com identificador maior. Se nenhum no maior responder, ele se torna lider. Se algum no maior responder, ele aguarda uma mensagem de coordenador. No experimento, simulamos a falha do maior no parando seu container.

## Implementacao

Descrever:

- Estrutura dos containers.
- Endpoints usados para mensagens internas.
- Como os scripts de demonstracao exercitam os algoritmos.
- Como os logs/status comprovam o comportamento observado.

## Experimentos

### Experimento 1 - Lamport

Comando:

```bash
make demo-lamport
```

Resultado esperado: os timestamps preservam a relacao causal entre envio e recebimento.

### Experimento 2 - Exclusao mutua

Comando:

```bash
make demo-mutex
```

Resultado esperado: mesmo com pedidos concorrentes, apenas um no entra na secao critica por vez.

### Experimento 3 - Eleicao

Comando:

```bash
make demo-election
```

Resultado esperado: apos parar o no 5, o no 4 e eleito lider.

## Referencias

- L. Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System", 1978.
- G. Ricart and A. K. Agrawala, "An Optimal Algorithm for Mutual Exclusion in Computer Networks", 1981.
- H. Garcia-Molina, "Elections in a Distributed Computing System", 1982.
- Documentacao do FastAPI: https://fastapi.tiangolo.com/
- Documentacao do Docker Compose: https://docs.docker.com/compose/

