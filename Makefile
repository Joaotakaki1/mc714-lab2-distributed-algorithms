.PHONY: build up down reset logs status demo-lamport demo-mutex demo-election demo-all demo-all-paused

build:
	docker compose build

up:
	docker compose up -d --build

down:
	docker compose down

reset: down up

logs:
	docker compose logs -f

status:
	python3 scripts/status.py

demo-lamport:
	python3 scripts/demo_lamport.py

demo-mutex:
	python3 scripts/demo_mutex.py

demo-election:
	python3 scripts/demo_election.py

demo-all:
	python3 scripts/demo_all.py

demo-all-paused:
	python3 scripts/demo_all.py --pause
