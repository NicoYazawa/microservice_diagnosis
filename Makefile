SHELL := /bin/bash

.PHONY: build vet test tidy up down ps logs run-orchestrator run-agent-log

build:
	go build ./cmd/...

vet:
	go vet ./...

test:
	go test ./...

tidy:
	go mod tidy

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

ps:
	docker compose -f deployments/docker-compose.yml ps

logs:
	docker compose -f deployments/docker-compose.yml logs -f

run-orchestrator:
	go run ./cmd/orchestrator -config configs/orchestrator.yaml

run-agent-log:
	go run ./cmd/agent-log -config configs/agent-log.yaml
