# Microservice Fault Diagnosis Hub (mfdh)

An event-driven microservice diagnosis platform: **diagnosis -> root cause -> fix suggestion -> (human approval for high risk) -> ticket / notification -> fix verification**.

![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/NicoYazawa/microservice_diagnosis?utm_source=oss&utm_medium=github&utm_campaign=NicoYazawa%2Fmicroservice_diagnosis&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews)

## Tech Stack

Go 1.25+ · gRPC · Kafka · ClickHouse · PostgreSQL · Redis · Consul · Prometheus · Gin + gRPC-Gateway

## Component Versions (verified stable as of 2026-08-16)

| Component | Version | Notes |
|---|---|---|
| Go | 1.26.3 (local toolchain) | go.mod requires at least 1.25 |
| Kafka | 4.3.1 | Official `apache/kafka` image (bitnami discontinued) |
| PostgreSQL | 18-alpine (18.6) | |
| ClickHouse | 26.7 (26.7.3.19-stable) | |
| Redis | 8-alpine (8.10.0) | |
| Consul | 2.0.3 | Latest on the 1.x line is 1.22.7 |
| Prometheus | v3.13.2 | |

## Quick Start

### 1. Start middleware (6 services)

```bash
docker compose -f deployments/docker-compose.yml up -d
```

| Component | Address | Notes |
|---|---|---|
| Kafka | `localhost:29092` | KRaft single node (host 9092 reserved, mapped to 29092) |
| PostgreSQL | `localhost:5432` | user/password `mfdh/mfdh`, database `diagnosis` |
| ClickHouse | `localhost:8123` (HTTP) / `19000` (native) | native 9000 reserved, mapped to 19000 |
| Redis | `localhost:6379` | AOF enabled |
| Consul | `localhost:8500` | Web UI |
| Prometheus | `localhost:29090` | host 9090 reserved, mapped to 29090 |

### 2. Build and run

```bash
go build ./...
go vet ./...
go run ./cmd/orchestrator -config configs/orchestrator.yaml
```

Health check: `curl http://localhost:8080/healthz` -> `ok`

### 3. Contract generation (M1)

Contracts live in `api/proto/v1/` (observation / orchestrator / agent). One proto serves both gRPC and REST (via gRPC-Gateway).

```bash
go run ./cmd/genproto          # or: make proto-gen
```

- Cross-platform (Windows / macOS / Linux): the Go tool bootstraps protoc + 4 generator plugins into `bin/` (gitignored, no system pollution) on first run.
- Flags: `-skip-bootstrap` to regenerate only, `-only-bootstrap` to just install/verify the toolchain.
- Generated code is committed: `api/gen/<pkg>/v1/*.pb.go`, `*.pb.gw.go`, `*_grpc.pb.go`.
- OpenAPI: `api/gen/openapi/mfdh.swagger.json` (merged spec, importable into Swagger UI).
- Contract tests: `go test ./internal/observation/ -cover` (round-trip / schema_version / illegal field rejection).

### 4. Kafka message bus (M2)

`internal/bus/` wraps `segmentio/kafka-go` into three small contracts used by every service:

| API | Purpose |
|---|---|
| `bus.Producer.Publish(ctx, msg)` | Publish evidence / commands (at-least-once, key-hash partition routing) |
| `bus.Consumer.Consume(ctx, handler)` | Consume from a per-agent consumer group; offsets commit only after the handler succeeds |
| `bus.Replayer.Replay(ctx, opts)` | Re-read a topic from the earliest (or any) offset without touching consumer groups |

- Topics are namespaced: `mfdh.observations` / `mfdh.commands` / `mfdh.events` / `mfdh.dlq`.
- Consumer groups follow `mfdh-<service>-<topic>` so each agent scales independently.
- The M1 `Observation` contract rides the bus via `bus.ObservationMessage` / `bus.DecodeObservation` (protobuf binary + routing headers).
- Broker address is configurable per service: `bus.brokers` in each `configs/*.yaml` (default `localhost:29092`).

```go
producer, _ := bus.NewProducer(bus.Config{Brokers: cfg.Bus.Brokers})
msg, _ := bus.ObservationMessage(obs)
_ = producer.Publish(ctx, msg)

consumer, _ := bus.NewConsumer(bus.Config{Brokers: cfg.Bus.Brokers},
    bus.ConsumerConfig{Topic: bus.TopicObservations, GroupID: bus.GroupID("agent-log", bus.TopicObservations)})
_ = consumer.Consume(ctx, func(ctx context.Context, m bus.Message) error {
    o, err := bus.DecodeObservation(m) // -> *observationv1.Observation
    return err
})
```

- Unit tests (in-memory broker double): `go test ./internal/bus/ -cover`
- Integration tests (real Kafka, skipped when unreachable): `make test-integration`

## Platform Notes

- **Go commands** (`go build ./...`, `go vet ./...`, `go test ./...`, `go run ./cmd/genproto`) work on Windows / macOS / Linux out of the box.
- **Make targets** require GNU make + a bash shell. On Windows, use Git Bash or WSL, or run the underlying commands directly:
  - `make build` -> `go build ./cmd/...`
  - `make proto-gen` -> `go run ./cmd/genproto`
  - `make up` / `down` / `ps` / `logs` -> `docker compose -f deployments/docker-compose.yml ...`
- **Line endings** are normalized to LF via `.gitattributes`; keep your editor set to LF to avoid spurious diffs.

## Directory Structure

See [PLAN.md](./PLAN.md) section 3.

## Milestone Progress

See [PLAN.md](./PLAN.md) section 10. Currently completed: **M0 (project skeleton + infrastructure + 6 service entrypoints) + M1 (contracts: 3 protos + generation pipeline + round-trip contract tests) + M2 (Kafka message bus: Producer / Consumer / Replay with unit tests + real-broker replay use case)**.

## Language Policy

All repository content MUST be English-only (no Chinese/CJK characters), enforced by the project skill `.claude/skills/english-only/`. `PLAN.md` is the only allowed exception.
