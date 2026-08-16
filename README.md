# 微服务故障诊断中枢系统（mfdh）

事件驱动的微服务诊断平台：**诊断 → 根因 → 修复建议 →（高风险人工审批）→ 工单 / 通知 → 修复验证**。

![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/NicoYazawa/microservice_diagnosis?utm_source=oss&utm_medium=github&utm_campaign=NicoYazawa%2Fmicroservice_diagnosis&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews)

## 技术栈

Go 1.25+ · gRPC · Kafka · ClickHouse · PostgreSQL · Redis · Consul · Prometheus · Gin + gRPC-Gateway

## 组件版本（2026-08-16 核实的当前最新稳定版）

| 组件 | 版本 | 说明 |
|---|---|---|
| Go | 1.26.3（本机工具链） | go.mod 最低要求 1.25 |
| Kafka | 4.3.1 | 官方 `apache/kafka` 镜像（bitnami 已停更，弃用） |
| PostgreSQL | 18-alpine（18.6） | |
| ClickHouse | 26.7（26.7.3.19-stable） | |
| Redis | 8-alpine（8.10.0） | |
| Consul | 2.0.3 | 1.x 线最新为 1.22.7 |
| Prometheus | v3.13.2 | |

## 快速开始

### 1. 启动中间件（6 个）

```bash
docker compose -f deployments/docker-compose.yml up -d
```

| 组件 | 地址 | 说明 |
|---|---|---|
| Kafka | `localhost:29092` | KRaft 单节点（宿主 9092 被系统保留，映射到 29092） |
| PostgreSQL | `localhost:5432` | 用户/密码 `mfdh/mfdh`，库 `diagnosis` |
| ClickHouse | `localhost:8123`（HTTP）/ `19000`（原生） | 原生 9000 被系统保留，映射到 19000 |
| Redis | `localhost:6379` | 开启 AOF |
| Consul | `localhost:8500` | Web UI |
| Prometheus | `localhost:29090` | 宿主 9090 被系统保留，映射到 29090 |

### 2. 构建与运行

```bash
go build ./...
go vet ./...
go run ./cmd/orchestrator -config configs/orchestrator.yaml
```

健康检查：`curl http://localhost:8080/healthz` → `ok`

## 目录结构

见 [PLAN.md](./PLAN.md) §3。

## 里程碑进度

见 [PLAN.md](./PLAN.md) §10。当前完成：**M0（工程骨架 + 基础设施 + 6 服务入口）**。
