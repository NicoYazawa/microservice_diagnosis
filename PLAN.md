# 微服务故障诊断中枢系统（mfdh）开发计划

> 版本：v1.0 定稿
> 拟定日期：2026-08-16
> 状态：已评审确认，按里程碑执行
> 模块路径：`github.com/microservice-diagnosis/diagnosis-hub`

## 1. 项目概述

事件驱动的微服务诊断平台，核心能力：

- **标准化证据模型**：统一 `Observation` 契约，作为 Agent 间交互的唯一数据契约。
- **事件驱动解耦**：Orchestrator 与 Agent 组通过 Kafka 完全异步通信。
- **可插拔 Agent**：Log / Metric / Trace / RCA / Fix 各自独立部署、升级、扩缩容。
- **轻量级工作流**：PostgreSQL 状态机驱动诊断会话生命周期，无分布式事务。
- **修复闭环**：诊断 → 根因 → 修复建议 →（高风险人工审批）→ 执行 / 工单 / 通知 → 修复验证。

## 2. 技术栈

| 层 | 组件 | 说明 |
|---|---|---|
| 语言 | Go 1.25+ | 全服务 Go 实现 |
| 消息总线 | Kafka（`segmentio/kafka-go`） | Agent 独立 Consumer Group + 消息回放 |
| 日志/链路存储 | ClickHouse（`clickhouse-go/v2`） | 按 timestamp/service_name 分区 + 物化视图 |
| 指标存储 | Prometheus | 指标异常检测 / PromQL |
| 关系/状态库 | PostgreSQL（`pgx/v5`） | 会话 / 规则 / fix / approval / 知识库表 |
| 缓存/锁 | Redis（`go-redis/v9`） | 定时扫描单实例执行锁 + 瞬时 TopK 缓存 |
| 状态机 | 自研 + PG `SELECT ... FOR UPDATE SKIP LOCKED` | 任务抢占；定时扫描驱动 |
| LLM 客户端 | 自研 HTTP Client | OpenAI/Claude/Ollama，指数退避重试 + Token 限流 |
| 服务发现 | Consul（`hashicorp/consul/api`） | 抽象接口；K8s Service DNS 为备选实现 |
| API | Gin + gRPC-Gateway | 一份 .proto 双用（gRPC + REST） |

### 2.1 已确认的关键决策

- **Q1 指标/链路数据源**：日志 + 链路 → ClickHouse；指标 → Prometheus（技术栈新增 Prometheus）。
- **Q2 Redis 分工**：PG `SKIP LOCKED` = 状态机任务抢占；Redis 锁 = 定时扫描单实例执行 + 瞬时 TopK 缓存。
- **Q3 触发方式**：REST 手动触发（MVP）+ `ALERT` 告警自动触发（增强），MVP 先做 REST 手动。
- **Q4 服务发现**：抽象接口，Consul 默认；K8s Service DNS 备选；MVP 仅实现 Consul。

### 2.2 组件版本（2026-08-16 核实的当前最新稳定版）

| 组件 | 版本 | 说明 |
|---|---|---|
| Go | 1.26.3（本机工具链） | go.mod 最低要求 1.25 |
| Kafka | 4.3.1 | 官方 `apache/kafka` 镜像（bitnami/kafka 已停更，弃用） |
| PostgreSQL | 18.6 | `postgres:18-alpine` |
| ClickHouse | 26.7.3.19-stable | |
| Redis | 8.10.0 | `redis:8-alpine` |
| Consul | 2.0.3 | 1.x 线最新为 1.22.7 |
| Prometheus | 3.13.2 | |
| Viper | v1.21.0 | Go 配置依赖（goproxy.cn 拉取） |

## 3. 目录结构（monorepo）

```
microservice_diagnosis/
├── go.mod / go.sum / Makefile / .gitignore / README.md
├── api/
│   ├── proto/v1/                 # observation.proto / orchestrator.proto / agent.proto
│   └── gen/                      # pb.go / grpc.pb.go / pb.gw.go / openapi（自动生成）
├── third_party/google/api/       # gRPC-Gateway 注解依赖
├── cmd/                          # orchestrator + agent-log/metric/trace/rca/fix（6 入口）
├── internal/
│   ├── config/                   # viper 配置加载
│   ├── logger/                   # slog 统一日志
│   ├── observation/              # Observation 封装 / 校验 / 构造器
│   ├── bus/                      # kafka-go：Producer / Consumer / 主题 / 回放
│   ├── store/                    # pg / clickhouse / redis 连接池 + DAO + 迁移
│   ├── workflow/                 # 状态机引擎（SKIP LOCKED + 定时扫描 + 转移表）
│   ├── agent/                    # Agent 接口 + 5 实现；agent/fix 内含知识库 + 风险评估
│   ├── llm/                      # LLM HTTP Client
│   ├── discovery/                # Consul 注册 / 发现（抽象接口）
│   ├── server/                   # Gin + gRPC-Gateway 装配 + SSE
│   ├── executor/                 # 自动执行器：noop / k8s / cloud
│   ├── notify/                   # 通知层：工单(jira/pagerduty) + webhook(feishu/dingtalk/slack)
│   ├── approval/                 # 人工审批门（client + 回调）
│   └── report/                   # 诊断报告渲染（Markdown/PDF + 图表）
├── web/                          # Web Dashboard（可选，Admin 模板）
├── migrations/                   # PG + ClickHouse DDL
├── configs/                      # 各服务 yaml
├── deployments/docker-compose.yml # 本地一键拉起中间件
├── deploy/k8s/                   # K8s 部署清单（base + overlays）
├── chart/                        # Helm Chart（mfdh）
├── demo/                         # 靶场：目标微服务 + loadgen + otel-collector + 场景
├── scripts/                      # gen-proto.ps1 / e2e / demo
└── docs/                         # architecture / api / ops / dev / reports
```

## 4. Observation 统一证据模型

### 4.1 字段定义（定稿，13 字段）

| # | 字段 | 类型 | 含义 |
|---|---|---|---|
| 1 | `id` | string | 证据唯一 ID（UUID/雪花），幂等去重与主键 |
| 2 | `session_id` | string | 关联诊断会话 ID |
| 3 | `source` | string | 产生证据的 Agent 逻辑名（agent-log / agent-rca / ...） |
| 4 | `type` | enum `EvidenceType` | 证据大类：LOG / METRIC / TRACE / ALERT / RCA_RESULT / FIX_ACTION |
| 5 | `sub_type` | string | 证据子类：log_pattern / metric_anomaly / trace_bottleneck / ... |
| 6 | `confidence` | double | 置信度 0.0 ~ 1.0 |
| 7 | `severity` | enum `Severity` | DEBUG ~ FATAL |
| 8 | `target_service` | string | 目标服务名 |
| 9 | `correlations` | map<string,string> | 关联标识：trace_id / span_id / request_id |
| 10 | `detail_json` | string | 具体证据 JSON 字符串（保真载荷） |
| 11 | `labels` | map<string,string> | 静态标签：env / region / pod / host |
| 12 | `timestamp` | google.protobuf.Timestamp | 证据产生时间 |
| 13 | `schema_version` | int64 | 契约版本号（演进兼容判断） |

### 4.2 proto 草案

```proto
syntax = "proto3";
package observation.v1;
option go_package = "github.com/microservice-diagnosis/diagnosis-hub/api/gen/observation/v1;observationv1";

import "google/protobuf/timestamp.proto";

enum EvidenceType {
  EVIDENCE_TYPE_UNSPECIFIED = 0;
  EVIDENCE_TYPE_LOG         = 1;
  EVIDENCE_TYPE_METRIC      = 2;
  EVIDENCE_TYPE_TRACE       = 3;
  EVIDENCE_TYPE_ALERT       = 4;
  EVIDENCE_TYPE_RCA_RESULT  = 5;
  EVIDENCE_TYPE_FIX_ACTION  = 6;
}

enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_DEBUG       = 1;
  SEVERITY_INFO        = 2;
  SEVERITY_WARN        = 3;
  SEVERITY_ERROR       = 4;
  SEVERITY_FATAL       = 5;
}

message Observation {
  string id                 = 1;
  string session_id         = 2;
  string source             = 3;
  EvidenceType type         = 4;
  string sub_type           = 5;
  double confidence         = 6;
  Severity severity         = 7;
  string target_service     = 8;
  map<string, string> correlations = 9;
  string detail_json        = 10;
  map<string, string> labels = 11;
  google.protobuf.Timestamp timestamp = 12;
  int64 schema_version      = 13;
}
```

### 4.3 子类映射（sub_type）

| 场景 | type | sub_type |
|---|---|---|
| 日志模式 | LOG | log_pattern |
| 指标异常 | METRIC | metric_anomaly |
| 链路瓶颈 | TRACE | trace_bottleneck |
| 扩展 | ALERT / RCA_RESULT / FIX_ACTION | 自定义 |

## 5. 状态机设计

### 5.1 状态集

```
CREATED → COLLECTING → ANALYZING → RCA_DONE → FIX_PROPOSED
   └─(含 HIGH 风险) → AWAITING_APPROVAL → FIX_EXECUTING → VERIFYING → RESOLVED
                                              └─(异常未消除) → ROLLED_BACK
终态：FIX_SUGGESTED / RESOLVED / REJECTED / ROLLED_BACK / FAILED / IGNORED
```

### 5.2 核心状态转移表

| 当前状态 | 事件/条件 | 下一状态 |
|---|---|---|
| CREATED | 下发采集 | COLLECTING |
| COLLECTING | 证据齐全 | ANALYZING |
| ANALYZING | RCA 完成 | RCA_DONE |
| RCA_DONE | Fix 生成 | FIX_PROPOSED |
| FIX_PROPOSED | 含 HIGH 风险步骤 | AWAITING_APPROVAL |
| FIX_PROPOSED | 无 HIGH 风险 | FIX_SUGGESTED（默认只给建议） |
| AWAITING_APPROVAL | 人工批准 | FIX_EXECUTING |
| AWAITING_APPROVAL | 人工驳回 | REJECTED |
| FIX_EXECUTING | 执行完成 | VERIFYING |
| VERIFYING | 异常消除 | RESOLVED |
| VERIFYING | 异常未消除 | ROLLED_BACK |
| 任意 | 用户忽略 | IGNORED |
| 任意 | 系统错误 | FAILED |

### 5.3 PostgreSQL 表结构

```sql
-- 诊断会话
CREATE TABLE diagnostic_sessions (
  id UUID PRIMARY KEY,
  status VARCHAR(32) NOT NULL,
  target_service VARCHAR(128),
  trigger_type VARCHAR(16),          -- manual / alert
  retry_count INT DEFAULT 0,
  report_url TEXT,
  created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
);
CREATE INDEX idx_sessions_status  ON diagnostic_sessions(status);
CREATE INDEX idx_sessions_service ON diagnostic_sessions(target_service);
CREATE INDEX idx_sessions_created ON diagnostic_sessions(created_at);

-- 修复动作
CREATE TABLE fix_actions (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL REFERENCES diagnostic_sessions(id),
  seq INT NOT NULL,
  action_type VARCHAR(64) NOT NULL,  -- restart_pod / scale_up / switch_master / ...
  target VARCHAR(128),
  risk VARCHAR(8) NOT NULL,          -- LOW / MEDIUM / HIGH
  rollback_plan TEXT NOT NULL,
  requires_approval BOOLEAN DEFAULT false,
  approval_status VARCHAR(16),       -- NONE / PENDING / APPROVED / REJECTED
  execution_status VARCHAR(16),      -- NOT_STARTED / RUNNING / SUCCEEDED / FAILED / ROLLED_BACK
  ticket_id VARCHAR(64),
  created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
);

-- 审批请求
CREATE TABLE approvals (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL,
  fix_action_id UUID NOT NULL,
  status VARCHAR(16) NOT NULL,       -- PENDING / APPROVED / REJECTED / EXPIRED
  request_token VARCHAR(128) NOT NULL,
  requested_at TIMESTAMPTZ, decided_at TIMESTAMPTZ, decided_by VARCHAR(64)
);

-- 修复知识库
CREATE TABLE fix_knowledge_base (
  id UUID PRIMARY KEY,
  root_cause_pattern TEXT NOT NULL,
  fix_steps JSONB NOT NULL,
  risk VARCHAR(8) NOT NULL,
  rollback_plan TEXT NOT NULL,
  times_used INT DEFAULT 0,
  success_rate REAL DEFAULT 0
);

-- Webhook 配置与投递日志
CREATE TABLE webhook_configs (
  id UUID PRIMARY KEY,
  name VARCHAR(64), channel VARCHAR(16),  -- feishu / dingtalk / slack / generic
  url TEXT NOT NULL, secret TEXT, enabled BOOLEAN DEFAULT true
);
CREATE TABLE webhook_deliveries (
  id UUID PRIMARY KEY,
  session_id UUID, channel VARCHAR(16),
  status VARCHAR(16), attempt INT DEFAULT 0,
  last_error TEXT, delivered_at TIMESTAMPTZ, created_at TIMESTAMPTZ
);
```

## 6. 修复闭环设计

| 环节 | 设计 |
|---|---|
| Fix Agent | 基于 RCA 查 `fix_knowledge_base` + LLM 组织步骤措辞，输出步骤序列 + 每步风险 + 回滚方案 |
| 风险评估 | 规则表兜底（restart_pod=LOW / scale_up=LOW / config_change=MEDIUM / scale_down·switch_master·data_migration=HIGH），LLM 不得降级风险 |
| 人工审批门 | HIGH 风险必须审批：Orchestrator 置 `AWAITING_APPROVAL` + 写 approvals 行 → API Gateway 发确认 → 人工回调 `POST /v1/sessions/{id}/approvals/{aid}/decision` |
| 自动执行器 | `Executor` 接口（noop 默认 / k8s / cloud），开关 `fix.auto_execute=false` 默认关闭；执行结果回写 FIX_ACTION Observation |
| 工单集成 | `IncidentNotifier` 接口（jira / pagerduty / noop），Fix 生成并审批通过后自动建单，附诊断报告链接 |

### 6.1 核心接口抽象（Go）

```go
// internal/agent/fix
type KnowledgeBase interface {
  Search(ctx context.Context, rca *RootCauseAnalysis) ([]FixCandidate, error)
}
type RiskAssessor interface {
  Assess(ctx context.Context, step FixStep) (RiskLevel, rollback string, err error)
}

// internal/executor
type Executor interface {
  Execute(ctx context.Context, a FixAction) (*ExecutionResult, error)
  Rollback(ctx context.Context, a FixAction) error
}

// internal/notify（工单 + webhook 统一通知层）
type IncidentNotifier interface {
  CreateIncident(ctx context.Context, i Incident) (ticketID string, err error)
}
type WebhookNotifier interface {
  Notify(ctx context.Context, e DiagnosticEvent) error
}

// internal/approval
type ApprovalClient interface {
  RequestApproval(ctx context.Context, req ApprovalRequest) (token string, err error)
}
```

## 7. 用户 API（RESTful，gRPC-Gateway 同源生成）

| 方法 | 端点 | 说明 |
|---|---|---|
| POST | `/v1/sessions` | 创建诊断会话 |
| GET | `/v1/sessions` | 列表：`page` / `page_size` + `status` / `target_service` / `from` / `to` / `keyword` 过滤 |
| GET | `/v1/sessions/{id}` | 详情：时间线 + 证据 + 根因 + 修复 |
| POST | `/v1/sessions/{id}/start` | 触发诊断 |
| POST | `/v1/sessions/{id}/retry` | 重试诊断 |
| POST | `/v1/sessions/{id}/ignore` | 忽略（抑制告警/工单） |
| POST | `/v1/sessions/{id}/approvals/{aid}/decision` | 人工审批回调 |
| GET | `/v1/sessions/{id}/report` | 报告（`format=markdown\|pdf`） |
| GET | `/v1/sessions/{id}/report/download` | 报告下载 |
| GET | `/v1/agents` | Agent 健康状态（Consul） |

## 8. 报告 / 通知 / Dashboard

### 8.1 诊断报告（internal/report/）
- 内容：时间线 + 证据（指标图 / 链路图 / 日志原文）+ 根因 + 修复建议。
- 格式：Markdown（主）+ PDF（Markdown→HTML→PDF；无外部二进制时退化 maroto/gofpdf）。
- 图表：go-echarts 渲染 Prometheus 指标时序图 / 调用链瀑布图。

### 8.2 Webhook 通知（internal/notify/）
- 渠道：飞书 / 钉钉 / Slack / 自定义（generic）。
- 触发：会话到达终态，推送摘要 + 报告链接。
- 可靠：指数退避重试 + `webhook_deliveries` 投递日志 + 加签校验。

### 8.3 Web Dashboard（web/，可选）
- 页面：会话列表、会话详情、实时状态看板（SSE）、Agent 健康。
- 方案：轻量页面 + 现成 Admin 模板（Tabler / AdminLTE）。

## 9. 部署与文档

### 9.1 K8s 部署（deploy/k8s/）
- 每服务：Deployment + Service + ConfigMap + HPA（min 1 / max 10 / CPU 70%）。
- Secret：External Secrets / SealedSecrets 引用，不提交明文。
- 消费型 Agent 后续可切换 KEDA（按 Kafka lag）扩缩容（增强）。

### 9.2 Helm Chart（chart/）
- 命令：`helm install mfdh ./chart -n diagnosis --create-namespace`。
- `values.yaml`：镜像仓库/tag、各服务副本数、资源限制、HPA 阈值、中间件端点、Secret、功能开关（`fix.auto_execute`、webhook 渠道等）。

### 9.3 文档（docs/）
- `ops/`：operations-manual（运维手册）、troubleshooting（Agent 故障排查）、backup-strategy（数据库备份策略：PG pg_dump + WAL PITR、ClickHouse clickhouse-backup / FREEZE、Redis RDB；RPO ≤ 24h / RTO ≤ 1h）。
- `dev/`：adding-an-agent（如何新增 Agent）、extending-knowledge-base（如何扩展知识库）、local-development。
- `api/`：protoc-gen-openapiv2 自动生成 OpenAPI/Swagger。

## 10. 里程碑 M0 ~ M11

| 里程碑 | 内容 | 完成定义（DoD） |
|---|---|---|
| M0 | 工程骨架 + 6 中间件 compose + 6 服务入口 | `go build ./...` 通过 + `/healthz` 可探活 + compose healthy |
| M1 | 契约（3 proto + 生成管线） | 生成成功 + round-trip 契约测试 |
| M2 | Kafka 消息总线 | 单测 + 消息回放用例 |
| M3 | 状态机引擎 + 数据表 | 状态转移用例 + SKIP LOCKED 并发测试 |
| M4 | 5 个 Agent（Fix 含知识库 + 风险评估） | 各 Agent 单测 |
| M5 | 网关 + Consul + 完整 REST API | 列表/详情/重试/忽略 + 审批/报告端点 |
| M6 | 修复闭环（审批门 + 执行器 + 工单） | HIGH 审批流转 + 工单创建 |
| M7 | 报告 + 通知 + Dashboard(可选) | Markdown/PDF 报告 + Webhook 投递 |
| M8 | 端到端联调 | 黄金路径闭环跑通 |
| M9 | 靶场验证（自研 + OpenTelemetry Demo） | 验证报告 + 根因命中率 |
| M10 | K8s 清单 + Helm Chart | `helm install mfdh` 一键部署 |
| M11 | 文档 | 运维手册 / 开发文档 / OpenAPI |

## 11. 验收方案

### 11.1 三层验收
- **契约层**：Observation round-trip、schema_version 校验、非法字段拒绝。
- **功能层**：黄金路径（故障 → 采集 → RCA → Fix → 审批 → 工单 → 通知 → 报告）。
- **非功能层**：消息不丢失/可回放、Agent 独立扩缩容、并发无重复处理。

### 11.2 靶场验证（M9）
- **自研靶场**：订单系统（gateway / order / payment / inventory + loadgen），5 个已知根因故障场景（N+1 慢查询、DB 连接池耗尽、超时级联、goroutine 泄漏、代码 bug 5xx）。
- **开源 Demo**：OpenTelemetry Demo，验证通用性（换数据源不换诊断逻辑）。
- **量化指标**：根因命中率（目标 ≥ 4/5）、平均诊断耗时、证据完整率。

### 11.3 工具链
- `go test ./... -cover`、`go vet ./...`、`golangci-lint run`、`go test -tags=integration ./...`、e2e/demo 脚本。

