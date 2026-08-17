-- Migrations for M3: State machine engine + data tables
-- PostgreSQL DDL aligned with PLAN 5.2/5.3
-- Run via: psql -f migrations/001_initial_schema.sql

-- ============================================================
-- diagnostic_sessions
-- ============================================================
CREATE TABLE IF NOT EXISTS diagnostic_sessions (
    id              UUID PRIMARY KEY,
    status          VARCHAR(32) NOT NULL,
    target_service  VARCHAR(128),
    trigger_type    VARCHAR(16),                     -- manual / alert
    retry_count     INT DEFAULT 0,
    report_url      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sessions_status   ON diagnostic_sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_service  ON diagnostic_sessions(target_service);
CREATE INDEX IF NOT EXISTS idx_sessions_created  ON diagnostic_sessions(created_at);

-- ============================================================
-- fix_actions
-- ============================================================
CREATE TABLE IF NOT EXISTS fix_actions (
    id               UUID PRIMARY KEY,
    session_id       UUID NOT NULL REFERENCES diagnostic_sessions(id) ON DELETE CASCADE,
    seq              INT NOT NULL,
    action_type      VARCHAR(64) NOT NULL,          -- restart_pod / scale_up / switch_master / ...
    target           VARCHAR(128),
    risk             VARCHAR(8) NOT NULL,           -- LOW / MEDIUM / HIGH
    rollback_plan    TEXT NOT NULL,
    requires_approval BOOLEAN DEFAULT false,
    approval_status  VARCHAR(16) DEFAULT 'NONE',   -- NONE / PENDING / APPROVED / REJECTED
    execution_status VARCHAR(16) DEFAULT 'NOT_STARTED', -- NOT_STARTED / RUNNING / SUCCEEDED / FAILED / ROLLED_BACK
    ticket_id        VARCHAR(64),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT      uq_fix_actions_session_seq UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_fix_actions_session ON fix_actions(session_id);
CREATE INDEX IF NOT EXISTS idx_fix_actions_status  ON fix_actions(execution_status);

-- ============================================================
-- approvals
-- ============================================================
CREATE TABLE IF NOT EXISTS approvals (
    id             UUID PRIMARY KEY,
    session_id     UUID NOT NULL REFERENCES diagnostic_sessions(id) ON DELETE CASCADE,
    fix_action_id  UUID NOT NULL REFERENCES fix_actions(id) ON DELETE CASCADE,
    status         VARCHAR(16) NOT NULL,            -- PENDING / APPROVED / REJECTED / EXPIRED
    request_token  VARCHAR(128) NOT NULL,
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at     TIMESTAMPTZ,
    decided_by     VARCHAR(64)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_approvals_token    ON approvals(request_token);
CREATE INDEX IF NOT EXISTS idx_approvals_session         ON approvals(session_id);
CREATE INDEX IF NOT EXISTS idx_approvals_status           ON approvals(status);

-- ============================================================
-- fix_knowledge_base
-- ============================================================
CREATE TABLE IF NOT EXISTS fix_knowledge_base (
    id                  UUID PRIMARY KEY,
    root_cause_pattern  TEXT NOT NULL,
    fix_steps           JSONB NOT NULL,             -- [{action_type, target, risk, rollback_plan}, ...]
    risk                VARCHAR(8) NOT NULL,
    rollback_plan       TEXT NOT NULL,
    times_used          INT DEFAULT 0,
    success_rate        REAL DEFAULT 0
);

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_kb_pattern ON fix_knowledge_base USING gin (root_cause_pattern gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_kb_success ON fix_knowledge_base (success_rate DESC, times_used DESC);

-- ============================================================
-- webhook_configs
-- ============================================================
CREATE TABLE IF NOT EXISTS webhook_configs (
    id       UUID PRIMARY KEY,
    name     VARCHAR(64),
    channel  VARCHAR(16),                           -- feishu / dingtalk / slack / generic
    url      TEXT NOT NULL,
    secret   TEXT,
    enabled  BOOLEAN DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_webhook_configs_channel ON webhook_configs(channel);
CREATE INDEX IF NOT EXISTS idx_webhook_configs_enabled ON webhook_configs(enabled);

-- ============================================================
-- webhook_deliveries
-- ============================================================
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id            UUID PRIMARY KEY,
    session_id    UUID REFERENCES diagnostic_sessions(id) ON DELETE SET NULL,
    channel       VARCHAR(16),
    status        VARCHAR(16),                      -- PENDING / SUCCESS / FAILED
    attempt       INT DEFAULT 0,
    last_error    TEXT,
    delivered_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_session ON webhook_deliveries(session_id);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_status  ON webhook_deliveries(status);
