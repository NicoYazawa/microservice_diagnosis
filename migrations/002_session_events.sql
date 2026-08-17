-- M7: Session event log for timeline tracking
-- Stores every state transition and key events for report rendering.
-- Run via: psql -f migrations/002_session_events.sql

CREATE TABLE IF NOT EXISTS session_events (
    id          UUID PRIMARY KEY,
    session_id  UUID NOT NULL REFERENCES diagnostic_sessions(id) ON DELETE CASCADE,
    event_type  VARCHAR(64) NOT NULL,   -- created / started / state_changed / approval_requested / approval_decided / fix_executed / resolved / ...
    source      VARCHAR(64) NOT NULL,   -- orchestrator / agent-log / agent-fix / approval / ...
    message     TEXT,
    metadata    JSONB,                  -- extra context (new_status, old_status, fix_action_id, ...)
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_session_events_session ON session_events(session_id);
CREATE INDEX IF NOT EXISTS idx_session_events_type    ON session_events(event_type);
CREATE INDEX IF NOT EXISTS idx_session_events_time    ON session_events(timestamp);
