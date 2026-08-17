// Package workflow implements the diagnostic session state machine engine.
// It drives PLAN 5.1/5.2 using PostgreSQL + SELECT ... FOR UPDATE SKIP LOCKED
// for task 抢占 and periodic polling for timed transitions.
//
// Concurrency model:
//   - Multiple orchestrator instances run the sweep loop concurrently.
//   - SKIP LOCKED ensures only one instance picks up a given session at a time.
//   - State transitions are idempotent: a transition that is already done is a no-op.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
)

// ErrInvalidTransition is returned when a state transition is not allowed.
var ErrInvalidTransition = errors.New("workflow: invalid state transition")

// ErrSessionNotFound is returned when the session does not exist.
var ErrSessionNotFound = errors.New("workflow: session not found")

// Transition events that drive the state machine.
const (
	EventStartCollect     = "START_COLLECT"
	EventCollectComplete  = "COLLECT_COMPLETE"
	EventAnalysisComplete = "ANALYSIS_COMPLETE"
	EventRCAGenerated     = "RCA_GENERATED"
	EventFixGenerated     = "FIX_GENERATED"
	EventApprove          = "APPROVE"
	EventReject           = "REJECT"
	EventExecute          = "EXECUTE"
	EventExecuteComplete  = "EXECUTE_COMPLETE"
	EventVerifySuccess    = "VERIFY_SUCCESS"
	EventVerifyFailure    = "VERIFY_FAILURE"
	EventIgnore           = "IGNORE"
	EventFail             = "FAIL"
	EventRetry            = "RETRY"
)

// Status constants (mirrors store package, re-declared here to avoid import cycle).
const (
	StatusCreated           = "CREATED"
	StatusCollecting        = "COLLECTING"
	StatusAnalyzing         = "ANALYZING"
	StatusRCADone           = "RCA_DONE"
	StatusFixProposed       = "FIX_PROPOSED"
	StatusAwaitingApproval  = "AWAITING_APPROVAL"
	StatusFixSuggested      = "FIX_SUGGESTED"
	StatusFixExecuting      = "FIX_EXECUTING"
	StatusVerifying         = "VERIFYING"
	StatusResolved          = "RESOLVED"
	StatusRolledBack        = "ROLLED_BACK"
	StatusRejected          = "REJECTED"
	StatusIgnored           = "IGNORED"
	StatusFailed            = "FAILED"
)

// ValidTransitions maps (currentStatus, event) -> nextStatus.
// Empty nextStatus means the transition is not allowed.
var ValidTransitions = map[string]map[string]string{
	StatusCreated: {
		EventStartCollect: store.StatusCollecting,
		EventIgnore:       store.StatusIgnored,
		EventFail:         store.StatusFailed,
	},
	StatusCollecting: {
		EventCollectComplete: store.StatusAnalyzing,
		EventIgnore:          store.StatusIgnored,
		EventFail:            store.StatusFailed,
	},
	StatusAnalyzing: {
		EventAnalysisComplete: store.StatusRCADone,
		EventIgnore:           store.StatusIgnored,
		EventFail:             store.StatusFailed,
	},
	StatusRCADone: {
		EventRCAGenerated: store.StatusFixProposed,
		EventIgnore:       store.StatusIgnored,
		EventFail:         store.StatusFailed,
	},
	StatusFixProposed: {
		// Next status determined by HasHighRiskFixAction (see determineFixProposedNextStatus).
		EventApprove:      store.StatusFixExecuting, // direct approve when no HIGH risk
		EventFixGenerated: store.StatusFixSuggested, // no HIGH risk steps
		EventIgnore:       store.StatusIgnored,
		EventFail:         store.StatusFailed,
	},
	StatusAwaitingApproval: {
		EventApprove: store.StatusFixExecuting,
		EventReject:  store.StatusRejected,
		EventIgnore:  store.StatusIgnored,
		EventFail:    store.StatusFailed,
	},
	StatusFixSuggested: {
		EventExecute: store.StatusFixExecuting,
		EventIgnore:  store.StatusIgnored,
		EventFail:    store.StatusFailed,
	},
	StatusFixExecuting: {
		EventExecuteComplete: store.StatusVerifying,
		EventIgnore:          store.StatusIgnored,
		EventFail:            store.StatusFailed,
	},
	StatusVerifying: {
		EventVerifySuccess: store.StatusResolved,
		EventVerifyFailure: store.StatusRolledBack,
		EventIgnore:        store.StatusIgnored,
		EventFail:          store.StatusFailed,
	},
	StatusResolved:   {}, // terminal
	StatusRejected:   {}, // terminal
	StatusRolledBack: {}, // terminal
	StatusIgnored:    {}, // terminal
	StatusFailed:     {}, // terminal
}

// Engine is the workflow state machine engine.
type Engine struct {
	pool           *pgxpool.Pool
	sessionDAO     *store.SessionDAO
	fixDAO         *store.FixActionDAO
	sessionEventDAO *store.SessionEventDAO
	log            *slog.Logger
}

// NewEngine creates a workflow engine.
func NewEngine(pool *pgxpool.Pool, log *slog.Logger) *Engine {
	return &Engine{
		pool:           pool,
		sessionDAO:     store.NewSessionDAO(pool),
		fixDAO:         store.NewFixActionDAO(pool),
		sessionEventDAO: store.NewSessionEventDAO(pool),
		log:            log,
	}
}

// SetSessionEventDAO replaces the session event DAO (useful when the engine
// is created before the DAO is available, e.g. in bootstrapping).
func (e *Engine) SetSessionEventDAO(dao *store.SessionEventDAO) {
	e.sessionEventDAO = dao
}

// Transition applies an event to a session and returns the new status.
// It uses a database transaction with FOR UPDATE SKIP LOCKED to ensure
// atomic state transitions under concurrent access.
func (e *Engine) Transition(ctx context.Context, sessionID uuid.UUID, event string) (string, error) {
	for attempt := 0; attempt < 3; attempt++ {
		newStatus, err := e.tryTransition(ctx, sessionID, event)
		if err == nil {
			return newStatus, nil
		}
		if errors.Is(err, ErrInvalidTransition) || errors.Is(err, ErrSessionNotFound) {
			return "", err
		}
		// Retry on transient errors (serialization failures, lock timeouts).
		time.Sleep(50 * time.Millisecond)
	}
	return "", fmt.Errorf("workflow: transition failed after 3 attempts")
}

func (e *Engine) tryTransition(ctx context.Context, sessionID uuid.UUID, event string) (string, error) {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("workflow: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Lock the session row and read current status with SKIP LOCKED.
	var status string
	err = tx.QueryRow(ctx, `
		SELECT status FROM diagnostic_sessions
		WHERE id = $1
		FOR UPDATE SKIP LOCKED`, sessionID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrSessionNotFound
		}
		return "", fmt.Errorf("workflow: lock session: %w", err)
	}

	// Look up the valid next status.
	eventMap, ok := ValidTransitions[status]
	if !ok {
		return "", fmt.Errorf("workflow: unknown current status %q", status)
	}
	nextStatus, allowed := eventMap[event]
	if !allowed {
		return "", fmt.Errorf("%w: %s + %s", ErrInvalidTransition, status, event)
	}

	// Special handling for FIX_PROPOSED: check for HIGH risk fix actions.
	if status == store.StatusFixProposed && event == EventFixGenerated {
		nextStatus, err = e.determineFixProposedNextStatus(ctx, tx, sessionID)
		if err != nil {
			return "", err
		}
	}

	// Update the status.
	_, err = tx.Exec(ctx, `
		UPDATE diagnostic_sessions SET status = $2, updated_at = $3 WHERE id = $1`,
		sessionID, nextStatus, time.Now().UTC())
	if err != nil {
		return "", fmt.Errorf("workflow: update status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("workflow: commit: %w", err)
	}

	e.log.Info("workflow transition", "session_id", sessionID, "from", status, "to", nextStatus, "event", event)

	// Record session event for timeline.
	e.sessionEventDAO.LogStateTransition(ctx, sessionID, status, nextStatus, "workflow")

	return nextStatus, nil
}

// determineFixProposedNextStatus checks whether any fix action has HIGH risk.
// If so, the session moves to AWAITING_APPROVAL; otherwise it goes to FIX_SUGGESTED.
func (e *Engine) determineFixProposedNextStatus(ctx context.Context, tx pgx.Tx, sessionID uuid.UUID) (string, error) {
	var count int
	err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM fix_actions
		WHERE session_id = $1 AND risk = $2 AND execution_status = $3`,
		sessionID, store.RiskHigh, store.ExecStatusNotStarted).Scan(&count)
	if err != nil {
		return "", fmt.Errorf("workflow: check high risk: %w", err)
	}
	if count > 0 {
		return store.StatusAwaitingApproval, nil
	}
	return store.StatusFixSuggested, nil
}

// HasHighRiskFixAction checks if a session has any HIGH risk fix actions pending.
func (e *Engine) HasHighRiskFixAction(ctx context.Context, sessionID uuid.UUID) (bool, error) {
	var count int
	err := e.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM fix_actions
		WHERE session_id = $1 AND risk = $2 AND execution_status = $3`,
		sessionID, store.RiskHigh, store.ExecStatusNotStarted).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("workflow: check high risk: %w", err)
	}
	return count > 0, nil
}

// Sweep scans for sessions that need periodic processing.
// It uses SKIP LOCKED to allow multiple instances to run concurrently.
func (e *Engine) Sweep(ctx context.Context) error {
	// Scan for sessions stuck in COLLECTING for too long → auto-fail after 30 min.
	deadline := time.Now().UTC().Add(-30 * time.Minute)
	rows, err := e.pool.Query(ctx, `
		SELECT id FROM diagnostic_sessions
		WHERE status = $1 AND updated_at < $2
		FOR UPDATE SKIP LOCKED`, store.StatusCollecting, deadline)
	if err != nil {
		return fmt.Errorf("workflow: sweep query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			e.log.Error("workflow: scan stuck session", "error", err)
			continue
		}
		if _, err := e.Transition(ctx, id, EventFail); err != nil {
			e.log.Error("workflow: auto-fail stuck session", "session_id", id, "error", err)
		}
	}
	return nil
}

// SweepFixProposed scans FIX_PROPOSED sessions and evaluates whether they
// need approval or can move to FIX_SUGGESTED. Also handles sessions ready to
// execute fixes or verify results.
func (e *Engine) SweepFixProposed(ctx context.Context) error {
	// Find FIX_PROPOSED sessions that need their approval status re-evaluated.
	rows, err := e.pool.Query(ctx, `
		SELECT id FROM diagnostic_sessions WHERE status = $1`,
		store.StatusFixProposed)
	if err != nil {
		return fmt.Errorf("workflow: sweep fix_proposed query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			e.log.Error("workflow: scan fix_proposed", "error", err)
			continue
		}
		// Re-evaluate: if no HIGH risk, move to FIX_SUGGESTED.
		if _, err := e.Transition(ctx, id, EventFixGenerated); err != nil {
			if !errors.Is(err, ErrInvalidTransition) {
				e.log.Error("workflow: sweep fix_proposed", "session_id", id, "error", err)
			}
		}
	}
	return nil
}

// SweepVerifying scans VERIFYING sessions and auto-resolves or rolls back
// based on whether all fix actions succeeded.
func (e *Engine) SweepVerifying(ctx context.Context) error {
	rows, err := e.pool.Query(ctx, `
		SELECT id FROM diagnostic_sessions WHERE status = $1`,
		store.StatusVerifying)
	if err != nil {
		return fmt.Errorf("workflow: sweep verifying query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			e.log.Error("workflow: scan verifying", "error", err)
			continue
		}
		fixes, _ := e.fixDAO.GetBySessionID(ctx, id)
		allSucceeded := len(fixes) > 0
		anyFailed := false
		for _, fa := range fixes {
			if fa.ExecutionStatus == store.ExecStatusFailed {
				anyFailed = true
			}
			if fa.ExecutionStatus != store.ExecStatusSucceeded {
				allSucceeded = false
			}
		}
		event := EventVerifySuccess
		if anyFailed {
			event = EventVerifyFailure
		} else if !allSucceeded && len(fixes) == 0 {
			// No fix actions found in VERIFYING state — treat as failure.
			event = EventVerifyFailure
		}
		if _, err := e.Transition(ctx, id, event); err != nil {
			if !errors.Is(err, ErrInvalidTransition) {
				e.log.Error("workflow: sweep verifying", "session_id", id, "error", err)
			}
		}
	}
	return nil
}

// GetStatus returns the current status of a session.
func (e *Engine) GetStatus(ctx context.Context, sessionID uuid.UUID) (string, error) {
	s, err := e.sessionDAO.GetByID(ctx, sessionID)
	if err != nil {
		return "", err
	}
	return s.Status, nil
}

// IsTerminal returns true if the status is a terminal state.
func IsTerminal(status string) bool {
	_, ok := ValidTransitions[status]
	return ok && len(ValidTransitions[status]) == 0
}
