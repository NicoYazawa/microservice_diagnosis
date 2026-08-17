//go:build integration

package workflow

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
)

// poolForIntegration is the shared pool for all integration tests.
// Initialized once in TestMain and closed after tests complete.
var poolForIntegration *pgxpool.Pool

// TestMain sets up the shared database pool for integration tests.
func TestMain(m *testing.M) {
	ctx := context.Background()
	pool, err := store.NewPGXPool(ctx, "postgres://mfdh:mfdh@localhost:5432/diagnosis")
	if err != nil {
		fmt.Fprintf(os.Stderr, "skipping integration tests: cannot connect to postgres: %v\n", err)
		os.Exit(0)
	}
	poolForIntegration = pool

	// Run migrations (idempotent).
	_, _ = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS diagnostic_sessions (
			id UUID PRIMARY KEY, status VARCHAR(32) NOT NULL,
			target_service VARCHAR(128), trigger_type VARCHAR(16),
			retry_count INT DEFAULT 0, report_url TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_sessions_status ON diagnostic_sessions(status);
	`)

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

// truncateAll truncates all workflow tables for a clean test state.
func truncateAll() {
	ctx := context.Background()
	_, _ = poolForIntegration.Exec(ctx,
		"TRUNCATE diagnostic_sessions, fix_actions, approvals, webhook_deliveries RESTART IDENTITY CASCADE")
}

// TestIntegration_CreateAndGetSession tests the basic session lifecycle.
func TestIntegration_CreateAndGetSession(t *testing.T) {
	ctx := context.Background()
	truncateAll()

	sessionDAO := store.NewSessionDAO(poolForIntegration)
	s, err := sessionDAO.Create(ctx, "payment-service", "manual")
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, s.ID)
	assert.Equal(t, store.StatusCreated, s.Status)
	assert.Equal(t, "payment-service", s.TargetService)
	assert.Equal(t, 0, s.RetryCount)

	s2, err := sessionDAO.GetByID(ctx, s.ID)
	require.NoError(t, err)
	assert.Equal(t, s.ID, s2.ID)
	assert.Equal(t, store.StatusCreated, s2.Status)
}

// TestIntegration_StateMachineTransition tests the full happy path.
func TestIntegration_StateMachineTransition(t *testing.T) {
	ctx := context.Background()
	truncateAll()

	sessionDAO := store.NewSessionDAO(poolForIntegration)
	s, err := sessionDAO.Create(ctx, "order-service", "manual")
	require.NoError(t, err)

	engine := NewEngine(poolForIntegration, slog.Default())

	// CREATED → COLLECTING
	newStatus, err := engine.Transition(ctx, s.ID, EventStartCollect)
	require.NoError(t, err)
	assert.Equal(t, store.StatusCollecting, newStatus)

	// COLLECTING → ANALYZING
	newStatus, err = engine.Transition(ctx, s.ID, EventCollectComplete)
	require.NoError(t, err)
	assert.Equal(t, store.StatusAnalyzing, newStatus)

	// ANALYZING → RCA_DONE
	newStatus, err = engine.Transition(ctx, s.ID, EventAnalysisComplete)
	require.NoError(t, err)
	assert.Equal(t, store.StatusRCADone, newStatus)

	// RCA_DONE → FIX_PROPOSED
	newStatus, err = engine.Transition(ctx, s.ID, EventRCAGenerated)
	require.NoError(t, err)
	assert.Equal(t, store.StatusFixProposed, newStatus)

	// FIX_PROPOSED → FIX_SUGGESTED (no HIGH risk actions)
	newStatus, err = engine.Transition(ctx, s.ID, EventFixGenerated)
	require.NoError(t, err)
	assert.Equal(t, store.StatusFixSuggested, newStatus)

	// FIX_SUGGESTED → FIX_EXECUTING → VERIFYING → RESOLVED
	newStatus, err = engine.Transition(ctx, s.ID, EventExecute)
	require.NoError(t, err)
	assert.Equal(t, store.StatusFixExecuting, newStatus)

	newStatus, err = engine.Transition(ctx, s.ID, EventExecuteComplete)
	require.NoError(t, err)
	assert.Equal(t, store.StatusVerifying, newStatus)

	newStatus, err = engine.Transition(ctx, s.ID, EventVerifySuccess)
	require.NoError(t, err)
	assert.Equal(t, store.StatusResolved, newStatus)

	assert.True(t, IsTerminal(store.StatusResolved))
}

// TestIntegration_HighRiskApprovalGate tests the HIGH risk approval branch.
func TestIntegration_HighRiskApprovalGate(t *testing.T) {
	ctx := context.Background()
	truncateAll()

	sessionDAO := store.NewSessionDAO(poolForIntegration)
	fixDAO := store.NewFixActionDAO(poolForIntegration)

	s, err := sessionDAO.Create(ctx, "order-service", "manual")
	require.NoError(t, err)

	engine := NewEngine(poolForIntegration, slog.Default())
	_, _ = engine.Transition(ctx, s.ID, EventStartCollect)
	_, _ = engine.Transition(ctx, s.ID, EventCollectComplete)
	_, _ = engine.Transition(ctx, s.ID, EventAnalysisComplete)
	_, _ = engine.Transition(ctx, s.ID, EventRCAGenerated)

	// Inject a HIGH risk fix action.
	now := time.Now().UTC()
	_, err = fixDAO.Upsert(ctx, &store.FixAction{
		ID:              uuid.New(),
		SessionID:       s.ID,
		Seq:             1,
		ActionType:      "switch_master",
		Target:          "mysql-primary",
		Risk:            store.RiskHigh,
		RollbackPlan:    "switch back",
		ApprovalStatus:  store.ApprovalStatusNone,
		ExecutionStatus: store.ExecStatusNotStarted,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	// FIX_PROPOSED → AWAITING_APPROVAL (due to HIGH risk)
	newStatus, err := engine.Transition(ctx, s.ID, EventFixGenerated)
	require.NoError(t, err)
	assert.Equal(t, store.StatusAwaitingApproval, newStatus)

	// AWAITING_APPROVAL → FIX_EXECUTING (after approval)
	newStatus, err = engine.Transition(ctx, s.ID, EventApprove)
	require.NoError(t, err)
	assert.Equal(t, store.StatusFixExecuting, newStatus)
}

// TestIntegration_InvalidTransition tests that invalid events are rejected.
func TestIntegration_InvalidTransition(t *testing.T) {
	ctx := context.Background()
	truncateAll()

	sessionDAO := store.NewSessionDAO(poolForIntegration)
	s, err := sessionDAO.Create(ctx, "order-service", "manual")
	require.NoError(t, err)

	engine := NewEngine(poolForIntegration, slog.Default())

	// Jump from CREATED → ANALYZING directly (invalid).
	_, err = engine.Transition(ctx, s.ID, EventCollectComplete)
	assert.ErrorIs(t, err, ErrInvalidTransition)

	// Non-existent session.
	_, err = engine.Transition(ctx, uuid.New(), EventStartCollect)
	assert.ErrorIs(t, err, ErrSessionNotFound)
}

// TestIntegration_SkipLockedConcurrency simulates concurrent transition attempts.
func TestIntegration_SkipLockedConcurrency(t *testing.T) {
	ctx := context.Background()
	truncateAll()

	sessionDAO := store.NewSessionDAO(poolForIntegration)
	s, err := sessionDAO.Create(ctx, "order-service", "manual")
	require.NoError(t, err)

	engine := NewEngine(poolForIntegration, slog.Default())
	_, _ = engine.Transition(ctx, s.ID, EventStartCollect)

	// Concurrently try COLLECTING → ANALYZING from 3 goroutines.
	results := make([]string, 3)
	errors := make([]error, 3)

	for i := 0; i < 3; i++ {
		go func(idx int) {
			status, err := engine.Transition(ctx, s.ID, EventCollectComplete)
			results[idx] = status
			errors[idx] = err
		}(i)
	}

	time.Sleep(500 * time.Millisecond)

	successCount := 0
	for _, err := range errors {
		if err == nil {
			successCount++
		}
	}
	assert.Equal(t, 1, successCount, "exactly one goroutine should succeed")
}

// TestIntegration_ListSessions tests the session list filter.
func TestIntegration_ListSessions(t *testing.T) {
	ctx := context.Background()
	truncateAll()

	sessionDAO := store.NewSessionDAO(poolForIntegration)
	engine := NewEngine(poolForIntegration, slog.Default())

	s1, err := sessionDAO.Create(ctx, "order-service", "manual")
	require.NoError(t, err)
	s2, err := sessionDAO.Create(ctx, "payment-service", "alert")
	require.NoError(t, err)
	_, err = sessionDAO.Create(ctx, "inventory-service", "manual") // stays CREATED
	require.NoError(t, err)

	_, _ = engine.Transition(ctx, s1.ID, EventStartCollect) // COLLECTING
	_, _ = engine.Transition(ctx, s2.ID, EventStartCollect) // COLLECTING

	// List all.
	_, total, err := sessionDAO.List(ctx, store.ListFilter{Page: 1, PageSize: 10})
	require.NoError(t, err)
	assert.Equal(t, 3, total)

	// Filter by status: only s1 and s2 are COLLECTING.
	sessions, total, err := sessionDAO.List(ctx, store.ListFilter{
		Status: store.StatusCollecting, Page: 1, PageSize: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, sessions, 2)

	// Filter by service.
	sessions, total, err = sessionDAO.List(ctx, store.ListFilter{
		TargetService: "payment-service", Page: 1, PageSize: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, "payment-service", sessions[0].TargetService)
}

// TestIntegration_IgnoreAndFail tests IGNORE and FAIL terminal transitions.
func TestIntegration_IgnoreAndFail(t *testing.T) {
	ctx := context.Background()
	truncateAll()

	sessionDAO := store.NewSessionDAO(poolForIntegration)
	engine := NewEngine(poolForIntegration, slog.Default())

	s1, err := sessionDAO.Create(ctx, "order-service", "manual")
	require.NoError(t, err)
	s2, err := sessionDAO.Create(ctx, "order-service", "manual")
	require.NoError(t, err)

	status, err := engine.Transition(ctx, s1.ID, EventIgnore)
	require.NoError(t, err)
	assert.Equal(t, store.StatusIgnored, status)
	assert.True(t, IsTerminal(store.StatusIgnored))

	status, err = engine.Transition(ctx, s2.ID, EventFail)
	require.NoError(t, err)
	assert.Equal(t, store.StatusFailed, status)
	assert.True(t, IsTerminal(store.StatusFailed))
}
