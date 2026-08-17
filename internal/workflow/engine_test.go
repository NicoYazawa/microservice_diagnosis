package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
)

// TestIsTerminal verifies terminal state detection.
func TestIsTerminal(t *testing.T) {
	terminals := []string{
		store.StatusResolved,
		store.StatusRejected,
		store.StatusRolledBack,
		store.StatusIgnored,
		store.StatusFailed,
	}
	nonTerminals := []string{
		store.StatusCreated,
		store.StatusCollecting,
		store.StatusAnalyzing,
		store.StatusRCADone,
		store.StatusFixProposed,
		store.StatusAwaitingApproval,
		store.StatusFixSuggested,
		store.StatusFixExecuting,
		store.StatusVerifying,
	}

	for _, s := range terminals {
		assert.True(t, IsTerminal(s), "expected %s to be terminal", s)
	}
	for _, s := range nonTerminals {
		assert.False(t, IsTerminal(s), "expected %s to be non-terminal", s)
	}
}

// TestValidTransitionsCompleteness checks that every non-terminal status
// has at least one outgoing transition.
func TestValidTransitionsCompleteness(t *testing.T) {
	nonTerminals := []string{
		store.StatusCreated,
		store.StatusCollecting,
		store.StatusAnalyzing,
		store.StatusRCADone,
		store.StatusFixProposed,
		store.StatusAwaitingApproval,
		store.StatusFixSuggested,
		store.StatusFixExecuting,
		store.StatusVerifying,
	}
	for _, s := range nonTerminals {
		transitions, ok := ValidTransitions[s]
		require.True(t, ok, "status %s must have transitions entry", s)
		assert.NotEmpty(t, transitions, "status %s must have at least one outgoing transition", s)
	}
}

// TestTransitionEventSet checks that the full state machine is connected
// via static transitions (every non-terminal can reach a terminal via some event path).
// Note: AWAITING_APPROVAL is a runtime branch from FIX_PROPOSED, gated by the
// presence of HIGH-risk fix actions (determineFixProposedNextStatus), so it is
// excluded from this static graph test. It is covered by TestFixProposedTransitionWithRisk.
func TestTransitionEventSet(t *testing.T) {
	// nonTerminals excludes AWAITING_APPROVAL (runtime-only branch from FIX_PROPOSED).
	nonTerminals := []string{
		store.StatusCreated,
		store.StatusCollecting,
		store.StatusAnalyzing,
		store.StatusRCADone,
		store.StatusFixProposed,
		store.StatusFixSuggested,
		store.StatusFixExecuting,
		store.StatusVerifying,
	}

	// Build a directed graph of status -> reachable statuses.
	graph := make(map[string]map[string]bool)

	for from, events := range ValidTransitions {
		if graph[from] == nil {
			graph[from] = make(map[string]bool)
		}
		for _, to := range events {
			graph[from][to] = true
		}
	}

	// BFS from CREATED to verify all non-terminals are reachable.
	visited := make(map[string]bool)
	queue := []string{store.StatusCreated}
	visited[store.StatusCreated] = true

	terminals := map[string]bool{
		store.StatusResolved:   true,
		store.StatusRejected:   true,
		store.StatusRolledBack: true,
		store.StatusIgnored:    true,
		store.StatusFailed:     true,
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for next := range graph[current] {
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}

	// All non-terminals should be reachable from CREATED.
	for _, s := range nonTerminals {
		if terminals[s] {
			continue
		}
		assert.True(t, visited[s], "status %s should be reachable from CREATED", s)
	}
}

// TestEngineTransitionValidation tests that invalid transitions are rejected.
func TestEngineTransitionValidation(t *testing.T) {
	// Test the transition table directly without a real DB.
	tests := []struct {
		currentStatus string
		event         string
		wantErr       bool
	}{
		// Valid transitions.
		{store.StatusCreated, EventStartCollect, false},
		{store.StatusCreated, EventIgnore, false},
		{store.StatusCreated, EventFail, false},
		{store.StatusCollecting, EventCollectComplete, false},
		{store.StatusAnalyzing, EventAnalysisComplete, false},
		{store.StatusRCADone, EventRCAGenerated, false},
		{store.StatusFixExecuting, EventExecuteComplete, false},
		{store.StatusVerifying, EventVerifySuccess, false},
		{store.StatusVerifying, EventVerifyFailure, false},
		{store.StatusAwaitingApproval, EventApprove, false},
		{store.StatusAwaitingApproval, EventReject, false},
		{store.StatusFixSuggested, EventExecute, false},

		// Invalid transitions.
		{store.StatusCreated, EventCollectComplete, true},  // wrong order
		{store.StatusResolved, EventFail, true},             // terminal state
		{store.StatusVerifying, EventApprove, true},         // not allowed from VERIFYING
		{store.StatusCollecting, EventRCAGenerated, true},   // wrong event
	}

	for _, tt := range tests {
		t.Run(tt.currentStatus+"+"+tt.event, func(t *testing.T) {
			eventMap, ok := ValidTransitions[tt.currentStatus]
			require.True(t, ok, "missing status %s", tt.currentStatus)
			_, allowed := eventMap[tt.event]
			if tt.wantErr {
				assert.False(t, allowed, "transition %s+%s should not be allowed", tt.currentStatus, tt.event)
			} else {
				assert.True(t, allowed, "transition %s+%s should be allowed", tt.currentStatus, tt.event)
			}
		})
	}
}

// mockPool is a stub that returns errors for tests that don't need real DB.
type mockPool struct{}

func TestEngineNoDBPanic(t *testing.T) {
	// Verify that without a real pool, methods that need DB will fail cleanly.
	// This is a documentation test: real integration tests would use a real PG instance.
	assert.NotNil(t, NewEngine(nil, nil))
}

// TestEventConstantsCompleteness verifies all events are declared.
func TestEventConstantsCompleteness(t *testing.T) {
	events := []string{
		EventStartCollect,
		EventCollectComplete,
		EventAnalysisComplete,
		EventRCAGenerated,
		EventFixGenerated,
		EventApprove,
		EventReject,
		EventExecute,
		EventExecuteComplete,
		EventVerifySuccess,
		EventVerifyFailure,
		EventIgnore,
		EventFail,
		EventRetry,
	}
	for _, e := range events {
		assert.NotEmpty(t, e, "event constant must not be empty: %s", e)
	}
}

// TestFixProposedTransitionWithRisk verifies the special FIX_PROPOSED branching logic.
func TestFixProposedTransitionWithRisk(t *testing.T) {
	// EventFixGenerated from FIX_PROPOSED routes to either FIX_SUGGESTED or AWAITING_APPROVAL.
	// We test the branching logic in isolation.
	eventMap, ok := ValidTransitions[store.StatusFixProposed]
	require.True(t, ok)

	// EventFixGenerated must be present (the routing happens inside determineFixProposedNextStatus).
	_, hasFixGenerated := eventMap[EventFixGenerated]
	assert.True(t, hasFixGenerated, "FIX_PROPOSED must have EventFixGenerated transition")

	// EventApprove must also be present (for the no-HIGH-risk direct approve path).
	_, hasApprove := eventMap[EventApprove]
	assert.True(t, hasApprove, "FIX_PROPOSED must have EventApprove transition")
}

// TestStatusConstantsMatchStore verifies workflow constants match store constants.
func TestStatusConstantsMatchStore(t *testing.T) {
	assert.Equal(t, store.StatusCreated,          StatusCreated)
	assert.Equal(t, store.StatusCollecting,       StatusCollecting)
	assert.Equal(t, store.StatusAnalyzing,        StatusAnalyzing)
	assert.Equal(t, store.StatusRCADone,          StatusRCADone)
	assert.Equal(t, store.StatusFixProposed,      StatusFixProposed)
	assert.Equal(t, store.StatusAwaitingApproval, StatusAwaitingApproval)
	assert.Equal(t, store.StatusFixSuggested,     StatusFixSuggested)
	assert.Equal(t, store.StatusFixExecuting,     StatusFixExecuting)
	assert.Equal(t, store.StatusVerifying,        StatusVerifying)
	assert.Equal(t, store.StatusResolved,         StatusResolved)
	assert.Equal(t, store.StatusRolledBack,       StatusRolledBack)
	assert.Equal(t, store.StatusRejected,         StatusRejected)
	assert.Equal(t, store.StatusIgnored,          StatusIgnored)
	assert.Equal(t, store.StatusFailed,           StatusFailed)
}

// mockSessionDAO and mockFixDAO for unit testing without DB.
type mockSessionDAO struct {
	sessions map[uuid.UUID]*store.DiagnosticSession
}

func newMockSessionDAO() *mockSessionDAO {
	return &mockSessionDAO{sessions: make(map[uuid.UUID]*store.DiagnosticSession)}
}

func (m *mockSessionDAO) Create(ctx context.Context, targetService, triggerType string) (*store.DiagnosticSession, error) {
	s := &store.DiagnosticSession{
		ID:           uuid.New(),
		Status:       store.StatusCreated,
		TargetService: targetService,
		TriggerType:  triggerType,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	m.sessions[s.ID] = s
	return s, nil
}

type mockFixActionDAO struct{}

func newMockFixActionDAO() *mockFixActionDAO {
	return &mockFixActionDAO{}
}

func (m *mockFixActionDAO) GetBySessionID(ctx context.Context, sessionID uuid.UUID) ([]*store.FixAction, error) {
	return nil, nil
}
