// Package executor implements fix action execution back-ends.
package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// FixAction carries all the information needed to execute a single fix step.
type FixAction struct {
	ID           uuid.UUID
	SessionID    uuid.UUID
	ActionType   string
	Target       string
	Risk         string
	RollbackPlan string
}

// ExecutionResult describes the outcome of an action execution.
type ExecutionResult struct {
	ID        uuid.UUID  `json:"id"`
	Status    string     `json:"status"` // SUCCEEDED / FAILED / ROLLED_BACK
	Message   string     `json:"message"`
	TicketID  string     `json:"ticket_id,omitempty"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   time.Time  `json:"ended_at"`
	Rollback  string     `json:"rollback_note,omitempty"`
}

// Executor defines the interface for executing fix actions.
// Implementations: NOOP (dev/test), Kubernetes, Cloud-provider-specific.
type Executor interface {
	// Execute performs the fix action and returns the result.
	Execute(ctx context.Context, action FixAction) (*ExecutionResult, error)

	// Rollback attempts to undo a previously executed action.
	Rollback(ctx context.Context, action FixAction) error
}

// NOOPExecutor does nothing — use for development and testing.
type NOOPExecutor struct{}

// NewNOOPExecutor creates a no-op executor that always succeeds.
func NewNOOPExecutor() *NOOPExecutor { return &NOOPExecutor{} }

// Execute logs the action and returns success.
func (e *NOOPExecutor) Execute(ctx context.Context, action FixAction) (*ExecutionResult, error) {
	return &ExecutionResult{
		ID:        action.ID,
		Status:    "SUCCEEDED",
		Message:   fmt.Sprintf("[NOOP] Would execute %s on %s", action.ActionType, action.Target),
		StartedAt: time.Now().UTC(),
		EndedAt:   time.Now().UTC(),
	}, nil
}

// Rollback is a no-op.
func (e *NOOPExecutor) Rollback(ctx context.Context, action FixAction) error {
	return nil
}
