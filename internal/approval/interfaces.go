// Package approval provides the human approval gate for HIGH-risk fix actions.
package approval

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ApprovalRequest captures a request for human approval of a fix action.
type ApprovalRequest struct {
	SessionID   uuid.UUID
	FixActionID uuid.UUID
	Summary     string // human-readable description of the action
	Risk        string
	Target      string
	RequestedBy string // optional: operator who triggered it
}

// ApprovalResult is the outcome of an approval decision.
type ApprovalResult struct {
	Decision   string     // APPROVED / REJECTED
	DecidedBy  string
	DecidedAt  time.Time
	Reason     string     // optional comment
}

// ApprovalClient defines the interface for submitting and querying approvals.
// Different back-ends (Slack, PagerDuty, custom HTTP callback) implement this.
type ApprovalClient interface {
	// RequestApproval submits a new approval request and returns a token
	// that can be used to poll or callback with a decision.
	RequestApproval(ctx context.Context, req ApprovalRequest) (token string, err error)

	// GetApproval retrieves the current state of an approval by token.
	GetApproval(ctx context.Context, token string) (*ApprovalRequest, *ApprovalResult, error)

	// CancelApproval retracts a pending approval request.
	CancelApproval(ctx context.Context, token string) error
}
