package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Risk levels for fix actions.
const (
	RiskLow    = "LOW"
	RiskMedium = "MEDIUM"
	RiskHigh   = "HIGH"
)

// Approval status values.
const (
	ApprovalStatusNone     = "NONE"
	ApprovalStatusPending  = "PENDING"
	ApprovalStatusApproved = "APPROVED"
	ApprovalStatusRejected = "REJECTED"
	ApprovalStatusExpired  = "EXPIRED"
)

// Execution status values.
const (
	ExecStatusNotStarted = "NOT_STARTED"
	ExecStatusRunning    = "RUNNING"
	ExecStatusSucceeded  = "SUCCEEDED"
	ExecStatusFailed     = "FAILED"
	ExecStatusRolledBack = "ROLLED_BACK"
)

// FixAction represents a single fix action step.
type FixAction struct {
	ID              uuid.UUID  `json:"id"`
	SessionID       uuid.UUID  `json:"session_id"`
	Seq             int        `json:"seq"`
	ActionType      string     `json:"action_type"`
	Target          string     `json:"target"`
	Risk            string     `json:"risk"`
	RollbackPlan    string     `json:"rollback_plan"`
	RequiresApproval bool      `json:"requires_approval"`
	ApprovalStatus  string     `json:"approval_status"`
	ExecutionStatus string     `json:"execution_status"`
	TicketID        *string    `json:"ticket_id"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// FixActionDAO provides database access for fix_actions.
type FixActionDAO struct {
	db Querier
}

// NewFixActionDAO creates a FixActionDAO.
func NewFixActionDAO(pool *pgxpool.Pool) *FixActionDAO {
	return &FixActionDAO{db: pool}
}

// WithTx returns a FixActionDAO bound to the provided transaction.
func (dao *FixActionDAO) WithTx(tx pgx.Tx) *FixActionDAO {
	return &FixActionDAO{db: tx}
}

// Create inserts a new fix action.
func (dao *FixActionDAO) Create(ctx context.Context, fa *FixAction) error {
	if fa.ID == uuid.Nil {
		fa.ID = uuid.New()
	}
	now := time.Now().UTC()
	if fa.CreatedAt.IsZero() {
		fa.CreatedAt = now
	}
	if fa.UpdatedAt.IsZero() {
		fa.UpdatedAt = now
	}
	_, err := dao.db.Exec(ctx, `
		INSERT INTO fix_actions (id, session_id, seq, action_type, target, risk, rollback_plan, requires_approval, approval_status, execution_status, ticket_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		fa.ID, fa.SessionID, fa.Seq, fa.ActionType, fa.Target, fa.Risk, fa.RollbackPlan,
		fa.RequiresApproval, fa.ApprovalStatus, fa.ExecutionStatus, fa.TicketID, fa.CreatedAt, fa.UpdatedAt)
	if err != nil {
		return fmt.Errorf("fix_action create: %w", err)
	}
	return nil
}

// GetByID retrieves a fix action by ID.
func (dao *FixActionDAO) GetByID(ctx context.Context, id uuid.UUID) (*FixAction, error) {
	row := dao.db.QueryRow(ctx, `
		SELECT id, session_id, seq, action_type, target, risk, rollback_plan, requires_approval, approval_status, execution_status, ticket_id, created_at, updated_at
		FROM fix_actions WHERE id = $1`, id)
	fa, err := scanFixAction(row)
	if err != nil {
		return nil, err
	}
	return fa, nil
}

func (dao *FixActionDAO) GetBySessionID(ctx context.Context, sessionID uuid.UUID) ([]*FixAction, error) {
	rows, err := dao.db.Query(ctx, `
		SELECT id, session_id, seq, action_type, target, risk, rollback_plan, requires_approval, approval_status, execution_status, ticket_id, created_at, updated_at
		FROM fix_actions WHERE session_id = $1 ORDER BY seq`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("fix_action list: %w", err)
	}
	defer rows.Close()
	var actions []*FixAction
	for rows.Next() {
		fa, err := scanFixActionRows(rows)
		if err != nil {
			return nil, err
		}
		actions = append(actions, fa)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fix_action list rows: %w", err)
	}
	return actions, nil
}

// UpdateApprovalStatus updates the approval status.
func (dao *FixActionDAO) UpdateApprovalStatus(ctx context.Context, id uuid.UUID, status string) error {
	_, err := dao.db.Exec(ctx, `
		UPDATE fix_actions SET approval_status = $2, updated_at = $3 WHERE id = $1`,
		id, status, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("fix_action update approval: %w", err)
	}
	return nil
}

// UpdateExecutionStatus updates the execution status.
func (dao *FixActionDAO) UpdateExecutionStatus(ctx context.Context, id uuid.UUID, status string) error {
	_, err := dao.db.Exec(ctx, `
		UPDATE fix_actions SET execution_status = $2, updated_at = $3 WHERE id = $1`,
		id, status, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("fix_action update execution: %w", err)
	}
	return nil
}

// SetTicketID sets the ticket ID for a fix action.
func (dao *FixActionDAO) SetTicketID(ctx context.Context, id uuid.UUID, ticketID string) error {
	_, err := dao.db.Exec(ctx, `
		UPDATE fix_actions SET ticket_id = $2, updated_at = $3 WHERE id = $1`,
		id, ticketID, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("fix_action set ticket: %w", err)
	}
	return nil
}

// Upsert creates or updates a fix action. Returns the action ID.
func (dao *FixActionDAO) Upsert(ctx context.Context, fa *FixAction) (uuid.UUID, error) {
	if fa.ID == uuid.Nil {
		fa.ID = uuid.New()
	}
	_, err := dao.db.Exec(ctx, `
		INSERT INTO fix_actions (id, session_id, seq, action_type, target, risk, rollback_plan, requires_approval, approval_status, execution_status, ticket_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (id) DO UPDATE SET
			action_type = EXCLUDED.action_type,
			target = EXCLUDED.target,
			risk = EXCLUDED.risk,
			rollback_plan = EXCLUDED.rollback_plan,
			requires_approval = EXCLUDED.requires_approval,
			approval_status = EXCLUDED.approval_status,
			execution_status = EXCLUDED.execution_status,
			ticket_id = EXCLUDED.ticket_id,
			updated_at = EXCLUDED.updated_at`,
		fa.ID, fa.SessionID, fa.Seq, fa.ActionType, fa.Target, fa.Risk, fa.RollbackPlan,
		fa.RequiresApproval, fa.ApprovalStatus, fa.ExecutionStatus, fa.TicketID, fa.CreatedAt, fa.UpdatedAt)
	if err != nil {
		return uuid.Nil, fmt.Errorf("fix_action upsert: %w", err)
	}
	return fa.ID, nil
}

func scanFixAction(row pgx.Row) (*FixAction, error) {
	var fa FixAction
	err := row.Scan(&fa.ID, &fa.SessionID, &fa.Seq, &fa.ActionType, &fa.Target, &fa.Risk, &fa.RollbackPlan,
		&fa.RequiresApproval, &fa.ApprovalStatus, &fa.ExecutionStatus, &fa.TicketID, &fa.CreatedAt, &fa.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan fix_action: %w", err)
	}
	return &fa, nil
}

func scanFixActionRows(rows pgx.Rows) (*FixAction, error) {
	var fa FixAction
	err := rows.Scan(&fa.ID, &fa.SessionID, &fa.Seq, &fa.ActionType, &fa.Target, &fa.Risk, &fa.RollbackPlan,
		&fa.RequiresApproval, &fa.ApprovalStatus, &fa.ExecutionStatus, &fa.TicketID, &fa.CreatedAt, &fa.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("scan fix_action rows: %w", err)
	}
	return &fa, nil
}

// Approval represents an approval request row.
type Approval struct {
	ID           uuid.UUID  `json:"id"`
	SessionID    uuid.UUID  `json:"session_id"`
	FixActionID  uuid.UUID  `json:"fix_action_id"`
	Status       string     `json:"status"`
	RequestToken string     `json:"request_token"`
	RequestedAt  time.Time  `json:"requested_at"`
	DecidedAt    *time.Time `json:"decided_at"`
	DecidedBy    *string    `json:"decided_by"`
}

// ApprovalDAO provides database access for approvals.
type ApprovalDAO struct {
	db Querier
}

// NewApprovalDAO creates an ApprovalDAO.
func NewApprovalDAO(pool *pgxpool.Pool) *ApprovalDAO {
	return &ApprovalDAO{db: pool}
}

// WithTx returns an ApprovalDAO bound to the provided transaction.
func (dao *ApprovalDAO) WithTx(tx pgx.Tx) *ApprovalDAO {
	return &ApprovalDAO{db: tx}
}

// Create inserts a new approval request.
func (dao *ApprovalDAO) Create(ctx context.Context, a *Approval) error {
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	if a.RequestedAt.IsZero() {
		a.RequestedAt = time.Now().UTC()
	}
	_, err := dao.db.Exec(ctx, `
		INSERT INTO approvals (id, session_id, fix_action_id, status, request_token, requested_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		a.ID, a.SessionID, a.FixActionID, a.Status, a.RequestToken, a.RequestedAt)
	if err != nil {
		return fmt.Errorf("approval create: %w", err)
	}
	return nil
}

// GetByID retrieves an approval by ID.
func (dao *ApprovalDAO) GetByID(ctx context.Context, id uuid.UUID) (*Approval, error) {
	row := dao.db.QueryRow(ctx, `
		SELECT id, session_id, fix_action_id, status, request_token, requested_at, decided_at, decided_by
		FROM approvals WHERE id = $1`, id)
	a, err := scanApproval(row)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// GetByToken retrieves an approval by request token.
func (dao *ApprovalDAO) GetByToken(ctx context.Context, token string) (*Approval, error) {
	if token == "" {
		return nil, ErrNotFound
	}
	row := dao.db.QueryRow(ctx, `
		SELECT id, session_id, fix_action_id, status, request_token, requested_at, decided_at, decided_by
		FROM approvals WHERE request_token = $1`, token)
	a, err := scanApproval(row)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// UpdateDecision records the approval decision.
func (dao *ApprovalDAO) UpdateDecision(ctx context.Context, id uuid.UUID, status, decidedBy string) error {
	now := time.Now().UTC()
	_, err := dao.db.Exec(ctx, `
		UPDATE approvals SET status = $2, decided_at = $3, decided_by = $4 WHERE id = $1`,
		id, status, now, decidedBy)
	if err != nil {
		return fmt.Errorf("approval update decision: %w", err)
	}
	return nil
}

// GetPendingBySessionID returns all pending approvals for a session.
func (dao *ApprovalDAO) GetPendingBySessionID(ctx context.Context, sessionID uuid.UUID) ([]*Approval, error) {
	rows, err := dao.db.Query(ctx, `
		SELECT id, session_id, fix_action_id, status, request_token, requested_at, decided_at, decided_by
		FROM approvals WHERE session_id = $1 AND status = $2 ORDER BY requested_at`,
		sessionID, ApprovalStatusPending)
	if err != nil {
		return nil, fmt.Errorf("approval list pending: %w", err)
	}
	defer rows.Close()
	var approvals []*Approval
	for rows.Next() {
		a, err := scanApprovalRows(rows)
		if err != nil {
			return nil, err
		}
		approvals = append(approvals, a)
	}
	return approvals, nil
}

func scanApproval(row pgx.Row) (*Approval, error) {
	var a Approval
	err := row.Scan(&a.ID, &a.SessionID, &a.FixActionID, &a.Status, &a.RequestToken, &a.RequestedAt, &a.DecidedAt, &a.DecidedBy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan approval: %w", err)
	}
	return &a, nil
}

func scanApprovalRows(rows pgx.Rows) (*Approval, error) {
	var a Approval
	err := rows.Scan(&a.ID, &a.SessionID, &a.FixActionID, &a.Status, &a.RequestToken, &a.RequestedAt, &a.DecidedAt, &a.DecidedBy)
	if err != nil {
		return nil, fmt.Errorf("scan approval rows: %w", err)
	}
	return &a, nil
}

// --- Knowledge Base DAO ---

// FixKnowledge represents a knowledge base entry.
type FixKnowledge struct {
	ID              uuid.UUID       `json:"id"`
	RootCausePattern string         `json:"root_cause_pattern"`
	FixSteps        json.RawMessage `json:"fix_steps"`
	Risk            string          `json:"risk"`
	RollbackPlan    string          `json:"rollback_plan"`
	TimesUsed       int             `json:"times_used"`
	SuccessRate     float32         `json:"success_rate"`
}

// KnowledgeBaseDAO provides database access for fix_knowledge_base.
type KnowledgeBaseDAO struct {
	db Querier
}

// NewKnowledgeBaseDAO creates a KnowledgeBaseDAO.
func NewKnowledgeBaseDAO(pool *pgxpool.Pool) *KnowledgeBaseDAO {
	return &KnowledgeBaseDAO{db: pool}
}

// WithTx returns a KnowledgeBaseDAO bound to the provided transaction.
func (dao *KnowledgeBaseDAO) WithTx(tx pgx.Tx) *KnowledgeBaseDAO {
	return &KnowledgeBaseDAO{db: tx}
}

// SearchByRootCause finds knowledge base entries matching the root cause pattern.
func (dao *KnowledgeBaseDAO) SearchByRootCause(ctx context.Context, pattern string) ([]*FixKnowledge, error) {
	rows, err := dao.db.Query(ctx, `
		SELECT id, root_cause_pattern, fix_steps, risk, rollback_plan, times_used, success_rate
		FROM fix_knowledge_base
		WHERE root_cause_pattern ILIKE $1
		ORDER BY success_rate DESC, times_used DESC`, "%"+pattern+"%")
	if err != nil {
		return nil, fmt.Errorf("knowledge_base search: %w", err)
	}
	defer rows.Close()
	var entries []*FixKnowledge
	for rows.Next() {
		var e FixKnowledge
		err := rows.Scan(&e.ID, &e.RootCausePattern, &e.FixSteps, &e.Risk, &e.RollbackPlan, &e.TimesUsed, &e.SuccessRate)
		if err != nil {
			return nil, fmt.Errorf("scan knowledge entry: %w", err)
		}
		entries = append(entries, &e)
	}
	return entries, nil
}

// IncrementUsage updates usage statistics after a fix is applied.
func (dao *KnowledgeBaseDAO) IncrementUsage(ctx context.Context, id uuid.UUID, success bool) error {
	if success {
		_, err := dao.db.Exec(ctx, `
			UPDATE fix_knowledge_base
			SET times_used = times_used + 1, success_rate = (success_rate * times_used + 1) / (times_used + 1)
			WHERE id = $1`, id)
		if err != nil {
			return fmt.Errorf("knowledge_base increment success: %w", err)
		}
	} else {
		_, err := dao.db.Exec(ctx, `
			UPDATE fix_knowledge_base
			SET times_used = times_used + 1, success_rate = (success_rate * times_used) / (times_used + 1)
			WHERE id = $1`, id)
		if err != nil {
			return fmt.Errorf("knowledge_base increment failure: %w", err)
		}
	}
	return nil
}
