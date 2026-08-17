package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Session status values (aligned with PLAN 5.1).
const (
	StatusCreated        = "CREATED"
	StatusCollecting     = "COLLECTING"
	StatusAnalyzing      = "ANALYZING"
	StatusRCADone        = "RCA_DONE"
	StatusFixProposed    = "FIX_PROPOSED"
	StatusAwaitingApproval = "AWAITING_APPROVAL"
	StatusFixSuggested   = "FIX_SUGGESTED"
	StatusFixExecuting   = "FIX_EXECUTING"
	StatusVerifying      = "VERIFYING"
	StatusResolved       = "RESOLVED"
	StatusRolledBack     = "ROLLED_BACK"
	StatusRejected       = "REJECTED"
	StatusIgnored        = "IGNORED"
	StatusFailed         = "FAILED"
)

// Trigger type values.
const (
	TriggerManual = "manual"
	TriggerAlert  = "alert"
)

// DiagnosticSession represents a diagnostic session row.
type DiagnosticSession struct {
	ID           uuid.UUID `json:"id"`
	Status       string    `json:"status"`
	TargetService string   `json:"target_service"`
	TriggerType  string    `json:"trigger_type"`
	RetryCount   int       `json:"retry_count"`
	ReportURL    *string   `json:"report_url"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// SessionDAO provides database access for diagnostic sessions.
type SessionDAO struct {
	pool *pgxpool.Pool
}

// NewSessionDAO creates a SessionDAO backed by the given pool.
func NewSessionDAO(pool *pgxpool.Pool) *SessionDAO {
	return &SessionDAO{pool: pool}
}

// Create inserts a new diagnostic session.
func (dao *SessionDAO) Create(ctx context.Context, targetService, triggerType string) (*DiagnosticSession, error) {
	if triggerType == "" {
		triggerType = TriggerManual
	}
	s := &DiagnosticSession{
		ID:           uuid.New(),
		Status:       StatusCreated,
		TargetService: targetService,
		TriggerType:  triggerType,
		RetryCount:   0,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	_, err := dao.pool.Exec(ctx, `
		INSERT INTO diagnostic_sessions (id, status, target_service, trigger_type, retry_count, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		s.ID, s.Status, s.TargetService, s.TriggerType, s.RetryCount, s.CreatedAt, s.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("session create: %w", err)
	}
	return s, nil
}

// GetByID retrieves a session by ID.
func (dao *SessionDAO) GetByID(ctx context.Context, id uuid.UUID) (*DiagnosticSession, error) {
	row := dao.pool.QueryRow(ctx, `
		SELECT id, status, target_service, trigger_type, retry_count, report_url, created_at, updated_at
		FROM diagnostic_sessions WHERE id = $1`, id)
	return scanSession(row)
}

// UpdateStatus updates the session status.
func (dao *SessionDAO) UpdateStatus(ctx context.Context, id uuid.UUID, status string) error {
	result, err := dao.pool.Exec(ctx, `
		UPDATE diagnostic_sessions SET status = $2, updated_at = $3 WHERE id = $1`,
		id, status, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("session update status: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetReportURL sets the report URL for a session.
func (dao *SessionDAO) SetReportURL(ctx context.Context, id uuid.UUID, url string) error {
	_, err := dao.pool.Exec(ctx, `
		UPDATE diagnostic_sessions SET report_url = $2, updated_at = $3 WHERE id = $1`,
		id, url, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("session set report url: %w", err)
	}
	return nil
}

// IncrementRetry increments the retry counter and returns the new count.
func (dao *SessionDAO) IncrementRetry(ctx context.Context, id uuid.UUID) (int, error) {
	var count int
	err := dao.pool.QueryRow(ctx, `
		UPDATE diagnostic_sessions
		SET retry_count = retry_count + 1, updated_at = $2
		WHERE id = $1
		RETURNING retry_count`, id, time.Now().UTC()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("session increment retry: %w", err)
	}
	return count, nil
}

// ListFilter holds filter criteria for session listing.
type ListFilter struct {
	Status        string
	TargetService string
	From, To      time.Time
	Keyword       string
	Page, PageSize int
}

// List returns sessions matching the filter.
func (dao *SessionDAO) List(ctx context.Context, f ListFilter) ([]*DiagnosticSession, int, error) {
	if f.Page < 1 {
		f.Page = 1
	}
	if f.PageSize < 1 {
		f.PageSize = 20
	}
	offset := (f.Page - 1) * f.PageSize

	var filterClause string
	var args []any
	argIdx := 1

	if f.Status != "" {
		filterClause += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, f.Status)
		argIdx++
	}
	if f.TargetService != "" {
		filterClause += fmt.Sprintf(" AND target_service = $%d", argIdx)
		args = append(args, f.TargetService)
		argIdx++
	}
	if !f.From.IsZero() {
		filterClause += fmt.Sprintf(" AND created_at >= $%d", argIdx)
		args = append(args, f.From)
		argIdx++
	}
	if !f.To.IsZero() {
		filterClause += fmt.Sprintf(" AND created_at <= $%d", argIdx)
		args = append(args, f.To)
		argIdx++
	}
	if f.Keyword != "" {
		filterClause += fmt.Sprintf(" AND (target_service ILIKE $%d OR id::text ILIKE $%d)", argIdx, argIdx)
		args = append(args, "%"+f.Keyword+"%")
		argIdx++
	}

	// Count total.
	var total int
	countQuery := "SELECT COUNT(*) FROM diagnostic_sessions WHERE 1=1" + filterClause
	if err := dao.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("session count: %w", err)
	}

	// List query with pagination.
	listQuery := fmt.Sprintf(
		`SELECT id, status, target_service, trigger_type, retry_count, report_url, created_at, updated_at
		FROM diagnostic_sessions WHERE 1=1%s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
		filterClause, argIdx, argIdx+1)
	listArgs := append(args, f.PageSize, offset)

	rows, err := dao.pool.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("session list: %w", err)
	}
	defer rows.Close()

	var sessions []*DiagnosticSession
	for rows.Next() {
		s, err := scanSessionRows(rows)
		if err != nil {
			return nil, 0, err
		}
		sessions = append(sessions, s)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("session list rows: %w", err)
	}
	return sessions, total, nil
}

func scanSession(row pgx.Row) (*DiagnosticSession, error) {
	var s DiagnosticSession
	var reportURL *string
	err := row.Scan(&s.ID, &s.Status, &s.TargetService, &s.TriggerType, &s.RetryCount, &reportURL, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan session: %w", err)
	}
	s.ReportURL = reportURL
	return &s, nil
}

func scanSessionRows(rows pgx.Rows) (*DiagnosticSession, error) {
	var s DiagnosticSession
	var reportURL *string
	err := rows.Scan(&s.ID, &s.Status, &s.TargetService, &s.TriggerType, &s.RetryCount, &reportURL, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("scan session rows: %w", err)
	}
	s.ReportURL = reportURL
	return &s, nil
}
