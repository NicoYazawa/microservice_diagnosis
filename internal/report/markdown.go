// Package report provides diagnostic report rendering in Markdown and PDF formats.
package report

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
)

// ReportData holds all data needed to render a diagnostic report.
type ReportData struct {
	Session      *store.DiagnosticSession
	Timeline     []*SessionEvent // state transition history
	Observations []*ObservationEntry
	RootCause    *RootCauseSummary
	FixActions   []*store.FixAction
	ApprovalLogs []*store.Approval
}

// ObservationEntry represents a single evidence observation for the report.
// In M8 this will be populated from ClickHouse; for M7 it is a placeholder.
type ObservationEntry struct {
	ID            uuid.UUID
	Type         string
	SubType      string
	Severity     string
	TargetService string
	Confidence    float64
	Message       string
	Labels        map[string]string
	Timestamp     time.Time
}

// RootCauseSummary summarizes the RCA result for the report.
type RootCauseSummary struct {
	Pattern     string  `json:"pattern"`
	Description string  `json:"description"`
	Confidence  float64 `json:"confidence"`
	EvidenceIDs []string `json:"evidence_ids"`
}

// SessionEvent represents a single state transition event.
type SessionEvent struct {
	EventType string
	Source    string
	Message   string
	Timestamp time.Time
}

// Renderer generates diagnostic reports.
type Renderer struct {
	pool *pgxpool.Pool
}

// NewRenderer creates a report renderer backed by the given connection pool.
func NewRenderer(pool *pgxpool.Pool) *Renderer {
	return &Renderer{pool: pool}
}

// RenderMarkdown renders the report as Markdown string.
func (r *Renderer) RenderMarkdown(ctx context.Context, data ReportData) (string, error) {
	var buf bytes.Buffer

	// Header
	buf.WriteString("# Diagnostic Report\n\n")
	buf.WriteString(fmt.Sprintf("**Session ID:** `%s`\n", data.Session.ID))
	buf.WriteString(fmt.Sprintf("**Target Service:** %s\n", data.Session.TargetService))
	buf.WriteString(fmt.Sprintf("**Status:** %s\n", data.Session.Status))
	buf.WriteString(fmt.Sprintf("**Trigger:** %s\n", data.Session.TriggerType))
	buf.WriteString(fmt.Sprintf("**Created:** %s\n", data.Session.CreatedAt.Format(time.RFC3339)))
	buf.WriteString(fmt.Sprintf("**Report Generated:** %s\n\n", time.Now().UTC().Format(time.RFC3339)))

	// Executive Summary
	buf.WriteString("## Executive Summary\n\n")
	if summary := buildExecutiveSummary(data); summary != "" {
		buf.WriteString(summary)
	} else {
		buf.WriteString("_(No summary available)_")
	}
	buf.WriteString("\n\n")

	// Timeline
	buf.WriteString("## Diagnostic Timeline\n\n")
	buf.WriteString("| Time (UTC) | Event | Source | Details |\n")
	buf.WriteString("|-----------|-------|--------|----------|\n")
	if len(data.Timeline) == 0 {
		buf.WriteString("| - | - | - | No events recorded |\n")
	}
	for _, ev := range data.Timeline {
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
			ev.Timestamp.Format(time.RFC3339), ev.EventType, ev.Source, ev.Message))
	}
	buf.WriteString("\n")

	// Observations / Evidence
	buf.WriteString("## Evidence Collected\n\n")
	if len(data.Observations) == 0 {
		buf.WriteString("_(No evidence observations available)_")
	} else {
		for _, obs := range data.Observations {
			buf.WriteString(fmt.Sprintf("### [%s] %s — %s\n", obs.Type, obs.SubType, obs.TargetService))
			buf.WriteString(fmt.Sprintf("- **Severity:** %s | **Confidence:** %.0f%%\n", obs.Severity, obs.Confidence*100))
			if obs.Message != "" {
				buf.WriteString(fmt.Sprintf("- **Message:** %s\n", obs.Message))
			}
			if len(obs.Labels) > 0 {
				buf.WriteString(fmt.Sprintf("- **Labels:** %v\n", obs.Labels))
			}
			buf.WriteString(fmt.Sprintf("- **ID:** `%s`\n\n", obs.ID))
		}
	}

	// Root Cause
	buf.WriteString("## Root Cause Analysis\n\n")
	if data.RootCause == nil {
		buf.WriteString("_(Root cause analysis not yet completed)_\n")
	} else {
		buf.WriteString(fmt.Sprintf("**Pattern:** %s\n\n", data.RootCause.Pattern))
		buf.WriteString(fmt.Sprintf("**Description:** %s\n\n", data.RootCause.Description))
		buf.WriteString(fmt.Sprintf("**Confidence:** %.0f%%\n\n", data.RootCause.Confidence*100))
		if len(data.RootCause.EvidenceIDs) > 0 {
			buf.WriteString(fmt.Sprintf("**Supporting Evidence IDs:**\n"))
			for _, id := range data.RootCause.EvidenceIDs {
				buf.WriteString(fmt.Sprintf("- `%s`\n", id))
			}
		}
	}
	buf.WriteString("\n")

	// Fix Actions
	buf.WriteString("## Fix Actions\n\n")
	if len(data.FixActions) == 0 {
		buf.WriteString("_(No fix actions generated)_\n")
	} else {
		buf.WriteString("| Seq | Action | Target | Risk | Approval | Execution |\n")
		buf.WriteString("|-----|--------|--------|------|----------|-------------|\n")
		for _, fa := range data.FixActions {
			buf.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %s | %s |\n",
				fa.Seq, fa.ActionType, fa.Target, fa.Risk, fa.ApprovalStatus, fa.ExecutionStatus))
			buf.WriteString(fmt.Sprintf("| **Rollback** | %s |\n", fa.RollbackPlan))
			if fa.TicketID != nil && *fa.TicketID != "" {
				buf.WriteString(fmt.Sprintf("| **Ticket** | %s |\n", *fa.TicketID))
			}
			buf.WriteString("|-----|--------|--------|------|----------|-------------|\n")
		}
	}
	buf.WriteString("\n")

	// Approval History
	if len(data.ApprovalLogs) > 0 {
		buf.WriteString("## Approval History\n\n")
		buf.WriteString("| Time | Fix Action | Decision | Decided By |\n")
		buf.WriteString("|------|------------|----------|-------------|\n")
		for _, al := range data.ApprovalLogs {
			decidedBy := "_(pending)_"
			if al.DecidedBy != nil {
				decidedBy = *al.DecidedBy
			}
			buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
				al.RequestedAt.Format(time.RFC3339), al.FixActionID, al.Status, decidedBy))
		}
		buf.WriteString("\n")
	}

	// Retry info
	if data.Session.RetryCount > 0 {
		buf.WriteString(fmt.Sprintf("**Retry Count:** %d\n\n", data.Session.RetryCount))
	}

	// Footer
	buf.WriteString("---\n")
	buf.WriteString("*Generated by microservice-diagnosis*\n")

	return buf.String(), nil
}

// buildExecutiveSummary produces a human-readable summary paragraph.
func buildExecutiveSummary(data ReportData) string {
	switch data.Session.Status {
	case store.StatusResolved:
		return fmt.Sprintf(
			"Diagnostic session for **%s** has been successfully resolved. "+
				"The root cause was identified and %d fix action(s) were executed.",
			data.Session.TargetService, len(data.FixActions))
	case store.StatusFailed:
		return fmt.Sprintf(
			"Diagnostic session for **%s** encountered a system failure and could not complete.",
			data.Session.TargetService)
	case store.StatusRejected:
		return fmt.Sprintf(
			"Diagnostic session for **%s** was resolved by rejecting the proposed fix actions.",
			data.Session.TargetService)
	case store.StatusIgnored:
		return fmt.Sprintf(
			"Diagnostic session for **%s** was ignored by an operator.",
			data.Session.TargetService)
	default:
		if len(data.FixActions) > 0 {
			return fmt.Sprintf(
				"Diagnostic session for **%s** produced %d fix action(s). "+
					"Current status: **%s**.",
				data.Session.TargetService, len(data.FixActions), data.Session.Status)
		}
		return fmt.Sprintf(
			"Diagnostic session for **%s** is currently in **%s** state.",
			data.Session.TargetService, data.Session.Status)
	}
}

// LoadReportData fetches all report-relevant data for a session.
func (r *Renderer) LoadReportData(ctx context.Context, sessionID uuid.UUID) (ReportData, error) {
	session, err := store.NewSessionDAO(r.pool).GetByID(ctx, sessionID)
	if err != nil {
		return ReportData{}, fmt.Errorf("load report: session: %w", err)
	}

	fixActions, _ := store.NewFixActionDAO(r.pool).GetBySessionID(ctx, sessionID)

	// Also get non-pending approvals via a different query — for now include all.
	allApprovals, _ := r.getAllApprovalsForSession(ctx, sessionID)

	// Observations would be loaded from ClickHouse in M8.
	// For M7, we use an empty slice as a placeholder.
	var observations []*ObservationEntry

	// Try session_events table first (M7), fall back to basic timeline if not available.
	var timeline []*SessionEvent
	if events, err := store.NewSessionEventDAO(r.pool).GetBySessionID(ctx, sessionID); err == nil && len(events) > 0 {
		// Convert store.SessionEvent to report.SessionEvent.
		timeline = make([]*SessionEvent, len(events))
		for i, e := range events {
			timeline[i] = &SessionEvent{
				EventType: e.EventType,
				Source:    e.Source,
				Message:   e.Message,
				Timestamp: e.Timestamp,
			}
		}
	} else {
		// Fallback: build from session state + approvals.
		timeline = r.buildTimelineFromSession(session, allApprovals)
	}

	return ReportData{
		Session:      session,
		Timeline:     timeline,
		Observations: observations,
		RootCause:    nil, // populated by RCA agent in M8
		FixActions:   fixActions,
		ApprovalLogs: allApprovals,
	}, nil
}

func (r *Renderer) buildTimelineFromSession(s *store.DiagnosticSession, approvals []*store.Approval) []*SessionEvent {
	events := []*SessionEvent{
		{
			EventType: "session_created",
			Source:    "orchestrator",
			Message:   fmt.Sprintf("Diagnostic session created for service %s (trigger: %s)", s.TargetService, s.TriggerType),
			Timestamp: s.CreatedAt,
		},
	}
	if s.Status != store.StatusCreated {
		events = append(events, &SessionEvent{
			EventType: "session_started",
			Source:    "orchestrator",
			Message:   fmt.Sprintf("Session transitioned to %s", s.Status),
			Timestamp: s.UpdatedAt,
		})
	}
	for _, a := range approvals {
		ev := &SessionEvent{
			EventType: "approval_" + a.Status,
			Source:    "approval",
			Message:   fmt.Sprintf("Approval %s for fix action %s", a.Status, a.FixActionID),
			Timestamp: a.RequestedAt,
		}
		if a.DecidedAt != nil {
			ev.Timestamp = *a.DecidedAt
		}
		events = append(events, ev)
	}
	if s.Status == store.StatusResolved {
		events = append(events, &SessionEvent{
			EventType: "session_resolved",
			Source:    "orchestrator",
			Message:   "Diagnostic session completed successfully",
			Timestamp: s.UpdatedAt,
		})
	}
	return events
}

func (r *Renderer) getAllApprovalsForSession(ctx context.Context, sessionID uuid.UUID) ([]*store.Approval, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, session_id, fix_action_id, status, request_token, requested_at, decided_at, decided_by
		FROM approvals WHERE session_id = $1 ORDER BY requested_at`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var approvals []*store.Approval
	for rows.Next() {
		var a store.Approval
		err := rows.Scan(&a.ID, &a.SessionID, &a.FixActionID, &a.Status, &a.RequestToken, &a.RequestedAt, &a.DecidedAt, &a.DecidedBy)
		if err != nil {
			return nil, err
		}
		approvals = append(approvals, &a)
	}
	return approvals, rows.Err()
}
