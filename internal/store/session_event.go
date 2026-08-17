package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SessionEvent represents a single event in a diagnostic session timeline.
type SessionEvent struct {
	ID        uuid.UUID       `json:"id"`
	SessionID uuid.UUID       `json:"session_id"`
	EventType string          `json:"event_type"`
	Source    string          `json:"source"`
	Message   string          `json:"message"`
	Metadata  json.RawMessage `json:"metadata"`
	Timestamp time.Time       `json:"timestamp"`
}

// SessionEventDAO provides database access for session_events.
type SessionEventDAO struct {
	db *pgxpool.Pool
}

// NewSessionEventDAO creates a SessionEventDAO.
func NewSessionEventDAO(pool *pgxpool.Pool) *SessionEventDAO {
	return &SessionEventDAO{db: pool}
}

// Create inserts a new session event.
func (dao *SessionEventDAO) Create(ctx context.Context, ev *SessionEvent) error {
	if ev.ID == uuid.Nil {
		ev.ID = uuid.New()
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	_, err := dao.db.Exec(ctx, `
		INSERT INTO session_events (id, session_id, event_type, source, message, metadata, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		ev.ID, ev.SessionID, ev.EventType, ev.Source, ev.Message, ev.Metadata, ev.Timestamp)
	if err != nil {
		return fmt.Errorf("session_event create: %w", err)
	}
	return nil
}

// GetBySessionID returns all events for a session ordered by timestamp.
func (dao *SessionEventDAO) GetBySessionID(ctx context.Context, sessionID uuid.UUID) ([]*SessionEvent, error) {
	rows, err := dao.db.Query(ctx, `
		SELECT id, session_id, event_type, source, message, metadata, timestamp
		FROM session_events WHERE session_id = $1 ORDER BY timestamp`,
		sessionID)
	if err != nil {
		return nil, fmt.Errorf("session_event list: %w", err)
	}
	defer rows.Close()
	var events []*SessionEvent
	for rows.Next() {
		var ev SessionEvent
		if err := rows.Scan(&ev.ID, &ev.SessionID, &ev.EventType, &ev.Source, &ev.Message, &ev.Metadata, &ev.Timestamp); err != nil {
			return nil, fmt.Errorf("scan session_event: %w", err)
		}
		events = append(events, &ev)
	}
	return events, rows.Err()
}

// LogStateTransition is a helper to record a state change event.
func (dao *SessionEventDAO) LogStateTransition(ctx context.Context, sessionID uuid.UUID, fromStatus, toStatus, source string) error {
	meta, _ := json.Marshal(map[string]string{"from": fromStatus, "to": toStatus})
	ev := &SessionEvent{
		SessionID: sessionID,
		EventType: "state_changed",
		Source:    source,
		Message:   fmt.Sprintf("State transition: %s → %s", fromStatus, toStatus),
		Metadata:  meta,
	}
	return dao.Create(ctx, ev)
}
