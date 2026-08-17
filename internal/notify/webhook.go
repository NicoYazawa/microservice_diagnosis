package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DiagnosticEvent is the payload sent to webhook channels on session state changes.
type DiagnosticEvent struct {
	EventType   string            `json:"event_type"` // session_resolved / session_failed / approval_required / ...
	SessionID   uuid.UUID         `json:"session_id"`
	Status      string            `json:"status"`
	Summary     string            `json:"summary"`
	Message     string            `json:"message,omitempty"`
	Service     string            `json:"target_service,omitempty"`
	ReportURL   string            `json:"report_url,omitempty"`
	Timestamp   time.Time         `json:"timestamp"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// WebhookConfig describes a single webhook channel.
type WebhookConfig struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   string    `json:"channel"` // feishu / dingtalk / slack / generic
	URL       string    `json:"url"`
	Secret    string    `json:"secret,omitempty"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

// WebhookDeliveryLog records each delivery attempt for observability and retry.
type WebhookDeliveryLog struct {
	ID          uuid.UUID  `json:"id"`
	SessionID    uuid.UUID  `json:"session_id"`
	Channel      string     `json:"channel"`
	Status       string     `json:"status"` // SUCCESS / FAIL / PENDING
	Attempt      int        `json:"attempt"`
	ResponseCode int        `json:"response_code"`
	LastError    string     `json:"last_error,omitempty"`
	DeliveredAt  *time.Time `json:"delivered_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

// WebhookDAO persists webhook config and delivery logs.
type WebhookDAO struct {
	pool *pgxpool.Pool
}

func NewWebhookDAO(pool *pgxpool.Pool) *WebhookDAO {
	return &WebhookDAO{pool: pool}
}

// ListEnabled returns all enabled webhook configurations.
func (d *WebhookDAO) ListEnabled(ctx context.Context) ([]*WebhookConfig, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT id, name, channel, url, secret, enabled, created_at
		FROM webhook_configs WHERE enabled = true`)
	if err != nil {
		return nil, fmt.Errorf("webhook list: %w", err)
	}
	defer rows.Close()
	var configs []*WebhookConfig
	for rows.Next() {
		var c WebhookConfig
		if err := rows.Scan(&c.ID, &c.Name, &c.Channel, &c.URL, &c.Secret, &c.Enabled, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan webhook config: %w", err)
		}
		configs = append(configs, &c)
	}
	return configs, nil
}

// LogDelivery records a delivery attempt.
func (d *WebhookDAO) LogDelivery(ctx context.Context, log *WebhookDeliveryLog) error {
	if log.ID == uuid.Nil {
		log.ID = uuid.New()
	}
	if log.CreatedAt.IsZero() {
		log.CreatedAt = time.Now().UTC()
	}
	_, err := d.pool.Exec(ctx, `
		INSERT INTO webhook_deliveries (id, session_id, channel, status, attempt, response_code, last_error, delivered_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		log.ID, log.SessionID, log.Channel, log.Status, log.Attempt,
		log.ResponseCode, log.LastError, log.DeliveredAt, log.CreatedAt)
	if err != nil {
		return fmt.Errorf("webhook log insert: %w", err)
	}
	return nil
}

// UpdateDeliverySuccess marks a delivery as successful.
func (d *WebhookDAO) UpdateDeliverySuccess(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	_, err := d.pool.Exec(ctx, `
		UPDATE webhook_deliveries SET status = 'SUCCESS', delivered_at = $2, response_code = 200 WHERE id = $1`,
		id, now)
	if err != nil {
		return fmt.Errorf("webhook update success: %w", err)
	}
	return nil
}

// WebhookNotifier dispatches diagnostic events to multiple configured channels.
type WebhookNotifier struct {
	dao    *WebhookDAO
	client *http.Client
	log    *slog.Logger
}

func NewWebhookNotifier(dao *WebhookDAO, log *slog.Logger) *WebhookNotifier {
	return &WebhookNotifier{
		dao: dao,
		client: &http.Client{Timeout: 10 * time.Second},
		log:   log,
	}
}

// Notify dispatches an event to all enabled webhook channels.
func (n *WebhookNotifier) Notify(ctx context.Context, e DiagnosticEvent) error {
	configs, err := n.dao.ListEnabled(ctx)
	if err != nil {
		return fmt.Errorf("notify: list configs: %w", err)
	}

	for _, cfg := range configs {
		go n.deliver(ctx, cfg, &e)
	}
	return nil
}

func (n *WebhookNotifier) deliver(ctx context.Context, cfg *WebhookConfig, e *DiagnosticEvent) {
	const maxAttempts = 3
	body, _ := json.Marshal(e)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		logEntry := &WebhookDeliveryLog{
			SessionID: e.SessionID,
			Channel:  cfg.Channel,
			Attempt:  attempt,
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
		if err != nil {
			n.log.Error("webhook: build request", "channel", cfg.Channel, "error", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if cfg.Secret != "" {
			sig := n.sign(body, cfg.Secret)
			req.Header.Set("X-Webhook-Signature", sig)
		}
		// Channel-specific headers.
		switch cfg.Channel {
		case "feishu":
			req.Header.Set("Content-Type", "application/json")
		case "dingtalk":
			req.Header.Set("Content-Type", "application/json")
		case "slack":
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := n.client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			logEntry.Status = "SUCCESS"
			logEntry.ResponseCode = resp.StatusCode
			now := time.Now().UTC()
			logEntry.DeliveredAt = &now
			n.dao.LogDelivery(ctx, logEntry)
			n.log.Info("webhook delivered", "channel", cfg.Channel, "session", e.SessionID)
			return
		}

		if err != nil {
			logEntry.LastError = err.Error()
		} else {
			logEntry.ResponseCode = resp.StatusCode
			if body, _ := io.ReadAll(resp.Body); len(body) > 0 {
				logEntry.LastError = string(body)
			}
		}
		logEntry.Status = "FAIL"
		n.dao.LogDelivery(ctx, logEntry)

		// Exponential backoff before retry.
		if attempt < maxAttempts {
			backoff := time.Duration(attempt*attempt) * time.Second
			time.Sleep(backoff)
		}
	}

	n.log.Error("webhook: all attempts failed", "channel", cfg.Channel, "session", e.SessionID)
}

// sign computes HMAC-SHA256 of the payload with the given secret.
func (n *WebhookNotifier) sign(payload []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(payload)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
