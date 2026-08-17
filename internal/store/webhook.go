package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WebhookChannel represents the notification channel type.
const (
	ChannelFeishu   = "feishu"
	ChannelDingtalk = "dingtalk"
	ChannelSlack    = "slack"
	ChannelGeneric  = "generic"
)

// DeliveryStatus values.
const (
	DeliveryStatusPending   = "PENDING"
	DeliveryStatusSuccess   = "SUCCESS"
	DeliveryStatusFailed    = "FAILED"
)

// WebhookConfig represents a webhook configuration.
type WebhookConfig struct {
	ID       uuid.UUID `json:"id"`
	Name     string    `json:"name"`
	Channel  string    `json:"channel"`
	URL      string    `json:"url"`
	Secret   string    `json:"-"`
	Enabled  bool      `json:"enabled"`
}

// WebhookDelivery represents a webhook delivery log entry.
type WebhookDelivery struct {
	ID          uuid.UUID  `json:"id"`
	SessionID   *uuid.UUID `json:"session_id"`
	Channel     string     `json:"channel"`
	Status      string     `json:"status"`
	Attempt     int        `json:"attempt"`
	LastError   *string    `json:"last_error"`
	DeliveredAt *time.Time `json:"delivered_at"`
	CreatedAt   time.Time  `json:"created_at"`
}

// WebhookDAO provides database access for webhook_configs and webhook_deliveries.
type WebhookDAO struct {
	db Querier
}

// NewWebhookDAO creates a WebhookDAO.
func NewWebhookDAO(pool *pgxpool.Pool) *WebhookDAO {
	return &WebhookDAO{db: pool}
}

// WithTx returns a WebhookDAO bound to the provided transaction.
func (dao *WebhookDAO) WithTx(tx pgx.Tx) *WebhookDAO {
	return &WebhookDAO{db: tx}
}

// --- Webhook Config ---

// CreateConfig inserts a new webhook config.
func (dao *WebhookDAO) CreateConfig(ctx context.Context, wc *WebhookConfig) error {
	if wc.ID == uuid.Nil {
		wc.ID = uuid.New()
	}
	_, err := dao.db.Exec(ctx, `
		INSERT INTO webhook_configs (id, name, channel, url, secret, enabled)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		wc.ID, wc.Name, wc.Channel, wc.URL, wc.Secret, wc.Enabled)
	if err != nil {
		return fmt.Errorf("webhook config create: %w", err)
	}
	return nil
}

// GetEnabledConfigs returns all enabled webhook configs for a channel (empty = all channels).
func (dao *WebhookDAO) GetEnabledConfigs(ctx context.Context, channel string) ([]*WebhookConfig, error) {
	rows, err := dao.db.Query(ctx, `
		SELECT id, name, channel, url, secret, enabled
		FROM webhook_configs WHERE enabled = true AND ($1 = '' OR channel = $1)`, channel)
	if err != nil {
		return nil, fmt.Errorf("webhook config list: %w", err)
	}
	defer rows.Close()
	var configs []*WebhookConfig
	for rows.Next() {
		var c WebhookConfig
		err := rows.Scan(&c.ID, &c.Name, &c.Channel, &c.URL, &c.Secret, &c.Enabled)
		if err != nil {
			return nil, fmt.Errorf("scan webhook config: %w", err)
		}
		configs = append(configs, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("webhook config list rows: %w", err)
	}
	return configs, nil
}

// --- Webhook Delivery ---

// CreateDelivery inserts a delivery log entry.
func (dao *WebhookDAO) CreateDelivery(ctx context.Context, d *WebhookDelivery) error {
	if d.ID == uuid.Nil {
		d.ID = uuid.New()
	}
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now().UTC()
	}
	if d.Status == "" {
		d.Status = DeliveryStatusPending
	}
	_, err := dao.db.Exec(ctx, `
		INSERT INTO webhook_deliveries (id, session_id, channel, status, attempt, last_error, delivered_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		d.ID, d.SessionID, d.Channel, d.Status, d.Attempt, d.LastError, d.DeliveredAt, d.CreatedAt)
	if err != nil {
		return fmt.Errorf("webhook delivery create: %w", err)
	}
	return nil
}

// UpdateDeliverySuccess marks a delivery as successful.
func (dao *WebhookDAO) UpdateDeliverySuccess(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	_, err := dao.db.Exec(ctx, `
		UPDATE webhook_deliveries SET status = $2, delivered_at = $3 WHERE id = $1`,
		id, DeliveryStatusSuccess, now)
	if err != nil {
		return fmt.Errorf("webhook delivery success: %w", err)
	}
	return nil
}

// UpdateDeliveryFailure records a failed delivery attempt.
func (dao *WebhookDAO) UpdateDeliveryFailure(ctx context.Context, id uuid.UUID, errMsg string) error {
	_, err := dao.db.Exec(ctx, `
		UPDATE webhook_deliveries SET status = $2, attempt = attempt + 1, last_error = $3 WHERE id = $1`,
		id, DeliveryStatusFailed, errMsg)
	if err != nil {
		return fmt.Errorf("webhook delivery failure: %w", err)
	}
	return nil
}

var urlPattern = regexp.MustCompile(`https?://[^\s"]+`)

// GetDeliveryByID retrieves a delivery by ID.
func (dao *WebhookDAO) GetDeliveryByID(ctx context.Context, id uuid.UUID) (*WebhookDelivery, error) {
	row := dao.db.QueryRow(ctx, `
		SELECT id, session_id, channel, status, attempt, last_error, delivered_at, created_at
		FROM webhook_deliveries WHERE id = $1`, id)
	d, err := scanWebhookDelivery(row)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func scanWebhookDelivery(row pgx.Row) (*WebhookDelivery, error) {
	var d WebhookDelivery
	err := row.Scan(&d.ID, &d.SessionID, &d.Channel, &d.Status, &d.Attempt, &d.LastError, &d.DeliveredAt, &d.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan webhook delivery: %w", err)
	}
	return &d, nil
}
