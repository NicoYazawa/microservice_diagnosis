// Package bus provides the Kafka messaging layer for mfdh (PLAN M2):
// a Producer / Consumer pair for the standardized Observation contract and a
// partition-level Replayer for the message-replay acceptance use case.
//
// Design notes (PLAN 2.1):
//   - Delivery semantics are at-least-once: the consumer commits an offset only
//     after the application handler succeeds, so a failed handler is redelivered
//     on the next run instead of being lost.
//   - Topics are namespaced with the "mfdh." prefix (see Topic).
//   - Each agent uses its own consumer group (see GroupID), which is what makes
//     agents independently deployable and horizontally scalable.
package bus

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Offset constants re-exported from kafka-go for convenience.
const (
	// FirstOffset replays / starts consuming from the earliest available offset.
	FirstOffset = kafka.FirstOffset // -2
	// LastOffset starts consuming from the newest available offset.
	LastOffset = kafka.LastOffset // -1
)

// Configuration errors (PLAN 11.1 contract-layer illegal input rejection).
var (
	ErrEmptyBrokers        = errors.New("bus: at least one broker is required")
	ErrEmptyBroker         = errors.New("bus: broker address must not be empty")
	ErrInvalidRequiredAcks = errors.New("bus: required_acks must be one of none/one/all")
	ErrEmptyTopic          = errors.New("bus: topic must not be empty")
	ErrNilHandler          = errors.New("bus: handler must not be nil")
	ErrEmptyGroupID        = errors.New("bus: consumer group id must not be empty")
	ErrInvalidStartOffset  = errors.New("bus: start offset must be 0, FirstOffset, or LastOffset")
	ErrTopicNotFound       = errors.New("bus: topic not found")
)

// Config tunes the Kafka producer, consumer, and replayer connections.
// All fields are optional; zero values fall back to the defaults applied by
// withDefaults. Brokers must always be provided.
type Config struct {
	Brokers      []string      `mapstructure:"brokers"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	BatchSize    int           `mapstructure:"batch_size"`
	BatchBytes   int64         `mapstructure:"batch_bytes"`
	BatchTimeout time.Duration `mapstructure:"batch_timeout"`
	RequiredAcks string        `mapstructure:"required_acks"` // none / one / all
	MinBytes     int           `mapstructure:"min_bytes"`
	MaxBytes     int           `mapstructure:"max_bytes"`
	MaxWait      time.Duration `mapstructure:"max_wait"`
	// ReplayIdle bounds how long the replayer waits for the next record before
	// declaring that it has caught up to the end of a partition log.
	ReplayIdle time.Duration `mapstructure:"replay_idle"`
	// Consumer-group tuning. Smaller values make cold-start (first join) faster
	// at the cost of more heartbeat traffic.
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	SessionTimeout    time.Duration `mapstructure:"session_timeout"`
	RebalanceTimeout  time.Duration `mapstructure:"rebalance_timeout"`
	JoinGroupBackoff  time.Duration `mapstructure:"join_group_backoff"`
}

// withDefaults fills every zero-valued field with a sensible default. Brokers
// are intentionally left untouched: an explicit broker list is required.
func (c Config) withDefaults() Config {
	if c.DialTimeout <= 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.BatchBytes <= 0 {
		c.BatchBytes = 1 << 20 // 1 MiB
	}
	if c.BatchTimeout <= 0 {
		// Short batch window keeps per-message publish latency low (diagnosis
		// evidence must arrive promptly) while still batching under load.
		c.BatchTimeout = 100 * time.Millisecond
	}
	if c.RequiredAcks == "" {
		c.RequiredAcks = "all"
	}
	if c.MinBytes <= 0 {
		c.MinBytes = 1
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 1 << 20 // 1 MiB
	}
	if c.MaxWait <= 0 {
		c.MaxWait = 500 * time.Millisecond
	}
	if c.ReplayIdle <= 0 {
		c.ReplayIdle = 2 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 3 * time.Second
	}
	if c.SessionTimeout <= 0 {
		c.SessionTimeout = 30 * time.Second
	}
	if c.RebalanceTimeout <= 0 {
		c.RebalanceTimeout = 30 * time.Second
	}
	if c.JoinGroupBackoff <= 0 {
		c.JoinGroupBackoff = 5 * time.Second
	}
	return c
}

// Validate checks the configuration invariants (defaults are expected to have
// been applied first).
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrEmptyBrokers
	}
	for _, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			return ErrEmptyBroker
		}
	}
	if _, err := c.requiredAcks(); err != nil {
		return err
	}
	if c.DialTimeout <= 0 {
		return fmt.Errorf("bus: dial_timeout must be > 0, got %s", c.DialTimeout)
	}
	if c.WriteTimeout <= 0 {
		return fmt.Errorf("bus: write_timeout must be > 0, got %s", c.WriteTimeout)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("bus: batch_size must be > 0, got %d", c.BatchSize)
	}
	if c.BatchBytes <= 0 {
		return fmt.Errorf("bus: batch_bytes must be > 0, got %d", c.BatchBytes)
	}
	if c.BatchTimeout <= 0 {
		return fmt.Errorf("bus: batch_timeout must be > 0, got %s", c.BatchTimeout)
	}
	if c.MinBytes <= 0 {
		return fmt.Errorf("bus: min_bytes must be > 0, got %d", c.MinBytes)
	}
	if c.MaxBytes < c.MinBytes {
		return fmt.Errorf("bus: max_bytes (%d) must be >= min_bytes (%d)", c.MaxBytes, c.MinBytes)
	}
	if c.MaxWait <= 0 {
		return fmt.Errorf("bus: max_wait must be > 0, got %s", c.MaxWait)
	}
	if c.ReplayIdle <= 0 {
		return fmt.Errorf("bus: replay_idle must be > 0, got %s", c.ReplayIdle)
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("bus: heartbeat_interval must be > 0, got %s", c.HeartbeatInterval)
	}
	if c.SessionTimeout < c.HeartbeatInterval {
		return fmt.Errorf("bus: session_timeout (%s) must be >= heartbeat_interval (%s)", c.SessionTimeout, c.HeartbeatInterval)
	}
	if c.RebalanceTimeout <= 0 {
		return fmt.Errorf("bus: rebalance_timeout must be > 0, got %s", c.RebalanceTimeout)
	}
	if c.JoinGroupBackoff <= 0 {
		return fmt.Errorf("bus: join_group_backoff must be > 0, got %s", c.JoinGroupBackoff)
	}
	return nil
}

// requiredAcks maps the textual config value to the kafka-go acknowledgement
// level. It returns ErrInvalidRequiredAcks for anything else.
func (c Config) requiredAcks() (kafka.RequiredAcks, error) {
	switch strings.ToLower(strings.TrimSpace(c.RequiredAcks)) {
	case "none":
		return kafka.RequireNone, nil
	case "one":
		return kafka.RequireOne, nil
	case "all":
		return kafka.RequireAll, nil
	default:
		return kafka.RequireAll, fmt.Errorf("%w: %q", ErrInvalidRequiredAcks, c.RequiredAcks)
	}
}
