package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Handler processes a single consumed message. Returning an error leaves the
// offset uncommitted, so the message is redelivered on the next run
// (at-least-once semantics, PLAN 11.1 "messages are not lost").
type Handler func(ctx context.Context, msg Message) error

// ConsumerConfig describes a consumer-group subscription.
type ConsumerConfig struct {
	Topic   string
	GroupID string
	// StartOffset is only applied when the group has no committed offset yet.
	// 0 (the zero value) and FirstOffset both mean "start from the earliest".
	StartOffset int64
}

// Validate checks the subscription invariants.
func (c ConsumerConfig) Validate() error {
	if c.Topic == "" {
		return ErrEmptyTopic
	}
	if c.GroupID == "" {
		return ErrEmptyGroupID
	}
	switch c.StartOffset {
	case 0, FirstOffset, LastOffset:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrInvalidStartOffset, c.StartOffset)
	}
}

// Consumer subscribes to a topic as part of a consumer group.
type Consumer interface {
	// Consume delivers messages to handler until ctx is cancelled or the
	// handler fails (in which case the error is returned and the message is
	// redelivered later).
	Consume(ctx context.Context, handler Handler) error
	Close() error
}

// kafkaConsumer adapts a kafka-go Reader configured with synchronous commits:
// each offset is committed right after the handler succeeds.
type kafkaConsumer struct {
	r *kafka.Reader
}

// NewConsumer builds a consumer-group subscription from validated configs.
func NewConsumer(cfg Config, cc ConsumerConfig) (Consumer, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := cc.Validate(); err != nil {
		return nil, err
	}
	start := cc.StartOffset
	if start == 0 {
		start = FirstOffset
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Brokers,
		Topic:             cc.Topic,
		GroupID:           cc.GroupID,
		MinBytes:          cfg.MinBytes,
		MaxBytes:          cfg.MaxBytes,
		MaxWait:           cfg.MaxWait,
		CommitInterval:    0, // synchronous commit => at-least-once per message
		StartOffset:       start,
		HeartbeatInterval: cfg.HeartbeatInterval,
		SessionTimeout:    cfg.SessionTimeout,
		RebalanceTimeout:  cfg.RebalanceTimeout,
		JoinGroupBackoff:  cfg.JoinGroupBackoff,
		Dialer:            &kafka.Dialer{Timeout: cfg.DialTimeout},
	})
	return &kafkaConsumer{r: r}, nil
}

func (c *kafkaConsumer) Consume(ctx context.Context, handler Handler) error {
	if handler == nil {
		return ErrNilHandler
	}
	for {
		m, err := c.r.FetchMessage(ctx)
		if err != nil {
			// Normal shutdown path: the caller cancelled the context.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("bus: fetch message: %w", err)
		}
		msg := fromKafkaMessage(m)
		if err := handler(ctx, msg); err != nil {
			// Deliberately NOT committing: the offset stays uncommitted so the
			// message is redelivered (at-least-once).
			return fmt.Errorf("bus: handler failed at topic=%q partition=%d offset=%d: %w",
				msg.Topic, msg.Partition, msg.Offset, err)
		}
		if err := c.r.CommitMessages(ctx, m); err != nil {
			return fmt.Errorf("bus: commit offset: %w", err)
		}
	}
}

func (c *kafkaConsumer) Close() error {
	return c.r.Close()
}
