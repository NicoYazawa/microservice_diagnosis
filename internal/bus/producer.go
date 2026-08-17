package bus

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Producer publishes messages to Kafka.
type Producer interface {
	// Publish sends a single message (envelope, at-least-once). A message with
	// an empty Topic is rejected.
	Publish(ctx context.Context, msg Message) error
	Close() error
}

// kafkaProducer adapts a kafka-go Writer. The Writer is multi-topic: messages
// carry their own Topic, so one producer instance serves the whole service.
type kafkaProducer struct {
	w *kafka.Writer
}

// NewProducer builds a Kafka Producer from a validated Config.
func NewProducer(cfg Config) (Producer, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	acks, err := cfg.requiredAcks()
	if err != nil {
		return nil, err
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.Hash{}, // key-hash => deterministic partition per key
		BatchSize:              1,             // flush immediately for low-latency diagnosis
		BatchBytes:             cfg.BatchBytes,
		BatchTimeout:           cfg.BatchTimeout,
		WriteTimeout:           cfg.WriteTimeout,
		RequiredAcks:           acks,
		AllowAutoTopicCreation: true,
	}
	return &kafkaProducer{w: w}, nil
}

func (p *kafkaProducer) Publish(ctx context.Context, msg Message) error {
	if msg.Topic == "" {
		return ErrEmptyTopic
	}
	if err := p.w.WriteMessages(ctx, toKafkaMessage(msg)); err != nil {
		return fmt.Errorf("bus: publish to %q: %w", msg.Topic, err)
	}
	return nil
}

func (p *kafkaProducer) Close() error {
	return p.w.Close()
}
