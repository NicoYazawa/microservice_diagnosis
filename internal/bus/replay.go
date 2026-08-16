package bus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

// ReplayOptions describes a replay run over a topic.
type ReplayOptions struct {
	Topic string
	// From is the absolute offset to start from. Use FirstOffset (-2) to start
	// from the earliest available offset.
	From    int64
	Handler Handler
}

// ReplayResult reports what one partition yielded during a replay.
type ReplayResult struct {
	Topic      string
	Partition  int
	From       int64
	LastOffset int64
	Count      int64
}

// Replayer re-reads the Kafka log of a topic (message replay, PLAN M2 DoD).
type Replayer interface {
	// Replay delivers every existing message from From to the current end of
	// the log, in offset order, once per partition. It returns one result per
	// partition. Replay is read-only: it never commits offsets and does not
	// affect consumer groups.
	Replay(ctx context.Context, opts ReplayOptions) ([]ReplayResult, error)
}

// kafkaReplayer implements Replay with raw partition connections and batched
// reads, which gives exact "replay up to the current end of the log" semantics
// without touching consumer groups.
type kafkaReplayer struct {
	cfg Config
}

// NewReplayer builds a Kafka Replayer from a validated Config.
func NewReplayer(cfg Config) (Replayer, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &kafkaReplayer{cfg: cfg}, nil
}

func (r *kafkaReplayer) Replay(ctx context.Context, opts ReplayOptions) ([]ReplayResult, error) {
	if opts.Topic == "" {
		return nil, ErrEmptyTopic
	}
	if opts.Handler == nil {
		return nil, ErrNilHandler
	}
	if opts.From < 0 && opts.From != FirstOffset {
		return nil, fmt.Errorf("bus: replay start offset %d is invalid: use %d (FirstOffset) or a non-negative offset", opts.From, FirstOffset)
	}

	partitions, err := r.partitions(ctx, opts.Topic)
	if err != nil {
		return nil, err
	}
	sort.Ints(partitions)

	results := make([]ReplayResult, 0, len(partitions))
	for _, p := range partitions {
		res, err := r.replayPartition(ctx, opts, p)
		if err != nil {
			return results, err
		}
		results = append(results, res)
	}
	return results, nil
}

// partitions lists the partition ids of a topic via a metadata request.
func (r *kafkaReplayer) partitions(ctx context.Context, topic string) ([]int, error) {
	client := &kafka.Client{
		Addr:    kafka.TCP(r.cfg.Brokers...),
		Timeout: r.cfg.DialTimeout,
	}
	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("bus: describe topic %q: %w", topic, err)
	}
	for _, t := range resp.Topics {
		if t.Name != topic {
			continue
		}
		if t.Error != nil {
			return nil, fmt.Errorf("bus: describe topic %q: %w", topic, t.Error)
		}
		ids := make([]int, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			if p.Error != nil {
				return nil, fmt.Errorf("bus: describe topic %q partition %d: %w", topic, p.ID, p.Error)
			}
			ids = append(ids, p.ID)
		}
		return ids, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
}

// replayPartition reads a single partition from opts.From up to the current
// end of the log. It stops when a read batch yields no records.
func (r *kafkaReplayer) replayPartition(ctx context.Context, opts ReplayOptions, partition int) (ReplayResult, error) {
	res := ReplayResult{Topic: opts.Topic, Partition: partition, From: opts.From}

	var (
		conn    *kafka.Conn
		lastErr error
	)
	for _, broker := range r.cfg.Brokers {
		conn, lastErr = kafka.DialLeader(ctx, "tcp", broker, opts.Topic, partition)
		if lastErr == nil {
			break
		}
	}
	if conn == nil || lastErr != nil {
		return res, fmt.Errorf("bus: dial leader for %s/%d: %w", opts.Topic, partition, lastErr)
	}
	defer conn.Close()

	switch {
	case opts.From >= 0:
		if _, err := conn.Seek(opts.From, kafka.SeekAbsolute); err != nil {
			return res, fmt.Errorf("bus: seek %s/%d to offset %d: %w", opts.Topic, partition, opts.From, err)
		}
	case opts.From == FirstOffset:
		if _, err := conn.Seek(0, kafka.SeekStart); err != nil {
			return res, fmt.Errorf("bus: seek %s/%d to first offset: %w", opts.Topic, partition, err)
		}
	default:
		return res, fmt.Errorf("bus: replay start offset %d is invalid: use %d (FirstOffset) or a non-negative offset", opts.From, FirstOffset)
	}

	for {
		if err := conn.SetReadDeadline(time.Now().Add(r.cfg.ReplayIdle)); err != nil {
			return res, fmt.Errorf("bus: set read deadline: %w", err)
		}
		batch := conn.ReadBatch(r.cfg.MinBytes, r.cfg.MaxBytes)
		n := int64(0)
		for {
			m, err := batch.ReadMessage()
			if err != nil {
				// End-of-batch / end-of-log: empty reads terminate the replay.
				if errors.Is(err, io.EOF) || errors.Is(err, kafka.RequestTimedOut) {
					break
				}
				_ = batch.Close()
				return res, fmt.Errorf("bus: replay read %s/%d: %w", opts.Topic, partition, err)
			}
			msg := fromKafkaMessage(m)
			if err := opts.Handler(ctx, msg); err != nil {
				_ = batch.Close()
				return res, fmt.Errorf("bus: replay handler failed at topic=%q partition=%d offset=%d: %w",
					msg.Topic, msg.Partition, msg.Offset, err)
			}
			n++
			res.Count++
			res.LastOffset = m.Offset
		}
		closeErr := batch.Close()
		if n == 0 {
			// No records in the batch (end-of-log or idle timeout): the
			// partition is caught up to the end. RequestTimedOut here is the
			// expected idle signal, not a failure.
			return res, nil
		}
		if closeErr != nil && !errors.Is(closeErr, kafka.RequestTimedOut) {
			return res, fmt.Errorf("bus: close read batch: %w", closeErr)
		}
	}
}
