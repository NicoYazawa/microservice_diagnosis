package bus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// In-memory test double implementing the Producer / Consumer / Replayer
// contracts. Offsets are contiguous per topic (single partition), mirroring a
// one-partition topic on a real broker.

type memBus struct {
	mu       sync.Mutex
	messages map[string][]Message        // topic -> stored messages (offset == index)
	groups   map[string]map[string]int64 // topic -> groupID -> next offset to deliver
}

func newMemBus() *memBus {
	return &memBus{
		messages: make(map[string][]Message),
		groups:   make(map[string]map[string]int64),
	}
}

func (b *memBus) Publish(_ context.Context, msg Message) error {
	if msg.Topic == "" {
		return ErrEmptyTopic
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	msgs := b.messages[msg.Topic]
	msg.Partition = 0
	msg.Offset = int64(len(msgs))
	if msg.Time.IsZero() {
		msg.Time = time.Now()
	}
	b.messages[msg.Topic] = append(msgs, msg)
	return nil
}

func (b *memBus) Close() error { return nil }

// memConsumer is a test double for Consumer.
// Unlike kafkaConsumer, it intentionally returns nil once it has drained the
// current in-memory topic instead of waiting for new messages or context
// cancellation; this makes tests assertable without changing the production
// streaming behavior.
type memConsumer struct {
	b  *memBus
	cc ConsumerConfig
}

func (c memConsumer) Consume(ctx context.Context, handler Handler) error {
	if err := c.cc.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return ErrNilHandler
	}
	b := c.b
	b.mu.Lock()
	if b.groups[c.cc.Topic] == nil {
		b.groups[c.cc.Topic] = make(map[string]int64)
	}
	off := b.groups[c.cc.Topic][c.cc.GroupID]
	b.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		b.mu.Lock()
		msgs := b.messages[c.cc.Topic]
		if off >= int64(len(msgs)) {
			b.mu.Unlock()
			return nil // caught up; fake returns so the test can assert
		}
		m := msgs[off]
		b.mu.Unlock()

		if err := handler(ctx, m); err != nil {
			// Offset intentionally not advanced: the message will be redelivered.
			return fmt.Errorf("handler failed at offset %d: %w", off, err)
		}
		off++
		b.mu.Lock()
		b.groups[c.cc.Topic][c.cc.GroupID] = off
		b.mu.Unlock()
	}
}

func (c memConsumer) Close() error { return nil }

type memReplayer struct{ b *memBus }

func (r memReplayer) Replay(ctx context.Context, opts ReplayOptions) ([]ReplayResult, error) {
	if opts.Topic == "" {
		return nil, ErrEmptyTopic
	}
	if opts.Handler == nil {
		return nil, ErrNilHandler
	}
	if opts.From < 0 && opts.From != FirstOffset {
		return nil, fmt.Errorf("bus: replay start offset %d is invalid: use %d (FirstOffset) or a non-negative offset", opts.From, FirstOffset)
	}
	r.b.mu.Lock()
	msgs := append([]Message(nil), r.b.messages[opts.Topic]...)
	r.b.mu.Unlock()

	from := opts.From
	if from < 0 {
		from = 0
	}
	res := ReplayResult{Topic: opts.Topic, Partition: 0, From: from}
	for i := from; i < int64(len(msgs)); i++ {
		m := msgs[i]
		if err := opts.Handler(ctx, m); err != nil {
			return []ReplayResult{res}, fmt.Errorf("replay handler failed at offset %d: %w", m.Offset, err)
		}
		res.Count++
		res.LastOffset = m.Offset
	}
	return []ReplayResult{res}, nil
}

// validObservation builds a contract-valid Observation with a stable session id.
func validObservation(t *testing.T, n int) *observationv1.Observation {
	t.Helper()
	o, err := observation.New(&observationv1.Observation{
		SessionId:     fmt.Sprintf("session-%02d", n),
		Source:        "agent-log",
		Type:          observationv1.EvidenceType_EVIDENCE_TYPE_LOG,
		SubType:       "log_pattern",
		Confidence:    0.9,
		Severity:      observationv1.Severity_SEVERITY_ERROR,
		TargetService: "order-service",
		DetailJson:    fmt.Sprintf(`{"pattern":"conn pool exhausted #%d"}`, n),
	})
	if err != nil {
		t.Fatalf("observation.New: %v", err)
	}
	return o
}

// consumeAll drains the consumer until the fake reports it caught up.
func consumeAll(t *testing.T, c Consumer) []Message {
	t.Helper()
	var got []Message
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Consume(ctx, func(_ context.Context, m Message) error {
		got = append(got, m)
		return nil
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	return got
}
