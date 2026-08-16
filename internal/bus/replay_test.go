package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestReplayUseCase is the M2 acceptance use case (PLAN M2 DoD, 消息回放用例):
// after messages are produced and consumed once, Replay re-delivers the full
// log from the earliest offset with identical payloads and strict ordering.
func TestReplayUseCase(t *testing.T) {
	b := newMemBus()
	topic := "mfdh.observations"
	const n = 10

	producer := Producer(b)
	for i := 0; i < n; i++ {
		o := validObservation(t, i)
		m, err := ObservationMessage(o)
		if err != nil {
			t.Fatalf("ObservationMessage: %v", err)
		}
		m.Topic = topic
		if err := producer.Publish(context.Background(), m); err != nil {
			t.Fatalf("Publish #%d: %v", i, err)
		}
	}

	// First pass: a normal consumer group drains the topic.
	first := consumeAll(t, Consumer(memConsumer{b: b, cc: ConsumerConfig{Topic: topic, GroupID: "mfdh-orchestrator-observations"}}))
	if len(first) != n {
		t.Fatalf("first consume got %d messages, want %d", len(first), n)
	}

	// Replay pass: re-read the log from the earliest offset.
	var replayed []Message
	replayer := Replayer(memReplayer{b: b})
	results, err := replayer.Replay(context.Background(), ReplayOptions{
		Topic: topic,
		From:  FirstOffset,
		Handler: func(_ context.Context, m Message) error {
			replayed = append(replayed, m)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(results) != 1 || results[0].Count != n {
		t.Fatalf("replay result = %+v, want count %d", results, n)
	}
	if len(replayed) != n {
		t.Fatalf("replayed %d messages, want %d", len(replayed), n)
	}
	for i := range replayed {
		if replayed[i].Offset != int64(i) {
			t.Errorf("replayed message %d offset = %d, want %d", i, replayed[i].Offset, i)
		}
		// Payloads must be byte-identical to the first pass (replay is a re-read).
		if string(replayed[i].Value) != string(first[i].Value) {
			t.Errorf("replayed message %d payload diverged from first pass", i)
		}
	}
}

func TestReplayFromOffset(t *testing.T) {
	b := newMemBus()
	topic := "mfdh.observations"
	const n = 6
	for i := 0; i < n; i++ {
		if err := b.Publish(context.Background(), Message{Topic: topic, Key: []byte("k"), Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	var replayed []Message
	replayer := Replayer(memReplayer{b: b})
	results, err := replayer.Replay(context.Background(), ReplayOptions{
		Topic: topic,
		From:  2,
		Handler: func(_ context.Context, m Message) error {
			replayed = append(replayed, m)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != n-2 {
		t.Fatalf("replayed %d messages, want %d", len(replayed), n-2)
	}
	for i, m := range replayed {
		if m.Offset != int64(i+2) {
			t.Errorf("replayed message %d offset = %d, want %d", i, m.Offset, i+2)
		}
		if len(results) != 1 {
			t.Fatalf("replay returned %d results, want 1", len(results))
		}
		if results[0].From != 2 || results[0].LastOffset != int64(n-1) {
			t.Errorf("unexpected result: %+v", results[0])
		}
	}
}

func TestReplayValidations(t *testing.T) {
	b := newMemBus()
	replayer := Replayer(memReplayer{b: b})
	if _, err := replayer.Replay(context.Background(), ReplayOptions{Topic: "", Handler: func(context.Context, Message) error { return nil }}); !errors.Is(err, ErrEmptyTopic) {
		t.Errorf("empty topic = %v, want ErrEmptyTopic", err)
	}
	if _, err := replayer.Replay(context.Background(), ReplayOptions{Topic: "t"}); !errors.Is(err, ErrNilHandler) {
		t.Errorf("nil handler = %v, want ErrNilHandler", err)
	}
	if _, err := replayer.Replay(context.Background(), ReplayOptions{Topic: "t", From: LastOffset, Handler: func(context.Context, Message) error { return nil }}); err == nil || !strings.Contains(err.Error(), "FirstOffset") || !strings.Contains(err.Error(), "non-negative") {
		t.Fatalf("LastOffset replay = %v, want descriptive invalid-offset error mentioning FirstOffset and non-negative offset", err)
	}
}
