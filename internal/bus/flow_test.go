package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

func TestKafkaMessageConversion(t *testing.T) {
	in := Message{
		Topic:     "mfdh.observations",
		Partition: 7,
		Offset:    42,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers:   map[string]string{"mfdh-source": "agent-log", "x": "y"},
		Time:      time.Now(),
	}
	km := toKafkaMessage(in)
	km.Partition = in.Partition
	km.Offset = in.Offset
	if km.Topic != in.Topic || string(km.Key) != "k" || string(km.Value) != "v" ||
		km.Partition != in.Partition || km.Offset != in.Offset || !km.Time.Equal(in.Time) {
		t.Error("kafka message fields diverged")
	}
	out := fromKafkaMessage(km)
	if out.Topic != in.Topic || out.Offset != in.Offset || out.Partition != in.Partition ||
		string(out.Key) != string(in.Key) || string(out.Value) != string(in.Value) || !out.Time.Equal(in.Time) {
		t.Errorf("round-trip envelope diverged: %+v", out)
	}
	if out.Headers["mfdh-source"] != "agent-log" || out.Headers["x"] != "y" {
		t.Errorf("round-trip headers diverged: %v", out.Headers)
	}
}

func TestObservationMessageRoundTrip(t *testing.T) {
	o := validObservation(t, 1)
	m, err := ObservationMessage(o)
	if err != nil {
		t.Fatalf("ObservationMessage: %v", err)
	}
	if m.Headers[HeaderSource] != o.Source || m.Headers[HeaderSessionID] != o.SessionId ||
		m.Headers[HeaderSchemaVersion] != "1" {
		t.Errorf("headers diverged: %v", m.Headers)
	}
	got, err := DecodeObservation(m)
	if err != nil {
		t.Fatalf("DecodeObservation: %v", err)
	}
	if !observation.Equal(o, got) {
		t.Error("observation round-trip through the bus changed the contract")
	}
}

func TestObservationMessageRejectsInvalid(t *testing.T) {
	bad := &observationv1.Observation{Id: "x", SessionId: "s", Source: "a"} // missing type/severity
	if _, err := ObservationMessage(bad); err == nil {
		t.Fatal("invalid observation should be rejected before publishing")
	}
}

func TestPublishConsumeFlow(t *testing.T) {
	b := newMemBus()
	producer := Producer(b)
	topic := "mfdh.observations"

	const n = 5
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

	consumer := Consumer(memConsumer{b: b, cc: ConsumerConfig{Topic: topic, GroupID: "mfdh-orchestrator-observations"}})
	got := consumeAll(t, consumer)
	if len(got) != n {
		t.Fatalf("consumed %d messages, want %d", len(got), n)
	}
	for i, m := range got {
		if m.Offset != int64(i) {
			t.Errorf("message %d offset = %d, want %d", i, m.Offset, i)
		}
		o, err := DecodeObservation(m)
		if err != nil {
			t.Fatalf("DecodeObservation #%d: %v", i, err)
		}
		if o.SessionId != fmt.Sprintf("session-%02d", i) {
			t.Errorf("message %d session = %q", i, o.SessionId)
		}
	}
}

// TestConsumeRedeliversFailedMessage verifies at-least-once semantics: a failed
// handler does not advance the offset, so the same message is delivered again
// on the next run.
func TestConsumeRedeliversFailedMessage(t *testing.T) {
	b := newMemBus()
	topic := "mfdh.observations"
	for i := 0; i < 3; i++ {
		if err := b.Publish(context.Background(), Message{Topic: topic, Key: []byte("k"), Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	consumer := Consumer(memConsumer{b: b, cc: ConsumerConfig{Topic: topic, GroupID: "g1"}})

	// First run: handler rejects message 1 -> Consume returns the error and
	// the offset stays at 1 (message 0 was committed).
	err := consumer.Consume(context.Background(), func(_ context.Context, m Message) error {
		if string(m.Value) == "v1" {
			return errors.New("boom")
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Consume should fail on v1, got %v", err)
	}

	// Second run: everything succeeds; v1 (the failed message) is redelivered,
	// while v0 (already committed) is not.
	got := consumeAll(t, consumer)
	if len(got) != 2 {
		t.Fatalf("redelivered %d messages, want 2 (v1..v2, at-least-once)", len(got))
	}
	for i, m := range got {
		if string(m.Value) != fmt.Sprintf("v%d", i+1) {
			t.Errorf("redelivered message %d = %q", i, m.Value)
		}
	}
}

func TestMemBusRejectsEmptyTopic(t *testing.T) {
	b := newMemBus()
	if err := b.Publish(context.Background(), Message{}); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("Publish with empty topic = %v, want ErrEmptyTopic", err)
	}
}
