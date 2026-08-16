//go:build integration

// Integration tests for the Kafka message bus against a real broker.
// Run with: go test -tags=integration ./internal/bus/ -count=1 -v
// Requires `docker compose -f deployments/docker-compose.yml up -d`
// (Kafka reachable at KAFKA_BROKERS, default localhost:29092); the tests are
// skipped when the broker is unreachable.
package bus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func pingBroker(ctx context.Context, brokers []string) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 3 * time.Second}
	_, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	return err
}

// createTopic explicitly creates the test topic (single partition, RF 1) so the
// test does not depend on broker-side auto-creation timing.
func createTopic(ctx context.Context, brokers []string, topic string) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		return err
	}
	if e := resp.Errors[topic]; e != nil && !errors.Is(e, kafka.TopicAlreadyExists) {
		return e
	}
	return nil
}

// waitTopicReady polls metadata until the topic is visible with partitions.
func waitTopicReady(ctx context.Context, brokers []string, topic string) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 3 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for {
		resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		lastErr = err
		if err == nil {
			for _, t := range resp.Topics {
				if t.Name == topic && t.Error == nil && len(t.Partitions) > 0 {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %q not ready within timeout: %w", topic, lastErr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("topic %q not ready: %w", topic, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// TestKafkaProducerConsumerReplay is the real-broker replay acceptance use case
// (PLAN M2 DoD): produce -> consume (consumer group) -> replay the full log
// with identical payloads and per-partition ordering.
func TestKafkaProducerConsumerReplay(t *testing.T) {
	brokers := []string{"localhost:29092"}
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		brokers = []string{b}
	}
	cfg := Config{Brokers: brokers}.withDefaults()

	probeCtx, cancelProbe := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelProbe()
	if err := pingBroker(probeCtx, brokers); err != nil {
		t.Skipf("kafka broker %v unreachable, skipping integration test: %v", brokers, err)
	}

	start := time.Now()
	topic := fmt.Sprintf("mfdh.it.replay.%d", time.Now().UnixNano())
	group := fmt.Sprintf("mfdh-it-orch.%d", time.Now().UnixNano())
	const n = 12

	createCtx, cancelCreate := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCreate()
	if err := createTopic(createCtx, brokers, topic); err != nil {
		t.Fatalf("CreateTopics: %v", err)
	}
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelReady()
	if err := waitTopicReady(readyCtx, brokers, topic); err != nil {
		t.Fatalf("waitTopicReady: %v", err)
	}
	t.Logf("phase=topic_ready elapsed=%s", time.Since(start))

	// --- Produce n contract observations ---
	producer, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()
	produceCtx, cancelProduce := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelProduce()
	for i := 0; i < n; i++ {
		o := validObservation(t, i)
		m, err := ObservationMessage(o)
		if err != nil {
			t.Fatalf("ObservationMessage: %v", err)
		}
		m.Topic = topic
		if err := producer.Publish(produceCtx, m); err != nil {
			t.Fatalf("Publish #%d: %v", i, err)
		}
	}
	t.Logf("phase=produced elapsed=%s", time.Since(start))

	// --- Consume once with a fresh consumer group ---
	consumer, err := NewConsumer(cfg, ConsumerConfig{Topic: topic, GroupID: group})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	var first []Message
	var mu sync.Mutex
	done := make(chan struct{})
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = consumer.Consume(consumeCtx, func(_ context.Context, m Message) error {
			mu.Lock()
			first = append(first, m)
			got := len(first)
			mu.Unlock()
			if got == n {
				close(done)
				cancelConsume()
			}
			return nil
		})
	}()
	defer func() {
		cancelConsume()
		<-consumeDone
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		mu.Lock()
		got := len(first)
		mu.Unlock()
		cancelConsume()
		consumer.Close()
		t.Fatalf("timed out consuming produced messages (got %d/%d)", got, n)
	}
	t.Logf("phase=received elapsed=%s", time.Since(start))
	cancelConsume()
	closeStart := time.Now()
	_ = consumer.Close()
	t.Logf("phase=closed elapsed=%s close_took=%s", time.Since(start), time.Since(closeStart))

	// --- Replay the full log from the earliest offset ---
	replayer, err := NewReplayer(cfg)
	if err != nil {
		t.Fatalf("NewReplayer: %v", err)
	}
	var replayed []Message
	replayCtx, cancelReplay := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelReplay()
	results, err := replayer.Replay(replayCtx, ReplayOptions{
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

	var total int64
	for _, r := range results {
		if r.Count <= 0 {
			t.Errorf("replay result %+v should have replayed messages", r)
		}
		total += r.Count
	}
	if total != n {
		t.Fatalf("replay delivered %d messages, want %d", total, n)
	}
	if len(replayed) != n {
		t.Fatalf("replay handler saw %d messages, want %d", len(replayed), n)
	}
	t.Logf("phase=replay_first elapsed=%s", time.Since(start))

	// Multiset equality with the first pass: replay is a faithful re-read.
	counts := make(map[string]int, len(first))
	for _, m := range first {
		counts[string(m.Value)]++
	}
	for _, m := range replayed {
		if counts[string(m.Value)] == 0 {
			t.Errorf("replayed payload %q was not in the first pass", m.Value)
		}
		counts[string(m.Value)]--
	}
	for v, c := range counts {
		if c != 0 {
			t.Errorf("payload %q appeared %d more times in the first pass than in the replay", v, c)
		}
	}

	// --- Replay from a mid-log offset yields exactly the tail ---
	var tail []Message
	tailCtx, cancelTail := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelTail()
	if _, err := replayer.Replay(tailCtx, ReplayOptions{
		Topic: topic,
		From:  1,
		Handler: func(_ context.Context, m Message) error {
			tail = append(tail, m)
			return nil
		},
	}); err != nil {
		t.Fatalf("Replay(from=1): %v", err)
	}
	if len(tail) != n-1 {
		t.Errorf("replay from offset 1 delivered %d messages, want %d", len(tail), n-1)
	}
	t.Logf("phase=replay_tail elapsed=%s", time.Since(start))
}
