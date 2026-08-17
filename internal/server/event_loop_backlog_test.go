//go:build integration

// Integration test for the orchestrator's backlog skip logic against a real
// broker. Run with: go test -tags=integration ./internal/server/ -count=1 -v
// Requires Kafka reachable at localhost:29092 (docker compose); skipped otherwise.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func pingBroker(ctx context.Context, brokers []string) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 3 * time.Second}
	_, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	return err
}

func createTopicOnce(ctx context.Context, brokers []string, topic string) error {
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

func deleteTopic(ctx context.Context, brokers []string, topic string) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	_, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{topic}})
	return err
}

// uniqueTopicName generates a topic name with a nanosecond-precision suffix
// so concurrent or repeated test runs never collide on the same topic.
func uniqueTopicName(prefix string) string {
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func commitGroupOffset(ctx context.Context, brokers []string, group, topic string, offset int64) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	resp, err := client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		GroupID:      group,
		GenerationID: -1,
		Topics: map[string][]kafka.OffsetCommit{
			topic: {{Partition: 0, Offset: offset}},
		},
	})
	if err != nil {
		return err
	}
	for _, parts := range resp.Topics {
		for _, p := range parts {
			if p.Error != nil {
				return p.Error
			}
		}
	}
	return nil
}

func committedOffset(ctx context.Context, brokers []string, group, topic string) (int64, error) {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	offsets, err := client.ConsumerOffsets(ctx, kafka.TopicAndGroup{Topic: topic, GroupId: group})
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

func publishN(ctx context.Context, brokers []string, topic string, n int) error {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.Hash{},
	})
	defer w.Close()
	var msgs []kafka.Message
	for i := 0; i < n; i++ {
		msgs = append(msgs, kafka.Message{Key: []byte("sess"), Value: []byte("{}")})
	}
	return w.WriteMessages(ctx, msgs...)
}

func topicLastOffset(ctx context.Context, brokers []string, topic string) (int64, error) {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	resp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topic: {kafka.LastOffsetOf(0)},
		},
	})
	if err != nil {
		return 0, err
	}
	for _, po := range resp.Topics[topic] {
		return po.LastOffset, nil
	}
	return 0, errors.New("no partition offsets returned")
}

// skipIntegrationIfNoBroker skips the test if the broker is unreachable.
// Used so a missing docker compose broker doesn't break `go test`.
func skipIntegrationIfNoBroker(t *testing.T, ctx context.Context, brokers []string) {
	t.Helper()
	if err := pingBroker(ctx, brokers); err != nil {
		t.Skipf("broker unreachable: %v", err)
	}
}

// TestSkipBacklogFastJumpsLargeLag verifies that a group trailing the topic by
// more than the threshold is repositioned to the latest offset, so a new
// session's messages are not buried behind stale history.
func TestSkipBacklogFastJumpsLargeLag(t *testing.T) {
	brokers := []string{"localhost:29092"}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	skipIntegrationIfNoBroker(t, ctx, brokers)

	topic := uniqueTopicName("mfdh.observations.test-backlog")
	group := "mfdh-orchestrator-test-backlog-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := createTopicOnce(ctx, brokers, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() {
		_ = deleteTopic(context.Background(), brokers, topic)
	})

	if err := publishN(ctx, brokers, topic, 6); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Commit offset 0: a 6-message lag against threshold 5.
	if err := commitGroupOffset(ctx, brokers, group, topic, 0); err != nil {
		t.Fatalf("commit offset 0: %v", err)
	}

	if err := skipBacklogFast(ctx, brokers, group, topic, 5, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("skipBacklogFast: %v", err)
	}

	got, err := committedOffset(ctx, brokers, group, topic)
	if err != nil {
		t.Fatalf("committed offset: %v", err)
	}
	last, err := topicLastOffset(ctx, brokers, topic)
	if err != nil {
		t.Fatalf("last offset: %v", err)
	}
	if got != last {
		t.Fatalf("committed=%d want latest=%d (group should jump to tail)", got, last)
	}
}

// TestSkipBacklogFastKeepsSmallLag verifies that a small, normal lag (e.g. the
// restart window) is left untouched and consumption resumes from the committed
// offset.
func TestSkipBacklogFastKeepsSmallLag(t *testing.T) {
	brokers := []string{"localhost:29092"}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	skipIntegrationIfNoBroker(t, ctx, brokers)

	topic := uniqueTopicName("mfdh.observations.test-smalllag")
	group := "mfdh-orchestrator-test-smalllag-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := createTopicOnce(ctx, brokers, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() {
		_ = deleteTopic(context.Background(), brokers, topic)
	})

	if err := publishN(ctx, brokers, topic, 6); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := commitGroupOffset(ctx, brokers, group, topic, 5); err != nil { // 1-message lag
		t.Fatalf("commit offset 5: %v", err)
	}

	if err := skipBacklogFast(ctx, brokers, group, topic, 5, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("skipBacklogFast: %v", err)
	}

	got, err := committedOffset(ctx, brokers, group, topic)
	if err != nil {
		t.Fatalf("committed offset: %v", err)
	}
	if got != 5 {
		t.Fatalf("committed=%d want 5 (small lag must not be touched)", got)
	}
}

// TestSkipBacklogFastRejectsWhenGroupActive verifies that a group with an
// active member (a previous orchestrator instance still shutting down) is not
// repositioned: the simple commit is rejected by the broker, so the function
// falls back to consuming from the committed offset.
//
// This test exercises the underlying broker behavior the function relies on
// (simple commit rejected while a group member is active). It does NOT
// exercise the function directly: skipBacklogFast uses the full 55s
// skipBacklogTimeout to wait for membership to expire, which is too long for
// a unit test. The retry loop in skipBacklogFast is covered by the
// integration smoke (manual run with -count=1 -timeout=120s).
func TestSkipBacklogFastRejectsWhenGroupActive(t *testing.T) {
	brokers := []string{"localhost:29092"}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	skipIntegrationIfNoBroker(t, ctx, brokers)

	topic := uniqueTopicName("mfdh.observations.test-active")
	group := "mfdh-orchestrator-test-active-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := createTopicOnce(ctx, brokers, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() {
		_ = deleteTopic(context.Background(), brokers, topic)
	})

	if err := publishN(ctx, brokers, topic, 6); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := commitGroupOffset(ctx, brokers, group, topic, 0); err != nil {
		t.Fatalf("commit offset 0: %v", err)
	}

	// Occupy the group with an active consumer (kafka-go joins on construction).
	active := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: group,
	})
	t.Cleanup(func() { _ = active.Close() })
	// Wait until the reader has actually joined the group. Polling r.Stats()
	// is more reliable than a fixed sleep on slow runners.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s := active.Stats()
		if s.Lag > 0 || s.Offset >= 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// With an active member the simple commit must be rejected; skipBacklogFast
	// would retry until timeout in production, so exercise the broker behaviour
	// directly here.
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	resp, err := client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		GroupID:      group,
		GenerationID: -1,
		Topics: map[string][]kafka.OffsetCommit{
			topic: {{Partition: 0, Offset: 6}},
		},
	})
	if err != nil {
		t.Fatalf("offset commit error: %v", err)
	}
	rejected := false
	for _, parts := range resp.Topics {
		for _, p := range parts {
			if p.Error != nil {
				rejected = true
			}
		}
	}
	if !rejected {
		t.Fatal("simple commit succeeded despite active member; expect rejection")
	}
	got, err := committedOffset(ctx, brokers, group, topic)
	if err != nil {
		t.Fatalf("committed offset: %v", err)
	}
	if got != 0 {
		t.Fatalf("committed=%d want 0 (rejected commit must not move the offset)", got)
	}
}

// suppress unused-import warning for fmt/os when running in environments where
// they may be pruned by build tags.
var _ = fmt.Sprintf
var _ = os.Stdout