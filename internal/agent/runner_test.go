package agent

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// fakeSource is an in-memory messageSource that records MarkAsProcessed calls.
type fakeSource struct {
	mu            sync.Mutex
	processed     []bus.Message
	subscribeCalls int
}

func (f *fakeSource) Subscribe(_ context.Context, _ string, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribeCalls++
	return nil
}

func (f *fakeSource) Messages() <-chan bus.Message {
	ch := make(chan bus.Message)
	close(ch)
	return ch
}

func (f *fakeSource) MarkAsProcessed(msg bus.Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.processed = append(f.processed, msg)
}

func (f *fakeSource) countProcessed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.processed)
}

// fakeAgent is an in-memory Agent that records every Handle call.
type fakeAgent struct {
	name string
	mu   sync.Mutex
	calls []string // session IDs of received handles
}

func (a *fakeAgent) Name() string                      { return a.name }
func (a *fakeAgent) InputTopic() string                { return bus.TopicObservations }
func (a *fakeAgent) OutputTopic() string               { return bus.TopicObservations }
func (a *fakeAgent) Handle(_ context.Context, sessionID string, _ []*observationv1.Observation) ([]*observationv1.Observation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, sessionID)
	return nil, nil
}

func (a *fakeAgent) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.calls)
}

// recordingProducer records published messages.
type recordingProducer struct {
	mu      sync.Mutex
	msgs    []bus.Message
}

func (p *recordingProducer) Publish(_ context.Context, msg bus.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msg)
	return nil
}

func (p *recordingProducer) Close() error { return nil }

func (p *recordingProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.msgs)
}

func mustObservation(t *testing.T, o *observationv1.Observation) []byte {
	t.Helper()
	b, err := observation.ToJSON(o)
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	return b
}

func newTestRunner(t *testing.T, a Agent) (*Runner, *fakeSource, *recordingProducer) {
	t.Helper()
	src := &fakeSource{}
	prod := &recordingProducer{}
	r := NewRunner(a, src, prod, slog.New(slog.DiscardHandler))
	return r, src, prod
}

// TestRunnerSkipsOwnMessages guards against the RCA/Fix feedback loop: an agent
// whose input and output share a topic must not feed its own observations back
// into Handle (that produced ~1.1M garbage RCA_RESULT messages in a past test
// run and buried the orchestrator's consumer group behind a huge backlog).
func TestRunnerSkipsOwnMessages(t *testing.T) {
	rca := &fakeAgent{name: "agent-rca"}
	r, src, prod := newTestRunner(t, rca)

	ctx := context.Background()

	// Message produced by the RCA agent itself: must be skipped without Handle.
	own := bus.Message{
		Topic:   bus.TopicObservations,
		Key:     []byte("sess-1"),
		Value: mustObservation(t, &observationv1.Observation{
			SessionId: "sess-1",
			Source:    "agent-rca",
			Type:      observationv1.EvidenceType_EVIDENCE_TYPE_RCA_RESULT,
			SubType:   "rca_result",
		}),
	}
	r.processMessage(ctx, own)
	if got := rca.callCount(); got != 0 {
		t.Fatalf("Handle called %d times for own message, want 0", got)
	}
	if got := src.countProcessed(); got != 1 {
		t.Fatalf("own message not marked as processed: %d", got)
	}
	if got := prod.count(); got != 0 {
		t.Fatalf("published %d messages for own message, want 0", got)
	}

	// Message produced by another agent (upstream evidence): must reach Handle.
	upstream := bus.Message{
		Topic:   bus.TopicObservations,
		Key:     []byte("sess-1"),
		Value: mustObservation(t, &observationv1.Observation{
			SessionId: "sess-1",
			Source:    "agent-log",
			Type:      observationv1.EvidenceType_EVIDENCE_TYPE_LOG,
			SubType:   "log_pattern",
		}),
	}
	r.processMessage(ctx, upstream)
	if got := rca.callCount(); got != 1 {
		t.Fatalf("Handle called %d times for upstream message, want 1", got)
	}
}

// TestRunnerSkipsOwnMessagesNonRCAAgent verifies the guard is generic: any
// agent (e.g. agent-fix) also skips its own output.
func TestRunnerSkipsOwnMessagesNonRCAAgent(t *testing.T) {
	fix := &fakeAgent{name: "agent-fix"}
	r, _, prod := newTestRunner(t, fix)

	ctx := context.Background()
	msg := bus.Message{
		Topic: bus.TopicObservations,
		Key:   []byte("sess-2"),
		Value: mustObservation(t, &observationv1.Observation{
			SessionId: "sess-2",
			Source:    "agent-fix",
			Type:      observationv1.EvidenceType_EVIDENCE_TYPE_FIX_ACTION,
			SubType:   "fix_action",
		}),
	}
	r.processMessage(ctx, msg)
	if got := fix.callCount(); got != 0 {
		t.Fatalf("Handle called %d times for own FIX_ACTION, want 0", got)
	}
	if got := prod.count(); got != 0 {
		t.Fatalf("published %d messages for own FIX_ACTION, want 0", got)
	}
}
