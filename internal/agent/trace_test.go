package agent

import (
	"context"
	"log/slog"
	"testing"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTraceAgent_NameAndTopics(t *testing.T) {
	a := NewTraceAgent(slog.Default())
	if got := a.Name(); got != "agent-trace" {
		t.Errorf("Name() = %q, want %q", got, "agent-trace")
	}
	if got := a.InputTopic(); got != bus.TopicCommandsTrace {
		t.Errorf("InputTopic() = %q, want %q", got, bus.TopicCommandsTrace)
	}
	if got := a.OutputTopic(); got != bus.TopicObservationsTrace {
		t.Errorf("OutputTopic() = %q, want %q", got, bus.TopicObservationsTrace)
	}
}

func TestTraceAgent_Handle_SlowSpan(t *testing.T) {
	a := NewTraceAgent(slog.Default())
	inputs := []*observationv1.Observation{
		newTraceObs("obs-1", "session-1", "svc-a",
			`{"span_id":"span-abc","trace_id":"trace-123","service_name":"order-service","operation_name":"GET /orders","duration_ms":5000.0,"p99_ms":500.0}`),
	}
	got, err := a.Handle(context.Background(), "session-1", inputs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) == 0 {
		t.Fatal("Handle() returned 0 outputs, want at least 1")
	}
	o := got[0]
	if o.GetSource() != "agent-trace" {
		t.Errorf("source = %q, want %q", o.GetSource(), "agent-trace")
	}
	if o.GetType() != observationv1.EvidenceType_EVIDENCE_TYPE_TRACE {
		t.Errorf("type = %v, want TRACE", o.GetType())
	}
	if o.GetSubType() != "trace_bottleneck" {
		t.Errorf("sub_type = %q, want %q", o.GetSubType(), "trace_bottleneck")
	}
}

func TestTraceAgent_Handle_NormalSpan(t *testing.T) {
	a := NewTraceAgent(slog.Default())
	inputs := []*observationv1.Observation{
		newTraceObs("obs-1", "session-1", "svc-a",
			`{"span_id":"span-abc","trace_id":"trace-123","service_name":"order-service","operation_name":"GET /orders","duration_ms":50.0,"p99_ms":500.0}`),
	}
	got, err := a.Handle(context.Background(), "session-1", inputs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Handle() returned %d outputs, want 0 (span not slow)", len(got))
	}
}

func newTraceObs(id, sessionID, svc, detail string) *observationv1.Observation {
	o, _ := observation.New(&observationv1.Observation{
		Id:            id,
		SessionId:     sessionID,
		Source:        "otel-collector",
		Type:          observationv1.EvidenceType_EVIDENCE_TYPE_TRACE,
		SubType:       "raw",
		Severity:      observationv1.Severity_SEVERITY_INFO,
		TargetService: svc,
		DetailJson:    detail,
		Timestamp:     timestamppb.Now(),
	})
	return o
}
