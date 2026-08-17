package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRCAgent_NameAndTopics(t *testing.T) {
	a := NewRCAgent(nil, slog.Default()) // nil LLM -> uses heuristic
	if got := a.Name(); got != "agent-rca" {
		t.Errorf("Name() = %q, want %q", got, "agent-rca")
	}
	if got := a.InputTopic(); got != "observations-log" {
		t.Errorf("InputTopic() = %q, want %q", got, "observations-log")
	}
	if got := a.OutputTopic(); got != "observations-rca" {
		t.Errorf("OutputTopic() = %q, want %q", got, "observations-rca")
	}
}

func TestRCAgent_Handle_NoInputs(t *testing.T) {
	a := NewRCAgent(nil, slog.Default())
	got, err := a.Handle(context.Background(), "session-1", nil)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Handle() returned %d outputs, want 0", len(got))
	}
}

func TestRCAgent_Handle_HeuristicRCA_TimeoutCascade(t *testing.T) {
	a := NewRCAgent(nil, slog.Default()) // force heuristic
	inputs := []*observationv1.Observation{
		newTraceObs("obs-2", "session-1", "svc-a",
			`{"span_id":"span-1","trace_id":"trace-1","service_name":"order-service","operation_name":"GET /orders","duration_ms":8000.0,"p99_ms":500.0}`),
	}
	for i := 0; i < 6; i++ {
		inputs = append(inputs, newRCAObs(fmt.Sprintf("obs-%d", i), "session-1", "svc-a",
			`{"pattern":"log_pattern","count":10}`))
	}
	got, err := a.Handle(context.Background(), "session-1", inputs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Handle() returned %d outputs, want 1", len(got))
	}
	o := got[0]
	if o.GetType() != observationv1.EvidenceType_EVIDENCE_TYPE_RCA_RESULT {
		t.Errorf("type = %v, want RCA_RESULT", o.GetType())
	}
	if o.GetSource() != "agent-rca" {
		t.Errorf("source = %q, want %q", o.GetSource(), "agent-rca")
	}
}

func TestRCAgent_Handle_HeuristicRCA_DBPool(t *testing.T) {
	a := NewRCAgent(nil, slog.Default())
	var errLogs []*observationv1.Observation
	for i := 0; i < 12; i++ {
		errLogs = append(errLogs, newLogObs(fmt.Sprintf("obs-%d", i), "session-1", "svc-a", observationv1.Severity_SEVERITY_ERROR))
	}
	got, err := a.Handle(context.Background(), "session-1", errLogs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Handle() returned %d outputs, want 1", len(got))
	}
	o := got[0]
	if o.GetSeverity() != observationv1.Severity_SEVERITY_ERROR {
		t.Errorf("severity = %v, want ERROR", o.GetSeverity())
	}
	var out RCAOutput
	if err := json.Unmarshal([]byte(o.GetDetailJson()), &out); err != nil {
		t.Fatalf("failed to unmarshal RCA output: %v", err)
	}
	if out.RootCausePattern == "" {
		t.Errorf("RootCausePattern is empty")
	}
}

// --- test helpers ---

func newRCAObs(id, sessionID, svc, detail string) *observationv1.Observation {
	o, _ := observation.New(&observationv1.Observation{
		Id:            id,
		SessionId:     sessionID,
		Source:        "agent-log",
		Type:          observationv1.EvidenceType_EVIDENCE_TYPE_LOG,
		SubType:       "log_pattern",
		Severity:      observationv1.Severity_SEVERITY_ERROR,
		TargetService: svc,
		DetailJson:    detail,
		Timestamp:     timestamppb.Now(),
	})
	return o
}
