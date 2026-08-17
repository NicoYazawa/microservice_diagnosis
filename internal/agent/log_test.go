package agent

import (
	"context"
	"log/slog"
	"testing"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLogAgent_NameAndTopics(t *testing.T) {
	a := NewLogAgent(slog.Default())
	if got := a.Name(); got != "agent-log" {
		t.Errorf("Name() = %q, want %q", got, "agent-log")
	}
	if got := a.InputTopic(); got != "observations-raw" {
		t.Errorf("InputTopic() = %q, want %q", got, "observations-raw")
	}
	if got := a.OutputTopic(); got != "observations-log" {
		t.Errorf("OutputTopic() = %q, want %q", got, "observations-log")
	}
}

func TestLogAgent_Handle_NoLogInputs(t *testing.T) {
	a := NewLogAgent(slog.Default())
	inputs := []*observationv1.Observation{
		{
			Id:        "obs-1",
			SessionId: "session-1",
			Source:    "agent-metric",
			Type:      observationv1.EvidenceType_EVIDENCE_TYPE_METRIC,
			Severity:  observationv1.Severity_SEVERITY_ERROR,
		},
	}
	got, err := a.Handle(context.Background(), "session-1", inputs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Handle() returned %d outputs, want 0", len(got))
	}
}

func TestLogAgent_Handle_ErrorLogs(t *testing.T) {
	a := NewLogAgent(slog.Default())
	inputs := []*observationv1.Observation{
		newLogObs("obs-1", "session-1", "svc-a", observationv1.Severity_SEVERITY_ERROR),
		newLogObs("obs-2", "session-1", "svc-a", observationv1.Severity_SEVERITY_ERROR),
		newLogObs("obs-3", "session-1", "svc-a", observationv1.Severity_SEVERITY_ERROR),
		newLogObs("obs-4", "session-1", "svc-a", observationv1.Severity_SEVERITY_ERROR),
		newLogObs("obs-5", "session-1", "svc-a", observationv1.Severity_SEVERITY_ERROR),
		newLogObs("obs-6", "session-1", "svc-a", observationv1.Severity_SEVERITY_FATAL),
	}
	got, err := a.Handle(context.Background(), "session-1", inputs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) == 0 {
		t.Fatal("Handle() returned 0 outputs, want at least 1")
	}
	for _, o := range got {
		if o.GetSource() != "agent-log" {
			t.Errorf("output source = %q, want %q", o.GetSource(), "agent-log")
		}
		if o.GetType() != observationv1.EvidenceType_EVIDENCE_TYPE_LOG {
			t.Errorf("output type = %v, want LOG", o.GetType())
		}
		if o.GetSubType() != "log_pattern" {
			t.Errorf("output sub_type = %q, want %q", o.GetSubType(), "log_pattern")
		}
		if o.GetSessionId() != "session-1" {
			t.Errorf("output session_id = %q, want %q", o.GetSessionId(), "session-1")
		}
	}
}

func TestLogAgent_Handle_OnlyInfoLogs(t *testing.T) {
	a := NewLogAgent(slog.Default())
	inputs := []*observationv1.Observation{
		newLogObs("obs-1", "session-1", "svc-a", observationv1.Severity_SEVERITY_INFO),
		newLogObs("obs-2", "session-1", "svc-a", observationv1.Severity_SEVERITY_INFO),
	}
	got, err := a.Handle(context.Background(), "session-1", inputs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Handle() returned %d outputs, want 0 (no errors to aggregate)", len(got))
	}
}

func newLogObs(id, sessionID, svc string, sev observationv1.Severity) *observationv1.Observation {
	o, _ := observation.New(&observationv1.Observation{
		Id:            id,
		SessionId:     sessionID,
		Source:        "raw-log-collector",
		Type:          observationv1.EvidenceType_EVIDENCE_TYPE_LOG,
		SubType:       "raw",
		Severity:      sev,
		TargetService: svc,
		Timestamp:     timestamppb.Now(),
	})
	return o
}
