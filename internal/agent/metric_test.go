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

func TestMetricAgent_NameAndTopics(t *testing.T) {
	a := NewMetricAgent(slog.Default())
	if got := a.Name(); got != "agent-metric" {
		t.Errorf("Name() = %q, want %q", got, "agent-metric")
	}
	if got := a.InputTopic(); got != bus.TopicCommandsMetric {
		t.Errorf("InputTopic() = %q, want %q", got, bus.TopicCommandsMetric)
	}
	if got := a.OutputTopic(); got != bus.TopicObservationsMetric {
		t.Errorf("OutputTopic() = %q, want %q", got, bus.TopicObservationsMetric)
	}
}

func TestMetricAgent_Handle_Anomaly(t *testing.T) {
	a := NewMetricAgent(slog.Default())
	inputs := []*observationv1.Observation{
		newMetricObs("obs-1", "session-1", "svc-a",
			`{"metric_name":"cpu_usage","value":95.0,"threshold":80.0,"relation":"gt"}`),
	}
	got, err := a.Handle(context.Background(), "session-1", inputs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) == 0 {
		t.Fatal("Handle() returned 0 outputs, want at least 1")
	}
	o := got[0]
	if o.GetSource() != "agent-metric" {
		t.Errorf("source = %q, want %q", o.GetSource(), "agent-metric")
	}
	if o.GetType() != observationv1.EvidenceType_EVIDENCE_TYPE_METRIC {
		t.Errorf("type = %v, want METRIC", o.GetType())
	}
	if o.GetSubType() != "metric_anomaly" {
		t.Errorf("sub_type = %q, want %q", o.GetSubType(), "metric_anomaly")
	}
}

func TestMetricAgent_Handle_NoAnomaly(t *testing.T) {
	a := NewMetricAgent(slog.Default())
	inputs := []*observationv1.Observation{
		newMetricObs("obs-1", "session-1", "svc-a",
			`{"metric_name":"cpu_usage","value":50.0,"threshold":80.0,"relation":"gt"}`),
	}
	got, err := a.Handle(context.Background(), "session-1", inputs)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Handle() returned %d outputs, want 0 (value below threshold)", len(got))
	}
}

func newMetricObs(id, sessionID, svc, detail string) *observationv1.Observation {
	o, _ := observation.New(&observationv1.Observation{
		Id:            id,
		SessionId:     sessionID,
		Source:        "prometheus",
		Type:          observationv1.EvidenceType_EVIDENCE_TYPE_METRIC,
		SubType:       "raw",
		Severity:      observationv1.Severity_SEVERITY_INFO,
		TargetService: svc,
		DetailJson:    detail,
		Timestamp:     timestamppb.Now(),
	})
	return o
}
