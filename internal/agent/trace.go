package agent

import (
	"context"
	"encoding/json"
	"log/slog"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// TraceAgent analyzes distributed trace observations and identifies bottlenecks.
type TraceAgent struct {
	log *slog.Logger
}

// NewTraceAgent creates a new TraceAgent.
func NewTraceAgent(log *slog.Logger) *TraceAgent {
	return &TraceAgent{log: log}
}

// Name implements Agent.
func (a *TraceAgent) Name() string { return "agent-trace" }

// InputTopic implements Agent (consumes commands from orchestrator).
func (a *TraceAgent) InputTopic() string { return bus.TopicCommandsTrace }

// OutputTopic implements Agent (emits to its own observations topic).
func (a *TraceAgent) OutputTopic() string { return bus.TopicObservationsTrace }

// Handle implements Agent. It processes TRACE type observations, identifies
// slow spans and bottlenecks, and emits TRACE type observations with
// sub_type=trace_bottleneck.
func (a *TraceAgent) Handle(ctx context.Context, sessionID string, inputs []*observationv1.Observation) ([]*observationv1.Observation, error) {
	// Check for collect command: ALERT with sub_type="collect_command".
	for _, o := range inputs {
		if o.GetType() == observationv1.EvidenceType_EVIDENCE_TYPE_ALERT && o.GetSubType() == "collect_command" {
			// Emit synthetic slow span bottleneck for demo.
			detail, _ := json.Marshal(traceBottleneck{
				SpanID:        "span-001",
				TraceID:       "trace-abc123",
				ServiceName:   o.GetTargetService(),
				OperationName: "/orders",
				DurationMs:    1200.0,
				P99Ms:         500.0,
				Severity:     observationv1.Severity_SEVERITY_ERROR,
				Confidence:    0.8,
				TargetService: o.GetTargetService(),
				Correlations:  map[string]string{"trace_id": "trace-abc123", "span_id": "span-001"},
				Labels:        o.GetLabels(),
			})
			o, err := observation.New(&observationv1.Observation{
				SessionId:     sessionID,
				Source:        a.Name(),
				Type:          observationv1.EvidenceType_EVIDENCE_TYPE_TRACE,
				SubType:       "trace_bottleneck",
				Confidence:    0.8,
				Severity:      observationv1.Severity_SEVERITY_ERROR,
				TargetService: o.GetTargetService(),
				Correlations:  map[string]string{"trace_id": "trace-abc123", "span_id": "span-001"},
				DetailJson:    string(detail),
				Labels:        o.GetLabels(),
			})
			if err != nil {
				a.log.Error("trace agent: emit synthetic observation", "error", err)
				continue
			}
			return []*observationv1.Observation{o}, nil
		}
	}

	var traces []*observationv1.Observation
	for _, o := range inputs {
		if o.GetType() == observationv1.EvidenceType_EVIDENCE_TYPE_TRACE {
			traces = append(traces, o)
		}
	}
	if len(traces) == 0 {
		return nil, nil
	}

	var outputs []*observationv1.Observation
	bottlenecks := detectTraceBottlenecks(traces)
	for _, bn := range bottlenecks {
		detail, _ := json.Marshal(bn)
		o, err := observation.New(&observationv1.Observation{
			SessionId:     sessionID,
			Source:        a.Name(),
			Type:          observationv1.EvidenceType_EVIDENCE_TYPE_TRACE,
			SubType:       "trace_bottleneck",
			Confidence:    bn.Confidence,
			Severity:      bn.Severity,
			TargetService: bn.TargetService,
			Correlations:  bn.Correlations,
			DetailJson:    string(detail),
			Labels:        bn.Labels,
		})
		if err != nil {
			a.log.Error("trace agent: emit bottleneck observation", "error", err)
			continue
		}
		outputs = append(outputs, o)
	}
	return outputs, nil
}

// traceBottleneck represents a detected trace bottleneck.
type traceBottleneck struct {
	SpanID        string                   `json:"span_id"`
	TraceID       string                   `json:"trace_id"`
	ServiceName   string                   `json:"service_name"`
	OperationName string                   `json:"operation_name"`
	DurationMs    float64                  `json:"duration_ms"`
	P99Ms         float64                  `json:"p99_ms"`
	Severity      observationv1.Severity   `json:"severity"`
	Confidence    float64                  `json:"confidence"`
	TargetService string                   `json:"target_service"`
	Correlations  map[string]string        `json:"correlations"`
	Labels        map[string]string        `json:"labels"`
}

// detectTraceBottlenecks analyzes trace observations to identify slow spans.
// Production would use ClickHouse materialized views over trace data.
func detectTraceBottlenecks(traces []*observationv1.Observation) []traceBottleneck {
	var bottlenecks []traceBottleneck
	for _, t := range traces {
		var payload struct {
			SpanID        string  `json:"span_id"`
			TraceID       string  `json:"trace_id"`
			ServiceName   string  `json:"service_name"`
			OperationName string  `json:"operation_name"`
			DurationMs    float64 `json:"duration_ms"`
			P99Ms         float64 `json:"p99_ms"`
		}
		if err := json.Unmarshal([]byte(t.GetDetailJson()), &payload); err != nil {
			continue
		}
		if payload.SpanID == "" || payload.DurationMs == 0 {
			continue
		}

		// Heuristic: if duration > p99 * 2, it's a bottleneck.
		threshold := payload.P99Ms * 2
		if threshold == 0 {
			threshold = 1000 // default 1s threshold
		}
		if payload.DurationMs <= threshold {
			continue
		}

		severity := observationv1.Severity_SEVERITY_WARN
		confidence := 0.75
		if payload.DurationMs > threshold*3 {
			severity = observationv1.Severity_SEVERITY_ERROR
			confidence = 0.85
		}
		if payload.DurationMs > threshold*5 {
			severity = observationv1.Severity_SEVERITY_FATAL
			confidence = 0.95
		}

		correlations := make(map[string]string)
		if payload.TraceID != "" {
			correlations["trace_id"] = payload.TraceID
		}
		correlations["span_id"] = payload.SpanID

		bottlenecks = append(bottlenecks, traceBottleneck{
			SpanID:        payload.SpanID,
			TraceID:       payload.TraceID,
			ServiceName:   payload.ServiceName,
			OperationName: payload.OperationName,
			DurationMs:    payload.DurationMs,
			P99Ms:         payload.P99Ms,
			Severity:      severity,
			Confidence:    confidence,
			TargetService: t.GetTargetService(),
			Correlations:  correlations,
			Labels:        t.GetLabels(),
		})
	}
	return bottlenecks
}
