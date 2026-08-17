package agent

import (
	"context"
	"encoding/json"
	"log/slog"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// MetricAgent analyzes metric observations and detects anomalies.
type MetricAgent struct {
	log *slog.Logger
}

// NewMetricAgent creates a new MetricAgent.
func NewMetricAgent(log *slog.Logger) *MetricAgent {
	return &MetricAgent{log: log}
}

// Name implements Agent.
func (a *MetricAgent) Name() string { return "agent-metric" }

// InputTopic implements Agent.
func (a *MetricAgent) InputTopic() string { return "observations-raw" }

// OutputTopic implements Agent.
func (a *MetricAgent) OutputTopic() string { return "observations-metric" }

// Handle implements Agent. It processes METRIC type observations, detects
// anomalies using simple threshold heuristics, and emits METRIC type
// observations with sub_type=metric_anomaly.
func (a *MetricAgent) Handle(ctx context.Context, sessionID string, inputs []*observationv1.Observation) ([]*observationv1.Observation, error) {
	var metrics []*observationv1.Observation
	for _, o := range inputs {
		if o.GetType() == observationv1.EvidenceType_EVIDENCE_TYPE_METRIC {
			metrics = append(metrics, o)
		}
	}
	if len(metrics) == 0 {
		return nil, nil
	}

	var outputs []*observationv1.Observation
	anomalies := detectMetricAnomalies(metrics)
	for _, ano := range anomalies {
		detail, _ := json.Marshal(ano)
		o, err := observation.New(&observationv1.Observation{
			SessionId:     sessionID,
			Source:        a.Name(),
			Type:          observationv1.EvidenceType_EVIDENCE_TYPE_METRIC,
			SubType:       "metric_anomaly",
			Confidence:    ano.Confidence,
			Severity:      ano.Severity,
			TargetService: ano.TargetService,
			Correlations:  ano.Correlations,
			DetailJson:    string(detail),
			Labels:        ano.Labels,
		})
		if err != nil {
			a.log.Error("metric agent: emit anomaly observation", "error", err)
			continue
		}
		outputs = append(outputs, o)
	}
	return outputs, nil
}

// metricAnomaly represents a detected metric anomaly.
type metricAnomaly struct {
	MetricName    string            `json:"metric_name"`
	Value         float64           `json:"value"`
	Threshold     float64           `json:"threshold"`
	Relation      string            `json:"relation"` // gt, lt, eq
	AnomalyType   string            `json:"anomaly_type"`
	Confidence    float64           `json:"confidence"`
	Severity      observationv1.Severity `json:"severity"`
	TargetService string            `json:"target_service"`
	Correlations  map[string]string `json:"correlations"`
	Labels        map[string]string `json:"labels"`
}

// detectMetricAnomalies applies simple threshold heuristics to detect anomalies.
// Production would use Prometheus / PromQL for real anomaly detection.
func detectMetricAnomalies(metrics []*observationv1.Observation) []metricAnomaly {
	var anomalies []metricAnomaly
	for _, m := range metrics {
		var payload struct {
			MetricName string  `json:"metric_name"`
			Value      float64 `json:"value"`
			Threshold  float64 `json:"threshold"`
			Relation   string  `json:"relation"` // gt, lt
		}
		if err := json.Unmarshal([]byte(m.GetDetailJson()), &payload); err != nil {
			// If we can't parse, skip
			continue
		}
		if payload.MetricName == "" || payload.Threshold == 0 {
			continue
		}
		isAnomaly := false
		switch payload.Relation {
		case "gt":
			isAnomaly = payload.Value > payload.Threshold
		case "lt":
			isAnomaly = payload.Value < payload.Threshold
		case "eq":
			isAnomaly = payload.Value == payload.Threshold
		}
		if !isAnomaly {
			continue
		}

		severity := observationv1.Severity_SEVERITY_WARN
		confidence := 0.75
		anomalyType := "threshold_breach"
		if payload.Value > payload.Threshold*2 {
			severity = observationv1.Severity_SEVERITY_ERROR
			confidence = 0.85
		}
		if payload.Value > payload.Threshold*5 {
			severity = observationv1.Severity_SEVERITY_FATAL
			confidence = 0.95
		}

		anomalies = append(anomalies, metricAnomaly{
			MetricName:    payload.MetricName,
			Value:         payload.Value,
			Threshold:     payload.Threshold,
			Relation:      payload.Relation,
			AnomalyType:   anomalyType,
			Confidence:    confidence,
			Severity:      severity,
			TargetService: m.GetTargetService(),
			Correlations:  m.GetCorrelations(),
			Labels:        m.GetLabels(),
		})
	}
	return anomalies
}
