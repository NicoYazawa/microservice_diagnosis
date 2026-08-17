// Package agent provides the interface and implementations for the 5 diagnostic agents.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// LogAgent collects and analyzes logs from the target service.
type LogAgent struct {
	log *slog.Logger
}

// NewLogAgent creates a new LogAgent.
func NewLogAgent(log *slog.Logger) *LogAgent {
	return &LogAgent{log: log}
}

// Name implements Agent.
func (a *LogAgent) Name() string { return "agent-log" }

// InputTopic implements Agent (Log agent consumes commands from orchestrator).
func (a *LogAgent) InputTopic() string { return bus.TopicCommandsLog }

// OutputTopic implements Agent (emits to its own observations topic).
func (a *LogAgent) OutputTopic() string { return bus.TopicObservationsLog }

// Handle implements Agent. It parses raw log observations, detects patterns,
// and emits LOG type observations with sub_type=log_pattern.
func (a *LogAgent) Handle(ctx context.Context, sessionID string, inputs []*observationv1.Observation) ([]*observationv1.Observation, error) {
	// Check for collect command: ALERT with sub_type="collect_command".
	for _, o := range inputs {
		if o.GetType() == observationv1.EvidenceType_EVIDENCE_TYPE_ALERT && o.GetSubType() == "collect_command" {
			// Received collect command: emit synthetic log pattern for demo.
			detail, _ := json.Marshal(logPattern{
				Pattern:       "slow_query_detected",
				Count:        10,
				FirstSeen:    "2026-08-17T10:00:00Z",
				LastSeen:     "2026-08-17T10:30:00Z",
				Confidence:   0.75,
				Severity:     observationv1.Severity_SEVERITY_ERROR,
				TargetService: o.GetTargetService(),
				Correlations:  map[string]string{},
				Labels:        o.GetLabels(),
			})
			o, err := observation.New(&observationv1.Observation{
				SessionId:     sessionID,
				Source:        a.Name(),
				Type:          observationv1.EvidenceType_EVIDENCE_TYPE_LOG,
				SubType:       "log_pattern",
				Confidence:    0.75,
				Severity:      observationv1.Severity_SEVERITY_ERROR,
				TargetService: o.GetTargetService(),
				Correlations:  map[string]string{},
				DetailJson:    string(detail),
				Labels:        o.GetLabels(),
			})
			if err != nil {
				a.log.Error("log agent: emit synthetic observation", "error", err)
				continue
			}
			return []*observationv1.Observation{o}, nil
		}
	}

	var rawLogs []*observationv1.Observation
	for _, o := range inputs {
		if o.GetType() == observationv1.EvidenceType_EVIDENCE_TYPE_LOG {
			rawLogs = append(rawLogs, o)
		}
	}
	if len(rawLogs) == 0 {
		return nil, nil
	}

	var outputs []*observationv1.Observation
	patterns := detectLogPatterns(rawLogs)
	for _, pat := range patterns {
		detail, err := json.Marshal(pat)
		if err != nil {
			a.log.Error("log agent: marshal pattern detail", "error", err, "session_id", sessionID)
			continue
		}
		o, err := observation.New(&observationv1.Observation{
			SessionId:     sessionID,
			Source:        a.Name(),
			Type:          observationv1.EvidenceType_EVIDENCE_TYPE_LOG,
			SubType:       "log_pattern",
			Confidence:    pat.Confidence,
			Severity:      pat.Severity,
			TargetService: pat.TargetService,
			Correlations:  pat.Correlations,
			DetailJson:    string(detail),
			Labels:        pat.Labels,
		})
		if err != nil {
			a.log.Error("log agent: emit pattern observation", "error", err)
			continue
		}
		outputs = append(outputs, o)
	}
	return outputs, nil
}

// logPattern represents a detected log pattern.
type logPattern struct {
	Pattern       string            `json:"pattern"`
	Count         int               `json:"count"`
	FirstSeen     string            `json:"first_seen"`
	LastSeen      string            `json:"last_seen"`
	Confidence    float64           `json:"confidence"`
	Severity      observationv1.Severity `json:"severity"`
	TargetService string            `json:"target_service"`
	Correlations  map[string]string `json:"correlations"`
	Labels        map[string]string `json:"labels"`
}

// detectLogPatterns analyzes raw log observations and extracts patterns.
// This is a simplified pattern detector; production would integrate with
// ClickHouse log storage and more sophisticated anomaly detection.
func detectLogPatterns(logs []*observationv1.Observation) []logPattern {
	// Group logs by target service and severity.
	groups := make(map[string][]*observationv1.Observation)
	for _, log := range logs {
		key := log.GetTargetService() + ":" + log.GetSeverity().String()
		groups[key] = append(groups[key], log)
	}

	var patterns []logPattern
	for key, group := range groups {
		if len(group) < 1 {
			continue
		}
		// Simple heuristic: count error/fatal logs as a pattern.
		var errCount int
		var latestTS string
		hasTraceID := false
		for _, l := range group {
			if l.GetSeverity() == observationv1.Severity_SEVERITY_ERROR ||
				l.GetSeverity() == observationv1.Severity_SEVERITY_FATAL {
				errCount++
			}
			if l.GetTimestamp() != nil {
				latestTS = l.GetTimestamp().AsTime().Format("2006-01-02T15:04:05Z")
			}
			if _, ok := l.GetCorrelations()["trace_id"]; ok {
				hasTraceID = true
			}
		}
		if errCount == 0 {
			continue
		}
		sev := observationv1.Severity_SEVERITY_ERROR
		if errCount > 5 {
			sev = observationv1.Severity_SEVERITY_FATAL
		}
		correlations := make(map[string]string)
		if hasTraceID && len(group) > 0 {
			for _, l := range group {
				if tid, ok := l.GetCorrelations()["trace_id"]; ok {
					correlations["trace_id"] = tid
					break
				}
			}
		}
		patterns = append(patterns, logPattern{
			Pattern:       fmt.Sprintf("error_count=%d", errCount),
			Count:         errCount,
			FirstSeen:     latestTS,
			LastSeen:      latestTS,
			Confidence:    0.7,
			Severity:      sev,
			TargetService: group[0].GetTargetService(),
			Correlations:  correlations,
			Labels:        group[0].GetLabels(),
		})
		_ = key // avoid unused warning
	}
	return patterns
}
