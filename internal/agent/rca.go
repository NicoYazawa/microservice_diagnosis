package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/llm"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// RCAgent performs root cause analysis by calling an LLM on aggregated evidence.
type RCAgent struct {
	llmClient *llm.Client
	log       *slog.Logger
}

// NewRCAgent creates a new RCAgent.
func NewRCAgent(llmClient *llm.Client, log *slog.Logger) *RCAgent {
	return &RCAgent{llmClient: llmClient, log: log}
}

// Name implements Agent.
func (a *RCAgent) Name() string { return "agent-rca" }

// InputTopic implements Agent (RCA consumes commands from orchestrator).
func (a *RCAgent) InputTopic() string { return bus.TopicCommandsRCA }

// OutputTopic implements Agent.
func (a *RCAgent) OutputTopic() string { return bus.TopicObservationsRCA }

// Handle implements Agent. It aggregates evidence from LOG/ METRIC / TRACE agents,
// calls the LLM to perform root cause analysis, and emits an RCA_RESULT type
// observation containing the root cause summary and suggested pattern.
func (a *RCAgent) Handle(ctx context.Context, sessionID string, inputs []*observationv1.Observation) ([]*observationv1.Observation, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	// Detect "analyze" command: the trigger observation is an ALERT with subType "collect_command".
	// In this case we perform RCA directly using the command payload as context.
	if len(inputs) == 1 && inputs[0].GetType() == observationv1.EvidenceType_EVIDENCE_TYPE_ALERT {
		var cmd bus.CommandMessage
		if err := json.Unmarshal([]byte(inputs[0].GetDetailJson()), &cmd); err == nil && cmd.Command == "analyze" {
			evidenceSummary := fmt.Sprintf("Analyze requested for session %s, target %s", sessionID, cmd.TargetService)
			// No aggregated evidence in the analyze path — pass nil to runRCA
			// so the heuristic fallback uses the no-evidence branch.
			rcaResult := a.runRCA(ctx, cmd.TargetService, evidenceSummary, nil)
			detail, _ := json.Marshal(rcaResult)
			severity := observationv1.Severity_SEVERITY_INFO
			o, err := observation.New(&observationv1.Observation{
				SessionId:     sessionID,
				Source:        a.Name(),
				Type:          observationv1.EvidenceType_EVIDENCE_TYPE_RCA_RESULT,
				SubType:       "rca_result",
				Confidence:    rcaResult.Confidence,
				Severity:      severity,
				TargetService: cmd.TargetService,
				DetailJson:    string(detail),
				Labels:        inputs[0].GetLabels(),
			})
			if err != nil {
				return nil, fmt.Errorf("rca agent: create observation: %w", err)
			}
			return []*observationv1.Observation{o}, nil
		}
	}

	// Build evidence summary for LLM.
	evidenceSummary := buildEvidenceSummary(inputs)
	targetService := inputs[0].GetTargetService()
	rcaResult := a.runRCA(ctx, targetService, evidenceSummary, inputs)

	// Determine severity from inputs.
	severity := observationv1.Severity_SEVERITY_INFO
	for _, o := range inputs {
		if o.GetSeverity() > severity {
			severity = o.GetSeverity()
		}
	}

	detail, _ := json.Marshal(rcaResult)
	o, err := observation.New(&observationv1.Observation{
		SessionId:     sessionID,
		Source:        a.Name(),
		Type:          observationv1.EvidenceType_EVIDENCE_TYPE_RCA_RESULT,
		SubType:       "rca_result",
		Confidence:    rcaResult.Confidence,
		Severity:      severity,
		TargetService: targetService,
		DetailJson:    string(detail),
		Labels:        inputs[0].GetLabels(),
	})
	if err != nil {
		return nil, fmt.Errorf("rca agent: create observation: %w", err)
	}
	return []*observationv1.Observation{o}, nil
}

// runRCA calls the LLM and falls back to a heuristic when no LLM is configured
// or the LLM call fails. inputs is the original evidence; when non-empty the
// evidence-based heuristic is used (so useful conclusions are produced even
// without an LLM).
func (a *RCAgent) runRCA(ctx context.Context, targetService, evidenceSummary string, inputs []*observationv1.Observation) RCAOutput {
	systemPrompt := `You are a microservice diagnostics expert. Based on the evidence provided, perform root cause analysis.
Return a JSON object with these fields:
- root_cause: a concise description of the root cause (1-2 sentences)
- root_cause_pattern: a searchable pattern tag (e.g. "database_connection_pool_exhaustion", "n_plus_one_query", "memory_leak", "timeout_cascade")
- confidence: a number between 0.0 and 1.0 indicating your confidence in this analysis
- evidence_summary: a 1-sentence summary of the key supporting evidence
Only return valid JSON. No markdown or additional text.`

	var rcaResult RCAOutput
	if a.llmClient != nil {
		answer, err := a.llmClient.Chat(ctx, systemPrompt, evidenceSummary)
		if err != nil {
			a.log.Error("rca agent: LLM call failed, falling back to heuristic", "error", err)
		} else if err := json.Unmarshal([]byte(answer), &rcaResult); err != nil {
			a.log.Warn("rca agent: failed to parse LLM response, falling back to heuristic", "error", err)
		} else if rcaResult.RootCause == "" || rcaResult.RootCausePattern == "" {
			a.log.Warn("rca agent: LLM response missing required fields, falling back to heuristic")
		} else {
			rcaResult.Confidence = math.Min(math.Max(rcaResult.Confidence, 0), 1)
			return rcaResult
		}
	}
	// LLM unavailable, call failed, response invalid, or response empty:
	// use the evidence-based heuristic when we have inputs.
	if len(inputs) > 0 {
		return heuristicRCA(inputs)
	}
	return heuristicRFACall(targetService)
}

// heuristicRFACall is the no-LLM fallback used in the analyze-command path
// where no upstream evidence is available.
func heuristicRFACall(targetService string) RCAOutput {
	return RCAOutput{
		RootCause:        fmt.Sprintf("Analysis triggered for %s (heuristic mode, no LLM)", targetService),
		RootCausePattern: "unknown",
		Confidence:       0.3,
		EvidenceSummary:  "No LLM configured; using heuristic fallback",
	}
}

// RCAOutput is the structured output from the RCA LLM analysis.
type RCAOutput struct {
	RootCause        string  `json:"root_cause"`
	RootCausePattern string  `json:"root_cause_pattern"`
	Confidence       float64 `json:"confidence"`
	EvidenceSummary  string  `json:"evidence_summary"`
}

// buildEvidenceSummary builds a text summary of input observations for LLM context.
func buildEvidenceSummary(inputs []*observationv1.Observation) string {
	var summary string
	for i, o := range inputs {
		if i >= 20 {
			summary += fmt.Sprintf("\n... and %d more observations", len(inputs)-i)
			break
		}
		summary += fmt.Sprintf("\n[%s] %s | severity=%s | service=%s | detail=%s",
			o.GetType().String(), o.GetSubType(), o.GetSeverity().String(),
			o.GetTargetService(), o.GetDetailJson())
	}
	return summary
}

// heuristicRCA performs rule-based RCA using aggregated evidence when LLM is
// unavailable or fails. Returns a meaningful pattern (timeout_cascade /
// database_connection_pool_exhaustion / n_plus_one_query / resource_leak)
// instead of the generic "unknown" fallback.
func heuristicRCA(inputs []*observationv1.Observation) RCAOutput {
	var errCount, warnCount int
	var traceIDs []string
	var slowSpans []string

	for _, o := range inputs {
		switch o.GetSeverity() {
		case observationv1.Severity_SEVERITY_ERROR, observationv1.Severity_SEVERITY_FATAL:
			errCount++
		case observationv1.Severity_SEVERITY_WARN:
			warnCount++
		}
		if o.GetType() == observationv1.EvidenceType_EVIDENCE_TYPE_TRACE {
			if tid, ok := o.GetCorrelations()["trace_id"]; ok {
				traceIDs = append(traceIDs, tid)
			}
			var payload map[string]interface{}
			if err := json.Unmarshal([]byte(o.GetDetailJson()), &payload); err == nil {
				if d, ok := payload["duration_ms"].(float64); ok && d > 5000 {
					slowSpans = append(slowSpans, fmt.Sprintf("span>5s:%.0fms", d))
				}
			}
		}
	}

	if errCount > 5 && len(slowSpans) > 0 {
		return RCAOutput{
			RootCause:        "Cascading timeout triggered by slow downstream service",
			RootCausePattern: "timeout_cascade",
			Confidence:       0.8,
			EvidenceSummary:  fmt.Sprintf("%d errors, %d slow spans including %s", errCount, len(slowSpans), slowSpans[0]),
		}
	}
	if errCount > 10 {
		return RCAOutput{
			RootCause:        "Database connection pool exhaustion causing repeated failures",
			RootCausePattern: "database_connection_pool_exhaustion",
			Confidence:       0.75,
			EvidenceSummary:  fmt.Sprintf("%d errors within the same service", errCount),
		}
	}
	if len(slowSpans) > 3 {
		return RCAOutput{
			RootCause:        "N+1 query pattern causing increased latency",
			RootCausePattern: "n_plus_one_query",
			Confidence:       0.7,
			EvidenceSummary:  fmt.Sprintf("%d slow spans detected", len(slowSpans)),
		}
	}
	if warnCount > 20 {
		return RCAOutput{
			RootCause:        "Memory or goroutine leak causing gradual degradation",
			RootCausePattern: "resource_leak",
			Confidence:       0.65,
			EvidenceSummary:  fmt.Sprintf("%d warnings indicating resource pressure", warnCount),
		}
	}
	return RCAOutput{
		RootCause:        "Unknown root cause, more evidence needed",
		RootCausePattern: "unknown",
		Confidence:       0.3,
		EvidenceSummary:  fmt.Sprintf("%d errors, %d warnings", errCount, warnCount),
	}
}