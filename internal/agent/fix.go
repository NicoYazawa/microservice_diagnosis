package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/llm"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
)

// FixAgent generates fix action sequences based on RCA results and the knowledge base.
type FixAgent struct {
	pool       *pgxpool.Pool
	llmClient  *llm.Client
	kbDAO      *store.KnowledgeBaseDAO
	fixDAO     *store.FixActionDAO
	log        *slog.Logger
}

// NewFixAgent creates a new FixAgent.
func NewFixAgent(pool *pgxpool.Pool, llmClient *llm.Client, log *slog.Logger) *FixAgent {
	return &FixAgent{
		pool:      pool,
		llmClient: llmClient,
		kbDAO:     store.NewKnowledgeBaseDAO(pool),
		fixDAO:    store.NewFixActionDAO(pool),
		log:       log,
	}
}

// Name implements Agent.
func (a *FixAgent) Name() string { return "agent-fix" }

// InputTopic implements Agent (Fix consumes RCA results).
func (a *FixAgent) InputTopic() string { return "observations-rca" }

// OutputTopic implements Agent.
func (a *FixAgent) OutputTopic() string { return "observations-fix" }

// Handle implements Agent. It processes RCA_RESULT observations, queries the
// knowledge base for matching fix candidates, generates fix steps with risk
// assessment, persists fix_actions to PostgreSQL, and emits FIX_ACTION
// type observations.
func (a *FixAgent) Handle(ctx context.Context, sessionID string, inputs []*observationv1.Observation) ([]*observationv1.Observation, error) {
	var rcaResult *observationv1.Observation
	for _, o := range inputs {
		if o.GetType() == observationv1.EvidenceType_EVIDENCE_TYPE_RCA_RESULT {
			rcaResult = o
			break
		}
	}
	if rcaResult == nil {
		return nil, nil
	}

	sessionUUID, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, fmt.Errorf("fix agent: invalid session id: %w", err)
	}

	// Parse RCA output to get root cause pattern.
	var rcaOutput RCAOutput
	if err := json.Unmarshal([]byte(rcaResult.GetDetailJson()), &rcaOutput); err != nil {
		return nil, fmt.Errorf("fix agent: parse rca result: %w", err)
	}

	// Search knowledge base.
	candidates, err := a.searchKnowledgeBase(ctx, rcaOutput.RootCausePattern)
	if err != nil {
		a.log.Warn("fix agent: knowledge base search failed, using LLM fallback", "error", err)
		candidates = nil
	}

	// Generate fix steps (from knowledge base or LLM).
	var steps []FixStep
	if len(candidates) > 0 {
		steps = candidates[0].Steps
	} else {
		steps, err = a.generateFixStepsWithLLM(ctx, rcaResult)
		if err != nil {
			a.log.Error("fix agent: LLM fix generation failed", "error", err)
			steps = a.defaultFixSteps(rcaOutput.RootCausePattern)
		}
	}

	// Persist fix actions and emit observations.
	var outputs []*observationv1.Observation
	for i, step := range steps {
		// Risk assessor determines risk level and rollback plan per step.
		risk, rollback := a.assessRisk(step)
		if rollback == "" {
			rollback = step.RollbackPlan
		}

		requiresApproval := risk == store.RiskHigh

		fa := &store.FixAction{
			SessionID:        sessionUUID,
			Seq:              i + 1,
			ActionType:      step.ActionType,
			Target:           step.Target,
			Risk:             risk,
			RollbackPlan:     rollback,
			RequiresApproval: requiresApproval,
			ApprovalStatus:   store.ApprovalStatusNone,
			ExecutionStatus:  store.ExecStatusNotStarted,
		}
		if err := a.fixDAO.Create(ctx, fa); err != nil {
			a.log.Error("fix agent: persist fix action", "error", err)
			continue
		}

		detail, err := json.Marshal(fixActionDetail{
			Step:            step,
			Risk:            risk,
			RollbackPlan:    rollback,
			RequiresApproval: requiresApproval,
		})
		if err != nil {
			a.log.Error("fix agent: marshal fix detail", "error", err, "fix_action_id", fa.ID.String())
			continue
		}
		o, err := observation.New(&observationv1.Observation{
			SessionId:     sessionID,
			Source:        a.Name(),
			Type:          observationv1.EvidenceType_EVIDENCE_TYPE_FIX_ACTION,
			SubType:       step.ActionType,
			Confidence:    rcaResult.GetConfidence(),
			Severity:      rcaResult.GetSeverity(),
			TargetService: rcaResult.GetTargetService(),
			Correlations:  map[string]string{"fix_action_id": fa.ID.String()},
			DetailJson:    string(detail),
			Labels:        rcaResult.GetLabels(),
		})
		if err != nil {
			a.log.Error("fix agent: emit fix observation", "error", err)
			continue
		}
		outputs = append(outputs, o)
	}
	return outputs, nil
}

// searchKnowledgeBase queries the fix_knowledge_base table for matching entries.
func (a *FixAgent) searchKnowledgeBase(ctx context.Context, pattern string) ([]FixCandidate, error) {
	entries, err := a.kbDAO.SearchByRootCause(ctx, pattern)
	if err != nil {
		return nil, err
	}
	var candidates []FixCandidate
	for _, e := range entries {
		var steps []FixStep
		if err := json.Unmarshal(e.FixSteps, &steps); err != nil {
			a.log.Warn("fix agent: unmarshal fix steps", "error", err)
			continue
		}
		candidates = append(candidates, FixCandidate{
			ID:               e.ID.String(),
			RootCausePattern: e.RootCausePattern,
			Steps:            steps,
			Risk:             e.Risk,
			RollbackPlan:     e.RollbackPlan,
			TimesUsed:        e.TimesUsed,
			SuccessRate:      e.SuccessRate,
		})
	}
	return candidates, nil
}

// generateFixStepsWithLLM calls the LLM to generate fix steps based on RCA result.
func (a *FixAgent) generateFixStepsWithLLM(ctx context.Context, rcaResult *observationv1.Observation) ([]FixStep, error) {
	if a.llmClient == nil {
		return nil, fmt.Errorf("no LLM client configured")
	}

	var rcaOutput RCAOutput
	if err := json.Unmarshal([]byte(rcaResult.GetDetailJson()), &rcaOutput); err != nil {
		return nil, err
	}

	systemPrompt := `You are a microservice reliability engineer. Given the root cause analysis, generate a fix action plan.
Return a JSON array of fix steps. Each step is an object with:
- action_type: one of restart_pod, scale_up, scale_down, switch_master, config_change, rollback_deploy, circuit_break, rate_limit, gc_tune, connection_pool_tune
- target: the specific target (pod name pattern, service name, config key, etc.)
- description: human-readable description of the action
- rollback_plan: how to rollback this action if it fails
Only return the JSON array. No markdown or additional text.`

	userPrompt := fmt.Sprintf("Root cause pattern: %s\nRoot cause: %s\nConfidence: %.2f\nTarget service: %s\nEvidence: %s",
		rcaOutput.RootCausePattern, rcaOutput.RootCause, rcaOutput.Confidence,
		rcaResult.GetTargetService(), rcaOutput.EvidenceSummary)

	answer, err := a.llmClient.Chat(ctx, systemPrompt, userPrompt)
	if err != nil {
		return nil, err
	}

	var steps []FixStep
	if err := json.Unmarshal([]byte(answer), &steps); err != nil {
		return nil, fmt.Errorf("parse LLM steps: %w", err)
	}
	return steps, nil
}

// defaultFixSteps returns a minimal fallback fix step when both KB and LLM fail.
func (a *FixAgent) defaultFixSteps(pattern string) []FixStep {
	switch pattern {
	case "database_connection_pool_exhaustion":
		return []FixStep{
			{ActionType: "scale_up", Target: "replicas=+2", Description: "Temporarily scale up to handle connection pressure", RollbackPlan: "Scale back to original replicas"},
			{ActionType: "config_change", Target: "max_connections", Description: "Increase database max connections config", RollbackPlan: "Restore original max_connections value"},
		}
	case "timeout_cascade":
		return []FixStep{
			{ActionType: "circuit_break", Target: "downstream-service", Description: "Enable circuit breaker for downstream service", RollbackPlan: "Disable circuit breaker"},
			{ActionType: "scale_up", Target: "replicas=+1", Description: "Add capacity to downstream service", RollbackPlan: "Scale back to original replicas"},
		}
	case "n_plus_one_query":
		return []FixStep{
			{ActionType: "config_change", Target: "db.pool.size", Description: "Increase DB pool size to absorb query burst", RollbackPlan: "Restore original pool size"},
			{ActionType: "restart_pod", Target: "api-gateway", Description: "Restart API gateway to clear stale connections", RollbackPlan: "Manual restart if needed"},
		}
	case "resource_leak":
		return []FixStep{
			{ActionType: "restart_pod", Target: "*", Description: "Restart affected pods to free leaked resources", RollbackPlan: "No automatic rollback, monitor after restart"},
			{ActionType: "config_change", Target: "gc_percent", Description: "Tune Go GC percent to run more frequently", RollbackPlan: "Restore original GC percent"},
		}
	default:
		return []FixStep{
			{ActionType: "restart_pod", Target: "*", Description: "Restart target service as fallback mitigation", RollbackPlan: "Monitor service health after restart"},
		}
	}
}

// assessRisk evaluates a fix step's risk level and rollback plan using rule tables.
// LLM must not downgrade the risk level determined by these rules.
func (a *FixAgent) assessRisk(step FixStep) (risk string, rollback string) {
	// Rule table from PLAN 6.
	switch step.ActionType {
	case "restart_pod":
		return store.RiskLow, "Pod will be automatically restarted by Kubernetes if health check fails"
	case "scale_up":
		return store.RiskLow, "Scale back to original replica count"
	case "scale_down":
		return store.RiskMedium, "Scale back to original replica count"
	case "switch_master":
		return store.RiskHigh, "Switch back to the original master node or database"
	case "data_migration":
		return store.RiskHigh, "Halt migration and restore from backup if possible"
	case "config_change":
		return store.RiskMedium, "Restore original configuration values"
	case "rollback_deploy":
		return store.RiskHigh, "Redeploy the previous known-good revision"
	case "circuit_break", "rate_limit":
		return store.RiskMedium, "Disable the circuit breaker or rate limit rule"
	case "gc_tune", "connection_pool_tune":
		return store.RiskMedium, "Restore original tuning parameters"
	default:
		// Unknown action type: fail closed so the step requires approval.
		return store.RiskHigh, "Unknown action type; manual intervention required for rollback"
	}
}

// fixActionDetail is serialized into the Observation detail_json.
type fixActionDetail struct {
	Step            FixStep `json:"step"`
	Risk            string  `json:"risk"`
	RollbackPlan    string  `json:"rollback_plan"`
	RequiresApproval bool   `json:"requires_approval"`
}
