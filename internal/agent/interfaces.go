// Package agent provides the interface and implementations for the 5 diagnostic agents:
// Log, Metric, Trace, RCA, and Fix.
//
// Each agent:
//   - Consumes Observations from Kafka (subscribes to its input topic).
//   - Produces typed Observations back to Kafka (emits evidence).
//   - Is idempotent: re-processing the same input yields the same output.
//
// The agent loop is driven by the consumer; agents do not pull work proactively.
package agent

import (
	"context"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
)

// Agent is the common interface for all diagnostic agents.
type Agent interface {
	// Name returns the agent's logical name (used as the Observation source field).
	Name() string
	// InputTopic returns the Kafka topic this agent subscribes to.
	InputTopic() string
	// OutputTopic returns the Kafka topic this agent publishes evidence to.
	OutputTopic() string
	// Handle processes a batch of input observations and returns emitted observations.
	// It is called by the consumer loop; the agent loop framework handles offset commit.
	Handle(ctx context.Context, sessionID string, inputs []*observationv1.Observation) ([]*observationv1.Observation, error)
}

// RootCauseAnalysis holds the RCA result produced by the RCA agent.
type RootCauseAnalysis struct {
	SessionID     string
	TargetService  string
	RootCause     string
	EvidenceIDs   []string
	Confidence    float64
	Summary       string
}

// FixStep represents a single step in a fix action sequence.
type FixStep struct {
	Seq             int
	ActionType     string // restart_pod / scale_up / switch_master / config_change / ...
	Target          string // pod name / service name / config key / ...
	Risk            string // LOW / MEDIUM / HIGH
	RollbackPlan    string
	Description     string
}

// FixCandidate represents a candidate fix from the knowledge base.
type FixCandidate struct {
	ID               string
	RootCausePattern string
	Steps           []FixStep
	Risk            string
	RollbackPlan    string
	TimesUsed       int
	SuccessRate     float32
}
