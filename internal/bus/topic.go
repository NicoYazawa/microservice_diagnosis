package bus

import "strings"

// Kafka topic naming conventions for mfdh. All topics carry the "mfdh." prefix
// so that mfdh and unrelated workloads can safely share a Kafka cluster.
const (
	// TopicPrefix namespaces every mfdh topic.
	TopicPrefix = "mfdh"

	// ── Pipeline topology (M8 fix: one topic per agent, linear flow) ─────────────
	// Commands: orchestrator → individual collector agents
	TopicCommandsLog    = "mfdh.commands.log"
	TopicCommandsMetric = "mfdh.commands.metric"
	TopicCommandsTrace  = "mfdh.commands.trace"
	TopicCommandsRCA    = "mfdh.commands.rca"
	// Observations: each collector emits to its own output topic
	TopicObservationsLog    = "mfdh.observations.log"
	TopicObservationsMetric = "mfdh.observations.metric"
	TopicObservationsTrace  = "mfdh.observations.trace"
	// Analysis chain: orchestrator aggregates → RCA → Fix
	TopicObservationsRCA = "mfdh.observations.rca"
	TopicObservationsFix = "mfdh.observations.fix"
	// ── Legacy unified topic (kept for existing tests, not used in pipeline) ─────
	TopicObservations = "mfdh.observations"

	// TopicCommands carries orchestrator -> agent diagnostic task commands.
	TopicCommands = "mfdh.commands"

	// TopicObservationsRaw carries raw evidence emitted by agents before aggregation.
	TopicObservationsRaw = "mfdh.observations.raw"

	// TopicEvents carries diagnostic session lifecycle events.
	TopicEvents = "mfdh.events"

	// TopicDLQ receives messages that repeatedly failed processing.
	TopicDLQ = "mfdh.dlq"
)

// CommandMessage is the JSON payload the orchestrator publishes on per-agent
// command topics. A single type lives here (bus package) to avoid three
// duplicate copies across orchestrator/agent/runner.
type CommandMessage struct {
	SessionID     string `json:"session_id"`
	TargetService string `json:"target_service"`
	Command       string `json:"command"`
	AgentKind     string `json:"agent_kind"`
}

// Topic joins a base name with the standard prefix, e.g. Topic("custom") ->
// "mfdh.custom". Already-prefixed names pass through unchanged.
func Topic(base string) string {
	if strings.HasPrefix(base, TopicPrefix+".") {
		return base
	}
	return TopicPrefix + "." + base
}

// GroupID returns the standard consumer group id for a service consuming a
// topic, e.g. GroupID("agent-log", "mfdh.observations") -> "mfdh-agent-log-observations".
// Each agent owns a distinct group, which is what makes agents independently
// deployable and horizontally scalable (PLAN 1 / 2.1).
func GroupID(service, topic string) string {
	base := strings.TrimPrefix(topic, TopicPrefix+".")
	return TopicPrefix + "-" + service + "-" + base
}