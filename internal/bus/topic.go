package bus

import "strings"

// Kafka topic naming conventions for mfdh. All topics carry the "mfdh." prefix
// so that mfdh and unrelated workloads can safely share a Kafka cluster.
const (
	// TopicPrefix namespaces every mfdh topic.
	TopicPrefix = "mfdh"

	// TopicObservations carries Observation evidence produced by agents
	// (LOG / METRIC / TRACE / ALERT / RCA_RESULT / FIX_ACTION).
	TopicObservations = "mfdh.observations"

	// TopicCommands carries orchestrator -> agent diagnostic task commands.
	TopicCommands = "mfdh.commands"

	// TopicEvents carries diagnostic session lifecycle events.
	TopicEvents = "mfdh.events"

	// TopicDLQ receives messages that repeatedly failed processing.
	TopicDLQ = "mfdh.dlq"
)

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
