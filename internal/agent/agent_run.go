// Package agent provides the interface and implementations for the 5 diagnostic agents.
package agent

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/config"
)

// Run starts the agent's consume → handle → produce loop.
// It is called from each agent's main.go entrypoint after bootstrap loads the config.
func Run(agentName string, cfg *config.Config, log *slog.Logger) error {
	busCfg := bus.Config{
		Brokers: cfg.Bus.Brokers,
	}

	// Build Kafka producer.
	producer, err := bus.NewProducer(busCfg)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Build Kafka consumer channel for this agent.
	consumer, err := bus.NewConsumerChannel(busCfg, bus.ConsumerConfig{
		Topic:   bus.TopicCommands,
		GroupID: bus.GroupID(agentName, bus.TopicCommands),
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Instantiate the correct agent.
	// Note: RCA and Fix agents require additional dependencies (LLM, pool) that are
	// currently not wired in agent_run.go. They are instantiated here with nil/nop
	// dependencies for compilation; full wiring is M8/M9 work.
	var agent Agent
	switch agentName {
	case "agent-log":
		agent = NewLogAgent(log)
	case "agent-metric":
		agent = NewMetricAgent(log)
	case "agent-trace":
		agent = NewTraceAgent(log)
	case "agent-rca":
		agent = NewRCAgent(nil, log) // LLM injected later
	case "agent-fix":
		agent = NewFixAgent(nil, nil, log) // pool and LLM injected later
	default:
		log.Error("unknown agent kind", "agent", agentName)
		os.Exit(1)
	}

	runner := NewRunner(agent, consumer, producer, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("agent starting",
		"agent", agent.Name(),
		"input_topic", agent.InputTopic(),
		"output_topic", agent.OutputTopic(),
		"brokers", cfg.Bus.Brokers)

	return runner.Run(ctx)
}
