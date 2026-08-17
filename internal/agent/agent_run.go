// Package agent provides the interface and implementations for the 5 diagnostic agents.
package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/config"
)

// Run starts the agent's consume → handle → produce loop.
// It is called from each agent's main.go entrypoint after bootstrap loads the config.
//
// Exit semantics:
//   - ctx cancelled (SIGINT/SIGTERM): returns nil. cmd main should exit 0.
//   - unexpected error: returns the error. cmd main should exit non-zero.
func Run(agentName string, cfg *config.Config, log *slog.Logger) error {
	busCfg := bus.Config{
		Brokers: cfg.Bus.Brokers,
	}

	// Instantiate the agent first so we know its input topic.
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
		agent = NewFixAgent(nil, nil, log) // pool and LLM nil for M8; graceful nil-check in Handle prevents panic
	default:
		log.Error("unknown agent kind", "agent", agentName)
		os.Exit(1)
	}

	// Build Kafka consumer for this agent's input topic.
	// Each agent subscribes to its own input topic so it only receives relevant messages.
	consumer, err := bus.NewConsumerChannel(busCfg, bus.ConsumerConfig{
		Topic:   agent.InputTopic(),
		GroupID: bus.GroupID(agentName, agent.InputTopic()),
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Build Kafka producer.
	producer, err := bus.NewProducer(busCfg)
	if err != nil {
		return err
	}
	defer producer.Close()

	runner := NewRunner(agent, consumer, producer, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("agent starting",
		"agent", agent.Name(),
		"input_topic", agent.InputTopic(),
		"output_topic", agent.OutputTopic(),
		"brokers", cfg.Bus.Brokers)

	err = runner.Run(ctx)
	if err == nil {
		return nil
	}
	// Distinguish clean shutdown (signal-cancelled ctx) from real failures.
	// runner.Run returns nil on context cancel, but cmd main may still see a
	// non-nil error if some other path returns one — treat any context
	// cancellation as a clean exit so the process returns 0.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		log.Info("agent stopped", "agent", agentName)
		return nil
	}
	if ctx.Err() != nil {
		log.Info("agent stopped", "agent", agentName)
		return nil
	}
	log.Error("agent stopped with error", "agent", agentName, "error", err)
	return err
}