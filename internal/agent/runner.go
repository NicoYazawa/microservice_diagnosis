// Package agent provides the interface and implementations for the 5 diagnostic agents.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// Runner drives an agent's consume → handle → produce loop.
type Runner struct {
	agent    Agent
	consumer *bus.ConsumerChannel
	producer bus.Producer
	log      *slog.Logger
	ready    chan struct{}
	closeMu  sync.Mutex
}

// NewRunner creates a Runner for the given agent.
func NewRunner(agent Agent, consumer *bus.ConsumerChannel, producer bus.Producer, log *slog.Logger) *Runner {
	return &Runner{
		agent:    agent,
		consumer: consumer,
		producer: producer,
		log:      log,
		ready:    make(chan struct{}),
	}
}

// Run starts the consume → handle → produce loop.
// It blocks until the context is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	r.log.Info("agent runner starting",
		"agent", r.agent.Name(),
		"input_topic", r.agent.InputTopic(),
		"output_topic", r.agent.OutputTopic())

	// Subscribe to input topic.
	if err := r.consumer.Subscribe(ctx, r.agent.InputTopic(), r.agent.Name()); err != nil {
		return fmt.Errorf("agent %s: subscribe to %s: %w", r.agent.Name(), r.agent.InputTopic(), err)
	}

	close(r.ready)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("agent runner shutting down", "agent", r.agent.Name())
			return ctx.Err()
		case msg, ok := <-r.consumer.Messages():
			if !ok {
				r.log.Warn("agent runner: message channel closed", "agent", r.agent.Name())
				return nil
			}
			r.processMessage(ctx, msg)
		}
	}
}

// Ready returns a channel that is closed once the runner has subscribed.
func (r *Runner) Ready() <-chan struct{} {
	return r.ready
}

func (r *Runner) processMessage(ctx context.Context, msg bus.Message) {
	var obs observationv1.Observation
	if err := json.Unmarshal(msg.Value, &obs); err != nil {
		r.log.Error("agent: unmarshal observation", "error", err, "topic", msg.Topic, "offset", msg.Offset)
		r.consumer.MarkAsProcessed(msg)
		return
	}

	sessionID := obs.GetSessionId()
	if sessionID == "" {
		r.log.Warn("agent: observation missing session_id, skipping", "topic", msg.Topic)
		r.consumer.MarkAsProcessed(msg)
		return
	}

	batch := []*observationv1.Observation{&obs}

	outputs, err := r.agent.Handle(ctx, sessionID, batch)
	if err != nil {
		r.log.Error("agent: handle failed",
			"agent", r.agent.Name(),
			"session_id", sessionID,
			"error", err)
		r.consumer.MarkAsProcessed(msg)
		return
	}

	for _, o := range outputs {
		o.SessionId = sessionID
		if err := r.producer.Publish(ctx, bus.Message{
			Topic: r.agent.OutputTopic(),
			Key:   []byte(sessionID),
			Value: mustMarshal(o),
		}); err != nil {
			r.log.Error("agent: publish observation",
				"agent", r.agent.Name(),
				"session_id", sessionID,
				"error", err)
		}
	}

	r.consumer.MarkAsProcessed(msg)
}

func mustMarshal(o *observationv1.Observation) []byte {
	b, _ := observation.ToJSON(o)
	return b
}
