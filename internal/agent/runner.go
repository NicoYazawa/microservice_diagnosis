// Package agent provides the interface and implementations for the 5 diagnostic agents.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	observationv1 "github.com/NicoYazawa/microservice_diagnosis/api/gen/observation/v1"
	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/observation"
)

// messageSource is the consume-side contract the Runner needs. It is satisfied
// by *bus.ConsumerChannel; tests substitute in-memory doubles.
type messageSource interface {
	Subscribe(ctx context.Context, topic, groupID string) error
	Messages() <-chan bus.Message
	MarkAsProcessed(msg bus.Message)
}

// Runner drives an agent's consume → handle → produce loop.
type Runner struct {
	agent    Agent
	consumer messageSource
	producer bus.Producer
	log      *slog.Logger
	ready    chan struct{}
}

// NewRunner creates a Runner for the given agent.
func NewRunner(agent Agent, consumer messageSource, producer bus.Producer, log *slog.Logger) *Runner {
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
//
// Returns:
//   - nil when the context is cancelled (clean shutdown).
//   - ctx.Err() when shutting down with an active context error.
//   - any other error for unexpected failures.
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
		var msgCh <-chan bus.Message
		for i := 0; i < 10; i++ {
			if ch := r.consumer.Messages(); ch != nil {
				msgCh = ch
				break
			}
			select {
			case <-ctx.Done():
				r.log.Info("agent runner shutting down", "agent", r.agent.Name())
				return nil
			case <-time.After(100 * time.Millisecond):
			}
		}
		if msgCh == nil {
			r.log.Warn("agent runner: message channel nil, subscription not ready", "agent", r.agent.Name())
			return nil
		}

		select {
		case <-ctx.Done():
			r.log.Info("agent runner shutting down", "agent", r.agent.Name())
			return nil
		case msg, ok := <-msgCh:
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
	// Try to parse as a command message first (dispatch from orchestrator).
	var cmd bus.CommandMessage
	if err := json.Unmarshal(msg.Value, &cmd); err == nil && cmd.SessionID != "" && cmd.Command != "" {
		// Validate the command targets this agent. Without this check any
		// payload with these two fields could trigger a synthetic Handle.
		if cmd.AgentKind != "" && cmd.AgentKind != r.agent.Name() {
			r.log.Warn("agent: skipping command for different agent_kind",
				"agent", r.agent.Name(),
				"command_agent_kind", cmd.AgentKind,
				"session_id", cmd.SessionID,
				"command", cmd.Command)
			r.consumer.MarkAsProcessed(msg)
			return
		}
		r.log.Info("agent: received command",
			"agent", r.agent.Name(),
			"session_id", cmd.SessionID,
			"command", cmd.Command,
			"target", cmd.TargetService)

		// Build a synthetic Observation as the "trigger" for the agent's Handle method.
		triggerDetail, _ := json.Marshal(cmd)
		triggerObs := &observationv1.Observation{
			SessionId:     cmd.SessionID,
			Source:        "orchestrator",
			Type:          observationv1.EvidenceType_EVIDENCE_TYPE_ALERT,
			SubType:       "collect_command",
			Confidence:    1.0,
			Severity:      observationv1.Severity_SEVERITY_INFO,
			TargetService: cmd.TargetService,
			DetailJson:    string(triggerDetail),
		}

		outputs, err := r.agent.Handle(ctx, cmd.SessionID, []*observationv1.Observation{triggerObs})
		if err != nil {
			r.log.Error("agent: handle failed",
				"agent", r.agent.Name(),
				"session_id", cmd.SessionID,
				"error", err)
			r.consumer.MarkAsProcessed(msg)
			return
		}
		if len(outputs) == 0 {
			r.log.Warn("agent: handle returned zero outputs",
				"agent", r.agent.Name(),
				"session_id", cmd.SessionID)
		} else {
			r.log.Info("agent: handle returned outputs",
				"agent", r.agent.Name(),
				"session_id", cmd.SessionID,
				"count", len(outputs))
		}
		for _, o := range outputs {
			o.SessionId = cmd.SessionID
			r.log.Info("agent: publishing observation",
				"agent", r.agent.Name(),
				"session_id", cmd.SessionID,
				"topic", r.agent.OutputTopic())
			if err := r.publishWithRetry(ctx, bus.Message{
				Topic: r.agent.OutputTopic(),
				Key:   []byte(cmd.SessionID),
				Value: mustMarshal(o),
			}, cmd.SessionID); err != nil {
				r.log.Error("agent: publish observation failed after retries",
					"agent", r.agent.Name(),
					"session_id", cmd.SessionID,
					"error", err)
			}
		}
		r.consumer.MarkAsProcessed(msg)
		return
	}

	// Fall back: try to parse as a regular Observation (from upstream agents).
	var obs observationv1.Observation
	if err := protojson.Unmarshal(msg.Value, &obs); err != nil {
		r.log.Warn("agent: unmarshal skipped", "error", err, "topic", msg.Topic, "offset", msg.Offset)
		r.consumer.MarkAsProcessed(msg)
		return
	}

	// Ignore messages this agent produced itself. With the per-agent topic
	// topology (each agent's input topic is distinct from its output topic)
	// this is mostly defensive: a misconfiguration could otherwise feed the
	// agent's own output back into Handle.
	if obs.GetSource() == r.agent.Name() {
		r.log.Warn("agent: skipping own message", "agent", r.agent.Name(), "offset", msg.Offset)
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
		if err := r.publishWithRetry(ctx, bus.Message{
			Topic: r.agent.OutputTopic(),
			Key:   []byte(sessionID),
			Value: mustMarshal(o),
		}, sessionID); err != nil {
			r.log.Error("agent: publish observation failed after retries",
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

// publishWithRetry attempts to publish a message up to 3 times with 500ms backoff.
func (r *Runner) publishWithRetry(ctx context.Context, msg bus.Message, sessionID string) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
		if err := r.producer.Publish(ctx, msg); err != nil {
			lastErr = err
			r.log.Warn("agent: publish attempt failed, retrying",
				"agent", r.agent.Name(),
				"session_id", sessionID,
				"attempt", attempt+1,
				"error", err)
			continue
		}
		return nil
	}
	return lastErr
}