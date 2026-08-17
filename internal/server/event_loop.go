// Package server provides HTTP server setup with Gin + gRPC-gateway.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/NicoYazawa/microservice_diagnosis/internal/bus"
	"github.com/NicoYazawa/microservice_diagnosis/internal/store"
	"github.com/NicoYazawa/microservice_diagnosis/internal/workflow"
)

// eventLoop runs in the orchestrator process and drives the session lifecycle.
// It consumes Observation messages from Kafka and advances the workflow state machine
// based on the evidence produced by agents.
type eventLoop struct {
	engine           *workflow.Engine
	sessionDAO       *store.SessionDAO
	fixDAO           *store.FixActionDAO
	sessionEventDAO   *store.SessionEventDAO
	onApprovalGate   func(ctx context.Context, sessionID uuid.UUID) error
	onExecuteFixes   func(ctx context.Context, sessionID uuid.UUID) error
	kafkaReader      *kafka.Reader
	log              *slog.Logger
}

// newEventLoop builds an eventLoop that reads from the observations topic.
// Callbacks are injected after OrchestratorHandler is constructed to avoid
// circular dependencies between eventLoop and OrchestratorHandler.
func newEventLoop(
	engine *workflow.Engine,
	sessionDAO *store.SessionDAO,
	fixDAO *store.FixActionDAO,
	sessionEventDAO *store.SessionEventDAO,
	brokers []string,
	log *slog.Logger,
) *eventLoop {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          bus.TopicObservations,
		GroupID:        bus.GroupID("orchestrator", bus.TopicObservations),
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // synchronous commit
		StartOffset:    kafka.FirstOffset,
	})
	return &eventLoop{
		engine:          engine,
		sessionDAO:      sessionDAO,
		fixDAO:          fixDAO,
		sessionEventDAO: sessionEventDAO,
		kafkaReader:    reader,
		log:             log,
	}
}

// SetCallbacks injects the OrchestratorHandler methods after it is constructed.
func (el *eventLoop) SetCallbacks(
	onApprovalGate func(ctx context.Context, sessionID uuid.UUID) error,
	onExecuteFixes func(ctx context.Context, sessionID uuid.UUID) error,
) {
	el.onApprovalGate = onApprovalGate
	el.onExecuteFixes = onExecuteFixes
}

// Run starts the event loop. It blocks until ctx is cancelled.
func (el *eventLoop) Run(ctx context.Context) error {
	el.log.Info("event loop started", "topic", bus.TopicObservations)
	for {
		m, err := el.kafkaReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("event loop: fetch: %w", err)
		}

		var obs observationPayload
		if err := json.Unmarshal(m.Value, &obs); err != nil {
			el.log.Warn("event loop: unmarshal skipped", "error", err)
			el.kafkaReader.CommitMessages(ctx, m)
			continue
		}

		if obs.SessionID == "" {
			el.kafkaReader.CommitMessages(ctx, m)
			continue
		}

		sessionID, err := uuid.Parse(obs.SessionID)
		if err != nil {
			el.log.Warn("event loop: invalid session_id", "session_id", obs.SessionID)
			el.kafkaReader.CommitMessages(ctx, m)
			continue
		}

		if err := el.handleObservation(ctx, sessionID, obs); err != nil {
			el.log.Error("event loop: handle observation failed",
				"session_id", obs.SessionID, "type", obs.Type, "error", err)
		}

		if err := el.kafkaReader.CommitMessages(ctx, m); err != nil {
			el.log.Error("event loop: commit failed", "error", err)
		}
	}
}

// observationPayload is a minimal subset of the Observation proto used for routing.
type observationPayload struct {
	ID         string  `json:"id"`
	SessionID  string  `json:"session_id"`
	Source     string  `json:"source"`
	Type       string  `json:"type"`          // LOG / METRIC / TRACE / RCA_RESULT / FIX_ACTION
	SubType    string  `json:"sub_type"`       // log_pattern / metric_anomaly / ...
	Confidence float64 `json:"confidence"`      // 0.0 ~ 1.0
	DetailJSON string  `json:"detail_json"`     // raw evidence JSON
}

// normalizeType converts the serialized type string to a canonical form.
// Protojson serializes enum values as their string names (e.g., "EVIDENCE_TYPE_LOG").
func normalizeType(t string) string {
	t = strings.TrimPrefix(t, "EVIDENCE_TYPE_")
	return strings.ToUpper(t)
}

// handleObservation advances the workflow based on the evidence type.
func (el *eventLoop) handleObservation(ctx context.Context, sessionID uuid.UUID, obs observationPayload) error {
	currentStatus, err := el.engine.GetStatus(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("get status: %w", err)
	}

	switch currentStatus {
	case store.StatusCollecting:
		// Collect evidence until all 3 agents have reported.
		el.log.Info("event loop: observation received in COLLECTING",
			"session_id", sessionID, "source", obs.Source, "type", obs.Type)

		// When all three base agents have reported, advance to ANALYZING.
		// We track this via observation count; for now, advance after first evidence.
		// In production this would use a proper aggregation gate.
		obsType := normalizeType(obs.Type)
		if obsType == "LOG" || obsType == "METRIC" || obsType == "TRACE" {
			// Check if we have enough evidence to transition.
			// Simple heuristic: any LOG/METRIC/TRACE observation triggers analysis.
			_, err := el.engine.Transition(ctx, sessionID, workflow.EventCollectComplete)
			if err != nil && !errors.Is(err, workflow.ErrInvalidTransition) {
				return fmt.Errorf("transition collect_complete: %w", err)
			}
		}

	case store.StatusAnalyzing:
		el.log.Info("event loop: observation in ANALYZING",
			"session_id", sessionID, "source", obs.Source, "type", obs.Type)

		// RCA_RESULT marks the RCA agent completing root cause analysis.
		if normalizeType(obs.Type) == "RCA_RESULT" {
			_, err := el.engine.Transition(ctx, sessionID, workflow.EventRCAGenerated)
			if err != nil && !errors.Is(err, workflow.ErrInvalidTransition) {
				return fmt.Errorf("transition rca_generated: %w", err)
			}
		}

	case store.StatusRCADone:
		// FIX_ACTION marks the Fix agent proposing fix steps.
		if normalizeType(obs.Type) == "FIX_ACTION" {
			el.log.Info("event loop: fix action proposed", "session_id", sessionID)
			_, err := el.engine.Transition(ctx, sessionID, workflow.EventFixGenerated)
			if err != nil && !errors.Is(err, workflow.ErrInvalidTransition) {
				return fmt.Errorf("transition fix_generated: %w", err)
			}

			// After transitioning to FIX_PROPOSED, check for HIGH-risk fixes.
			newStatus, _ := el.engine.GetStatus(ctx, sessionID)
			if newStatus == store.StatusAwaitingApproval {
				// Trigger approval gate for HIGH-risk fix actions.
				if el.onApprovalGate != nil {
					if err := el.onApprovalGate(ctx, sessionID); err != nil {
						el.log.Error("event loop: trigger approval gate", "error", err)
					}
				}
			} else if newStatus == store.StatusFixSuggested {
				// No HIGH risk: auto-execute if enabled (M6).
				_, err := el.engine.Transition(ctx, sessionID, workflow.EventExecute)
				if err != nil && !errors.Is(err, workflow.ErrInvalidTransition) {
					el.log.Warn("event loop: auto-execute transition", "error", err)
				}
			}
		}
	}

	return nil
}

// Close shuts down the event loop.
func (el *eventLoop) Close() error {
	return el.kafkaReader.Close()
}

// StartEventLoop launches the orchestrator event loop and sweep goroutines.
// It returns a shutdown func.
func StartEventLoop(
	ctx context.Context,
	engine *workflow.Engine,
	sessionDAO *store.SessionDAO,
	fixDAO *store.FixActionDAO,
	sessionEventDAO *store.SessionEventDAO,
	onApprovalGate func(ctx context.Context, sessionID uuid.UUID) error,
	onExecuteFixes func(ctx context.Context, sessionID uuid.UUID) error,
	brokers []string,
	log *slog.Logger,
) {
	el := newEventLoop(engine, sessionDAO, fixDAO, sessionEventDAO, brokers, log)
	el.SetCallbacks(onApprovalGate, onExecuteFixes)

	// Event consumer loop.
	go func() {
		if err := el.Run(ctx); err != nil {
			log.Error("event loop exited with error", "error", err)
		}
	}()

	// Sweep loop: runs every 30 seconds to handle stuck sessions and
	// re-evaluate sessions in FIX_PROPOSED / VERIFYING states.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				el.Close()
				return
			case <-ticker.C:
				if err := engine.Sweep(ctx); err != nil {
					log.Error("sweep stuck sessions", "error", err)
				}
				if err := engine.SweepFixProposed(ctx); err != nil {
					log.Error("sweep fix_proposed", "error", err)
				}
				if err := engine.SweepVerifying(ctx); err != nil {
					log.Error("sweep verifying", "error", err)
				}
			}
		}
	}()

	log.Info("event loop and sweep started")
}
