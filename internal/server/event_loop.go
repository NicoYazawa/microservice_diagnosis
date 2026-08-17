// Package server provides HTTP server setup with Gin + gRPC-gateway.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
	engine          *workflow.Engine
	sessionDAO      *store.SessionDAO
	fixDAO          *store.FixActionDAO
	sessionEventDAO *store.SessionEventDAO
	producer        bus.Producer
	brokers         []string
	log             *slog.Logger

	// M5/M6 callbacks. Injected after OrchestratorHandler is constructed to
	// avoid circular dependencies between eventLoop and OrchestratorHandler.
	onApprovalGate func(ctx context.Context, sessionID uuid.UUID) error
	onExecuteFixes func(ctx context.Context, sessionID uuid.UUID) error
}

// newEventLoop builds an eventLoop that reads from the observations topic.
func newEventLoop(
	engine *workflow.Engine,
	sessionDAO *store.SessionDAO,
	fixDAO *store.FixActionDAO,
	sessionEventDAO *store.SessionEventDAO,
	producer bus.Producer,
	brokers []string,
	log *slog.Logger,
) *eventLoop {
	return &eventLoop{
		engine:          engine,
		sessionDAO:      sessionDAO,
		fixDAO:          fixDAO,
		sessionEventDAO: sessionEventDAO,
		producer:        producer,
		brokers:         brokers,
		log:             log,
	}
}

// SetCallbacks injects the OrchestratorHandler methods after it is constructed.
// These drive the M5/M6 approval gate and fix-execution pipeline once a session
// reaches the corresponding state.
func (el *eventLoop) SetCallbacks(
	onApprovalGate func(ctx context.Context, sessionID uuid.UUID) error,
	onExecuteFixes func(ctx context.Context, sessionID uuid.UUID) error,
) {
	el.onApprovalGate = onApprovalGate
	el.onExecuteFixes = onExecuteFixes
}

// DispatchCollect sends a collect command to all base agents (log/metric/trace)
// via their dedicated command topics when a session transitions to COLLECTING.
func (el *eventLoop) DispatchCollect(ctx context.Context, sessionID, targetService string) error {
	if el.producer == nil {
		return fmt.Errorf("event loop: no producer configured")
	}

	// Pipeline: each collector agent has its own command topic.
	agents := []struct {
		kind  string
		topic string
	}{
		{"agent-log", bus.TopicCommandsLog},
		{"agent-metric", bus.TopicCommandsMetric},
		{"agent-trace", bus.TopicCommandsTrace},
	}
	for _, a := range agents {
		cmd := bus.CommandMessage{
			SessionID:     sessionID,
			TargetService: targetService,
			Command:       "collect",
			AgentKind:     a.kind,
		}
		data, err := json.Marshal(cmd)
		if err != nil {
			el.log.Warn("event loop: marshal collect command", "error", err)
			continue
		}
		msg := bus.Message{
			Topic: a.topic,
			Key:   []byte(sessionID),
			Value: data,
		}
		if err := el.producer.Publish(ctx, msg); err != nil {
			el.log.Error("event loop: dispatch collect command",
				"agent", a.kind, "session_id", sessionID, "error", err)
			// Continue with the remaining agents — partial dispatch is
			// recoverable via the per-session Sweep loop (M4 onward).
		} else {
			el.log.Info("event loop: dispatched collect command",
				"agent", a.kind, "session_id", sessionID, "target", targetService)
		}
	}
	return nil
}

// DispatchAnalyze sends an "analyze" command to the RCA agent via TopicCommandsRCA
// when a session transitions to ANALYZING, triggering root cause analysis.
func (el *eventLoop) DispatchAnalyze(ctx context.Context, sessionID, targetService string) error {
	if el.producer == nil {
		return fmt.Errorf("event loop: no producer configured")
	}
	cmd := bus.CommandMessage{
		SessionID:     sessionID,
		TargetService: targetService,
		Command:       "analyze",
		AgentKind:     "agent-rca",
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal analyze command: %w", err)
	}
	msg := bus.Message{
		Topic: bus.TopicCommandsRCA,
		Key:   []byte(sessionID),
		Value: data,
	}
	if err := el.producer.Publish(ctx, msg); err != nil {
		return fmt.Errorf("publish analyze command: %w", err)
	}
	el.log.Info("event loop: dispatched analyze command", "session_id", sessionID, "target", targetService)
	return nil
}

// defaultBacklogSkip is the default consumer-group lag (in messages) beyond
// which the event loop abandons its committed offset and starts from the topic
// tail. Historical backlog on the observations topic has no replay value for
// the orchestrator: the sessions it describes have long resolved, and
// replaying stale evidence would delay live messages for new sessions by
// minutes to hours (each stale message still triggers a DB lookup in
// handleObservation).
const defaultBacklogSkip int64 = 10_000

// skipBacklogTimeout bounds how long skipBacklogFast waits for a previous
// orchestrator instance's consumer-group membership to expire. A gracefully
// stopped instance leaves the group immediately; a killed one lingers until
// its session times out (default 45s).
var skipBacklogTimeout = 55 * time.Second

// skipBacklogFast checks the orchestrator's consumer-group lag on the
// observations topic. If the committed offset trails the latest offset by more
// than the backlog threshold, it commits the group offset to the topic's
// latest position so the consumer starts from the tail instead of replaying
// stale history.
//
// IMPORTANT: it must be called BEFORE the kafka.Reader is created. kafka-go's
// NewReader joins the consumer group immediately (it spawns its consumption
// goroutine at construction), and a group with an active member rejects the
// simple offset commit used here (UNKNOWN_MEMBER_ID), leaving the group stuck
// behind the backlog.
//
// A simple commit is only accepted while the group has no active members, so
// when a previous orchestrator instance is still shutting down (its group
// membership has not yet expired) the commit is retried until the group frees
// up or skipBacklogTimeout elapses, in which case an error is returned and the
// caller falls back to consuming from the committed offset.
//
// A fresh group with no committed offset on a topic that has history is
// passed through: the caller (Run) starts the reader with FirstOffset so the
// agent backfills naturally; we do not commit the high watermark to the
// tail (which would skip the backfill entirely).
func skipBacklogFast(ctx context.Context, brokers []string, groupID, topic string, backlogSkip int64, log *slog.Logger) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}

	md, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return fmt.Errorf("skip backlog: metadata: %w", err)
	}
	var partitions []int
	for _, t := range md.Topics {
		if t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			partitions = append(partitions, p.ID)
		}
	}
	if len(partitions) == 0 {
		return nil
	}

	committed, err := client.ConsumerOffsets(ctx, kafka.TopicAndGroup{
		Topic: topic, GroupId: groupID,
	})
	if err != nil {
		return fmt.Errorf("skip backlog: fetch committed offsets: %w", err)
	}

	listReq := &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{}}
	for _, p := range partitions {
		listReq.Topics[topic] = append(listReq.Topics[topic], kafka.LastOffsetOf(p))
	}
	listResp, err := client.ListOffsets(ctx, listReq)
	if err != nil {
		return fmt.Errorf("skip backlog: fetch latest offsets: %w", err)
	}

	// Lag of an uncommitted partition counts from the topic start: a fresh
	// group on a topic with stale history is just as blocked as one with a
	// committed offset.
	var lag int64
	uncommitted := false
	lastByPartition := make(map[int]int64, len(partitions))
	for _, po := range listResp.Topics[topic] {
		if po.Error != nil {
			return fmt.Errorf("skip backlog: partition %d: %w", po.Partition, po.Error)
		}
		lastByPartition[po.Partition] = po.LastOffset
		c := committed[po.Partition]
		if c == -1 {
			c = 0
			uncommitted = true
		}
		if pLag := po.LastOffset - c; pLag > lag {
			lag = pLag
		}
	}
	threshold := backlogSkip
	if threshold == 0 {
		threshold = defaultBacklogSkip
	}
	if lag <= threshold {
		log.Info("event loop: backlog within threshold, consuming from committed offset",
			"group", groupID, "lag", lag)
		return nil
	}

	// Fresh group + history → let the reader backfill from FirstOffset.
	if uncommitted {
		log.Info("event loop: fresh group on topic with backlog, leaving for FirstOffset backfill",
			"group", groupID, "lag", lag, "topic", topic)
		return nil
	}

	// Large backlog detected: commit group offset to the latest position so
	// consumption starts from the topic tail. Old sessions have resolved and
	// their stale evidence is not useful; skipping to latest avoids replaying
	// minutes-to-hours of history while new sessions pile up.
	//
	// The simple commit (GenerationID: -1) only succeeds when no active member
	// occupies the group. If a previous orchestrator instance is still shutting
	// down, retry until the group frees up or timeout elapses.
	log.Warn("event loop: large backlog detected, repositioning to topic tail",
		"group", groupID, "topic", topic, "lag", lag)

	retryCtx, retryCancel := context.WithTimeout(ctx, skipBacklogTimeout)
	defer retryCancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		// Commit to latest offset per partition (LastOffset = high watermark + 1
		// = next read position). Use the per-partition LastOffset rather than
		// partition 0's value for all partitions — multi-partition topics were
		// previously committed with the wrong offset.
		commitReq := &kafka.OffsetCommitRequest{
			GroupID:      groupID,
			GenerationID: -1,
			Topics:       map[string][]kafka.OffsetCommit{},
		}
		for _, p := range partitions {
			commitReq.Topics[topic] = append(commitReq.Topics[topic], kafka.OffsetCommit{
				Partition: p,
				Offset:    lastByPartition[p],
			})
		}

		resp, err := client.OffsetCommit(retryCtx, commitReq)
		if err == nil {
			allCommitted := true
			for _, parts := range resp.Topics {
				for _, p := range parts {
					if p.Error != nil {
						allCommitted = false
					}
				}
			}
			if allCommitted {
				log.Info("event loop: backlog skip committed, consuming from topic tail",
					"group", groupID, "topic", topic, "lag", lag)
				return nil
			}
		}

		select {
		case <-retryCtx.Done():
			log.Warn("event loop: backlog skip timed out, consuming from committed offset",
				"group", groupID, "topic", topic, "lag", lag)
			return nil
		case <-ticker.C:
			// Retry.
		}
	}
}

// topicReader pairs a kafka.Reader with its topic name so the main loop can
// commit offsets against the correct reader.
type topicReader struct {
	topic string
	r     *kafka.Reader
}

// Run starts the event loop. It blocks until ctx is cancelled.
//
// Orchestrator consumes 5 observation topics: log / metric / trace / rca / fix.
// Each topic uses its own reader so the pipeline is truly linear (no fan-in).
func (el *eventLoop) Run(ctx context.Context) error {
	topics := []string{
		bus.TopicObservationsLog,
		bus.TopicObservationsMetric,
		bus.TopicObservationsTrace,
		bus.TopicObservationsRCA,
		bus.TopicObservationsFix,
	}
	brokers := el.brokers

	// Before joining the consumer groups, attempt to skip past a large
	// historical backlog so we don't replay stale evidence on restart.
	// skipBacklogFast is a no-op when lag <= threshold.
	for _, topic := range topics {
		groupID := bus.GroupID("orchestrator", topic)
		if err := skipBacklogFast(ctx, brokers, groupID, topic, 0, el.log); err != nil {
			el.log.Warn("event loop: skipBacklogFast", "topic", topic, "error", err)
		}
	}

	// Build one reader per topic. StartOffset=FirstOffset: a fresh group on a
	// topic with history backfills from the start; an existing group resumes
	// from its committed offset (kafka-go handles both).
	readers := make([]*topicReader, 0, len(topics))
	for _, topic := range topics {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        bus.GroupID("orchestrator", topic),
			MinBytes:       1,
			MaxBytes:       1 << 20,
			MaxWait:        500 * time.Millisecond,
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
		})
		readers = append(readers, &topicReader{topic: topic, r: r})
	}

	var wg sync.WaitGroup
	defer func() {
		// Close readers first to unblock any FetchMessage call, then wait
		// for the forwarding goroutines to drain and exit. Without the wait
		// the goroutines could leak after Run returns.
		for _, tr := range readers {
			tr.r.Close()
		}
		wg.Wait()
	}()

	el.log.Info("event loop started", "topics", topics)

	// Fan-in: each reader goroutine forwards messages to a single channel.
	// Capacity tuned to absorb burst skew between 5 topic readers; if the
	// channel fills up the reader goroutine blocks, applying back-pressure
	// to the broker consumer so no messages are silently dropped.
	msgCh := make(chan kafka.Message, 1024)

	for _, tr := range readers {
		tr := tr
		wg.Add(1)
		go func() {
			defer wg.Done()
			backoff := 500 * time.Millisecond
			for {
				if ctx.Err() != nil {
					return
				}
				m, err := tr.r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					el.log.Warn("event loop: fetch error",
						"topic", tr.topic, "error", err, "retry_in", backoff)
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					if backoff < 10*time.Second {
						backoff *= 2
					}
					continue
				}
				backoff = 500 * time.Millisecond
				select {
				case msgCh <- m:
				case <-ctx.Done():
					// Do not commit — message will be redelivered on next start.
					return
				}
			}
		}()
	}

	commitOffset := func(m kafka.Message) {
		for _, tr := range readers {
			if tr.topic != m.Topic {
				continue
			}
			if err := tr.r.CommitMessages(ctx, m); err != nil {
				el.log.Error("event loop: commit offset failed",
					"topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "error", err)
			}
			return
		}
		// Unknown topic — should never happen; commit via first reader as fallback.
		if len(readers) > 0 {
			if err := readers[0].r.CommitMessages(ctx, m); err != nil {
				el.log.Error("event loop: commit offset failed (fallback)",
					"topic", m.Topic, "error", err)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-msgCh:
			var obs observationPayload
			if err := json.Unmarshal(m.Value, &obs); err != nil {
				el.log.Warn("event loop: unmarshal skipped", "error", err,
					"topic", m.Topic, "offset", m.Offset)
				commitOffset(m)
				continue
			}
			if obs.SessionID == "" {
				el.log.Warn("event loop: empty session_id, skipping",
					"topic", m.Topic, "offset", m.Offset)
				commitOffset(m)
				continue
			}
			sessionID, err := uuid.Parse(obs.SessionID)
			if err != nil {
				el.log.Warn("event loop: invalid session_id",
					"session_id", obs.SessionID, "topic", m.Topic, "offset", m.Offset)
				commitOffset(m)
				continue
			}
			if err := el.handleObservation(ctx, sessionID, obs); err != nil {
				el.log.Error("event loop: handle observation failed",
					"session_id", obs.SessionID, "type", obs.Type,
					"topic", m.Topic, "offset", m.Offset, "error", err)
				// Do NOT commit on handler error — let kafka redeliver the
				// message after restart so the session can make progress.
				continue
			}
			commitOffset(m)
		}
	}
}

// observationPayload is a minimal subset of the Observation proto used for routing.
// Field names match protojson output (camelCase).
type observationPayload struct {
	ID            string  `json:"id"`
	SessionID     string  `json:"sessionId"` // protojson uses camelCase
	Source        string  `json:"source"`
	Type          string  `json:"type"` // e.g. "EVIDENCE_TYPE_LOG"
	SubType       string  `json:"subType"`
	Confidence    float64 `json:"confidence"`
	DetailJSON    string  `json:"detailJson"`
	TargetService string  `json:"targetService"`
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

	obsType := normalizeType(obs.Type)

	switch currentStatus {
	case store.StatusCollecting:
		// Collect evidence until all 3 agents have reported.
		el.log.Debug("event loop: observation received in COLLECTING",
			"session_id", sessionID, "source", obs.Source, "type", obs.Type)

		// When all three base agents have reported, advance to ANALYZING.
		// We track this via observation count; for now, advance after first evidence.
		if obsType == "LOG" || obsType == "METRIC" || obsType == "TRACE" {
			newStatus, err := el.engine.Transition(ctx, sessionID, workflow.EventCollectComplete)
			if err != nil {
				return fmt.Errorf("transition COLLECTING→ANALYZING: %w", err)
			}
			// Transition succeeded: dispatch analyze command to RCA agent.
			if newStatus == store.StatusAnalyzing {
				if err := el.DispatchAnalyze(ctx, sessionID.String(), obs.TargetService); err != nil {
					el.log.Error("event loop: dispatch analyze failed", "session_id", sessionID, "error", err)
				}
			}
		}

	case store.StatusAnalyzing:
		// RCA_RESULT marks the RCA agent completing root cause analysis.
		if obsType == "RCA_RESULT" {
			newStatus, err := el.engine.Transition(ctx, sessionID, workflow.EventAnalysisComplete)
			if err != nil {
				return fmt.Errorf("transition ANALYZING→RCA_DONE: %w", err)
			}
			el.log.Info("event loop: transitioned to RCA_DONE",
				"session_id", sessionID, "new_status", newStatus)
			return nil
		}
		// The FIX_ACTION may arrive before RCA_RESULT if the Fix agent and
		// orchestrator process RCA_RESULT at different rates (each has its own
		// consumer group on TopicObservationsRCA). The state machine does not
		// permit FIX_ACTION in ANALYZING, so we drop it. The orchestrator will
		// eventually see RCA_RESULT, transition to RCA_DONE, and stop polling
		// for evidence — fix actions whose FIX_ACTION was already emitted by
		// the fix agent are observable via the fix_actions DB table; the
		// session will be re-evaluated by SweepFixProposed after RCA_DONE.
		if obsType == "FIX_ACTION" {
			el.log.Warn("event loop: FIX_ACTION arrived in ANALYZING (cross-topic race); will not process until RCA_RESULT",
				"session_id", sessionID, "source", obs.Source)
			return nil
		}

	case store.StatusRCADone:
		if obsType == "FIX_ACTION" {
			el.log.Info("event loop: FIX_ACTION in RCA_DONE, transitioning",
				"session_id", sessionID, "source", obs.Source)

			// Step 1: RCA_DONE → FIX_PROPOSED
			newStatus, err := el.engine.Transition(ctx, sessionID, workflow.EventRCAGenerated)
			if err != nil {
				return fmt.Errorf("transition RCA_DONE→FIX_PROPOSED: %w", err)
			}
			el.log.Info("event loop: transitioned to fix_proposed",
				"session_id", sessionID, "new_status", newStatus)

			// Step 2: immediately evaluate HIGH risk (don't wait for the 30s
			// sweep) — EventFixGenerated routes to AWAITING_APPROVAL or
			// FIX_SUGGESTED depending on whether any fix_action has HIGH risk.
			newStatus, err = el.engine.Transition(ctx, sessionID, workflow.EventFixGenerated)
			if err != nil {
				if errors.Is(err, workflow.ErrInvalidTransition) {
					// The 30s sweep already moved the session — fine.
					return nil
				}
				return fmt.Errorf("transition evaluate fix_proposed: %w", err)
			}

			// Step 3: drive M5/M6 based on the resulting status.
			switch newStatus {
			case store.StatusAwaitingApproval:
				if el.onApprovalGate != nil {
					if err := el.onApprovalGate(ctx, sessionID); err != nil {
						el.log.Error("event loop: approval gate failed",
							"session_id", sessionID, "error", err)
					}
				} else {
					el.log.Warn("event loop: AWAITING_APPROVAL but no approval callback wired",
						"session_id", sessionID)
				}
			case store.StatusFixSuggested:
				// No HIGH-risk steps — advance straight into execution.
				if _, err := el.engine.Transition(ctx, sessionID, workflow.EventExecute); err != nil {
					if !errors.Is(err, workflow.ErrInvalidTransition) {
						return fmt.Errorf("transition FIX_SUGGESTED→FIX_EXECUTING: %w", err)
					}
					break
				}
				if el.onExecuteFixes != nil {
					if err := el.onExecuteFixes(ctx, sessionID); err != nil {
						el.log.Error("event loop: execute fixes failed",
							"session_id", sessionID, "error", err)
					}
				} else {
					el.log.Warn("event loop: FIX_EXECUTING but no execute callback wired",
						"session_id", sessionID)
				}
			}
			return nil
		}
	}

	return nil
}

// Close shuts down the event loop (no-op: readers are closed in Run's defer).
func (el *eventLoop) Close() error {
	return nil
}

// StartEventLoop launches the orchestrator event loop and sweep goroutines.
// Returns the eventLoop instance so callers can access DispatchCollect.
func StartEventLoop(
	ctx context.Context,
	engine *workflow.Engine,
	sessionDAO *store.SessionDAO,
	fixDAO *store.FixActionDAO,
	sessionEventDAO *store.SessionEventDAO,
	producer bus.Producer,
	onApprovalGate func(ctx context.Context, sessionID uuid.UUID) error,
	onExecuteFixes func(ctx context.Context, sessionID uuid.UUID) error,
	brokers []string,
	log *slog.Logger,
) *eventLoop {
	el := newEventLoop(engine, sessionDAO, fixDAO, sessionEventDAO, producer, brokers, log)
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
	return el
}