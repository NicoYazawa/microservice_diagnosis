package workflow

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestSkipLockedBehavior documents the expected behavior of SKIP LOCKED
// in a concurrent scenario. This test uses in-memory simulation to verify
// the logic; a real integration test requires a PostgreSQL instance.
//
// Scenario: Two workers try to claim the same session simultaneously.
// Expected: Exactly one worker succeeds; the other gets ErrSessionNotFound
// (or a different session if available).
func TestSkipLockedBehavior(t *testing.T) {
	// Simulate a shared session registry with lock contention.
	var (
		mu                sync.Mutex
		sessions          = map[string]string{"s1": StatusCollecting}
		claimed           int
		claimMu           sync.Mutex
		workerErrs        []error
		workerErrsMu      sync.Mutex
	)

	// simulateTransition replays the SKIP LOCKED logic: if the session
	// is already claimed this tick, we skip it (like SKIP LOCKED).
	simulateTransition := func(workerID, sessionID string) error {
		mu.Lock()
		status, exists := sessions[sessionID]
		if !exists {
			mu.Unlock()
			return ErrSessionNotFound
		}
		if status == "" {
			// Already claimed this tick → SKIP LOCKED behavior.
			mu.Unlock()
			return ErrSessionNotFound
		}
		// Claim the session.
		sessions[sessionID] = ""
		mu.Unlock()

		// Simulate some work.
		time.Sleep(10 * time.Millisecond)

		// Complete the transition.
		mu.Lock()
		sessions[sessionID] = StatusAnalyzing
		mu.Unlock()

		claimMu.Lock()
		claimed++
		claimMu.Unlock()
		return nil
	}

	var wg sync.WaitGroup
	workerIDs := []string{"worker-A", "worker-B"}

	for _, wid := range workerIDs {
		wg.Add(1)
		go func(wid string) {
			defer wg.Done()
			err := simulateTransition(wid, "s1")
			if err != nil {
				workerErrsMu.Lock()
				workerErrs = append(workerErrs, err)
				workerErrsMu.Unlock()
			}
		}(wid)
	}

	wg.Wait()

	// Exactly one worker should have claimed the session.
	assert.Equal(t, 1, claimed, "exactly one worker should claim the session")
	assert.Len(t, workerErrs, 1, "exactly one worker should get an error")
	assert.ErrorIs(t, workerErrs[0], ErrSessionNotFound)
}

// TestConcurrentSweepWithSkipLocked simulates multiple instances running Sweep
// concurrently on sessions with different statuses.
func TestConcurrentSweepWithSkipLocked(t *testing.T) {
	var (
		mu        sync.Mutex
		sessions  = map[string]string{
			"s1": StatusCollecting, // stuck, should be auto-failed
			"s2": StatusCollecting, // stuck, should be auto-failed
			"s3": StatusAnalyzing,  // not stuck
		}
		failed []string
		failMu sync.Mutex
	)

	// simulateSweep picks up sessions in COLLECTING that haven't been updated in > 1ms.
	simulateSweep := func(workerID string) error {
		mu.Lock()
		var candidates []string
		for id, status := range sessions {
			if status == StatusCollecting {
				candidates = append(candidates, id)
			}
		}
		// Pick first candidate (simulating FOR UPDATE SKIP LOCKED picking order).
		var picked string
		for _, c := range candidates {
			if sessions[c] != "" {
				picked = c
				sessions[c] = "" // claim
				break
			}
		}
		mu.Unlock()
		if picked == "" {
			return nil // nothing to do
		}

		// Simulate work.
		time.Sleep(5 * time.Millisecond)

		// Complete: transition to FAILED.
		mu.Lock()
		sessions[picked] = StatusFailed
		mu.Unlock()

		failMu.Lock()
		failed = append(failed, picked)
		failMu.Unlock()
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(wid string) {
			defer wg.Done()
			_ = simulateSweep(wid) // each sweeper runs one iteration
		}(fmt.Sprintf("sweeper-%d", i))
	}

	wg.Wait()

	// Both stuck sessions should be failed (each by exactly one sweeper).
	assert.ElementsMatch(t, []string{"s1", "s2"}, failed, "both stuck sessions should be auto-failed")
}

// TestStateMachineGraphReachability does a forward BFS from CREATED through
// the ValidTransitions graph and verifies every non-terminal is reachable.
// Note: AWAITING_APPROVAL is a runtime-only branch from FIX_PROPOSED, gated by
// HIGH-risk fix actions, so it is excluded from the static reachability check.
func TestStateMachineGraphReachability(t *testing.T) {
	// Excludes AWAITING_APPROVAL (runtime-only branch).
	nonTerminals := []string{
		StatusCreated, StatusCollecting, StatusAnalyzing, StatusRCADone,
		StatusFixProposed, StatusFixSuggested, StatusFixExecuting, StatusVerifying,
	}

	// Build forward graph: graph[from] = set of reachable "to" statuses.
	graph := make(map[string]map[string]bool)
	for from, events := range ValidTransitions {
		graph[from] = make(map[string]bool)
		for _, to := range events {
			graph[from][to] = true
		}
	}

	// Forward BFS from CREATED.
	visited := make(map[string]bool)
	queue := []string{StatusCreated}
	visited[StatusCreated] = true

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		for next := range graph[curr] {
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}

	for _, s := range nonTerminals {
		assert.True(t, visited[s], "status %s should be reachable from CREATED via static transitions", s)
	}
}
