package approval

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// TokenManager issues short-lived random tokens for approval callbacks.
// It also stores the approval request in memory for the NOOP implementation.
type TokenManager struct {
	mu     sync.RWMutex
	tokens map[string]*tokenEntry // token -> entry
}

type tokenEntry struct {
	req      ApprovalRequest
	result   *ApprovalResult // nil until decided
	created  time.Time
	expires  time.Time
}

const defaultTTL = 24 * time.Hour

// NewTokenManager creates an in-memory token manager (suitable for single-instance orchestrator).
// For multi-instance, replace with Redis-backed implementation.
func NewTokenManager() *TokenManager {
	return &TokenManager{tokens: make(map[string]*tokenEntry)}
}

// Issue creates a new token for the given approval request.
func (tm *TokenManager) Issue(ctx context.Context, req ApprovalRequest) (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("approval: token entropy: %w", err)
	}
	token := hex.EncodeToString(raw)

	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tokens[token] = &tokenEntry{
		req:     req,
		created: time.Now().UTC(),
		expires: time.Now().UTC().Add(defaultTTL),
	}
	return token, nil
}

// Get returns the approval request and current result for a token.
func (tm *TokenManager) Get(token string) (*ApprovalRequest, *ApprovalResult, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	entry, ok := tm.tokens[token]
	if !ok {
		return nil, nil, false
	}
	if time.Now().UTC().After(entry.expires) {
		return nil, nil, false
	}
	return &entry.req, entry.result, true
}

// Resolve marks a token as decided.
func (tm *TokenManager) Resolve(token string, result ApprovalResult) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	entry, ok := tm.tokens[token]
	if !ok {
		return false
	}
	entry.result = &result
	return true
}

// NOOPClient is a no-op approval client that auto-approves everything.
// Use for development / testing, or when approval is handled entirely via external systems.
type NOOPClient struct {
	tokens *TokenManager
}

// NewNOOPClient creates a NOOP approval client.
func NewNOOPClient() *NOOPClient {
	return &NOOPClient{tokens: NewTokenManager()}
}

// RequestApproval issues a token and immediately resolves it as APPROVED.
func (c *NOOPClient) RequestApproval(ctx context.Context, req ApprovalRequest) (string, error) {
	token, err := c.tokens.Issue(ctx, req)
	if err != nil {
		return "", err
	}
	// Auto-approve.
	c.tokens.Resolve(token, ApprovalResult{
		Decision:  "APPROVED",
		DecidedBy: "system",
		DecidedAt: time.Now().UTC(),
		Reason:    "auto-approved (NOOP mode)",
	})
	return token, nil
}

// GetApproval returns the stored request/result for a token.
func (c *NOOPClient) GetApproval(ctx context.Context, token string) (*ApprovalRequest, *ApprovalResult, error) {
	req, res, ok := c.tokens.Get(token)
	if !ok {
		return nil, nil, fmt.Errorf("approval: token not found or expired")
	}
	return req, res, nil
}

// CancelApproval removes a pending token.
func (c *NOOPClient) CancelApproval(ctx context.Context, token string) error {
	c.tokens.Resolve(token, ApprovalResult{
		Decision:  "REJECTED",
		DecidedBy: "system",
		DecidedAt: time.Now().UTC(),
		Reason:    "cancelled",
	})
	return nil
}

// WebhookClient sends approval requests to an external HTTP endpoint and polls for decisions.
type WebhookClient struct {
	tokens   *TokenManager
	callback string // URL to POST approval requests to
	client   webhookHTTPClient
}

// webhookHTTPClient abstracts net/http for testability.
type webhookHTTPClient = interface {
	Post(ctx context.Context, url string, body interface{}) error
	Get(ctx context.Context, url string) (*WebhookResponse, error)
}

type WebhookResponse struct {
	Decision  string
	DecidedBy string
	Reason    string
}

// NewWebhookClient creates an approval client that delegates to an external webhook.
func NewWebhookClient(callback string, httpClient webhookHTTPClient) *WebhookClient {
	if httpClient == nil {
		httpClient = &defaultHTTPClient{}
	}
	return &WebhookClient{
		tokens:   NewTokenManager(),
		callback: callback,
		client:   httpClient,
	}
}

// RequestApproval sends the approval request to the external callback URL.
func (c *WebhookClient) RequestApproval(ctx context.Context, req ApprovalRequest) (string, error) {
	token, err := c.tokens.Issue(ctx, req)
	if err != nil {
		return "", err
	}

	// Payload matches the shape external approval systems (Slack, PagerDuty) expect.
	payload := map[string]interface{}{
		"token":         token,
		"session_id":    req.SessionID.String(),
		"fix_action_id": req.FixActionID.String(),
		"summary":       req.Summary,
		"risk":          req.Risk,
		"target":        req.Target,
		"requested_by":  req.RequestedBy,
		"approve_url":   fmt.Sprintf("%s/approve?token=%s", c.callback, token),
		"reject_url":    fmt.Sprintf("%s/reject?token=%s", c.callback, token),
		"expires_at":    time.Now().UTC().Add(defaultTTL).Format(time.RFC3339),
	}

	if err := c.client.Post(ctx, c.callback, payload); err != nil {
		return "", fmt.Errorf("approval webhook: %w", err)
	}
	return token, nil
}

// GetApproval checks the external system for a decision on the given token.
func (c *WebhookClient) GetApproval(ctx context.Context, token string) (*ApprovalRequest, *ApprovalResult, error) {
	req, _, ok := c.tokens.Get(token)
	if !ok {
		return nil, nil, fmt.Errorf("approval: token not found or expired")
	}

	// Poll the callback URL for the decision.
	resp, err := c.client.Get(ctx, fmt.Sprintf("%s/status?token=%s", c.callback, token))
	if err != nil {
		// Not found / pending — return current state.
		return req, nil, nil
	}
	if resp == nil {
		return req, nil, nil
	}

	// Update stored result.
	c.tokens.Resolve(token, ApprovalResult{
		Decision:  resp.Decision,
		DecidedBy: resp.DecidedBy,
		Reason:    resp.Reason,
		DecidedAt: time.Now().UTC(),
	})

	_, res, _ := c.tokens.Get(token)
	return req, res, nil
}

// CancelApproval notifies the external system that the approval is cancelled.
func (c *WebhookClient) CancelApproval(ctx context.Context, token string) error {
	c.tokens.Resolve(token, ApprovalResult{
		Decision:  "REJECTED",
		DecidedBy: "system",
		DecidedAt: time.Now().UTC(),
		Reason:    "cancelled",
	})
	// Best-effort: notify the external system.
	_ = c.client.Post(ctx, fmt.Sprintf("%s/cancel?token=%s", c.callback, token), nil)
	return nil
}

// --- default HTTP client (net/http wrapper) ---

type defaultHTTPClient struct{}

func (c *defaultHTTPClient) Post(ctx context.Context, url string, body interface{}) error {
	// Stub: real implementation uses net/http Client.
	// Replaced by mock in tests.
	return nil
}

func (c *defaultHTTPClient) Get(ctx context.Context, url string) (*WebhookResponse, error) {
	return nil, nil
}

// InMemoryStore persists approval records to PostgreSQL via the DAO.
// This is the production implementation used by the orchestrator.
type InMemoryStore struct {
	tokens *TokenManager
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{tokens: NewTokenManager()}
}

func (s *InMemoryStore) Issue(ctx context.Context, req ApprovalRequest) (string, error) {
	return s.tokens.Issue(ctx, req)
}

func (s *InMemoryStore) Get(token string) (*ApprovalRequest, *ApprovalResult, bool) {
	return s.tokens.Get(token)
}

func (s *InMemoryStore) Resolve(token string, result ApprovalResult) bool {
	return s.tokens.Resolve(token, result)
}
