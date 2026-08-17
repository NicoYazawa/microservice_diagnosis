// Package llm provides a simple HTTP client for calling LLM APIs (OpenAI / Claude / Ollama).
// It implements exponential backoff retry and token rate limiting.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Client is an LLM HTTP client with retry and rate limiting.
type Client struct {
	baseURL     string
	apiKey      string
	model       string
	client      *http.Client
	mu          sync.Mutex
	minInterval time.Duration // rate limit: minimum interval between requests
	lastRequest time.Time
	log         *slog.Logger
}

// Config holds LLM client configuration.
type Config struct {
	BaseURL     string        // e.g. https://api.openai.com/v1 or http://localhost:11434/v1
	APIKey      string        // or Ollama API key (optional)
	Model       string        // e.g. gpt-4o, claude-3-5-sonnet-20241002, llama3.1
	MinInterval time.Duration // rate limit minimum interval between requests (default 100ms)
}

// NewClient creates a new LLM client.
func NewClient(cfg Config, log *slog.Logger) *Client {
	if cfg.MinInterval == 0 {
		cfg.MinInterval = 100 * time.Millisecond
	}
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		baseURL:     cfg.BaseURL,
		apiKey:      cfg.APIKey,
		model:       cfg.Model,
		client:      &http.Client{Timeout: 60 * time.Second},
		minInterval: cfg.MinInterval,
		log:         log,
	}
}

// ChatRequest forms a chat completions request.
type ChatRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
}

// ChatMessage is a single chat message.
type ChatMessage struct {
	Role    string `json:"role"`    // system / user / assistant
	Content string `json:"content"`
}

// ChatResponse is the API response structure (OpenAI-compatible).
type ChatResponse struct {
	ID      string   `json:"id"`
	Choices []Choice `json:"choices"`
	Error   *APIError `json:"error,omitempty"`
}

// Choice is a single completion choice.
type Choice struct {
	Message ChatMessage `json:"message"`
}

// APIError represents an LLM API error.
type APIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// Chat calls the LLM chat completions endpoint with exponential backoff retry.
func (c *Client) Chat(ctx context.Context, system, user string) (string, error) {
	reqBody := ChatRequest{
		Model: c.model,
		Messages: []ChatMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
	}
	return c.chatWithRetry(ctx, reqBody)
}

func (c *Client) chatWithRetry(ctx context.Context, reqBody ChatRequest) (string, error) {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(200<<uint(attempt-1)) * time.Millisecond
			if err := sleepCtx(ctx, backoff); err != nil {
				return "", err
			}
		}

		// Rate limit: ensure minimum interval between requests.
		c.mu.Lock()
		elapsed := time.Since(c.lastRequest)
		wait := c.minInterval - elapsed
		c.lastRequest = time.Now().Add(maxDuration(wait, 0))
		c.mu.Unlock()
		if wait > 0 {
			if err := sleepCtx(ctx, wait); err != nil {
				return "", err
			}
		}

		resp, err := c.doChatRequest(ctx, reqBody)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		c.log.Warn("llm: request failed, will retry", "attempt", attempt+1, "error", err)
	}
	return "", fmt.Errorf("llm: chat failed after %d attempts: %w", maxAttempts, lastErr)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (c *Client) doChatRequest(ctx context.Context, reqBody ChatRequest) (string, error) {
	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("llm: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("llm: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("llm: do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("llm: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(respBody, &apiErr); err == nil && apiErr.Message != "" {
			return "", fmt.Errorf("llm: api error %d: %s", resp.StatusCode, apiErr.Message)
		}
		return "", fmt.Errorf("llm: unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	var chatResp ChatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return "", fmt.Errorf("llm: unmarshal response: %w", err)
	}
	if chatResp.Error != nil {
		return "", fmt.Errorf("llm: api error: %s", chatResp.Error.Message)
	}
	if len(chatResp.Choices) == 0 {
		return "", fmt.Errorf("llm: no completion choices returned")
	}
	return chatResp.Choices[0].Message.Content, nil
}
