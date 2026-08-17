package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrSubscriptionRemoved is returned by the forwarding handler when its
// subscription was removed via Close(); the inner Consume loop will then
// return and the goroutine will exit.
var ErrSubscriptionRemoved = errors.New("bus: subscription removed")

// ConsumerChannel is a Consumer that delivers messages via a Go channel.
// It wraps a kafkaConsumer and forwards messages to a channel per topic.
//
// Delivery semantics: at-least-once. The forwarding handler performs a
// blocking send on the subscription channel and only returns success after
// the message has been accepted by the runner. On context cancellation or
// subscription removal, the handler returns an error so the inner consumer
// does NOT commit the offset and the message is redelivered on next start.
type ConsumerChannel struct {
	inner     *kafkaConsumer
	subs      map[string]chan Message
	svcCtx    context.Context
	svcCancel context.CancelFunc
	mu        sync.RWMutex
}

// NewConsumerChannel builds a channel-based consumer from a validated Config.
func NewConsumerChannel(cfg Config, cc ConsumerConfig) (*ConsumerChannel, error) {
	inner, err := NewConsumer(cfg, cc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerChannel{
		inner:     inner.(*kafkaConsumer),
		subs:      make(map[string]chan Message),
		svcCtx:    ctx,
		svcCancel: cancel,
	}, nil
}

// Subscribe registers a channel to receive messages from topic.
//
// The channel is buffered (capacity 100) to absorb short bursts, but the
// forwarding goroutine BLOCKS on send when the buffer is full so no message
// is silently dropped: the inner consumer will not commit the offset until
// the message has been accepted (at-least-once).
//
// On Close() or ctx cancellation, the forwarding goroutine unblocks via the
// inner consumer's shutdown, then closes and removes the subscription
// channel. Close() never closes a channel that may still be receiving a
// send, eliminating the send-on-closed-channel panic.
func (c *ConsumerChannel) Subscribe(ctx context.Context, topic, groupID string) error {
	c.mu.Lock()
	ch := make(chan Message, 100)
	c.subs[topic] = ch
	c.mu.Unlock()

	// groupID is fixed at construction (NewConsumer) — the parameter is kept
	// for API stability with future multi-group consumers but currently unused.
	_ = groupID

	go func() {
		handler := func(ctx context.Context, msg Message) error {
			c.mu.RLock()
			sub, ok := c.subs[topic]
			c.mu.RUnlock()
			if !ok {
				// Subscription was removed (Close). Return an error so the
				// inner consumer does NOT commit the offset; the message will
				// be redelivered on next start.
				return ErrSubscriptionRemoved
			}
			// Blocking send preserves at-least-once: we only ack (return
			// nil → inner commits offset) after the runner has accepted the
			// message.
			select {
			case sub <- msg:
				return nil
			case <-ctx.Done():
				// Caller cancelled (or Close cancelled svcCtx). Do not commit.
				return ctx.Err()
			}
		}

		backoff := 500 * time.Millisecond
		for {
			if err := c.inner.Consume(ctx, handler); err != nil {
				// ctx cancelled — clean exit
				if ctx.Err() != nil || c.svcCtx.Err() != nil {
					break
				}
				// Subscription was removed mid-consume
				if errors.Is(err, ErrSubscriptionRemoved) {
					break
				}
				// Transient broker error — back off and retry
				select {
				case <-ctx.Done():
					break
				case <-c.svcCtx.Done():
					break
				case <-time.After(backoff):
				}
				if backoff < 10*time.Second {
					backoff *= 2
				}
				continue
			}
			// inner.Consume returned nil — ctx cancelled.
			break
		}

		// Consume loop exited: close and remove the subscription channel.
		// Safe because the handler is no longer being invoked (inner.Consume
		// has returned), so no concurrent sender can hit the close.
		c.mu.Lock()
		if live, ok := c.subs[topic]; ok && live == ch {
			close(live)
			delete(c.subs, topic)
		}
		c.mu.Unlock()
	}()
	return nil
}

// Messages returns the message channel for the first subscribed topic.
// The channel is closed when the consumer is closed or the context is cancelled.
// Returns nil if no subscription exists yet.
//
// NOTE: this returns the FIRST channel found via map iteration, which is
// non-deterministic. Callers that subscribe to more than one topic should
// track channels explicitly via Subscribe+Messages refactor. The current
// single-topic-per-agent model is unaffected.
func (c *ConsumerChannel) Messages() <-chan Message {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ch := range c.subs {
		return ch
	}
	return nil
}

// MarkAsProcessed commits the offset for the given message.
// It is a no-op for ConsumerChannel since offset commit is handled
// internally by the inner consumer after each handler call succeeds.
//
// Callers SHOULD keep calling this for API stability with other Consumer
// implementations, but it has no effect here.
func (c *ConsumerChannel) MarkAsProcessed(msg Message) {
	_ = msg
}

// Close cancels all in-flight subscriptions and closes the inner consumer.
// It does NOT directly close subscription channels: each Subscribe goroutine
// closes its own channel on exit to avoid the send-on-closed-channel race.
func (c *ConsumerChannel) Close() error {
	c.mu.RLock()
	hadSubs := len(c.subs) > 0
	c.mu.RUnlock()
	if hadSubs {
		// Cancel the service context to unblock any goroutine stuck on a
		// blocking send. The handler returns ctx.Err() so the inner consumer
		// does not commit the in-flight message and it will be redelivered.
		c.svcCancel()
	}
	if err := c.inner.Close(); err != nil {
		return fmt.Errorf("bus: close inner consumer: %w", err)
	}
	return nil
}

// --- compatibility shim: expose the inner kafkaConsumer's Consume for handler-based use ---

// Consume is the handler-based interface (exposes underlying consumer).
// Use Messages() channel-based API for agent runner.
func (c *ConsumerChannel) Consume(ctx context.Context, handler Handler) error {
	return c.inner.Consume(ctx, handler)
}