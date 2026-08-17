package bus

import (
	"context"
	"sync"
)

// ConsumerChannel is a Consumer that delivers messages via a Go channel.
// It wraps a kafkaConsumer and forwards messages to a channel per topic.
type ConsumerChannel struct {
	inner *kafkaConsumer
	subs  map[string]chan Message // keyed by topic (bidirectional so we can close)
	mu    sync.RWMutex
}

// NewConsumerChannel builds a channel-based consumer from a validated Config.
func NewConsumerChannel(cfg Config, cc ConsumerConfig) (*ConsumerChannel, error) {
	inner, err := NewConsumer(cfg, cc)
	if err != nil {
		return nil, err
	}
	return &ConsumerChannel{
		inner: inner.(*kafkaConsumer),
		subs:  make(map[string]chan Message),
	}, nil
}

// Subscribe registers a channel to receive messages from topic.
// The channel must be buffered; messages that cannot be sent without blocking
// are dropped (non-blocking delivery to avoid head-of-line blocking).
func (c *ConsumerChannel) Subscribe(ctx context.Context, topic, groupID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan Message, 100) // buffered channel
	c.subs[topic] = ch

	// Start a goroutine that fetches from the inner consumer and forwards.
	go func() {
		handler := func(ctx context.Context, msg Message) error {
			c.mu.RLock()
			ch, ok := c.subs[topic]
			c.mu.RUnlock()
			if !ok {
				return nil
			}
			// Non-blocking send: drop if channel is full.
			select {
			case ch <- msg:
			default:
			}
			return nil
		}
		if err := c.inner.Consume(ctx, handler); err != nil {
			// Consume loop exited; close the channel.
			c.mu.Lock()
			if ch, ok := c.subs[topic]; ok {
				close(ch)
				delete(c.subs, topic)
			}
			c.mu.Unlock()
		}
	}()
	return nil
}

// Messages returns the message channel for the subscribed topic.
// The channel is closed when the consumer is closed or the context is cancelled.
func (c *ConsumerChannel) Messages() <-chan Message {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ch := range c.subs {
		return ch
	}
	// No subscription yet; return a closed channel (blocks forever).
	closed := make(chan Message)
	close(closed)
	return closed
}

// MarkAsProcessed commits the offset for the given message.
// It is a no-op for ConsumerChannel since offset commit is handled
// internally by the inner consumer after each handler call succeeds.
func (c *ConsumerChannel) MarkAsProcessed(msg Message) {
	// Offset commit is synchronous in the inner consumer; nothing to do here.
}

// Close closes all topic channels and the inner consumer.
func (c *ConsumerChannel) Close() error {
	c.mu.Lock()
	for topic, ch := range c.subs {
		close(ch)
		delete(c.subs, topic)
	}
	c.mu.Unlock()
	return c.inner.Close()
}

// --- compatibility shim: expose the inner kafkaConsumer's Consume for handler-based use ---

// Consume is the handler-based interface (exposes underlying consumer).
// Use Messages() channel-based API for agent runner.
func (c *ConsumerChannel) Consume(ctx context.Context, handler Handler) error {
	return c.inner.Consume(ctx, handler)
}
