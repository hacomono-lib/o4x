package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hacomono-lib/o4x/core"
)

// Handler processes incoming messages
// Implementations should be idempotent
type Handler interface {
	// Handle processes a single message
	// Returns nil on success, error on failure
	Handle(ctx context.Context, msg *SQSMessage) error
}

// HandlerFunc is an adapter to allow use of ordinary functions as Handler
type HandlerFunc func(ctx context.Context, msg *SQSMessage) error

// Handle calls f(ctx, msg)
func (f HandlerFunc) Handle(ctx context.Context, msg *SQSMessage) error {
	return f(ctx, msg)
}

// ErrUnknownTopic is returned when a message has an unknown topic and no fallback is set
var ErrUnknownTopic = fmt.Errorf("unknown topic")

// UnknownTopicBehavior defines how to handle messages with unknown topics
type UnknownTopicBehavior int

const (
	// UnknownTopicError returns an error for unknown topics (default, recommended)
	UnknownTopicError UnknownTopicBehavior = iota
	// UnknownTopicIgnore silently ignores unknown topics (use with caution)
	UnknownTopicIgnore
)

// TopicRouter routes messages to handlers based on topic
type TopicRouter struct {
	handlers             map[string]Handler
	fallback             Handler
	unknownTopicBehavior UnknownTopicBehavior
}

// NewTopicRouter creates a new TopicRouter.
// By default, unknown topics return an error. Use SetUnknownTopicBehavior to change this.
func NewTopicRouter() *TopicRouter {
	return &TopicRouter{
		handlers:             make(map[string]Handler),
		unknownTopicBehavior: UnknownTopicError,
	}
}

// Register registers a handler for a specific topic
func (r *TopicRouter) Register(topic string, handler Handler) {
	r.handlers[topic] = handler
}

// RegisterFunc registers a handler function for a specific topic
func (r *TopicRouter) RegisterFunc(topic string, fn HandlerFunc) {
	r.handlers[topic] = fn
}

// SetFallback sets the fallback handler for unknown topics
func (r *TopicRouter) SetFallback(handler Handler) {
	r.fallback = handler
}

// SetUnknownTopicBehavior sets how to handle messages with unknown topics when no fallback is set.
// Default is UnknownTopicError which returns an error.
func (r *TopicRouter) SetUnknownTopicBehavior(behavior UnknownTopicBehavior) {
	r.unknownTopicBehavior = behavior
}

// Topics returns the list of registered topics
func (r *TopicRouter) Topics() []string {
	topics := make([]string, 0, len(r.handlers))
	for topic := range r.handlers {
		topics = append(topics, topic)
	}
	return topics
}

// Handle routes the message to the appropriate handler
func (r *TopicRouter) Handle(ctx context.Context, msg *SQSMessage) error {
	handler, ok := r.handlers[msg.Topic]
	if !ok {
		if r.fallback != nil {
			return r.fallback.Handle(ctx, msg)
		}
		// No handler found and no fallback
		switch r.unknownTopicBehavior {
		case UnknownTopicIgnore:
			return nil
		default:
			return fmt.Errorf("%w: %s", ErrUnknownTopic, msg.Topic)
		}
	}
	return handler.Handle(ctx, msg)
}

// TypedHandler wraps a handler that works with a specific message type
type TypedHandler[T any] struct {
	fn func(ctx context.Context, msg *SQSMessage, payload T) error
}

// NewTypedHandler creates a handler that unmarshals the payload to type T
func NewTypedHandler[T any](fn func(ctx context.Context, msg *SQSMessage, payload T) error) *TypedHandler[T] {
	return &TypedHandler[T]{fn: fn}
}

// Handle unmarshals the message body and calls the typed handler.
// JSON unmarshal errors are wrapped with PermanentError since they cannot be fixed by retrying.
func (h *TypedHandler[T]) Handle(ctx context.Context, msg *SQSMessage) error {
	var payload T
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		// JSON unmarshal errors are permanent - retrying won't fix malformed data
		return core.NewPermanentError(fmt.Errorf("failed to unmarshal message body: %w", err))
	}
	return h.fn(ctx, msg, payload)
}
