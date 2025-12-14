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

// ErrUnknownEventType is returned when a message has an unknown event_type and no fallback is set
var ErrUnknownEventType = fmt.Errorf("unknown event_type")

// UnknownEventTypeBehavior defines how to handle messages with unknown event types
type UnknownEventTypeBehavior int

const (
	// UnknownEventTypeError returns an error for unknown event types (default, recommended)
	UnknownEventTypeError UnknownEventTypeBehavior = iota
	// UnknownEventTypeIgnore silently ignores unknown event types (use with caution)
	UnknownEventTypeIgnore
)

// EventTypeRouter routes messages to handlers based on event_type
type EventTypeRouter struct {
	handlers                 map[string]Handler
	fallback                 Handler
	unknownEventTypeBehavior UnknownEventTypeBehavior
}

// NewEventTypeRouter creates a new EventTypeRouter.
// By default, unknown event types return an error. Use SetUnknownEventTypeBehavior to change this.
func NewEventTypeRouter() *EventTypeRouter {
	return &EventTypeRouter{
		handlers:                 make(map[string]Handler),
		unknownEventTypeBehavior: UnknownEventTypeError,
	}
}

// Register registers a handler for a specific event_type
func (r *EventTypeRouter) Register(eventType string, handler Handler) {
	r.handlers[eventType] = handler
}

// RegisterFunc registers a handler function for a specific event_type
func (r *EventTypeRouter) RegisterFunc(eventType string, fn HandlerFunc) {
	r.handlers[eventType] = fn
}

// SetFallback sets the fallback handler for unknown event types
func (r *EventTypeRouter) SetFallback(handler Handler) {
	r.fallback = handler
}

// SetUnknownEventTypeBehavior sets how to handle messages with unknown event types when no fallback is set.
// Default is UnknownEventTypeError which returns an error.
func (r *EventTypeRouter) SetUnknownEventTypeBehavior(behavior UnknownEventTypeBehavior) {
	r.unknownEventTypeBehavior = behavior
}

// EventTypes returns the list of registered event types
func (r *EventTypeRouter) EventTypes() []string {
	eventTypes := make([]string, 0, len(r.handlers))
	for eventType := range r.handlers {
		eventTypes = append(eventTypes, eventType)
	}
	return eventTypes
}

// Handle routes the message to the appropriate handler
func (r *EventTypeRouter) Handle(ctx context.Context, msg *SQSMessage) error {
	handler, ok := r.handlers[msg.EventType]
	if !ok {
		if r.fallback != nil {
			return r.fallback.Handle(ctx, msg)
		}
		// No handler found and no fallback
		switch r.unknownEventTypeBehavior {
		case UnknownEventTypeIgnore:
			return nil
		default:
			return fmt.Errorf("%w: %s", ErrUnknownEventType, msg.EventType)
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
