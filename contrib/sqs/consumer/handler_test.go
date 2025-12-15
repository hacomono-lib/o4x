package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// HandlerFuncSuite tests HandlerFunc adapter
type HandlerFuncSuite struct {
	suite.Suite
}

func TestHandlerFuncSuite(t *testing.T) {
	suite.Run(t, new(HandlerFuncSuite))
}

func (s *HandlerFuncSuite) TestHandlerFunc_CallsUnderlyingFunction() {
	// Arrange
	var calledWith *SQSMessage
	fn := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		calledWith = msg
		return nil
	})

	msg := &SQSMessage{
		MessageID: "test-id",
		EventType: "test.event",
		Body:      json.RawMessage(`{"key":"value"}`),
	}

	// Act
	err := fn.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), msg, calledWith)
}

func (s *HandlerFuncSuite) TestHandlerFunc_PropagatesError() {
	// Arrange
	expectedErr := errors.New("handler error")
	fn := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		return expectedErr
	})

	msg := &SQSMessage{
		MessageID: "test-id",
	}

	// Act
	err := fn.Handle(context.Background(), msg)

	// Assert
	assert.ErrorIs(s.T(), err, expectedErr)
}

// EventTypeRouterSuite tests EventTypeRouter functionality
type EventTypeRouterSuite struct {
	suite.Suite
}

func TestEventTypeRouterSuite(t *testing.T) {
	suite.Run(t, new(EventTypeRouterSuite))
}

func (s *EventTypeRouterSuite) TestNewEventTypeRouter_CreatesEmptyRouter() {
	// Arrange & Act
	router := NewEventTypeRouter()

	// Assert
	assert.NotNil(s.T(), router)
	assert.NotNil(s.T(), router.handlers)
	assert.Nil(s.T(), router.fallback)
}

func (s *EventTypeRouterSuite) TestRegister_AddsHandler() {
	// Arrange
	router := NewEventTypeRouter()
	var handlerCalled bool
	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		handlerCalled = true
		return nil
	})

	// Act
	router.Register("test.event", handler)
	msg := &SQSMessage{EventType: "test.event"}
	err := router.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), handlerCalled)
}

func (s *EventTypeRouterSuite) TestRegisterFunc_AddsHandlerFunction() {
	// Arrange
	router := NewEventTypeRouter()
	var handlerCalled bool

	// Act
	router.RegisterFunc("test.event", func(ctx context.Context, msg *SQSMessage) error {
		handlerCalled = true
		return nil
	})
	msg := &SQSMessage{EventType: "test.event"}
	err := router.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), handlerCalled)
}

func (s *EventTypeRouterSuite) TestHandle_RoutesToCorrectHandler() {
	// Arrange
	router := NewEventTypeRouter()
	var handledEventTypes []string

	router.RegisterFunc("event.a", func(ctx context.Context, msg *SQSMessage) error {
		handledEventTypes = append(handledEventTypes, "a")
		return nil
	})
	router.RegisterFunc("event.b", func(ctx context.Context, msg *SQSMessage) error {
		handledEventTypes = append(handledEventTypes, "b")
		return nil
	})

	// Act
	_ = router.Handle(context.Background(), &SQSMessage{EventType: "event.b"})
	_ = router.Handle(context.Background(), &SQSMessage{EventType: "event.a"})
	_ = router.Handle(context.Background(), &SQSMessage{EventType: "event.b"})

	// Assert
	assert.Equal(s.T(), []string{"b", "a", "b"}, handledEventTypes)
}

func (s *EventTypeRouterSuite) TestHandle_UseFallbackForUnknownEventType() {
	// Arrange
	router := NewEventTypeRouter()
	var fallbackCalled bool

	router.SetFallback(HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		fallbackCalled = true
		return nil
	}))

	// Act
	err := router.Handle(context.Background(), &SQSMessage{EventType: "unknown.event"})

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), fallbackCalled)
}

func (s *EventTypeRouterSuite) TestHandle_ReturnsErrorForUnknownEventTypeByDefault() {
	// Arrange
	router := NewEventTypeRouter()
	router.RegisterFunc("known.event", func(ctx context.Context, msg *SQSMessage) error {
		return errors.New("should not be called")
	})

	// Act
	err := router.Handle(context.Background(), &SQSMessage{EventType: "unknown.event"})

	// Assert
	assert.Error(s.T(), err)
	assert.ErrorIs(s.T(), err, ErrUnknownEventType)
	assert.Contains(s.T(), err.Error(), "unknown.event")
}

func (s *EventTypeRouterSuite) TestHandle_IgnoresUnknownEventTypeWhenConfigured() {
	// Arrange
	router := NewEventTypeRouter()
	router.SetUnknownEventTypeBehavior(UnknownEventTypeIgnore)
	router.RegisterFunc("known.event", func(ctx context.Context, msg *SQSMessage) error {
		return errors.New("should not be called")
	})

	// Act
	err := router.Handle(context.Background(), &SQSMessage{EventType: "unknown.event"})

	// Assert
	assert.NoError(s.T(), err) // Silently ignores when configured
}

func (s *EventTypeRouterSuite) TestHandle_PropagatesHandlerError() {
	// Arrange
	router := NewEventTypeRouter()
	expectedErr := errors.New("handler error")

	router.RegisterFunc("test.event", func(ctx context.Context, msg *SQSMessage) error {
		return expectedErr
	})

	// Act
	err := router.Handle(context.Background(), &SQSMessage{EventType: "test.event"})

	// Assert
	assert.ErrorIs(s.T(), err, expectedErr)
}

func (s *EventTypeRouterSuite) TestHandle_PropagatesFallbackError() {
	// Arrange
	router := NewEventTypeRouter()
	expectedErr := errors.New("fallback error")

	router.SetFallback(HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		return expectedErr
	}))

	// Act
	err := router.Handle(context.Background(), &SQSMessage{EventType: "unknown.event"})

	// Assert
	assert.ErrorIs(s.T(), err, expectedErr)
}

// TypedHandlerSuite tests TypedHandler functionality
type TypedHandlerSuite struct {
	suite.Suite
}

func TestTypedHandlerSuite(t *testing.T) {
	suite.Run(t, new(TypedHandlerSuite))
}

type TestPayload struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func (s *TypedHandlerSuite) TestTypedHandler_UnmarshalsPayload() {
	// Arrange
	var receivedPayload TestPayload
	var receivedMsg *SQSMessage

	handler := NewTypedHandler(func(ctx context.Context, msg *SQSMessage, payload TestPayload) error {
		receivedMsg = msg
		receivedPayload = payload
		return nil
	})

	msg := &SQSMessage{
		MessageID: "test-msg-id",
		EventType: "test.event",
		Body:      json.RawMessage(`{"name":"test","value":42}`),
	}

	// Act
	err := handler.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "test.event", receivedMsg.EventType)
	assert.Equal(s.T(), "test-msg-id", receivedMsg.MessageID)
	assert.Equal(s.T(), "test", receivedPayload.Name)
	assert.Equal(s.T(), 42, receivedPayload.Value)
}

func (s *TypedHandlerSuite) TestTypedHandler_ReturnsErrorOnInvalidJSON() {
	// Arrange
	handler := NewTypedHandler(func(ctx context.Context, msg *SQSMessage, payload TestPayload) error {
		return nil
	})

	msg := &SQSMessage{
		EventType: "test.event",
		Body:      json.RawMessage(`{invalid json`),
	}

	// Act
	err := handler.Handle(context.Background(), msg)

	// Assert
	assert.Error(s.T(), err)
	// JSON errors should be wrapped as PermanentError (non-retryable)
	var permErr interface{ IsRetryable() bool }
	if assert.ErrorAs(s.T(), err, &permErr) {
		assert.False(s.T(), permErr.IsRetryable(), "JSON unmarshal errors should not be retryable")
	}
}

func (s *TypedHandlerSuite) TestTypedHandler_PropagatesHandlerError() {
	// Arrange
	expectedErr := errors.New("handler error")
	handler := NewTypedHandler(func(ctx context.Context, msg *SQSMessage, payload TestPayload) error {
		return expectedErr
	})

	msg := &SQSMessage{
		EventType: "test.event",
		Body:      json.RawMessage(`{"name":"test","value":42}`),
	}

	// Act
	err := handler.Handle(context.Background(), msg)

	// Assert
	assert.ErrorIs(s.T(), err, expectedErr)
}

func (s *TypedHandlerSuite) TestTypedHandler_WorksWithSlicePayload() {
	// Arrange
	var receivedPayload []string

	handler := NewTypedHandler(func(ctx context.Context, msg *SQSMessage, payload []string) error {
		receivedPayload = payload
		return nil
	})

	msg := &SQSMessage{
		EventType: "test.event",
		Body:      json.RawMessage(`["a","b","c"]`),
	}

	// Act
	err := handler.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), []string{"a", "b", "c"}, receivedPayload)
}

func (s *TypedHandlerSuite) TestTypedHandler_WorksWithMapPayload() {
	// Arrange
	var receivedPayload map[string]interface{}

	handler := NewTypedHandler(func(ctx context.Context, msg *SQSMessage, payload map[string]interface{}) error {
		receivedPayload = payload
		return nil
	})

	msg := &SQSMessage{
		EventType: "test.event",
		Body:      json.RawMessage(`{"key":"value","number":123}`),
	}

	// Act
	err := handler.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "value", receivedPayload["key"])
	assert.Equal(s.T(), float64(123), receivedPayload["number"])
}

// TestEventTypeRouter_EventTypes tests the EventTypes() method
func TestEventTypeRouter_EventTypes(t *testing.T) {
	router := NewEventTypeRouter()

	// Register some handlers
	router.Register("order.created", HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		return nil
	}))
	router.Register("user.registered", HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		return nil
	}))

	eventTypes := router.EventTypes()
	if len(eventTypes) != 2 {
		t.Errorf("expected 2 event types, got %d", len(eventTypes))
	}

	// Check that both event types are present
	hasOrder := false
	hasUser := false
	for _, et := range eventTypes {
		if et == "order.created" {
			hasOrder = true
		}
		if et == "user.registered" {
			hasUser = true
		}
	}
	if !hasOrder || !hasUser {
		t.Error("missing expected event types")
	}
}
