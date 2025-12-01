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
		Topic:     "test.topic",
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

// TopicRouterSuite tests TopicRouter functionality
type TopicRouterSuite struct {
	suite.Suite
}

func TestTopicRouterSuite(t *testing.T) {
	suite.Run(t, new(TopicRouterSuite))
}

func (s *TopicRouterSuite) TestNewTopicRouter_CreatesEmptyRouter() {
	// Arrange & Act
	router := NewTopicRouter()

	// Assert
	assert.NotNil(s.T(), router)
	assert.NotNil(s.T(), router.handlers)
	assert.Nil(s.T(), router.fallback)
}

func (s *TopicRouterSuite) TestRegister_AddsHandler() {
	// Arrange
	router := NewTopicRouter()
	var handlerCalled bool
	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		handlerCalled = true
		return nil
	})

	// Act
	router.Register("test.topic", handler)
	msg := &SQSMessage{Topic: "test.topic"}
	err := router.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), handlerCalled)
}

func (s *TopicRouterSuite) TestRegisterFunc_AddsHandlerFunction() {
	// Arrange
	router := NewTopicRouter()
	var handlerCalled bool

	// Act
	router.RegisterFunc("test.topic", func(ctx context.Context, msg *SQSMessage) error {
		handlerCalled = true
		return nil
	})
	msg := &SQSMessage{Topic: "test.topic"}
	err := router.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), handlerCalled)
}

func (s *TopicRouterSuite) TestHandle_RoutesToCorrectHandler() {
	// Arrange
	router := NewTopicRouter()
	var handledTopics []string

	router.RegisterFunc("topic.a", func(ctx context.Context, msg *SQSMessage) error {
		handledTopics = append(handledTopics, "a")
		return nil
	})
	router.RegisterFunc("topic.b", func(ctx context.Context, msg *SQSMessage) error {
		handledTopics = append(handledTopics, "b")
		return nil
	})

	// Act
	_ = router.Handle(context.Background(), &SQSMessage{Topic: "topic.b"})
	_ = router.Handle(context.Background(), &SQSMessage{Topic: "topic.a"})
	_ = router.Handle(context.Background(), &SQSMessage{Topic: "topic.b"})

	// Assert
	assert.Equal(s.T(), []string{"b", "a", "b"}, handledTopics)
}

func (s *TopicRouterSuite) TestHandle_UseFallbackForUnknownTopic() {
	// Arrange
	router := NewTopicRouter()
	var fallbackCalled bool

	router.SetFallback(HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		fallbackCalled = true
		return nil
	}))

	// Act
	err := router.Handle(context.Background(), &SQSMessage{Topic: "unknown.topic"})

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), fallbackCalled)
}

func (s *TopicRouterSuite) TestHandle_ReturnsErrorForUnknownTopicByDefault() {
	// Arrange
	router := NewTopicRouter()
	router.RegisterFunc("known.topic", func(ctx context.Context, msg *SQSMessage) error {
		return errors.New("should not be called")
	})

	// Act
	err := router.Handle(context.Background(), &SQSMessage{Topic: "unknown.topic"})

	// Assert
	assert.Error(s.T(), err)
	assert.ErrorIs(s.T(), err, ErrUnknownTopic)
	assert.Contains(s.T(), err.Error(), "unknown.topic")
}

func (s *TopicRouterSuite) TestHandle_IgnoresUnknownTopicWhenConfigured() {
	// Arrange
	router := NewTopicRouter()
	router.SetUnknownTopicBehavior(UnknownTopicIgnore)
	router.RegisterFunc("known.topic", func(ctx context.Context, msg *SQSMessage) error {
		return errors.New("should not be called")
	})

	// Act
	err := router.Handle(context.Background(), &SQSMessage{Topic: "unknown.topic"})

	// Assert
	assert.NoError(s.T(), err) // Silently ignores when configured
}

func (s *TopicRouterSuite) TestHandle_PropagatesHandlerError() {
	// Arrange
	router := NewTopicRouter()
	expectedErr := errors.New("handler error")

	router.RegisterFunc("test.topic", func(ctx context.Context, msg *SQSMessage) error {
		return expectedErr
	})

	// Act
	err := router.Handle(context.Background(), &SQSMessage{Topic: "test.topic"})

	// Assert
	assert.ErrorIs(s.T(), err, expectedErr)
}

func (s *TopicRouterSuite) TestHandle_PropagatesFallbackError() {
	// Arrange
	router := NewTopicRouter()
	expectedErr := errors.New("fallback error")

	router.SetFallback(HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		return expectedErr
	}))

	// Act
	err := router.Handle(context.Background(), &SQSMessage{Topic: "unknown.topic"})

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
	var receivedTopic string

	handler := NewTypedHandler(func(ctx context.Context, topic string, payload TestPayload) error {
		receivedTopic = topic
		receivedPayload = payload
		return nil
	})

	msg := &SQSMessage{
		Topic: "test.topic",
		Body:  json.RawMessage(`{"name":"test","value":42}`),
	}

	// Act
	err := handler.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "test.topic", receivedTopic)
	assert.Equal(s.T(), "test", receivedPayload.Name)
	assert.Equal(s.T(), 42, receivedPayload.Value)
}

func (s *TypedHandlerSuite) TestTypedHandler_ReturnsErrorOnInvalidJSON() {
	// Arrange
	handler := NewTypedHandler(func(ctx context.Context, topic string, payload TestPayload) error {
		return nil
	})

	msg := &SQSMessage{
		Topic: "test.topic",
		Body:  json.RawMessage(`{invalid json`),
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
	handler := NewTypedHandler(func(ctx context.Context, topic string, payload TestPayload) error {
		return expectedErr
	})

	msg := &SQSMessage{
		Topic: "test.topic",
		Body:  json.RawMessage(`{"name":"test","value":42}`),
	}

	// Act
	err := handler.Handle(context.Background(), msg)

	// Assert
	assert.ErrorIs(s.T(), err, expectedErr)
}

func (s *TypedHandlerSuite) TestTypedHandler_WorksWithSlicePayload() {
	// Arrange
	var receivedPayload []string

	handler := NewTypedHandler(func(ctx context.Context, topic string, payload []string) error {
		receivedPayload = payload
		return nil
	})

	msg := &SQSMessage{
		Topic: "test.topic",
		Body:  json.RawMessage(`["a","b","c"]`),
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

	handler := NewTypedHandler(func(ctx context.Context, topic string, payload map[string]interface{}) error {
		receivedPayload = payload
		return nil
	})

	msg := &SQSMessage{
		Topic: "test.topic",
		Body:  json.RawMessage(`{"key":"value","number":123}`),
	}

	// Act
	err := handler.Handle(context.Background(), msg)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "value", receivedPayload["key"])
	assert.Equal(s.T(), float64(123), receivedPayload["number"])
}
