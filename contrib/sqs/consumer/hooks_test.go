package consumer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// ConsumerHooksSuite tests consumer-specific Hooks functionality
type ConsumerHooksSuite struct {
	suite.Suite
	ctx context.Context
	msg *SQSMessage
}

func TestConsumerHooksSuite(t *testing.T) {
	suite.Run(t, new(ConsumerHooksSuite))
}

func (s *ConsumerHooksSuite) SetupTest() {
	s.ctx = context.Background()
	s.msg = &SQSMessage{
		MessageID:     "test-msg-id",
		ReceiptHandle: "test-receipt",
		Body:          []byte(`{"test":"data"}`),
		EventType:     "test.event",
	}
}

func (s *ConsumerHooksSuite) TestNilHooks_AllCallsAreSafe() {
	// Arrange
	var nilHooks *Hooks

	// Act & Assert (no panic)
	assert.NotPanics(s.T(), func() {
		nilHooks.callOnConsumeStart(s.ctx, s.msg)
		nilHooks.callOnConsumeSuccess(s.ctx, s.msg, time.Second)
		nilHooks.callOnConsumeFailure(s.ctx, s.msg, nil, time.Second, true)
		nilHooks.callOnMessageDead(s.ctx, s.msg, nil)
		nilHooks.callOnDeleteFailure(s.ctx, s.msg, nil)
	})
}

func (s *ConsumerHooksSuite) TestEmptyHooks_AllCallsAreSafe() {
	// Arrange
	emptyHooks := &Hooks{}

	// Act & Assert (no panic)
	assert.NotPanics(s.T(), func() {
		emptyHooks.callOnConsumeStart(s.ctx, s.msg)
		emptyHooks.callOnConsumeSuccess(s.ctx, s.msg, time.Second)
		emptyHooks.callOnConsumeFailure(s.ctx, s.msg, nil, time.Second, true)
		emptyHooks.callOnMessageDead(s.ctx, s.msg, nil)
		emptyHooks.callOnDeleteFailure(s.ctx, s.msg, nil)
	})
}

func (s *ConsumerHooksSuite) TestOnConsumeStart_CallsHookWithMessage() {
	// Arrange
	var called int32
	var capturedMsg *SQSMessage
	hooks := &Hooks{
		OnConsumeStart: func(ctx context.Context, msg *SQSMessage) {
			atomic.AddInt32(&called, 1)
			capturedMsg = msg
		},
	}

	// Act
	hooks.callOnConsumeStart(s.ctx, s.msg)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Same(s.T(), s.msg, capturedMsg)
}

func (s *ConsumerHooksSuite) TestOnConsumeSuccess_CallsHookWithDuration() {
	// Arrange
	var called int32
	var capturedDuration time.Duration
	hooks := &Hooks{
		OnConsumeSuccess: func(ctx context.Context, msg *SQSMessage, duration time.Duration) {
			atomic.AddInt32(&called, 1)
			capturedDuration = duration
		},
	}
	expectedDuration := 200 * time.Millisecond

	// Act
	hooks.callOnConsumeSuccess(s.ctx, s.msg, expectedDuration)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Equal(s.T(), expectedDuration, capturedDuration)
}

func (s *ConsumerHooksSuite) TestOnConsumeFailure_CapturesAllParameters() {
	// Arrange
	var called int32
	var capturedErr error
	var capturedRetryable bool
	var capturedDuration time.Duration
	hooks := &Hooks{
		OnConsumeFailure: func(ctx context.Context, msg *SQSMessage, err error, duration time.Duration, retryable bool) {
			atomic.AddInt32(&called, 1)
			capturedErr = err
			capturedRetryable = retryable
			capturedDuration = duration
		},
	}
	expectedErr := errors.New("consume failed")
	expectedDuration := 500 * time.Millisecond

	// Act
	hooks.callOnConsumeFailure(s.ctx, s.msg, expectedErr, expectedDuration, true)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Equal(s.T(), expectedErr, capturedErr)
	assert.True(s.T(), capturedRetryable)
	assert.Equal(s.T(), expectedDuration, capturedDuration)
}

func (s *ConsumerHooksSuite) TestOnMessageDead_CallsHookWithMessage() {
	// Arrange
	var called int32
	var capturedMsg *SQSMessage
	hooks := &Hooks{
		OnMessageDead: func(ctx context.Context, msg *SQSMessage, err error) {
			atomic.AddInt32(&called, 1)
			capturedMsg = msg
		},
	}

	// Act
	hooks.callOnMessageDead(s.ctx, s.msg, nil)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Equal(s.T(), "test-msg-id", capturedMsg.MessageID)
}

func (s *ConsumerHooksSuite) TestOnDeleteFailure_CallsHookWithMessage() {
	// Arrange
	var called int32
	var capturedMsg *SQSMessage
	var capturedErr error
	hooks := &Hooks{
		OnDeleteFailure: func(ctx context.Context, msg *SQSMessage, err error) {
			atomic.AddInt32(&called, 1)
			capturedMsg = msg
			capturedErr = err
		},
	}
	deleteErr := errors.New("delete failed")

	// Act
	hooks.callOnDeleteFailure(s.ctx, s.msg, deleteErr)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Same(s.T(), s.msg, capturedMsg)
	assert.Equal(s.T(), deleteErr, capturedErr)
}

func (s *ConsumerHooksSuite) TestHook_PanicsAreRecoveredAndDoNotCrash() {
	// Test all hooks recover from panics
	hooks := &Hooks{
		OnConsumeStart: func(ctx context.Context, msg *SQSMessage) {
			panic("OnConsumeStart panic")
		},
		OnConsumeSuccess: func(ctx context.Context, msg *SQSMessage, duration time.Duration) {
			panic("OnConsumeSuccess panic")
		},
		OnConsumeFailure: func(ctx context.Context, msg *SQSMessage, err error, duration time.Duration, retryable bool) {
			panic("OnConsumeFailure panic")
		},
		OnMessageDead: func(ctx context.Context, msg *SQSMessage, err error) {
			panic("OnMessageDead panic")
		},
		OnDeleteFailure: func(ctx context.Context, msg *SQSMessage, err error) {
			panic("OnDeleteFailure panic")
		},
	}

	// Act & Assert - none of these should panic
	assert.NotPanics(s.T(), func() {
		hooks.callOnConsumeStart(s.ctx, s.msg)
	})
	assert.NotPanics(s.T(), func() {
		hooks.callOnConsumeSuccess(s.ctx, s.msg, time.Second)
	})
	assert.NotPanics(s.T(), func() {
		hooks.callOnConsumeFailure(s.ctx, s.msg, nil, time.Second, true)
	})
	assert.NotPanics(s.T(), func() {
		hooks.callOnMessageDead(s.ctx, s.msg, nil)
	})
	assert.NotPanics(s.T(), func() {
		hooks.callOnDeleteFailure(s.ctx, s.msg, nil)
	})
}
