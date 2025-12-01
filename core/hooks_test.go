package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// HooksSuite tests Hooks functionality
type HooksSuite struct {
	suite.Suite
	ctx context.Context
	msg *Outbox
}

func TestHooksSuite(t *testing.T) {
	suite.Run(t, new(HooksSuite))
}

func (s *HooksSuite) SetupTest() {
	s.ctx = context.Background()
	s.msg = &Outbox{ID: "test-id", Topic: "test-topic"}
}

func (s *HooksSuite) TestNilHooks_AllCallsAreSafe() {
	// Arrange
	var nilHooks *Hooks

	// Act & Assert (no panic)
	assert.NotPanics(s.T(), func() {
		nilHooks.callOnPublishStart(s.ctx, s.msg)
		nilHooks.callOnPublishSuccess(s.ctx, s.msg, time.Second)
		nilHooks.callOnPublishFailure(s.ctx, s.msg, nil, time.Second, true)
		nilHooks.callOnMessageDead(s.ctx, s.msg, nil)
		nilHooks.callOnBatchPublishStart(s.ctx, []*Outbox{s.msg})
		nilHooks.callOnBatchPublishComplete(s.ctx, 1, 0, time.Second)
	})
}

func (s *HooksSuite) TestEmptyHooks_AllCallsAreSafe() {
	// Arrange
	emptyHooks := &Hooks{}

	// Act & Assert (no panic)
	assert.NotPanics(s.T(), func() {
		emptyHooks.callOnPublishStart(s.ctx, s.msg)
		emptyHooks.callOnPublishSuccess(s.ctx, s.msg, time.Second)
		emptyHooks.callOnPublishFailure(s.ctx, s.msg, nil, time.Second, true)
		emptyHooks.callOnMessageDead(s.ctx, s.msg, nil)
		emptyHooks.callOnBatchPublishStart(s.ctx, []*Outbox{s.msg})
		emptyHooks.callOnBatchPublishComplete(s.ctx, 1, 0, time.Second)
	})
}

func (s *HooksSuite) TestOnPublishStart_CallsHookWithMessage() {
	// Arrange
	var called int32
	var capturedMsg *Outbox
	hooks := &Hooks{
		OnPublishStart: func(ctx context.Context, msg *Outbox) {
			atomic.AddInt32(&called, 1)
			capturedMsg = msg
		},
	}

	// Act
	hooks.callOnPublishStart(s.ctx, s.msg)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Same(s.T(), s.msg, capturedMsg)
}

func (s *HooksSuite) TestOnPublishSuccess_CallsHookWithDuration() {
	// Arrange
	var called int32
	var capturedDuration time.Duration
	hooks := &Hooks{
		OnPublishSuccess: func(ctx context.Context, msg *Outbox, duration time.Duration) {
			atomic.AddInt32(&called, 1)
			capturedDuration = duration
		},
	}
	expectedDuration := 100 * time.Millisecond

	// Act
	hooks.callOnPublishSuccess(s.ctx, s.msg, expectedDuration)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Equal(s.T(), expectedDuration, capturedDuration)
}

func (s *HooksSuite) TestOnPublishFailure_CapturesAllParameters() {
	// Arrange
	var capturedErr error
	var capturedRetryable bool
	var capturedDuration time.Duration
	hooks := &Hooks{
		OnPublishFailure: func(ctx context.Context, msg *Outbox, err error, duration time.Duration, retryable bool) {
			capturedErr = err
			capturedRetryable = retryable
			capturedDuration = duration
		},
	}
	expectedErr := errors.New("publish failed")
	expectedDuration := 500 * time.Millisecond

	// Act
	hooks.callOnPublishFailure(s.ctx, s.msg, expectedErr, expectedDuration, true)

	// Assert
	assert.Equal(s.T(), expectedErr, capturedErr)
	assert.True(s.T(), capturedRetryable)
	assert.Equal(s.T(), expectedDuration, capturedDuration)
}

func (s *HooksSuite) TestOnMessageDead_CallsHookWithMessage() {
	// Arrange
	var called int32
	var capturedMsg *Outbox
	hooks := &Hooks{
		OnMessageDead: func(ctx context.Context, msg *Outbox, err error) {
			atomic.AddInt32(&called, 1)
			capturedMsg = msg
		},
	}
	deadMsg := &Outbox{ID: "dead-msg", Topic: "test-topic"}

	// Act
	hooks.callOnMessageDead(s.ctx, deadMsg, nil)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Equal(s.T(), "dead-msg", capturedMsg.ID)
}

func (s *HooksSuite) TestOnBatchPublishStart_CallsHookWithMessages() {
	// Arrange
	var called int32
	var capturedCount int
	hooks := &Hooks{
		OnBatchPublishStart: func(ctx context.Context, msgs []*Outbox) {
			atomic.AddInt32(&called, 1)
			capturedCount = len(msgs)
		},
	}
	msgs := []*Outbox{
		{ID: "msg-1"},
		{ID: "msg-2"},
		{ID: "msg-3"},
	}

	// Act
	hooks.callOnBatchPublishStart(s.ctx, msgs)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Equal(s.T(), 3, capturedCount)
}

func (s *HooksSuite) TestOnBatchPublishComplete_CapturesAllParameters() {
	// Arrange
	var called int32
	var capturedSuccess, capturedFailure int
	var capturedDuration time.Duration
	hooks := &Hooks{
		OnBatchPublishComplete: func(ctx context.Context, successCount, failureCount int, duration time.Duration) {
			atomic.AddInt32(&called, 1)
			capturedSuccess = successCount
			capturedFailure = failureCount
			capturedDuration = duration
		},
	}
	expectedDuration := 500 * time.Millisecond

	// Act
	hooks.callOnBatchPublishComplete(s.ctx, 8, 2, expectedDuration)

	// Assert
	assert.Equal(s.T(), int32(1), atomic.LoadInt32(&called))
	assert.Equal(s.T(), 8, capturedSuccess)
	assert.Equal(s.T(), 2, capturedFailure)
	assert.Equal(s.T(), expectedDuration, capturedDuration)
}
