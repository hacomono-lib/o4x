package core

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// WorkerSuite tests Worker functionality
type WorkerSuite struct {
	suite.Suite
	repo      *MockOutboxRepository
	publisher *MockPublisher
	logger    *slog.Logger
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, new(WorkerSuite))
}

func (s *WorkerSuite) SetupTest() {
	s.repo = NewMockOutboxRepository()
	s.publisher = NewMockPublisher()
	s.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (s *WorkerSuite) TestNewWorker_CreatesWorkerWithCorrectConfig() {
	// Arrange
	pollInterval := 100 * time.Millisecond
	maxPollInterval := 3200 * time.Millisecond

	// Act
	worker := NewWorker(1, s.repo, s.publisher, s.logger, nil, pollInterval, maxPollInterval, 10*time.Second)

	// Assert
	assert.NotNil(s.T(), worker)
	assert.Equal(s.T(), 1, worker.id)
	assert.Equal(s.T(), pollInterval, worker.pollInterval)
	assert.Equal(s.T(), maxPollInterval, worker.maxPollInterval)
}

func (s *WorkerSuite) TestWorker_ProcessesMessageSuccessfully() {
	// Arrange
	msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	worker := NewWorker(0, s.repo, s.publisher, s.logger, nil, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	// Assert
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusPublished, updatedMsg.Status)
	assert.Greater(s.T(), len(s.publisher.PublishCalls), 0)
}

func (s *WorkerSuite) TestWorker_HandlesPublishFailure() {
	// Arrange
	msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return errors.New("publish failed")
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, nil, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	// Assert
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusFailed, updatedMsg.Status)
	assert.Equal(s.T(), 1, updatedMsg.RetryCount)
}

func (s *WorkerSuite) TestWorker_MarksMessageDeadAfterMaxRetries() {
	// Arrange
	msg := createTestOutboxWithRetry("test.topic", map[string]string{"key": "value"}, 2, 3)
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return errors.New("publish failed")
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, nil, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	// Assert
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusDead, updatedMsg.Status)
}

func (s *WorkerSuite) TestWorker_MarksMessageDeadOnPermanentError() {
	// Arrange
	msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return NewPermanentError(errors.New("validation failed"))
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, nil, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	// Assert
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusDead, updatedMsg.Status)
}

func (s *WorkerSuite) TestWorker_CallsOnPublishStartHook() {
	// Arrange
	msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	hookCalled := make(chan *Outbox, 1)

	hooks := &Hooks{
		OnPublishStart: func(ctx context.Context, m *Outbox) {
			select {
			case hookCalled <- m:
			default:
			}
		},
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, hooks, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)

	// Assert
	select {
	case hookedMsg := <-hookCalled:
		assert.Equal(s.T(), msg.ID, hookedMsg.ID)
	case <-time.After(100 * time.Millisecond):
		s.T().Fatal("hook was not called")
	}
}

func (s *WorkerSuite) TestWorker_CallsOnPublishSuccessHook() {
	// Arrange
	msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	hookCalled := make(chan time.Duration, 1)

	hooks := &Hooks{
		OnPublishSuccess: func(ctx context.Context, m *Outbox, d time.Duration) {
			select {
			case hookCalled <- d:
			default:
			}
		},
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, hooks, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)

	// Assert
	select {
	case duration := <-hookCalled:
		assert.Greater(s.T(), duration, time.Duration(0))
	case <-time.After(100 * time.Millisecond):
		s.T().Fatal("hook was not called")
	}
}

func (s *WorkerSuite) TestWorker_CallsOnPublishFailureHook() {
	// Arrange
	msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return errors.New("publish failed")
	}

	type failureResult struct {
		err       error
		retryable bool
	}
	hookCalled := make(chan failureResult, 1)

	hooks := &Hooks{
		OnPublishFailure: func(ctx context.Context, m *Outbox, err error, d time.Duration, retryable bool) {
			select {
			case hookCalled <- failureResult{err: err, retryable: retryable}:
			default:
			}
		},
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, hooks, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)

	// Assert
	select {
	case result := <-hookCalled:
		assert.NotNil(s.T(), result.err)
		assert.True(s.T(), result.retryable)
	case <-time.After(100 * time.Millisecond):
		s.T().Fatal("hook was not called")
	}
}

func (s *WorkerSuite) TestWorker_CallsOnMessageDeadHook() {
	// Arrange
	msg := createTestOutboxWithRetry("test.topic", map[string]string{"key": "value"}, 2, 3)
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return errors.New("publish failed")
	}

	hookCalled := make(chan *Outbox, 1)

	hooks := &Hooks{
		OnMessageDead: func(ctx context.Context, m *Outbox, err error) {
			select {
			case hookCalled <- m:
			default:
			}
		},
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, hooks, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)

	// Assert
	select {
	case deadMsg := <-hookCalled:
		assert.Equal(s.T(), msg.ID, deadMsg.ID)
	case <-time.After(100 * time.Millisecond):
		s.T().Fatal("hook was not called")
	}
}

func (s *WorkerSuite) TestWorker_AdaptivePolling_IncreasesIntervalWhenNoMessages() {
	// Arrange - no messages in repo
	var fetchCount int32

	s.repo.FetchAndLockToPublishingFunc = func(ctx context.Context) (*Outbox, error) {
		atomic.AddInt32(&fetchCount, 1)
		return nil, ErrNoMessage
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, nil, 10*time.Millisecond, 80*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)
	time.Sleep(140 * time.Millisecond)

	// Assert
	// With exponential backoff: 10ms -> 20ms -> 40ms -> 80ms
	// In 140ms, we should see fewer calls than if we polled every 10ms (which would be ~14 calls)
	// Expecting roughly: 1 at 10ms, 1 at 30ms (10+20), 1 at 70ms (30+40), 1 at 150ms (70+80) = ~3-4 calls
	count := atomic.LoadInt32(&fetchCount)
	assert.Less(s.T(), count, int32(10), "should have fewer calls due to exponential backoff")
	assert.Greater(s.T(), count, int32(2), "should have at least a few calls")
}

func (s *WorkerSuite) TestWorker_AdaptivePolling_ResetsIntervalWhenMessageFound() {
	// Arrange
	var fetchCount int32
	var messageReturned bool

	s.repo.FetchAndLockToPublishingFunc = func(ctx context.Context) (*Outbox, error) {
		count := atomic.AddInt32(&fetchCount, 1)
		// Return a message on the 3rd call to reset interval
		if count == 3 && !messageReturned {
			messageReturned = true
			msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
			msg.Status = OutboxStatusPublishing // Already marked as PUBLISHING
			return msg, nil
		}
		return nil, ErrNoMessage
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, nil, 10*time.Millisecond, 160*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)
	time.Sleep(180 * time.Millisecond)

	// Assert
	count := atomic.LoadInt32(&fetchCount)
	// After finding a message, interval should reset to 10ms, so we should see more calls
	assert.Greater(s.T(), count, int32(3), "should have more calls after interval reset")
}

func (s *WorkerSuite) TestWorker_StopsOnContextCancellation() {
	// Arrange
	worker := NewWorker(0, s.repo, s.publisher, s.logger, nil, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()

	// Act
	time.Sleep(20 * time.Millisecond)
	cancel()

	// Assert - worker should stop within reasonable time
	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		s.T().Fatal("worker did not stop after context cancellation")
	}
}

func (s *WorkerSuite) TestWorker_UpdatesStatusToPublishingBeforePublish() {
	// Arrange
	msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	publishDone := make(chan OutboxStatus, 1)
	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		// Check status during publish
		status := s.repo.GetMessage(m.ID).Status
		select {
		case publishDone <- status:
		default:
		}
		return nil
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, nil, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)

	// Assert
	select {
	case statusDuringPublish := <-publishDone:
		assert.Equal(s.T(), OutboxStatusPublishing, statusDuringPublish)
	case <-time.After(100 * time.Millisecond):
		s.T().Fatal("publish was not called")
	}
}

func (s *WorkerSuite) TestWorker_HandlesTransientErrorAsRetryable() {
	// Arrange
	msg := createTestOutbox("test.topic", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return NewTransientError(errors.New("temporary failure"))
	}

	hookCalled := make(chan bool, 1)
	hooks := &Hooks{
		OnPublishFailure: func(ctx context.Context, m *Outbox, err error, d time.Duration, retryable bool) {
			select {
			case hookCalled <- retryable:
			default:
			}
		},
	}

	worker := NewWorker(0, s.repo, s.publisher, s.logger, hooks, 10*time.Millisecond, 100*time.Millisecond, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	go worker.Run(ctx)

	// Assert
	select {
	case retryable := <-hookCalled:
		assert.True(s.T(), retryable)
		// Give time for status update
		time.Sleep(10 * time.Millisecond)
		updatedMsg := s.repo.GetMessage(msg.ID)
		assert.Equal(s.T(), OutboxStatusFailed, updatedMsg.Status) // Not DEAD
	case <-time.After(100 * time.Millisecond):
		s.T().Fatal("hook was not called")
	}
}
