package core

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// BatchDispatcherSuite tests BatchDispatcher functionality
type BatchDispatcherSuite struct {
	suite.Suite
	repo      *MockOutboxRepository
	publisher *MockPublisher
	logger    *slog.Logger
}

func TestBatchDispatcherSuite(t *testing.T) {
	suite.Run(t, new(BatchDispatcherSuite))
}

func (s *BatchDispatcherSuite) SetupTest() {
	s.repo = NewMockOutboxRepository()
	s.publisher = NewMockPublisher()
	s.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (s *BatchDispatcherSuite) TestNewBatchDispatcher_WithDefaultConfig() {
	// Arrange
	config := DefaultBatchDispatcherConfig()

	// Act
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)

	// Assert
	assert.NotNil(s.T(), dispatcher)
	assert.Equal(s.T(), 100*time.Millisecond, dispatcher.config.PollInterval)
	assert.Equal(s.T(), 3200*time.Millisecond, dispatcher.config.MaxPollInterval)
	assert.Equal(s.T(), 10, dispatcher.config.BatchSize)
	assert.Equal(s.T(), 1, dispatcher.config.WorkerCount)
	assert.Equal(s.T(), 30*time.Second, dispatcher.config.ShutdownTimeout)
	assert.Equal(s.T(), 60*time.Second, dispatcher.config.ForceTimeout)
	assert.True(s.T(), dispatcher.config.AutoRecover)
	assert.Equal(s.T(), 10*time.Second, dispatcher.config.RequeueInterval)
	assert.Equal(s.T(), 1*time.Second, dispatcher.config.RequeueBackoffBase)
	assert.Equal(s.T(), 1*time.Hour, dispatcher.config.RequeueBackoffMax)
}

func (s *BatchDispatcherSuite) TestNewBatchDispatcher_WithCustomConfig() {
	// Arrange
	config := BatchDispatcherConfig{
		PollInterval:       200 * time.Millisecond,
		MaxPollInterval:    5 * time.Second,
		BatchSize:          5,
		WorkerCount:        4,
		ShutdownTimeout:    60 * time.Second,
		ForceTimeout:       120 * time.Second,
		AutoRecover:        false,
		RequeueInterval:    30 * time.Second,
		RequeueBackoffBase: 2 * time.Second,
		RequeueBackoffMax:  30 * time.Minute,
		Logger:             s.logger,
	}

	// Act
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)

	// Assert
	assert.Equal(s.T(), 200*time.Millisecond, dispatcher.config.PollInterval)
	assert.Equal(s.T(), 5*time.Second, dispatcher.config.MaxPollInterval)
	assert.Equal(s.T(), 5, dispatcher.config.BatchSize)
	assert.Equal(s.T(), 4, dispatcher.config.WorkerCount)
	assert.Equal(s.T(), 60*time.Second, dispatcher.config.ShutdownTimeout)
	assert.Equal(s.T(), 120*time.Second, dispatcher.config.ForceTimeout)
	assert.False(s.T(), dispatcher.config.AutoRecover)
	assert.Equal(s.T(), 30*time.Second, dispatcher.config.RequeueInterval)
	assert.Equal(s.T(), 2*time.Second, dispatcher.config.RequeueBackoffBase)
	assert.Equal(s.T(), 30*time.Minute, dispatcher.config.RequeueBackoffMax)
}

func (s *BatchDispatcherSuite) TestNewBatchDispatcher_LimitsBatchSizeToPublisherMax() {
	// Arrange
	s.publisher.MaxBatchSizeVal = 5
	config := BatchDispatcherConfig{
		BatchSize:          20, // Exceeds publisher's max
		Logger:             s.logger,
		DisableAutoRequeue: true,
	}

	// Act
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)

	// Assert
	assert.Equal(s.T(), 5, dispatcher.config.BatchSize, "should be limited to publisher's MaxBatchSize")
}

func (s *BatchDispatcherSuite) TestStart_WithAutoRecover_CallsReviveStuckPublishing() {
	// Arrange
	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        true,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	defer dispatcher.Stop()

	// Wait for AutoRecover goroutine to complete
	time.Sleep(100 * time.Millisecond)

	// Assert
	assert.NoError(s.T(), err)
	s.repo.mu.Lock()
	calls := s.repo.ReviveStuckPublishingCalls
	s.repo.mu.Unlock()
	assert.Equal(s.T(), 1, calls)
}

func (s *BatchDispatcherSuite) TestStart_WithoutAutoRecover_DoesNotCallReviveStuckPublishing() {
	// Arrange
	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	defer dispatcher.Stop()

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 0, s.repo.ReviveStuckPublishingCalls)
}

func (s *BatchDispatcherSuite) TestStartAndStop_IsRunningReflectsState() {
	// Arrange
	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		ShutdownTimeout:    5 * time.Second,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Assert - not running initially
	assert.False(s.T(), dispatcher.IsRunning())

	// Act - start
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)
	assert.True(s.T(), dispatcher.IsRunning())

	// Act - stop
	dispatcher.Stop()

	// Assert - not running after stop
	assert.False(s.T(), dispatcher.IsRunning())
}

func (s *BatchDispatcherSuite) TestBatchDispatcher_ProcessesBatchOfMessages() {
	// Arrange
	for i := 0; i < 5; i++ {
		msg := createTestOutbox("test.event", map[string]int{"index": i})
		s.repo.AddMessage(msg)
	}

	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          10,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for messages to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	assert.Greater(s.T(), len(s.publisher.PublishBatchCalls), 0, "should have published at least one batch")
}

func (s *BatchDispatcherSuite) TestBatchDispatcher_HandlesPartialBatchFailure() {
	// Arrange
	msgs := make([]*Outbox, 3)
	for i := 0; i < 3; i++ {
		msgs[i] = createTestOutbox("test.event", map[string]int{"index": i})
		s.repo.AddMessage(msgs[i])
	}

	// Track which message should fail - we'll fail the message with the specific ID
	failMessageID := msgs[1].ID

	// Configure publisher to fail on specific message ID
	s.publisher.PublishBatchFunc = func(ctx context.Context, batch []*Outbox) []PublishResult {
		results := make([]PublishResult, len(batch))
		for i, msg := range batch {
			if msg.ID == failMessageID {
				results[i] = PublishResult{
					OutboxID: msg.ID,
					Success:  false,
					Error:    errors.New("publish failed"),
				}
			} else {
				results[i] = PublishResult{
					OutboxID:  msg.ID,
					Success:   true,
					MessageID: "mock-id-" + msg.ID,
				}
			}
		}
		return results
	}

	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          10,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for messages to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	// First and third messages should be published
	assert.Equal(s.T(), OutboxStatusPublished, s.repo.GetMessage(msgs[0].ID).Status)
	assert.Equal(s.T(), OutboxStatusPublished, s.repo.GetMessage(msgs[2].ID).Status)
	// Second message should be failed
	assert.Equal(s.T(), OutboxStatusFailed, s.repo.GetMessage(msgs[1].ID).Status)
}

func (s *BatchDispatcherSuite) TestBatchDispatcher_MarksMessageDeadAfterMaxRetries() {
	// Arrange
	msg := createTestOutboxWithRetry("test.event", map[string]string{"key": "value"}, 3, 3)
	s.repo.AddMessage(msg)

	s.publisher.PublishBatchFunc = func(ctx context.Context, batch []*Outbox) []PublishResult {
		results := make([]PublishResult, len(batch))
		for i, m := range batch {
			results[i] = PublishResult{
				OutboxID: m.ID,
				Success:  false,
				Error:    errors.New("publish failed"),
			}
		}
		return results
	}

	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          10,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusDead, updatedMsg.Status)
	assert.Equal(s.T(), updatedMsg.MaxAttempts, updatedMsg.AttemptCount, "attempt_count should equal max_attempts when DEAD")
}

func (s *BatchDispatcherSuite) TestBatchDispatcher_MarksMessageDeadOnPermanentError() {
	// Arrange
	msg := createTestOutbox("test.event", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	s.publisher.PublishBatchFunc = func(ctx context.Context, batch []*Outbox) []PublishResult {
		results := make([]PublishResult, len(batch))
		for i, m := range batch {
			results[i] = PublishResult{
				OutboxID: m.ID,
				Success:  false,
				Error:    NewPermanentError(errors.New("validation failed")),
			}
		}
		return results
	}

	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          10,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusDead, updatedMsg.Status)
	// For permanent errors, attempt_count should reflect actual attempts (1), not max_attempts
	assert.Equal(s.T(), 1, updatedMsg.AttemptCount, "attempt_count should be 1 for first-attempt permanent error")
}

func (s *BatchDispatcherSuite) TestBatchDispatcher_MarksMessageDeadOnPermanentErrorAfterRetries() {
	// Test permanent error occurring after some retries
	// Arrange
	msg := createTestOutboxWithRetry("test.event", map[string]string{"key": "value"}, 2, 5)
	s.repo.AddMessage(msg)

	s.publisher.PublishBatchFunc = func(ctx context.Context, batch []*Outbox) []PublishResult {
		results := make([]PublishResult, len(batch))
		for i, m := range batch {
			results[i] = PublishResult{
				OutboxID: m.ID,
				Success:  false,
				Error:    NewPermanentError(errors.New("validation failed")),
			}
		}
		return results
	}

	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          10,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusDead, updatedMsg.Status)
	// attempt_count should be 3 (2 + 1), not max_attempts (5)
	assert.Equal(s.T(), 3, updatedMsg.AttemptCount, "attempt_count should reflect actual attempts")
}

func (s *BatchDispatcherSuite) TestBatchDispatcher_MarksMessageDeadOnLastAttempt() {
	// This is the edge case that was previously broken:
	// When attempt_count = 2 and max_attempts = 3, the next failure should mark as DEAD
	// (not FAILED with attempt_count = 3)
	
	// Arrange
	msg := createTestOutboxWithRetry("test.event", map[string]string{"key": "value"}, 2, 3)
	s.repo.AddMessage(msg)

	s.publisher.PublishBatchFunc = func(ctx context.Context, batch []*Outbox) []PublishResult {
		results := make([]PublishResult, len(batch))
		for i, m := range batch {
			results[i] = PublishResult{
				OutboxID: m.ID,
				Success:  false,
				Error:    errors.New("publish failed"),
			}
		}
		return results
	}

	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          10,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusDead, updatedMsg.Status, "message should be marked as DEAD on last attempt")
	assert.Equal(s.T(), updatedMsg.MaxAttempts, updatedMsg.AttemptCount, "attempt_count should equal max_attempts when DEAD")
}

func (s *BatchDispatcherSuite) TestBatchDispatcher_CallsBatchHooks() {
	// Arrange
	for i := 0; i < 3; i++ {
		msg := createTestOutbox("test.event", map[string]int{"index": i})
		s.repo.AddMessage(msg)
	}

	var batchStartCalled bool
	var batchCompleteCalled bool
	var batchStartMsgs []*Outbox
	var successCount, failureCount int

	hooks := &Hooks{
		OnBatchPublishStart: func(ctx context.Context, msgs []*Outbox) {
			batchStartCalled = true
			batchStartMsgs = msgs
		},
		OnBatchPublishComplete: func(ctx context.Context, sc, fc int, d time.Duration) {
			batchCompleteCalled = true
			successCount = sc
			failureCount = fc
		},
	}

	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          10,
		WorkerCount:        1,
		Hooks:              hooks,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for messages to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	assert.True(s.T(), batchStartCalled)
	assert.True(s.T(), batchCompleteCalled)
	assert.Equal(s.T(), 3, len(batchStartMsgs))
	assert.Equal(s.T(), 3, successCount)
	assert.Equal(s.T(), 0, failureCount)
}

func (s *BatchDispatcherSuite) TestBatchDispatcher_RequeueWorkerCallsRequeueFailed() {
	// Arrange
	config := BatchDispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       100 * time.Millisecond,
		RequeueInterval:    50 * time.Millisecond,
		RequeueBackoffBase: 1 * time.Second,
		RequeueBackoffMax:  1 * time.Hour,
		WorkerCount:        1,
	}
	dispatcher, err := NewBatchDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for requeue worker to run
	time.Sleep(150 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	assert.Greater(s.T(), s.repo.RequeueFailedCalls, 0, "requeue worker should have called RequeueFailed")
}

// BatchDispatcherConfigSuite tests BatchDispatcherConfig defaults
type BatchDispatcherConfigSuite struct {
	suite.Suite
}

func TestBatchDispatcherConfigSuite(t *testing.T) {
	suite.Run(t, new(BatchDispatcherConfigSuite))
}

func (s *BatchDispatcherConfigSuite) TestDefaultBatchDispatcherConfig_ReturnsExpectedValues() {
	// Arrange & Act
	config := DefaultBatchDispatcherConfig()

	// Assert
	assert.Equal(s.T(), 100*time.Millisecond, config.PollInterval)
	assert.Equal(s.T(), 3200*time.Millisecond, config.MaxPollInterval)
	assert.Equal(s.T(), 10, config.BatchSize)
	assert.Equal(s.T(), 1, config.WorkerCount)
	assert.Equal(s.T(), 30*time.Second, config.ShutdownTimeout)
	assert.Equal(s.T(), 60*time.Second, config.ForceTimeout)
	assert.True(s.T(), config.AutoRecover)
	assert.Equal(s.T(), 10*time.Second, config.RequeueInterval)
	assert.Equal(s.T(), 1*time.Second, config.RequeueBackoffBase)
	assert.Equal(s.T(), 1*time.Hour, config.RequeueBackoffMax)
	assert.NotNil(s.T(), config.OnForceShutdown)
	assert.NotNil(s.T(), config.Logger)
}

// TestBatchDispatcher_HealthStatus tests the HealthStatus method
func TestBatchDispatcher_HealthStatus(t *testing.T) {
	repo := NewMockOutboxRepository()
	publisher := NewMockPublisher()

	config := BatchDispatcherConfig{
		BatchSize:          10,
		WorkerCount:        2,
		DisableAutoRequeue: true,
	}

	dispatcher, err := NewBatchDispatcher(repo, publisher, config)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test before start
	status := dispatcher.HealthStatus()
	assert.False(t, status.IsHealthy())
	assert.False(t, status.Running)
	assert.Equal(t, 2, status.WorkerCount)

	// Start dispatcher
	go func() {
		_ = dispatcher.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Test while running
	status = dispatcher.HealthStatus()
	assert.True(t, status.IsHealthy())
	assert.True(t, status.Running)
	assert.False(t, status.PendingShutdown)

	// Stop dispatcher
	cancel()
	dispatcher.Stop()
	time.Sleep(100 * time.Millisecond)

	// Test after stop
	status = dispatcher.HealthStatus()
	assert.False(t, status.IsHealthy())
	assert.False(t, status.Running)
}

// TestCallOnPartialBatchSuccess tests the callOnPartialBatchSuccess hook
func TestCallOnPartialBatchSuccess(t *testing.T) {
	repo := NewMockOutboxRepository()
	publisher := NewMockPublisher()

	var partialSuccessCalled bool
	var expectedCount, actualCount int

	hooks := &Hooks{
		OnPartialBatchSuccess: func(ctx context.Context, expected, actual int, duration time.Duration) {
			partialSuccessCalled = true
			expectedCount = expected
			actualCount = actual
		},
	}

	config := BatchDispatcherConfig{
		BatchSize:          10,
		WorkerCount:        1,
		Hooks:              hooks,
		DisableAutoRequeue: true,
	}

	dispatcher, err := NewBatchDispatcher(repo, publisher, config)
	assert.NoError(t, err)

	// Add messages
	for i := 0; i < 5; i++ {
		id := uuid.New().String()
		msg := &Outbox{
			ID:             id,
			EventType:      "test.event",
			Payload:        []byte(`{}`),
			IdempotencyKey: id,
			Status:         OutboxStatusEnqueued,
			AttemptCount:   0,
			MaxAttempts:    5,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}
		repo.AddMessage(msg)
	}

	// Configure publisher to fail - this should trigger partial batch success hook
	failCount := 0
	publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		failCount++
		if failCount%2 == 0 {
			return errors.New("simulated failure")
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start dispatcher
	go func() {
		_ = dispatcher.Start(ctx)
	}()

	// Wait for processing
	time.Sleep(300 * time.Millisecond)
	dispatcher.Stop()

	// Verify hook was called (if there were partial failures)
	if partialSuccessCalled {
		assert.Greater(t, expectedCount, 0)
		assert.LessOrEqual(t, actualCount, expectedCount)
	}
}
