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

// DispatcherSuite tests Dispatcher functionality
type DispatcherSuite struct {
	suite.Suite
	repo      *MockOutboxRepository
	publisher *MockPublisher
	logger    *slog.Logger
}

func TestDispatcherSuite(t *testing.T) {
	suite.Run(t, new(DispatcherSuite))
}

func (s *DispatcherSuite) SetupTest() {
	s.repo = NewMockOutboxRepository()
	s.publisher = NewMockPublisher()
	s.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (s *DispatcherSuite) TestNewDispatcher_WithDefaultConfig() {
	// Arrange
	config := DefaultDispatcherConfig()

	// Act
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)

	// Assert
	assert.NotNil(s.T(), dispatcher)
	assert.Equal(s.T(), 100*time.Millisecond, dispatcher.config.PollInterval)
	assert.Equal(s.T(), 3200*time.Millisecond, dispatcher.config.MaxPollInterval)
	assert.Equal(s.T(), 1, dispatcher.config.WorkerCount)
	assert.Equal(s.T(), 30*time.Second, dispatcher.config.ShutdownTimeout)
	assert.Equal(s.T(), 60*time.Second, dispatcher.config.ForceTimeout)
	assert.True(s.T(), dispatcher.config.AutoRecover)
}

func (s *DispatcherSuite) TestNewDispatcher_WithCustomConfig() {
	// Arrange
	config := DispatcherConfig{
		PollInterval:       200 * time.Millisecond,
		MaxPollInterval:    5 * time.Second,
		WorkerCount:        4,
		ShutdownTimeout:    60 * time.Second,
		ForceTimeout:       120 * time.Second,
		AutoRecover:        false,
		Logger:             s.logger,
		DisableAutoRequeue: true,
	}

	// Act
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)

	// Assert
	assert.Equal(s.T(), 200*time.Millisecond, dispatcher.config.PollInterval)
	assert.Equal(s.T(), 5*time.Second, dispatcher.config.MaxPollInterval)
	assert.Equal(s.T(), 4, dispatcher.config.WorkerCount)
	assert.Equal(s.T(), 60*time.Second, dispatcher.config.ShutdownTimeout)
	assert.Equal(s.T(), 120*time.Second, dispatcher.config.ForceTimeout)
	assert.False(s.T(), dispatcher.config.AutoRecover)
}

func (s *DispatcherSuite) TestStart_WhenAlreadyRunning_ReturnsError() {
	// Arrange
	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)
	defer dispatcher.Stop()

	// Act
	err = dispatcher.Start(ctx)

	// Assert
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "already running")
}

func (s *DispatcherSuite) TestStart_WithAutoRecover_CallsReviveStuckPublishing() {
	// Arrange
	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        true,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
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

func (s *DispatcherSuite) TestStart_WithoutAutoRecover_DoesNotCallReviveStuckPublishing() {
	// Arrange
	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	defer dispatcher.Stop()

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 0, s.repo.ReviveStuckPublishingCalls)
}

func (s *DispatcherSuite) TestStartAndStop_IsRunningReflectsState() {
	// Arrange
	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		ShutdownTimeout:    5 * time.Second,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
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

func (s *DispatcherSuite) TestDispatcher_ProcessesMessages() {
	// Arrange
	msg := createTestOutbox("test.event", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	assert.Greater(s.T(), len(s.publisher.PublishCalls), 0, "should have published at least one message")
	updatedMsg := s.repo.GetMessage(msg.ID)
	assert.Equal(s.T(), OutboxStatusPublished, updatedMsg.Status)
}

func (s *DispatcherSuite) TestDispatcher_HandlesPublishFailure() {
	// Arrange
	msg := createTestOutbox("test.event", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return errors.New("publish failed")
	}

	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
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
	assert.Equal(s.T(), OutboxStatusFailed, updatedMsg.Status)
	assert.Equal(s.T(), 1, updatedMsg.RetryCount)
}

func (s *DispatcherSuite) TestDispatcher_MarksMessageDeadAfterMaxRetries() {
	// Arrange
	msg := createTestOutboxWithRetry("test.event", map[string]string{"key": "value"}, 2, 3)
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return errors.New("publish failed")
	}

	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
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
}

func (s *DispatcherSuite) TestDispatcher_MarksMessageDeadOnPermanentError() {
	// Arrange
	msg := createTestOutbox("test.event", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return NewPermanentError(errors.New("validation failed"))
	}

	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		WorkerCount:        1,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
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
}

func (s *DispatcherSuite) TestDispatcher_CallsHooksOnPublishStart() {
	// Arrange
	msg := createTestOutbox("test.event", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	var hookCalled bool
	var hookedMsg *Outbox

	hooks := &Hooks{
		OnPublishStart: func(ctx context.Context, m *Outbox) {
			hookCalled = true
			hookedMsg = m
		},
	}

	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		WorkerCount:        1,
		Hooks:              hooks,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	assert.True(s.T(), hookCalled)
	assert.Equal(s.T(), msg.ID, hookedMsg.ID)
}

func (s *DispatcherSuite) TestDispatcher_CallsHooksOnPublishSuccess() {
	// Arrange
	msg := createTestOutbox("test.event", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	var hookCalled bool
	var duration time.Duration

	hooks := &Hooks{
		OnPublishSuccess: func(ctx context.Context, m *Outbox, d time.Duration) {
			hookCalled = true
			duration = d
		},
	}

	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		WorkerCount:        1,
		Hooks:              hooks,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	assert.True(s.T(), hookCalled)
	assert.Greater(s.T(), duration, time.Duration(0))
}

func (s *DispatcherSuite) TestDispatcher_CallsHooksOnPublishFailure() {
	// Arrange
	msg := createTestOutbox("test.event", map[string]string{"key": "value"})
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return errors.New("publish failed")
	}

	var hookCalled bool
	var hookErr error
	var hookRetryable bool

	hooks := &Hooks{
		OnPublishFailure: func(ctx context.Context, m *Outbox, err error, d time.Duration, retryable bool) {
			hookCalled = true
			hookErr = err
			hookRetryable = retryable
		},
	}

	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		WorkerCount:        1,
		Hooks:              hooks,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	assert.True(s.T(), hookCalled)
	assert.NotNil(s.T(), hookErr)
	assert.True(s.T(), hookRetryable)
}

func (s *DispatcherSuite) TestDispatcher_CallsHooksOnMessageDead() {
	// Arrange
	msg := createTestOutboxWithRetry("test.event", map[string]string{"key": "value"}, 2, 3)
	s.repo.AddMessage(msg)

	s.publisher.PublishFunc = func(ctx context.Context, m *Outbox) error {
		return errors.New("publish failed")
	}

	var hookCalled bool
	var deadMsg *Outbox

	hooks := &Hooks{
		OnMessageDead: func(ctx context.Context, m *Outbox, err error) {
			hookCalled = true
			deadMsg = m
		},
	}

	config := DispatcherConfig{
		Logger:             s.logger,
		AutoRecover:        false,
		PollInterval:       10 * time.Millisecond,
		WorkerCount:        1,
		Hooks:              hooks,
		DisableAutoRequeue: true,
	}
	dispatcher, err := NewDispatcher(s.repo, s.publisher, config)
	assert.NoError(s.T(), err)
	ctx := context.Background()

	// Act
	err = dispatcher.Start(ctx)
	assert.NoError(s.T(), err)

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)
	dispatcher.Stop()

	// Assert
	assert.True(s.T(), hookCalled)
	assert.Equal(s.T(), msg.ID, deadMsg.ID)
}

// DispatcherConfigSuite tests DispatcherConfig defaults
type DispatcherConfigSuite struct {
	suite.Suite
}

func TestDispatcherConfigSuite(t *testing.T) {
	suite.Run(t, new(DispatcherConfigSuite))
}

func (s *DispatcherConfigSuite) TestDefaultDispatcherConfig_ReturnsExpectedValues() {
	// Arrange & Act
	config := DefaultDispatcherConfig()

	// Assert
	assert.Equal(s.T(), 100*time.Millisecond, config.PollInterval)
	assert.Equal(s.T(), 3200*time.Millisecond, config.MaxPollInterval)
	assert.Equal(s.T(), 1, config.WorkerCount)
	assert.Equal(s.T(), 30*time.Second, config.ShutdownTimeout)
	assert.Equal(s.T(), 60*time.Second, config.ForceTimeout)
	assert.True(s.T(), config.AutoRecover)
	assert.NotNil(s.T(), config.OnForceShutdown)
	assert.NotNil(s.T(), config.Logger)
}

// TestDispatcher_HealthStatus tests the HealthStatus method for non-batch dispatcher
func TestDispatcher_HealthStatus(t *testing.T) {
	repo := NewMockOutboxRepository()
	publisher := NewMockPublisher()

	config := DispatcherConfig{
		WorkerCount:        3,
		DisableAutoRequeue: true,
	}

	dispatcher, err := NewDispatcher(repo, publisher, config)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test before start
	status := dispatcher.HealthStatus()
	assert.False(t, status.IsHealthy())
	assert.False(t, status.Running)
	assert.Equal(t, 3, status.WorkerCount)

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

// TestDispatcher_RunRequeueWorker tests the requeue worker
func TestDispatcher_RunRequeueWorker(t *testing.T) {
	repo := NewMockOutboxRepository()
	publisher := NewMockPublisher()

	// Add an enqueued message
	msg := &Outbox{
		ID:             uuid.New().String(),
		EventType:      "test.event",
		Payload:        []byte(`{}`),
		IdempotencyKey: "test-key",
		Status:         OutboxStatusEnqueued,
		RetryCount:     1,
		MaxRetries:     5,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	repo.AddMessage(msg)

	var requeueCalled bool
	repo.RequeueFailedFunc = func(ctx context.Context) (int64, error) {
		requeueCalled = true
		return 1, nil
	}

	config := DispatcherConfig{
		WorkerCount:     1,
		RequeueInterval: 100 * time.Millisecond,
	}

	dispatcher, err := NewDispatcher(repo, publisher, config)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	// Start dispatcher
	go func() {
		_ = dispatcher.Start(ctx)
	}()

	// Wait for requeue worker to run
	time.Sleep(250 * time.Millisecond)
	dispatcher.Stop()

	// Verify RequeueFailed was called
	assert.True(t, requeueCalled, "RequeueFailed should have been called by requeue worker")
}
