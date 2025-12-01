package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/hacomono-lib/o4x/core"
)

// ServiceConfig holds configuration for the Consumer service
type ServiceConfig struct {
	QueueURL            string
	MaxNumberOfMessages int32
	WaitTimeSeconds     int32
	VisibilityTimeout   int32
	MaxRetries          int
	WorkerCount         int
	// ShutdownTimeout is the time to wait for graceful shutdown before warning
	ShutdownTimeout time.Duration
	// ForceTimeout is the hard limit after which the process exits forcefully
	// Must be greater than ShutdownTimeout. If zero, defaults to ShutdownTimeout * 2
	ForceTimeout time.Duration
	// OnForceShutdown is called when the force timeout is exceeded.
	// If nil, defaults to os.Exit(1). Set to a custom function for graceful handling
	// or set to an empty function to disable forced shutdown.
	OnForceShutdown func()
	Logger          *slog.Logger
	// Hooks provides callbacks for observability and metrics collection.
	// All hooks are optional.
	Hooks *Hooks
}

// DefaultServiceConfig returns sensible defaults
func DefaultServiceConfig(queueURL string) ServiceConfig {
	return ServiceConfig{
		QueueURL:            queueURL,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20,
		VisibilityTimeout:   30,
		MaxRetries:          5,
		WorkerCount:         1,
		ShutdownTimeout:     30 * time.Second,
		ForceTimeout:        60 * time.Second,
		OnForceShutdown:     func() { os.Exit(1) },
		Logger:              slog.Default(),
	}
}

// Service is the main consumer service that polls SQS and processes messages
type Service struct {
	sqsClient       SQSClient
	repo            Repository // Optional: can be nil
	handler         Handler
	config          ServiceConfig
	cancelFunc      context.CancelFunc // Cancel function for graceful shutdown
	wg              sync.WaitGroup
	mu              sync.Mutex
	running         bool
	pendingShutdown bool
	lastProcessedAt *time.Time
}

// NewService creates a new consumer service.
//
// If repo is nil, a NopRepository will be used automatically.
// This allows the consumer to function without a database, relying solely on
// SQS visibility timeout and DLQ for retry handling.
//
// Example with database:
//
//	repo := pgx.NewConsumerRepository(pool)
//	service := consumer.NewService(sqsClient, repo, handler, config)
//
// Example without database:
//
//	service := consumer.NewService(sqsClient, nil, handler, config)
func NewService(sqsClient SQSClient, repo Repository, handler Handler, config ServiceConfig) *Service {
	if config.MaxNumberOfMessages == 0 {
		config.MaxNumberOfMessages = 10
	}
	if config.WaitTimeSeconds == 0 {
		config.WaitTimeSeconds = 20
	}
	if config.VisibilityTimeout == 0 {
		config.VisibilityTimeout = 30
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 5
	}
	if config.WorkerCount == 0 {
		config.WorkerCount = 1
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = 30 * time.Second
	}
	if config.ForceTimeout == 0 {
		config.ForceTimeout = config.ShutdownTimeout * 2
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.OnForceShutdown == nil {
		config.OnForceShutdown = func() { os.Exit(1) }
	}

	// Use NopRepository if repo is nil (Null Object Pattern)
	if repo == nil {
		repo = NewNopRepository()
	}

	return &Service{
		sqsClient: sqsClient,
		repo:      repo,
		handler:   handler,
		config:    config,
	}
}

// Start begins the consumer service
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = true

	// Create cancellable context for workers
	workerCtx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel

	s.mu.Unlock()

	s.config.Logger.InfoContext(ctx, "starting consumer service",
		"queue_url", s.config.QueueURL,
		"worker_count", s.config.WorkerCount,
		"repository_enabled", s.repo != nil,
	)

	for i := 0; i < s.config.WorkerCount; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.runWorker(workerCtx, workerID)
		}(i)
	}

	return nil
}

// Stop gracefully shuts down the consumer service
// It waits up to ShutdownTimeout for graceful completion,
// then up to ForceTimeout before forcefully exiting.
func (s *Service) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.pendingShutdown = true
	s.running = false
	cancelFunc := s.cancelFunc
	s.mu.Unlock()

	s.config.Logger.Info("stopping consumer service",
		"shutdown_timeout", s.config.ShutdownTimeout,
		"force_timeout", s.config.ForceTimeout,
	)

	// Cancel context to interrupt long polling
	if cancelFunc != nil {
		cancelFunc()
	}

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.config.Logger.Info("consumer service stopped gracefully")
		return
	case <-time.After(s.config.ShutdownTimeout):
		s.config.Logger.Warn("consumer service graceful shutdown timed out, waiting for force timeout",
			"shutdown_timeout", s.config.ShutdownTimeout,
			"force_timeout", s.config.ForceTimeout,
		)
	}

	// Wait until ForceTimeout
	remainingTime := s.config.ForceTimeout - s.config.ShutdownTimeout
	select {
	case <-done:
		s.config.Logger.Info("consumer service stopped after extended wait")
		return
	case <-time.After(remainingTime):
		s.config.Logger.Error("consumer service force shutdown - workers did not stop in time",
			"force_timeout", s.config.ForceTimeout,
		)
		if s.config.OnForceShutdown != nil {
			s.config.OnForceShutdown()
		}
	}
}

// runWorker runs a single consumer worker
func (s *Service) runWorker(ctx context.Context, workerID int) {
	logger := s.config.Logger.With("worker_id", workerID)
	logger.InfoContext(ctx, "consumer worker started")

	for {
		// Check if context is cancelled
		if ctx.Err() != nil {
			logger.InfoContext(ctx, "consumer worker stopped")
			return
		}
		// Poll and process messages
		// Note: pollAndProcess will respect ctx cancellation during SQS ReceiveMessage
		s.pollAndProcess(ctx, logger)
	}
}

// pollAndProcess polls SQS and processes received messages
func (s *Service) pollAndProcess(ctx context.Context, logger *slog.Logger) {
	// Check context before polling
	if ctx.Err() != nil {
		return
	}

	output, err := s.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(s.config.QueueURL),
		MaxNumberOfMessages:         s.config.MaxNumberOfMessages,
		WaitTimeSeconds:             s.config.WaitTimeSeconds,
		VisibilityTimeout:           s.config.VisibilityTimeout,
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		// Don't log error if context was cancelled (graceful shutdown)
		if ctx.Err() != nil {
			return
		}
		logger.ErrorContext(ctx, "failed to receive messages", "error", err)
		time.Sleep(time.Second) // Back off on error
		return
	}

	for _, sqsMsg := range output.Messages {
		// Check context before processing each message
		if ctx.Err() != nil {
			logger.DebugContext(ctx, "skipping message processing due to shutdown")
			return
		}
		s.processMessage(ctx, sqsMsg, logger)
	}
}

// processMessage processes a single SQS message.
//
// Flow:
//  1. Parse SQS message into SQSMessage
//  2. Insert into consumer_messages with status=CONSUMING (or no-op if using NopRepository)
//  3. Call handler.Handle(payload)
//  4. Success: Mark as CONSUMED, then DeleteMessage from SQS
//  5. Error: receive_count < max_retries -> FAILED, else -> DEAD
//
// Note: The repository may be a NopRepository (Null Object Pattern) which performs no actual
// persistence but allows the same code path to be used regardless of database usage.
//
// Important: Consumer NEVER updates outbox.status
func (s *Service) processMessage(ctx context.Context, sqsMsg sqstypes.Message, logger *slog.Logger) {
	// Parse message
	msg := s.parseSQSMessage(sqsMsg)
	logger = logger.With("message_id", msg.MessageID, "topic", msg.Topic)

	logger.DebugContext(ctx, "processing message")

	s.processMessageInternal(ctx, msg, logger)
}

// processMessageInternal processes a message using the repository (which may be NopRepository).
// This unified implementation works for both database-backed and no-database scenarios.
func (s *Service) processMessageInternal(ctx context.Context, msg *SQSMessage, logger *slog.Logger) {
	// Record in consumer_messages with CONSUMING status
	consumerMsg, err := s.repo.InsertOrUpdate(ctx, ConsumerMessageInsertParams{
		OutboxID:      msg.OutboxID,
		MessageID:     msg.MessageID,
		ReceiptHandle: msg.ReceiptHandle,
		ReceiveCount:  msg.ReceiveCount,
		QueueURL:      s.config.QueueURL,
		MaxRetries:    s.config.MaxRetries,
	})
	if err != nil {
		logger.ErrorContext(ctx, "failed to record message", "error", err)
		return
	}

	// Hook: OnConsumeStart
	s.config.Hooks.callOnConsumeStart(ctx, msg)
	startTime := time.Now()

	// Call handler
	handleErr := s.handler.Handle(ctx, msg)
	duration := time.Since(startTime)

	// Handle result
	if handleErr != nil {
		s.handleFailureInternal(ctx, consumerMsg, msg, handleErr, duration, logger)
		return
	}

	// Success: First mark as CONSUMED in DB, then delete from SQS
	// This order is important: if DeleteMessage fails after CONSUMED, the message
	// may be redelivered by SQS but can be safely ignored via idempotency check
	// (the handler should be idempotent anyway).
	// If we delete first and then UpdateToConsumed fails, DB remains CONSUMING
	// which is incorrect since the message won't be redelivered.
	if err := s.repo.UpdateToConsumed(ctx, consumerMsg.ID); err != nil {
		logger.ErrorContext(ctx, "failed to update to CONSUMED", "error", err)
		// Don't delete from SQS - let it be redelivered
		return
	}

	// Now delete from SQS
	_, deleteErr := s.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.config.QueueURL),
		ReceiptHandle: aws.String(msg.ReceiptHandle),
	})
	if deleteErr != nil {
		// DB is already CONSUMED, so even if redelivered, handler should be idempotent
		logger.WarnContext(ctx, "failed to delete message from SQS (message processed but may be redelivered)",
			"error", deleteErr)
		// Hook: OnDeleteFailure
		s.config.Hooks.callOnDeleteFailure(ctx, msg, deleteErr)
		// Continue - the message is logically consumed
	}

	// Hook: OnConsumeSuccess
	s.config.Hooks.callOnConsumeSuccess(ctx, msg, duration)

	logger.InfoContext(ctx, "message consumed successfully")

	// Update last processed timestamp for health checks
	now := time.Now()
	s.mu.Lock()
	s.lastProcessedAt = &now
	s.mu.Unlock()
}

// handleFailureInternal handles a failed message processing attempt.
// Works with both real repositories and NoOpRepository.
func (s *Service) handleFailureInternal(ctx context.Context, consumerMsg *ConsumerMessage, msg *SQSMessage, handleErr error, duration time.Duration, logger *slog.Logger) {
	errMsg := core.TruncateErrorMessage(handleErr.Error())

	// Check if max retries exceeded
	if consumerMsg.ReceiveCount >= s.config.MaxRetries {
		logger.WarnContext(ctx, "message marked as DEAD",
			"error", errMsg,
			"receive_count", consumerMsg.ReceiveCount,
			"max_retries", s.config.MaxRetries,
		)
		// Hook: OnMessageDead
		s.config.Hooks.callOnMessageDead(ctx, msg, handleErr)
		if err := s.repo.UpdateToDead(ctx, consumerMsg.ID, errMsg); err != nil {
			logger.ErrorContext(ctx, "failed to update to DEAD", "error", err)
		}
		// Delete from SQS to prevent further processing
		_, _ = s.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(s.config.QueueURL),
			ReceiptHandle: aws.String(msg.ReceiptHandle),
		})
		return
	}

	// Hook: OnConsumeFailure (retryable)
	s.config.Hooks.callOnConsumeFailure(ctx, msg, handleErr, duration, true)

	// Mark as FAILED - message will be redelivered by SQS after visibility timeout
	logger.WarnContext(ctx, "message marked as FAILED",
		"error", errMsg,
		"receive_count", consumerMsg.ReceiveCount,
		"max_retries", s.config.MaxRetries,
	)
	if err := s.repo.UpdateToFailed(ctx, consumerMsg.ID, errMsg); err != nil {
		logger.ErrorContext(ctx, "failed to update to FAILED", "error", err)
	}
}

// parseSQSMessage converts an SQS message to our internal format
func (s *Service) parseSQSMessage(sqsMsg sqstypes.Message) *SQSMessage {
	msg := &SQSMessage{
		MessageID:     aws.ToString(sqsMsg.MessageId),
		ReceiptHandle: aws.ToString(sqsMsg.ReceiptHandle),
		Body:          json.RawMessage(aws.ToString(sqsMsg.Body)),
	}

	// Parse receive count from attributes
	if rcStr, ok := sqsMsg.Attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)]; ok {
		if rc, err := strconv.Atoi(rcStr); err == nil {
			msg.ReceiveCount = rc
		}
	}

	// Parse custom message attributes
	if topicAttr, ok := sqsMsg.MessageAttributes["topic"]; ok {
		msg.Topic = aws.ToString(topicAttr.StringValue)
	}

	if outboxIDAttr, ok := sqsMsg.MessageAttributes["outbox_id"]; ok {
		if idStr := aws.ToString(outboxIDAttr.StringValue); idStr != "" {
			msg.OutboxID = &idStr
		}
	}

	if idempotencyAttr, ok := sqsMsg.MessageAttributes["idempotency_key"]; ok {
		msg.IdempotencyKey = aws.ToString(idempotencyAttr.StringValue)
	}

	return msg
}

// IsRunning returns whether the service is currently running
func (s *Service) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// HealthStatus returns the current health status of the consumer service.
// Use this to implement health check endpoints for containerized deployments (ECS, Kubernetes, etc.).
//
// Example usage:
//
//	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
//	    status := service.HealthStatus()
//	    if !status.IsHealthy() {
//	        w.WriteHeader(http.StatusServiceUnavailable)
//	        return
//	    }
//	    // Optional: Check for stale processing
//	    if status.IsStale(5 * time.Minute) {
//	        w.WriteHeader(http.StatusServiceUnavailable)
//	        return
//	    }
//	    w.WriteHeader(http.StatusOK)
//	})
func (s *Service) HealthStatus() core.HealthStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return core.HealthStatus{
		Running:         s.running,
		LastProcessedAt: s.lastProcessedAt,
		WorkerCount:     s.config.WorkerCount,
		PendingShutdown: s.pendingShutdown,
	}
}
