package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
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
	// MessageConcurrency controls how many messages are processed concurrently
	// within a single worker goroutine.
	//
	// - 0 or 1: Sequential processing (default, safe for all queue types)
	// - >1: Parallel processing (recommended for Standard queues only)
	//
	// Example:
	//   WorkerCount=5, MessageConcurrency=10
	//   → Up to 5*10=50 messages processed in parallel
	//
	// IMPORTANT: MessageConcurrency>1 is NOT compatible with FIFO queues
	// (queues ending with .fifo suffix), as it breaks message ordering guarantees.
	// NewService will return an error if MessageConcurrency>1 is used with a FIFO queue.
	//
	// Performance tip: For Standard queues with fast handlers (<100ms),
	// MessageConcurrency can significantly improve throughput.
	MessageConcurrency int
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
		MessageConcurrency:  1, // Sequential processing by default
		ShutdownTimeout:     30 * time.Second,
		ForceTimeout:        60 * time.Second,
		OnForceShutdown:     func() { os.Exit(1) },
		Logger:              slog.Default(),
	}
}

// isFIFO returns true if the queue URL indicates a FIFO queue.
// FIFO queue names must end with the .fifo suffix.
func isFIFO(queueURL string) bool {
	return len(queueURL) >= 5 && queueURL[len(queueURL)-5:] == ".fifo"
}

// Service is the main consumer service that polls SQS and processes messages
type Service struct {
	sqsClient       SQSClient
	handler         Handler
	config          ServiceConfig
	cancelFunc      context.CancelFunc // Cancel function for graceful shutdown
	wg              sync.WaitGroup
	mu              sync.Mutex
	running         bool
	pendingShutdown bool
	lastProcessedAt atomic.Pointer[time.Time]
}

// NewService creates a new consumer service.
//
// The service polls SQS, calls the handler, and manages retries via SQS visibility timeout.
// For idempotency, use InboxRepository within your handler implementation.
//
// Example:
//
//	service := consumer.NewService(sqsClient, handler, config)
//	service.Start(ctx)
func NewService(sqsClient SQSClient, handler Handler, config ServiceConfig) *Service {
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
	if config.MessageConcurrency == 0 {
		config.MessageConcurrency = 1
	}

	return &Service{
		sqsClient: sqsClient,
		handler:   handler,
		config:    config,
	}
}

// Start begins the consumer service
func (s *Service) Start(ctx context.Context) error {
	// Validate configuration
	if s.config.MessageConcurrency > 1 && isFIFO(s.config.QueueURL) {
		return fmt.Errorf("MessageConcurrency>1 is not compatible with FIFO queues (queue: %s): parallel processing breaks message ordering guarantees", s.config.QueueURL)
	}

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

	// Warn about potential DB connection pool exhaustion with high concurrency
	totalConcurrency := s.config.WorkerCount * s.config.MessageConcurrency
	if totalConcurrency > 50 {
		s.config.Logger.WarnContext(ctx, "high message concurrency may exhaust DB connection pool",
			"worker_count", s.config.WorkerCount,
			"message_concurrency", s.config.MessageConcurrency,
			"total_concurrency", totalConcurrency,
			"recommendation", "ensure DB connection pool size >= total_concurrency + margin (e.g., 20% extra)",
		)
	}

	s.config.Logger.InfoContext(ctx, "starting consumer service",
		"queue_url", s.config.QueueURL,
		"worker_count", s.config.WorkerCount,
		"message_concurrency", s.config.MessageConcurrency,
		"total_concurrency", totalConcurrency,
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
		QueueUrl:              aws.String(s.config.QueueURL),
		MaxNumberOfMessages:   s.config.MaxNumberOfMessages,
		WaitTimeSeconds:       s.config.WaitTimeSeconds,
		VisibilityTimeout:     s.config.VisibilityTimeout,
		AttributeNames:        []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll}, // System attributes
		MessageAttributeNames: []string{"All"},                                               // Custom message attributes
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

	// Sequential processing (default, safe for all queue types)
	if s.config.MessageConcurrency <= 1 {
		for _, sqsMsg := range output.Messages {
			// Check context before processing each message
			if ctx.Err() != nil {
				logger.DebugContext(ctx, "skipping message processing due to shutdown")
				return
			}
			s.processMessage(ctx, sqsMsg, logger)
		}
		return
	}

	// Parallel processing (for Standard queues only)
	// Use semaphore pattern to limit concurrency
	sem := make(chan struct{}, s.config.MessageConcurrency)
	var wg sync.WaitGroup

messageLoop:
	for _, sqsMsg := range output.Messages {
		// Check context before starting goroutine
		if ctx.Err() != nil {
			logger.DebugContext(ctx, "skipping remaining messages due to shutdown")
			break
		}

		// Acquire semaphore
		select {
		case sem <- struct{}{}:
			// Semaphore acquired
		case <-ctx.Done():
			// Context cancelled while waiting for semaphore
			logger.DebugContext(ctx, "shutdown during semaphore acquisition")
			break messageLoop
		}

		wg.Add(1)
		go func(msg sqstypes.Message) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			// Create message-specific logger for better observability in parallel processing
			msgLogger := logger.With("message_id", aws.ToString(msg.MessageId))
			s.processMessage(ctx, msg, msgLogger)
		}(sqsMsg)
	}

	// Wait for all goroutines to complete
	wg.Wait()
}

// processMessage processes a single SQS message.
//
// Flow:
//  1. Parse SQS message into SQSMessage
//  2. Call handler.Handle(payload) - handler must implement idempotency via InboxRepository
//  3. Success: DeleteMessage from SQS
//  4. Error: Let SQS retry via visibility timeout or delete if max retries exceeded
//
// Important: Handler MUST be idempotent. Use InboxRepository or application-level
// idempotency (message_id column) to prevent duplicate processing.
func (s *Service) processMessage(ctx context.Context, sqsMsg sqstypes.Message, logger *slog.Logger) {
	// Parse message
	msg := s.parseSQSMessage(sqsMsg)
	logger = logger.With("message_id", msg.MessageID, "topic", msg.Topic)

	logger.DebugContext(ctx, "processing message")

	// Hook: OnConsumeStart
	s.config.Hooks.callOnConsumeStart(ctx, msg)
	startTime := time.Now()

	// Call handler
	handleErr := s.handler.Handle(ctx, msg)
	duration := time.Since(startTime)

	// Handle result
	if handleErr != nil {
		s.handleFailure(ctx, msg, handleErr, duration, logger)
		return
	}

	// Success: Delete from SQS
	_, deleteErr := s.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.config.QueueURL),
		ReceiptHandle: aws.String(msg.ReceiptHandle),
	})
	if deleteErr != nil {
		logger.WarnContext(ctx, "failed to delete message from SQS (message processed but may be redelivered)",
			"error", deleteErr)
		// Hook: OnDeleteFailure
		s.config.Hooks.callOnDeleteFailure(ctx, msg, deleteErr)
		// Continue - handler should be idempotent
	}

	// Hook: OnConsumeSuccess
	s.config.Hooks.callOnConsumeSuccess(ctx, msg, duration)

	logger.InfoContext(ctx, "message consumed successfully")

	// Update last processed timestamp for health checks (lock-free)
	now := time.Now()
	s.lastProcessedAt.Store(&now)
}

// handleFailure handles a failed message processing attempt.
func (s *Service) handleFailure(ctx context.Context, msg *SQSMessage, handleErr error, duration time.Duration, logger *slog.Logger) {
	errMsg := core.TruncateErrorMessage(handleErr.Error())

	// Check if max retries exceeded
	if msg.ReceiveCount >= s.config.MaxRetries {
		logger.WarnContext(ctx, "max retries exceeded, deleting message",
			"error", errMsg,
			"receive_count", msg.ReceiveCount,
			"max_retries", s.config.MaxRetries,
		)
		// Hook: OnMessageDead
		s.config.Hooks.callOnMessageDead(ctx, msg, handleErr)
		// Delete from SQS to prevent further processing
		// Configure SQS Dead Letter Queue (DLQ) if you need to preserve these messages
		_, _ = s.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(s.config.QueueURL),
			ReceiptHandle: aws.String(msg.ReceiptHandle),
		})
		return
	}

	// Hook: OnConsumeFailure (retryable)
	s.config.Hooks.callOnConsumeFailure(ctx, msg, handleErr, duration, true)

	// Message will be redelivered by SQS after visibility timeout
	logger.WarnContext(ctx, "message processing failed, will retry",
		"error", errMsg,
		"receive_count", msg.ReceiveCount,
		"max_retries", s.config.MaxRetries,
	)
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
	running := s.running
	pendingShutdown := s.pendingShutdown
	workerCount := s.config.WorkerCount
	s.mu.Unlock()

	return core.HealthStatus{
		Running:         running,
		LastProcessedAt: s.lastProcessedAt.Load(),
		WorkerCount:     workerCount,
		PendingShutdown: pendingShutdown,
	}
}
