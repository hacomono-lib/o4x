package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// BatchDispatcherConfig holds configuration for the BatchDispatcher
type BatchDispatcherConfig struct {
	// PollInterval is the base interval for polling when messages are found.
	// When no messages are found, the interval increases exponentially up to MaxPollInterval.
	PollInterval time.Duration
	// MaxPollInterval is the maximum polling interval during idle periods.
	// When no messages are found, the interval doubles until reaching this limit.
	// Defaults to PollInterval * 32 (about 3.2 seconds with default 100ms).
	MaxPollInterval time.Duration
	// BatchSize is the number of messages to fetch per batch (max depends on publisher)
	BatchSize int
	// WorkerCount is the number of concurrent batch workers
	WorkerCount int
	// ShutdownTimeout is the time to wait for graceful shutdown before warning
	ShutdownTimeout time.Duration
	// ForceTimeout is the hard limit after which the process exits forcefully
	ForceTimeout time.Duration
	// OnForceShutdown is called when the force timeout is exceeded.
	// If nil, defaults to os.Exit(1). Set to a custom function for graceful handling
	// or set to an empty function to disable forced shutdown.
	OnForceShutdown func()
	// AutoRecover enables automatic recovery of stuck PUBLISHING messages at startup.
	// When true, ReviveStuckPublishing is called if the repository implements OutboxRecovery.
	// Defaults to true.
	AutoRecover bool
	// RequeueInterval is how often to run RequeueFailed.
	// IMPORTANT: If set to 0, FAILED messages will NEVER be retried automatically.
	// Recommended: 10s for normal workloads, 1s for high-priority messages.
	RequeueInterval time.Duration
	// DisableAutoRequeue explicitly disables automatic requeue of FAILED messages.
	// If true, RequeueInterval validation is skipped and FAILED messages will NOT
	// be retried automatically. Use with caution - only when an external system
	// handles FAILED messages or for testing purposes.
	DisableAutoRequeue bool
	// RequeueBackoffBase is the base interval for exponential backoff when requeuing failed messages.
	// The actual backoff is: RequeueBackoffBase * 2^(retry_count-1), capped at RequeueBackoffMax.
	// Defaults to 1 second.
	RequeueBackoffBase time.Duration
	// RequeueBackoffMax is the maximum backoff interval for requeuing failed messages.
	// Defaults to 1 hour.
	RequeueBackoffMax time.Duration
	// Logger for dispatcher operations
	Logger *slog.Logger
	// Hooks for observability and metrics collection (optional)
	Hooks *Hooks
	// CleanupTimeout is the timeout for database cleanup operations during shutdown.
	// This ensures DB updates (UpdateToPublished, UpdateToFailed) complete even when
	// the parent context is cancelled. Defaults to 10 seconds.
	CleanupTimeout time.Duration
}

// DefaultBatchDispatcherConfig returns sensible defaults
func DefaultBatchDispatcherConfig() BatchDispatcherConfig {
	return BatchDispatcherConfig{
		PollInterval:       100 * time.Millisecond,
		MaxPollInterval:    3200 * time.Millisecond, // 100ms * 32
		BatchSize:          10,
		WorkerCount:        1,
		ShutdownTimeout:    30 * time.Second,
		ForceTimeout:       60 * time.Second,
		OnForceShutdown:    func() { os.Exit(1) },
		AutoRecover:        true,
		RequeueInterval:    10 * time.Second,
		RequeueBackoffBase: 1 * time.Second,
		RequeueBackoffMax:  1 * time.Hour,
		CleanupTimeout:     10 * time.Second,
		Logger:             slog.Default(),
	}
}

// BatchDispatcher orchestrates batch processing of outbox messages
// It requires BatchOutboxRepository and BatchPublisher for optimal performance
type BatchDispatcher struct {
	repo            BatchOutboxRepository
	publisher       BatchPublisher
	config          BatchDispatcherConfig
	cancelFunc      context.CancelFunc
	wg              sync.WaitGroup
	mu              sync.Mutex
	running         bool
	pendingShutdown bool
	lastProcessedAt atomic.Pointer[time.Time]
}

// NewBatchDispatcher creates a new BatchDispatcher with configuration validation
// Returns an error if the configuration is invalid.
func NewBatchDispatcher(repo BatchOutboxRepository, publisher BatchPublisher, config BatchDispatcherConfig) (*BatchDispatcher, error) {
	// Validate negative values
	if config.PollInterval < 0 {
		return nil, ErrInvalidConfig
	}
	if config.MaxPollInterval < 0 {
		return nil, ErrInvalidConfig
	}
	if config.BatchSize < 0 {
		return nil, ErrInvalidConfig
	}
	if config.WorkerCount < 0 {
		return nil, ErrInvalidConfig
	}
	if config.ShutdownTimeout < 0 {
		return nil, ErrInvalidConfig
	}
	if config.ForceTimeout < 0 {
		return nil, ErrInvalidConfig
	}
	if config.RequeueInterval < 0 {
		return nil, ErrInvalidConfig
	}
	if config.RequeueBackoffBase < 0 {
		return nil, ErrInvalidConfig
	}
	if config.RequeueBackoffMax < 0 {
		return nil, ErrInvalidConfig
	}
	if config.CleanupTimeout < 0 {
		return nil, ErrInvalidConfig
	}

	// Apply defaults
	if config.PollInterval == 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.MaxPollInterval == 0 {
		config.MaxPollInterval = config.PollInterval * 32
	}
	if config.BatchSize == 0 {
		config.BatchSize = publisher.MaxBatchSize()
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
	if config.RequeueBackoffBase == 0 {
		config.RequeueBackoffBase = 1 * time.Second
	}
	if config.RequeueBackoffMax == 0 {
		config.RequeueBackoffMax = 1 * time.Hour
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = 10 * time.Second
	}

	// Validate BatchSize against publisher's maximum
	if config.BatchSize > publisher.MaxBatchSize() {
		config.Logger.Warn("BatchSize exceeds publisher limit, adjusting to publisher's MaxBatchSize",
			"requested_batch_size", config.BatchSize,
			"publisher_max_batch_size", publisher.MaxBatchSize())
		config.BatchSize = publisher.MaxBatchSize()
	}

	// Validate timeout consistency
	if config.ForceTimeout < config.ShutdownTimeout {
		config.Logger.Warn("ForceTimeout is less than ShutdownTimeout, adjusting ForceTimeout",
			"shutdown_timeout", config.ShutdownTimeout,
			"force_timeout", config.ForceTimeout,
			"adjusted_force_timeout", config.ShutdownTimeout*2)
		config.ForceTimeout = config.ShutdownTimeout * 2
	}

	// CRITICAL: RequeueInterval must be set unless explicitly disabled
	// Without requeue, FAILED messages from partial batch failures will never retry
	if config.RequeueInterval == 0 && !config.DisableAutoRequeue {
		return nil, fmt.Errorf("%w: RequeueInterval must be > 0 (e.g., 10s for production, 500ms-1s for bench). "+
			"FAILED messages will never retry without this. Set DisableAutoRequeue=true to explicitly disable", ErrInvalidConfig)
	}

	return &BatchDispatcher{
		repo:      repo,
		publisher: publisher,
		config:    config,
	}, nil
}

// Start begins the batch dispatcher
func (d *BatchDispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return ErrAlreadyRunning
	}
	d.running = true

	workerCtx, cancel := context.WithCancel(ctx)
	d.cancelFunc = cancel

	d.mu.Unlock()

	// Auto-recover stuck messages if enabled and repository supports it
	// Run asynchronously to avoid delaying startup (especially when many stuck messages exist)
	if d.config.AutoRecover {
		go func() {
			if recovery, ok := d.repo.(OutboxRecovery); ok {
				count, err := recovery.ReviveStuckPublishing(ctx)
				if err != nil {
					d.config.Logger.ErrorContext(ctx, "failed to recover stuck messages at startup", "error", err)
				} else if count > 0 {
					d.config.Logger.InfoContext(ctx, "recovered stuck messages at startup", "count", count)
				}
			} else {
				d.config.Logger.WarnContext(ctx, "AutoRecover is enabled but repository does not implement OutboxRecovery. "+
					"Consider calling ReviveStuckPublishing manually at startup to prevent message loss.")
			}
		}()
	}

	d.config.Logger.InfoContext(ctx, "starting batch dispatcher",
		"worker_count", d.config.WorkerCount,
		"batch_size", d.config.BatchSize,
		"poll_interval", d.config.PollInterval,
		"requeue_interval", d.config.RequeueInterval,
	)

	// Start batch workers
	for i := 0; i < d.config.WorkerCount; i++ {
		d.wg.Add(1)
		go func(workerID int) {
			defer d.wg.Done()
			d.runBatchWorker(workerCtx, workerID)
		}(i)
	}

	// Start requeue worker if enabled
	if d.config.RequeueInterval > 0 {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.runRequeueWorker(workerCtx)
		}()
	}

	return nil
}

// Stop gracefully shuts down the batch dispatcher
func (d *BatchDispatcher) Stop() {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return
	}
	d.pendingShutdown = true
	d.running = false
	cancelFunc := d.cancelFunc
	d.mu.Unlock()

	if cancelFunc != nil {
		cancelFunc()
	}

	GracefulShutdown("batch dispatcher", &d.wg, ShutdownConfig{
		ShutdownTimeout: d.config.ShutdownTimeout,
		ForceTimeout:    d.config.ForceTimeout,
		OnForceShutdown: d.config.OnForceShutdown,
		Logger:          d.config.Logger,
	})
}

// runBatchWorker runs a single batch worker with adaptive polling.
// When messages are found, polling happens immediately.
// When no messages are found, the interval doubles up to maxPollInterval.
func (d *BatchDispatcher) runBatchWorker(ctx context.Context, workerID int) {
	logger := d.config.Logger.With("worker_id", workerID, "worker_type", "batch")
	logger.InfoContext(ctx, "batch worker started",
		"poll_interval", d.config.PollInterval,
		"max_poll_interval", d.config.MaxPollInterval,
	)

	currentInterval := d.config.PollInterval
	timer := time.NewTimer(currentInterval)
	defer func() {
		if !timer.Stop() {
			// Drain the channel if timer fired between Stop and defer
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.InfoContext(ctx, "batch worker stopped")
			return
		case <-timer.C:
			count := d.processBatch(ctx, logger)
			if count > 0 {
				// Messages found - reset to base interval for quick polling
				currentInterval = d.config.PollInterval
			} else {
				// No messages - exponential backoff
				currentInterval = min(currentInterval*2, d.config.MaxPollInterval)
			}
			// Properly reset timer: stop it first, drain channel if needed, then reset
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(currentInterval)
		}
	}
}

// processBatch fetches and processes a batch of messages.
// Returns the number of messages processed.
func (d *BatchDispatcher) processBatch(ctx context.Context, logger *slog.Logger) int {
	// Atomically fetch, lock, and mark as PUBLISHING in a single operation
	msgs, err := d.repo.FetchLockAndMarkPublishing(ctx, d.config.BatchSize)
	if err != nil {
		logger.ErrorContext(ctx, "failed to fetch and lock batch", "error", err)
		return 0
	}

	if len(msgs) == 0 {
		return 0
	}

	logger.DebugContext(ctx, "processing batch", "count", len(msgs))

	// Hook: OnBatchPublishStart
	d.config.Hooks.callOnBatchPublishStart(ctx, msgs)
	startTime := time.Now()

	// Publish batch
	results := d.publisher.PublishBatch(ctx, msgs)
	duration := time.Since(startTime)

	// Process results
	var successIDs []string
	var successMsgs []*Outbox
	failureCount := 0
	for i, result := range results {
		msg := msgs[i]

		if result.Success {
			successIDs = append(successIDs, msg.ID)
			successMsgs = append(successMsgs, msg)
			logger.DebugContext(ctx, "message published", "outbox_id", msg.ID, "message_id", result.MessageID)
		} else {
			failureCount++
			// Handle individual failure
			d.handlePublishFailure(ctx, msg, result.Error, duration, logger)
		}
	}

	// Batch update successful messages
	if len(successIDs) > 0 {
		// Use context without cancellation to ensure DB update completes even during shutdown.
		// This prevents messages from being stuck in PUBLISHING state.
		cleanupCtx, cancel := createCleanupContext(ctx, d.config.CleanupTimeout)
		defer cancel()

		updatedCount, err := d.repo.UpdateBatchToPublished(cleanupCtx, successIDs)
		if err != nil {
			logger.ErrorContext(cleanupCtx, "failed to update batch to PUBLISHED", "error", err, "count", len(successIDs))
			// CRITICAL: Messages remain in PUBLISHING state and were already published to SQS.
			// ReviveStuckPublishing will mark them as FAILED, causing duplicate delivery.
			// Monitor this error carefully - it indicates database or network issues.
		} else if updatedCount < int64(len(successIDs)) {
			// Partial success - some messages were not in PUBLISHING state
			logger.WarnContext(cleanupCtx, "partial success updating batch to PUBLISHED",
				"expected", len(successIDs),
				"updated", updatedCount,
				"missing", len(successIDs)-int(updatedCount),
			)
			// Hook: OnPartialBatchSuccess for metrics tracking
			d.config.Hooks.callOnPartialBatchSuccess(ctx, len(successIDs), int(updatedCount), duration)
		} else {
			logger.InfoContext(ctx, "batch published successfully", "count", updatedCount)
			// Hook: OnPublishSuccess for each successful message
			for _, msg := range successMsgs {
				d.config.Hooks.callOnPublishSuccess(ctx, msg, duration)
			}
		}
	}

	// Hook: OnBatchPublishComplete
	d.config.Hooks.callOnBatchPublishComplete(ctx, len(successIDs), failureCount, duration)

	// Update last processed timestamp for health checks (lock-free)
	if len(msgs) > 0 {
		now := time.Now()
		d.lastProcessedAt.Store(&now)
	}

	return len(msgs)
}

// handlePublishFailure handles a failed publish for a single message
func (d *BatchDispatcher) handlePublishFailure(ctx context.Context, msg *Outbox, publishErr error, duration time.Duration, logger *slog.Logger) {
	errMsg := TruncateErrorMessage(publishErr.Error())
	msgLogger := logger.With("outbox_id", msg.ID, "event_type", msg.EventType)

	// Check if error is permanently non-retryable
	retryable := IsRetryable(publishErr)

	// Use context without cancellation to ensure DB update completes even during shutdown.
	// This prevents messages from being stuck in PUBLISHING state.
	cleanupCtx, cancel := createCleanupContext(ctx, d.config.CleanupTimeout)
	defer cancel()

	// Check if retry limit will be exceeded after this failure OR error is not retryable
	if !retryable || msg.RetryCount+1 >= msg.MaxRetries {
		reason := "max retries exceeded"
		if !retryable {
			reason = "permanent error (not retryable)"
		}
		msgLogger.WarnContext(cleanupCtx, "message marked as DEAD",
			"error", errMsg,
			"reason", reason,
			"retry_count", msg.RetryCount+1,
			"max_retries", msg.MaxRetries,
			"retryable", retryable,
		)
		if err := d.repo.UpdateToDead(cleanupCtx, msg.ID, errMsg); err != nil {
			// If the message is already in a valid state, log warning but continue
			if errors.Is(err, ErrInvalidStatus) {
				msgLogger.WarnContext(cleanupCtx, "message not in PUBLISHING state during UpdateToDead (may have been recovered)", "error", err)
			} else {
				msgLogger.ErrorContext(cleanupCtx, "failed to update to DEAD", "error", err)
			}
		}
		// Hook: OnPublishFailure (not retryable) and OnMessageDead
		d.config.Hooks.callOnPublishFailure(ctx, msg, publishErr, duration, false)
		d.config.Hooks.callOnMessageDead(ctx, msg, publishErr)
		return
	}

	msgLogger.WarnContext(cleanupCtx, "message marked as FAILED",
		"error", errMsg,
		"retry_count", msg.RetryCount+1,
		"max_retries", msg.MaxRetries,
	)
	if err := d.repo.UpdateToFailed(cleanupCtx, msg.ID, errMsg); err != nil {
		// If the message is already in a valid state, log warning but continue
		if errors.Is(err, ErrInvalidStatus) {
			msgLogger.WarnContext(cleanupCtx, "message not in PUBLISHING state during UpdateToFailed (may have been recovered)", "error", err)
		} else {
			msgLogger.ErrorContext(cleanupCtx, "failed to update to FAILED", "error", err)
		}
	}

	// Hook: OnPublishFailure (retryable)
	d.config.Hooks.callOnPublishFailure(ctx, msg, publishErr, duration, true)
}

// runRequeueWorker periodically moves FAILED messages back to ENQUEUED
func (d *BatchDispatcher) runRequeueWorker(ctx context.Context) {
	logger := d.config.Logger.With("worker_type", "requeue")
	logger.InfoContext(ctx, "requeue worker started",
		"interval", d.config.RequeueInterval,
	)

	ticker := time.NewTicker(d.config.RequeueInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.InfoContext(ctx, "requeue worker stopped")
			return
		case <-ticker.C:
			count, err := d.repo.RequeueFailed(ctx)
			if err != nil {
				logger.ErrorContext(ctx, "failed to requeue failed messages", "error", err)
			} else if count > 0 {
				logger.InfoContext(ctx, "requeued failed messages", "count", count)
			} else {
				logger.DebugContext(ctx, "requeue completed, no messages eligible")
			}
		}
	}
}

// IsRunning returns whether the dispatcher is currently running
func (d *BatchDispatcher) IsRunning() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.running
}

// HealthStatus returns the current health status of the dispatcher.
// Use this to implement health check endpoints for containerized deployments (ECS, Kubernetes, etc.).
//
// Example usage:
//
//	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
//	    status := dispatcher.HealthStatus()
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
func (d *BatchDispatcher) HealthStatus() HealthStatus {
	d.mu.Lock()
	running := d.running
	pendingShutdown := d.pendingShutdown
	workerCount := d.config.WorkerCount
	d.mu.Unlock()

	return HealthStatus{
		Running:         running,
		LastProcessedAt: d.lastProcessedAt.Load(),
		WorkerCount:     workerCount,
		PendingShutdown: pendingShutdown,
	}
}
