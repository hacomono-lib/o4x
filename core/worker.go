package core

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Worker processes outbox messages one at a time
// Uses SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1
// For batch processing, use BatchDispatcher instead
type Worker struct {
	id                 int
	repo               OutboxRepository
	publisher          Publisher
	logger             *slog.Logger
	hooks              *Hooks
	pollInterval       time.Duration
	maxPollInterval    time.Duration
	cleanupTimeout     time.Duration
	onMessageProcessed func() // Called when a message is successfully processed
}

// NewWorker creates a new Worker
func NewWorker(id int, repo OutboxRepository, publisher Publisher, logger *slog.Logger, hooks *Hooks, pollInterval, maxPollInterval, cleanupTimeout time.Duration) *Worker {
	return &Worker{
		id:              id,
		repo:            repo,
		publisher:       publisher,
		logger:          logger.With("worker_id", id),
		hooks:           hooks,
		pollInterval:    pollInterval,
		maxPollInterval: maxPollInterval,
		cleanupTimeout:  cleanupTimeout,
	}
}

// Run starts the worker loop with adaptive polling.
// When messages are found, polling happens immediately.
// When no messages are found, the interval doubles up to maxPollInterval.
func (w *Worker) Run(ctx context.Context) {
	w.logger.InfoContext(ctx, "worker started",
		"poll_interval", w.pollInterval,
		"max_poll_interval", w.maxPollInterval,
	)

	currentInterval := w.pollInterval
	timer := time.NewTimer(currentInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.InfoContext(ctx, "worker stopped")
			return
		case <-timer.C:
			err := w.processOne(ctx)
			if err == nil {
				// Message found and processed - reset to base interval for quick polling
				currentInterval = w.pollInterval
			} else if errors.Is(err, ErrNoMessage) {
				// No message - exponential backoff
				currentInterval = min(currentInterval*2, w.maxPollInterval)
			} else {
				// Error occurred - log and use exponential backoff
				w.logger.ErrorContext(ctx, "process error", "error", err)
				currentInterval = min(currentInterval*2, w.maxPollInterval)
			}
			timer.Reset(currentInterval)
		}
	}
}

// processOne fetches and processes a single message
// Flow:
//  1. Atomically: SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1 WHERE status='ENQUEUED' + UPDATE status = PUBLISHING
//  2. publisher.Publish()
//  3. Success: status = PUBLISHED
//  4. Failure: retry_count++
//     - retry_count < max_retries -> FAILED
//     - retry_count >= max_retries -> DEAD
func (w *Worker) processOne(ctx context.Context) error {
	// Step 1: Atomically fetch, lock, and mark as PUBLISHING
	msg, err := w.repo.FetchAndLockToPublishing(ctx)
	if err != nil {
		if errors.Is(err, ErrNoMessage) {
			return ErrNoMessage
		}
		return err
	}

	logger := w.logger.With("outbox_id", msg.ID, "topic", msg.Topic)
	logger.DebugContext(ctx, "processing message")

	// Hook: OnPublishStart
	w.hooks.callOnPublishStart(ctx, msg)
	startTime := time.Now()

	// Step 2: Attempt publish
	publishErr := w.publisher.Publish(ctx, msg)
	duration := time.Since(startTime)

	// Step 3 & 4: Handle result
	if publishErr != nil {
		return w.handlePublishFailure(ctx, msg, publishErr, duration, logger)
	}

	// Success: Mark as PUBLISHED
	// Use context without cancellation to ensure DB update completes even during shutdown.
	// This prevents messages from being stuck in PUBLISHING state.
	// Note: We use context.WithoutCancel to preserve parent's values (trace ID, logger)
	// while removing cancellation. A new timeout is added for the cleanup operation.
	cleanupCtx, cancel := createCleanupContext(ctx, w.cleanupTimeout)
	defer cancel()

	if err := w.repo.UpdateToPublished(cleanupCtx, msg.ID); err != nil {
		// If the message is already in a valid state (not PUBLISHING), log warning but don't fail
		if errors.Is(err, ErrInvalidStatus) {
			logger.WarnContext(cleanupCtx, "message not in PUBLISHING state (may have been processed by another worker or recovered)", "error", err)
		} else {
			logger.ErrorContext(cleanupCtx, "failed to update to PUBLISHED", "error", err)
			return err
		}
	}

	// Hook: OnPublishSuccess
	w.hooks.callOnPublishSuccess(ctx, msg, duration)

	logger.InfoContext(ctx, "message published successfully")

	// Notify dispatcher of successful processing (for health checks)
	if w.onMessageProcessed != nil {
		w.onMessageProcessed()
	}

	return nil
}

// handlePublishFailure handles a failed publish attempt
func (w *Worker) handlePublishFailure(ctx context.Context, msg *Outbox, publishErr error, duration time.Duration, logger *slog.Logger) error {
	errMsg := TruncateErrorMessage(publishErr.Error())

	// Check if error is permanently non-retryable
	retryable := IsRetryable(publishErr)

	// Use context without cancellation to ensure DB update completes even during shutdown.
	// This prevents messages from being stuck in PUBLISHING state.
	cleanupCtx, cancel := createCleanupContext(ctx, w.cleanupTimeout)
	defer cancel()

	// Check if retry limit will be exceeded after this failure OR error is not retryable
	// Note: retry_count is incremented by UpdateToFailed
	if !retryable || msg.RetryCount+1 >= msg.MaxRetries {
		// Mark as DEAD
		reason := "max retries exceeded"
		if !retryable {
			reason = "permanent error (not retryable)"
		}
		logger.WarnContext(cleanupCtx, "message marked as DEAD",
			"error", errMsg,
			"reason", reason,
			"retry_count", msg.RetryCount+1,
			"max_retries", msg.MaxRetries,
			"retryable", retryable,
		)
		if err := w.repo.UpdateToDead(cleanupCtx, msg.ID, errMsg); err != nil {
			// If the message is already in a valid state (not PUBLISHING), log warning but don't fail
			if errors.Is(err, ErrInvalidStatus) {
				logger.WarnContext(cleanupCtx, "message not in PUBLISHING state during UpdateToDead (may have been recovered)", "error", err)
			} else {
				logger.ErrorContext(cleanupCtx, "failed to update to DEAD", "error", err)
				return err
			}
		}
		// Hook: OnPublishFailure (not retryable) and OnMessageDead
		w.hooks.callOnPublishFailure(ctx, msg, publishErr, duration, false)
		w.hooks.callOnMessageDead(ctx, msg, publishErr)
		return nil
	}

	// Mark as FAILED (retryable)
	logger.WarnContext(cleanupCtx, "message marked as FAILED",
		"error", errMsg,
		"retry_count", msg.RetryCount+1,
		"max_retries", msg.MaxRetries,
	)
	if err := w.repo.UpdateToFailed(cleanupCtx, msg.ID, errMsg); err != nil {
		// If the message is already in a valid state (not PUBLISHING), log warning but don't fail
		if errors.Is(err, ErrInvalidStatus) {
			logger.WarnContext(cleanupCtx, "message not in PUBLISHING state during UpdateToFailed (may have been recovered)", "error", err)
		} else {
			logger.ErrorContext(cleanupCtx, "failed to update to FAILED", "error", err)
			return err
		}
	}

	// Hook: OnPublishFailure (retryable)
	w.hooks.callOnPublishFailure(ctx, msg, publishErr, duration, true)

	return nil
}
