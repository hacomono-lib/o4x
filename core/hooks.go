package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Hooks provides callbacks for observability and metrics collection.
// All hooks are optional - nil hooks are safely ignored.
type Hooks struct {
	// OnPublishStart is called before attempting to publish a message.
	OnPublishStart func(ctx context.Context, msg *Outbox)

	// OnPublishSuccess is called after a message is successfully published.
	// duration is the time taken to publish the message.
	OnPublishSuccess func(ctx context.Context, msg *Outbox, duration time.Duration)

	// OnPublishFailure is called when publishing a message fails.
	// duration is the time taken before the failure.
	// retryable indicates whether the message will be retried.
	OnPublishFailure func(ctx context.Context, msg *Outbox, err error, duration time.Duration, retryable bool)

	// OnMessageDead is called when a message exceeds max retries and is marked DEAD.
	OnMessageDead func(ctx context.Context, msg *Outbox, err error)

	// OnBatchPublishStart is called before attempting to publish a batch.
	OnBatchPublishStart func(ctx context.Context, msgs []*Outbox)

	// OnBatchPublishComplete is called after a batch publish attempt completes.
	// successCount and failureCount indicate the results.
	OnBatchPublishComplete func(ctx context.Context, successCount, failureCount int, duration time.Duration)

	// OnPartialBatchSuccess is called when UpdateBatchToPublished succeeds for fewer messages
	// than expected. This indicates that some messages were not in PUBLISHING state,
	// possibly due to crash recovery or concurrent processing.
	// expectedCount: number of message IDs we tried to update
	// actualCount: number of messages actually updated to PUBLISHED
	OnPartialBatchSuccess func(ctx context.Context, expectedCount, actualCount int, duration time.Duration)
}

// callOnPublishStart safely calls the OnPublishStart hook if set.
// Panics in user hooks are recovered and logged to prevent worker crashes.
func (h *Hooks) callOnPublishStart(ctx context.Context, msg *Outbox) {
	if h != nil && h.OnPublishStart != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnPublishStart",
					"panic", r,
					"outbox_id", msg.ID,
					"topic", msg.Topic,
				)
			}
		}()
		h.OnPublishStart(ctx, msg)
	}
}

// callOnPublishSuccess safely calls the OnPublishSuccess hook if set.
// Panics in user hooks are recovered and logged to prevent worker crashes.
func (h *Hooks) callOnPublishSuccess(ctx context.Context, msg *Outbox, duration time.Duration) {
	if h != nil && h.OnPublishSuccess != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnPublishSuccess",
					"panic", r,
					"outbox_id", msg.ID,
					"topic", msg.Topic,
				)
			}
		}()
		h.OnPublishSuccess(ctx, msg, duration)
	}
}

// callOnPublishFailure safely calls the OnPublishFailure hook if set.
// Panics in user hooks are recovered and logged to prevent worker crashes.
func (h *Hooks) callOnPublishFailure(ctx context.Context, msg *Outbox, err error, duration time.Duration, retryable bool) {
	if h != nil && h.OnPublishFailure != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnPublishFailure",
					"panic", r,
					"outbox_id", msg.ID,
					"topic", msg.Topic,
				)
			}
		}()
		h.OnPublishFailure(ctx, msg, err, duration, retryable)
	}
}

// callOnMessageDead safely calls the OnMessageDead hook if set.
// Panics in user hooks are recovered and logged to prevent worker crashes.
func (h *Hooks) callOnMessageDead(ctx context.Context, msg *Outbox, err error) {
	if h != nil && h.OnMessageDead != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnMessageDead",
					"panic", r,
					"outbox_id", msg.ID,
					"topic", msg.Topic,
				)
			}
		}()
		h.OnMessageDead(ctx, msg, err)
	}
}

// callOnBatchPublishStart safely calls the OnBatchPublishStart hook if set.
// Panics in user hooks are recovered and logged to prevent worker crashes.
func (h *Hooks) callOnBatchPublishStart(ctx context.Context, msgs []*Outbox) {
	if h != nil && h.OnBatchPublishStart != nil {
		defer func() {
			if r := recover(); r != nil {
				msgIDs := make([]string, len(msgs))
				for i, msg := range msgs {
					msgIDs[i] = msg.ID
				}
				slog.Error("hook panic recovered",
					"hook", "OnBatchPublishStart",
					"panic", r,
					"batch_size", len(msgs),
					"outbox_ids", fmt.Sprintf("%v", msgIDs),
				)
			}
		}()
		h.OnBatchPublishStart(ctx, msgs)
	}
}

// callOnBatchPublishComplete safely calls the OnBatchPublishComplete hook if set.
// Panics in user hooks are recovered and logged to prevent worker crashes.
func (h *Hooks) callOnBatchPublishComplete(ctx context.Context, successCount, failureCount int, duration time.Duration) {
	if h != nil && h.OnBatchPublishComplete != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnBatchPublishComplete",
					"panic", r,
					"success_count", successCount,
					"failure_count", failureCount,
				)
			}
		}()
		h.OnBatchPublishComplete(ctx, successCount, failureCount, duration)
	}
}

// callOnPartialBatchSuccess safely calls the OnPartialBatchSuccess hook if set.
// Panics in user hooks are recovered and logged to prevent worker crashes.
func (h *Hooks) callOnPartialBatchSuccess(ctx context.Context, expectedCount, actualCount int, duration time.Duration) {
	if h != nil && h.OnPartialBatchSuccess != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnPartialBatchSuccess",
					"panic", r,
					"expected_count", expectedCount,
					"actual_count", actualCount,
				)
			}
		}()
		h.OnPartialBatchSuccess(ctx, expectedCount, actualCount, duration)
	}
}
