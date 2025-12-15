package consumer

import (
	"context"
	"log/slog"
	"time"
)

// Hooks provides callbacks for consumer observability and metrics collection.
// All hooks are optional - nil hooks are safely ignored.
type Hooks struct {
	// OnConsumeStart is called before attempting to process a message.
	OnConsumeStart func(ctx context.Context, msg *SQSMessage)

	// OnConsumeSuccess is called after a message is successfully processed.
	// duration is the time taken to process the message.
	OnConsumeSuccess func(ctx context.Context, msg *SQSMessage, duration time.Duration)

	// OnConsumeFailure is called when processing a message fails.
	// duration is the time taken before the failure.
	// retryable indicates whether the message will be retried (based on receive count vs max retries).
	OnConsumeFailure func(ctx context.Context, msg *SQSMessage, err error, duration time.Duration, retryable bool)

	// OnMessageDead is called when a message exceeds max retries and is marked DEAD.
	OnMessageDead func(ctx context.Context, msg *SQSMessage, err error)

	// OnDeleteFailure is called when deleting a message from SQS fails after successful processing.
	// This is a critical error that may cause duplicate processing.
	OnDeleteFailure func(ctx context.Context, msg *SQSMessage, err error)
}

// callOnConsumeStart safely calls the OnConsumeStart hook if set.
// Panics in the hook are recovered and logged.
func (h *Hooks) callOnConsumeStart(ctx context.Context, msg *SQSMessage) {
	if h != nil && h.OnConsumeStart != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnConsumeStart",
					"panic", r,
					"message_id", msg.MessageID,
					"event_type", msg.EventType,
				)
			}
		}()
		h.OnConsumeStart(ctx, msg)
	}
}

// callOnConsumeSuccess safely calls the OnConsumeSuccess hook if set.
// Panics in the hook are recovered and logged.
func (h *Hooks) callOnConsumeSuccess(ctx context.Context, msg *SQSMessage, duration time.Duration) {
	if h != nil && h.OnConsumeSuccess != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnConsumeSuccess",
					"panic", r,
					"message_id", msg.MessageID,
					"event_type", msg.EventType,
				)
			}
		}()
		h.OnConsumeSuccess(ctx, msg, duration)
	}
}

// callOnConsumeFailure safely calls the OnConsumeFailure hook if set.
// Panics in the hook are recovered and logged.
//
//nolint:unparam // retryable parameter kept for future extensibility
func (h *Hooks) callOnConsumeFailure(ctx context.Context, msg *SQSMessage, err error, duration time.Duration, retryable bool) {
	if h != nil && h.OnConsumeFailure != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnConsumeFailure",
					"panic", r,
					"message_id", msg.MessageID,
					"event_type", msg.EventType,
				)
			}
		}()
		h.OnConsumeFailure(ctx, msg, err, duration, retryable)
	}
}

// callOnMessageDead safely calls the OnMessageDead hook if set.
// Panics in the hook are recovered and logged.
func (h *Hooks) callOnMessageDead(ctx context.Context, msg *SQSMessage, err error) {
	if h != nil && h.OnMessageDead != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnMessageDead",
					"panic", r,
					"message_id", msg.MessageID,
					"event_type", msg.EventType,
				)
			}
		}()
		h.OnMessageDead(ctx, msg, err)
	}
}

// callOnDeleteFailure safely calls the OnDeleteFailure hook if set.
// Panics in the hook are recovered and logged.
func (h *Hooks) callOnDeleteFailure(ctx context.Context, msg *SQSMessage, err error) {
	if h != nil && h.OnDeleteFailure != nil {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("hook panic recovered",
					"hook", "OnDeleteFailure",
					"panic", r,
					"message_id", msg.MessageID,
				)
			}
		}()
		h.OnDeleteFailure(ctx, msg, err)
	}
}
