package core

import (
	"context"
	"time"
)

// OutboxRepository defines the interface for outbox persistence operations
// NOTE: This is for Publisher/Dispatcher only. Consumer has its own repository.
type OutboxRepository interface {
	// Insert adds a new message to the outbox with ENQUEUED status
	// Called by application code within a business transaction
	Insert(ctx context.Context, params OutboxInsertParams) (*Outbox, error)

	// FetchAndLockToPublishing atomically fetches one ENQUEUED message,
	// locks it, and updates its status to PUBLISHING in a single transaction.
	// This prevents race conditions where messages could be left in an inconsistent state.
	// Uses SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1 with atomic update.
	// Returns ErrNoMessage if no message is available.
	FetchAndLockToPublishing(ctx context.Context) (*Outbox, error)

	// UpdateToPublished marks the message as PUBLISHED
	// Called after successful publish
	UpdateToPublished(ctx context.Context, id string) error

	// UpdateToFailed marks the message as FAILED with error info
	// Increments retry_count and sets next_retry_at based on exponential backoff.
	// The backoff is calculated using repository's configured baseInterval and maxInterval.
	// Called on publish failure.
	UpdateToFailed(ctx context.Context, id, errMsg string) error

	// UpdateToDead marks the message as DEAD
	// Called when retry_count >= max_retries
	UpdateToDead(ctx context.Context, id, errMsg string) error

	// RequeueFailed moves FAILED messages back to ENQUEUED.
	// Only messages whose next_retry_at has elapsed are requeued.
	// Called periodically to retry failed messages.
	RequeueFailed(ctx context.Context) (int64, error)

	// GetByID retrieves an outbox message by ID
	GetByID(ctx context.Context, id string) (*Outbox, error)

	// GetByIdempotencyKey retrieves an outbox message by topic and idempotency key
	GetByIdempotencyKey(ctx context.Context, topic, idempotencyKey string) (*Outbox, error)
}

// BatchOutboxRepository extends OutboxRepository with batch operations
// Implement this interface for better throughput when using BatchPublisher
type BatchOutboxRepository interface {
	OutboxRepository

	// FetchLockAndMarkPublishing atomically fetches ENQUEUED messages,
	// locks them, and updates their status to PUBLISHING in a single transaction.
	// This prevents race conditions where messages could be left in an inconsistent state.
	// Uses SELECT ... FOR UPDATE SKIP LOCKED LIMIT $limit with CTE for atomic update.
	// Returns messages already marked as PUBLISHING.
	FetchLockAndMarkPublishing(ctx context.Context, limit int) ([]*Outbox, error)

	// UpdateBatchToPublished marks multiple messages as PUBLISHED
	// Returns the number of successfully updated messages.
	// If the returned count is less than len(ids), some messages were not in PUBLISHING state.
	// This can happen during crash recovery scenarios where messages were already processed.
	// The caller should handle partial success appropriately.
	UpdateBatchToPublished(ctx context.Context, ids []string) (int64, error)
}

// OutboxCleaner provides methods to clean up old outbox records
// Implement this interface to prevent table bloat from PUBLISHED/DEAD records
type OutboxCleaner interface {
	// DeleteOlderThan deletes outbox records with the given status
	// that are older than the specified duration.
	// Returns the number of deleted records.
	// Typical usage: delete PUBLISHED records older than 7 days
	DeleteOlderThan(ctx context.Context, status OutboxStatus, olderThan time.Duration) (int64, error)
}

// OutboxRecovery provides methods to recover from crash states
// Implement this interface to enable automatic recovery at startup
type OutboxRecovery interface {
	// ReviveStuckPublishing recovers messages stuck in PUBLISHING state.
	// This moves PUBLISHING -> FAILED so they can be retried.
	// Should be called at startup to recover from crashes.
	// Returns the number of messages recovered.
	ReviveStuckPublishing(ctx context.Context) (int64, error)
}
