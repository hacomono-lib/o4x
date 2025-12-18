package core

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// InboxRepository defines the interface for Transactional Inbox (Idempotency Store).
//
// CRITICAL DESIGN PRINCIPLES:
//   - Inbox is NOT a message broker
//   - Inbox does NOT manage retries (SQS handles that)
//   - Inbox does NOT track in-flight processing (SQS visibility timeout handles that)
//   - Inbox ONLY answers: "Has this EVENT been COMPLETED by this consumer?"
//
// Semantic Model:
//   - Record exists: Event COMPLETED (skip forever)
//   - Record missing: Event not yet completed (proceed)
//
// Why This Design:
//   - SQS is point-to-point (1 message → 1 queue → 1 consumer)
//   - SQS visibility timeout prevents concurrent processing
//   - SQS redelivery handles retries
//   - Database should not duplicate broker responsibilities
//
// Use Cases:
//   - ✅ RECOMMENDED: Handlers with DB operations (within transaction)
//   - ✅ RECOMMENDED: Handlers with external API calls (if API supports idempotency keys)
//   - ❌ NEVER: Handlers with non-idempotent external APIs (don't use async messaging)
//
// CRITICAL: Idempotency Key is event_id (Outbox ID), NOT SQS MessageID
//
// Why event_id, not SQS MessageID:
//   - SQS MessageID changes on EVERY redelivery (Outbox republish, visibility timeout, DLQ requeue)
//   - event_id is the LOGICAL event identity from Outbox
//   - Same event = same event_id, regardless of how many times it's sent to SQS
//   - This ensures true exactly-once processing at the event level
//
// Design:
//   - Primary key: (consumer_name, event_id)
//   - consumer_name: Identifies the handler/service
//   - event_id: Outbox event ID (UUID from Outbox.ID)
//   - completed_at: When Complete() was called (NOT NULL)
//
// Usage Pattern (Transaction):
//
//	tx := h.db.Begin()
//	defer tx.Rollback()
//
//	txInbox := h.inboxRepo.WithTx(tx)
//	processed, err := txInbox.IsProcessed(ctx, "OrderHandler", msg.EventID)
//	if err != nil { return err }
//	if processed { return nil } // Already completed
//
//	// DB operations
//	tx.Create(&order)
//
//	// Mark completed
//	txInbox.Complete(ctx, "OrderHandler", msg.EventID)
//	tx.Commit()
//
// Usage Pattern (Auto-commit for idempotent logic):
//
//	processed, err := h.inboxRepo.IsProcessed(ctx, "EmailHandler", msg.EventID)
//	if err != nil { return err }
//	if processed { return nil } // Already completed
//
//	// Idempotent operation (ON CONFLICT DO NOTHING, etc.)
//	h.db.Exec("INSERT INTO emails ... ON CONFLICT DO NOTHING")
//
//	// Mark completed
//	h.inboxRepo.Complete(ctx, "EmailHandler", msg.EventID)
//
// Thread-Safety:
//   - IsProcessed is a simple SELECT EXISTS query (read-only, no locks)
//   - Complete uses INSERT with ON CONFLICT DO NOTHING (idempotent)
//   - No locking required (SQS visibility timeout is primary control)
//   - Atomic at database level via composite primary key
//
// Storage Implementations:
//   - contrib/pgx: PostgreSQL using pgx
//   - contrib/gorm: PostgreSQL/MySQL using GORM
type InboxRepository interface {
	// IsProcessed checks if an event has already been completed.
	//
	// CRITICAL: Uses event_id (Outbox ID), NOT SQS MessageID
	//
	// Implementation:
	//   SELECT EXISTS(
	//       SELECT 1 FROM consumer_inbox
	//       WHERE consumer_name = ? AND event_id = ?
	//   )
	//
	// Returns:
	//   - (true, nil): Event already completed, skip processing
	//   - (false, nil): Event not yet completed, proceed with processing
	//   - (_, error): Database error occurred
	//
	// CRITICAL: This is a lightweight existence check only.
	// Multiple workers may read "not processed" simultaneously.
	// The final deduplication happens at Complete() via ON CONFLICT.
	//
	// Design Rationale:
	//   - Inbox only answers: "Has this event been COMPLETED?"
	//   - No locking, no in-flight tracking, no retry management
	//   - SQS visibility timeout prevents concurrent processing
	//   - Handler idempotency + Complete() ensure exactly-once semantics
	//
	// Example (DB-only):
	//   tx := db.Begin()
	//   txInbox := repo.WithTx(tx)
	//   processed, _ := txInbox.IsProcessed(ctx, "OrderHandler", msg.EventID)
	//   if processed { tx.Rollback(); return nil }
	//   // Business logic...
	//   txInbox.Complete(ctx, "OrderHandler", msg.EventID)
	//   tx.Commit()
	//
	// Example (External API with idempotency key):
	//   processed, _ := repo.IsProcessed(ctx, "PaymentHandler", msg.EventID)
	//   if processed { return nil }
	//   // CRITICAL: Pass msg.EventID (NOT msg.MessageID) as idempotency key
	//   err := api.Charge(orderID, amount, msg.EventID.String())
	//   if err != nil { return err }
	//   repo.Complete(ctx, "PaymentHandler", msg.EventID)
	IsProcessed(ctx context.Context, consumerName string, eventID uuid.UUID) (bool, error)

	// Complete records that an event has been successfully processed.
	//
	// CRITICAL: Uses event_id (Outbox ID), NOT SQS MessageID
	//
	// Implementation:
	//   INSERT INTO consumer_inbox (consumer_name, event_id, completed_at)
	//   VALUES (?, ?, NOW())
	//   ON CONFLICT (consumer_name, event_id) DO NOTHING
	//
	// Returns:
	//   - nil: Successfully recorded or already exists (idempotent)
	//   - error: Database error occurred
	//
	// CRITICAL: Always call this after successful processing.
	// ON CONFLICT DO NOTHING ensures multiple workers can safely call this.
	//
	// Example (DB-only):
	//   tx := db.Begin()
	//   txInbox := repo.WithTx(tx)
	//   // ... business logic ...
	//   txInbox.Complete(ctx, "OrderHandler", msg.EventID)
	//   tx.Commit()
	//
	// Example (External API):
	//   err := api.Charge(orderID, amount, msg.EventID.String())
	//   if err != nil { return err }
	//   repo.Complete(ctx, "PaymentHandler", msg.EventID) // Safe even if concurrent
	Complete(ctx context.Context, consumerName string, eventID uuid.UUID) error

	// GetByEventID retrieves an inbox record by consumer name and event ID.
	// Returns ErrNotFound if the record doesn't exist.
	GetByEventID(ctx context.Context, consumerName string, eventID uuid.UUID) (*ConsumerInbox, error)
}

// InboxCleaner provides methods to clean up old inbox records.
// Implement this interface to prevent unbounded table growth.
type InboxCleaner interface {
	// DeleteOlderThan deletes inbox records older than the specified duration.
	//
	// Recommendation:
	//   - Retention: 7-30 days (for audit trail and debugging)
	//   - Run as scheduled job (daily or weekly)
	//
	// Returns the number of deleted records.
	//
	// Example:
	//   // Delete messages older than 7 days
	//   deleted, err := repo.DeleteOlderThan(ctx, 7*24*time.Hour)
	//   log.Info("cleanup completed", "deleted", deleted)
	DeleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
}

// ConsumerInbox represents a record in the consumer_inbox table.
// A record's existence means the event has been completed.
//
// CRITICAL: EventID is the Outbox event ID (Outbox.ID), NOT SQS MessageID.
type ConsumerInbox struct {
	ConsumerName string
	EventID      uuid.UUID
	CompletedAt  time.Time
}

// NopInboxRepository is a no-op implementation of InboxRepository.
// It implements the Null Object Pattern, allowing the consumer to function
// without idempotency checking at the storage level.
//
// WARNING: NopInboxRepository MUST NOT be used in production environments
// where at-least-once delivery is required.
//
// Using NopInboxRepository means you MUST implement idempotency
// in your handler logic (e.g., using business data unique constraints,
// Redis cache, or other application-level mechanisms).
//
// All methods return success without performing any operations.
type NopInboxRepository struct{}

// NewNopInboxRepository creates a new no-op inbox repository.
func NewNopInboxRepository() *NopInboxRepository {
	return &NopInboxRepository{}
}

// IsProcessed always returns false (allowing all events to be processed).
func (r *NopInboxRepository) IsProcessed(ctx context.Context, consumerName string, eventID uuid.UUID) (bool, error) {
	return false, nil
}

// Complete always succeeds without persisting anything.
func (r *NopInboxRepository) Complete(ctx context.Context, consumerName string, eventID uuid.UUID) error {
	return nil
}

// GetByEventID always returns ErrNotFound.
func (r *NopInboxRepository) GetByEventID(ctx context.Context, consumerName string, eventID uuid.UUID) (*ConsumerInbox, error) {
	return nil, ErrNotFound
}

// DeleteOlderThan always returns 0 deleted records.
func (r *NopInboxRepository) DeleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error) {
	return 0, nil
}
