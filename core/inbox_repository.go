package core

import (
	"context"
	"time"
)

// InboxRepository defines the interface for Transactional Inbox (Idempotency Store).
//
// CRITICAL DESIGN PRINCIPLES:
//   - Inbox is NOT a message broker
//   - Inbox does NOT manage retries (SQS handles that)
//   - Inbox does NOT track in-flight processing (SQS visibility timeout handles that)
//   - Inbox ONLY answers: "Has this message been COMPLETED by this consumer?"
//
// Semantic Model:
//   - Record exists: Message COMPLETED (skip forever)
//   - Record missing: Message not yet completed (proceed)
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
// Design:
//   - Primary key: (consumer_name, message_id)
//   - consumer_name: Identifies the handler/service
//   - message_id: SQS message ID
//   - completed_at: When Complete() was called (NOT NULL)
//
// Usage Pattern (Transaction):
//
//	tx := h.db.Begin()
//	defer tx.Rollback()
//
//	txInbox := h.inboxRepo.WithTx(tx)
//	ok, err := txInbox.TryStart(ctx, "OrderHandler", msg.MessageID)
//	if err != nil { return err }
//	if !ok { return nil } // Already completed
//
//	// DB operations
//	tx.Create(&order)
//
//	// Mark completed
//	txInbox.Complete(ctx, "OrderHandler", msg.MessageID)
//	tx.Commit()
//
// Usage Pattern (Auto-commit for idempotent logic):
//
//	ok, err := h.inboxRepo.TryStart(ctx, "EmailHandler", msg.MessageID)
//	if err != nil { return err }
//	if !ok { return nil } // Already completed
//
//	// Idempotent operation (ON CONFLICT DO NOTHING, etc.)
//	h.db.Exec("INSERT INTO emails ... ON CONFLICT DO NOTHING")
//
//	// Mark completed
//	h.inboxRepo.Complete(ctx, "EmailHandler", msg.MessageID)
//
// Thread-Safety:
//   - TryStart uses optimistic INSERT with ON CONFLICT DO NOTHING
//   - No locking required (SQS visibility timeout is primary control)
//   - Atomic at database level via composite primary key
//
// Storage Implementations:
//   - contrib/pgx: PostgreSQL using pgx
//   - contrib/gorm: PostgreSQL/MySQL using GORM
type InboxRepository interface {
	// TryStart checks if a message has already been completed.
	//
	// Implementation:
	//   SELECT EXISTS(
	//       SELECT 1 FROM consumer_inbox
	//       WHERE consumer_name = ? AND message_id = ?
	//   )
	//
	// Returns:
	//   - (true, nil): Message not yet completed, proceed with processing
	//   - (false, nil): Already completed, skip processing
	//   - (false, error): Database error occurred
	//
	// CRITICAL: This is a lightweight existence check only.
	// Multiple workers may pass this check simultaneously.
	// Use Complete() after successful processing to record completion.
	//
	// Design Rationale:
	//   - DB-only handlers: Use within transaction for consistency
	//   - External API handlers: API must support idempotency keys
	//
	// Example (DB-only):
	//   tx := db.Begin()
	//   txInbox := repo.WithTx(tx)
	//   ok, _ := txInbox.TryStart(ctx, "OrderHandler", msg.MessageID)
	//   if !ok { tx.Rollback(); return nil }
	//   // Business logic...
	//   txInbox.Complete(ctx, "OrderHandler", msg.MessageID)
	//   tx.Commit()
	//
	// Example (External API):
	//   ok, _ := repo.TryStart(ctx, "PaymentHandler", msg.MessageID)
	//   if !ok { return nil }
	//   // API call with idempotency key (multiple workers may reach here)
	//   err := api.Charge(orderID, amount, msg.MessageID)
	//   if err != nil { return err }
	//   repo.Complete(ctx, "PaymentHandler", msg.MessageID)
	TryStart(ctx context.Context, consumerName, messageID string) (bool, error)

	// Complete records that a message has been successfully processed.
	//
	// Implementation:
	//   INSERT INTO consumer_inbox (consumer_name, message_id, completed_at)
	//   VALUES (?, ?, NOW())
	//   ON CONFLICT (consumer_name, message_id) DO NOTHING
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
	//   txInbox.Complete(ctx, "OrderHandler", msg.MessageID)
	//   tx.Commit()
	//
	// Example (External API):
	//   err := api.Charge(orderID, amount, msg.MessageID)
	//   if err != nil { return err }
	//   repo.Complete(ctx, "PaymentHandler", msg.MessageID) // Safe even if concurrent
	Complete(ctx context.Context, consumerName, messageID string) error

	// GetByMessageID retrieves an inbox record by consumer name and message ID.
	// Returns ErrNotFound if the record doesn't exist.
	GetByMessageID(ctx context.Context, consumerName, messageID string) (*ConsumerInbox, error)
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
// A record's existence means the message has been completed.
type ConsumerInbox struct {
	ConsumerName string
	MessageID    string
	CompletedAt  time.Time
}

// NopInboxRepository is a no-op implementation of InboxRepository.
// It implements the Null Object Pattern, allowing the consumer to function
// without idempotency checking at the storage level.
//
// WARNING: Using NopInboxRepository means you MUST implement idempotency
// in your handler logic (e.g., using business data unique constraints,
// Redis cache, or other application-level mechanisms).
//
// All methods return success without performing any operations.
type NopInboxRepository struct{}

// NewNopInboxRepository creates a new no-op inbox repository.
func NewNopInboxRepository() *NopInboxRepository {
	return &NopInboxRepository{}
}

// TryStart always returns true (allowing all messages to be processed).
func (r *NopInboxRepository) TryStart(ctx context.Context, consumerName, messageID string) (bool, error) {
	return true, nil
}

// Complete always succeeds without persisting anything.
func (r *NopInboxRepository) Complete(ctx context.Context, consumerName, messageID string) error {
	return nil
}

// GetByMessageID always returns ErrNotFound.
func (r *NopInboxRepository) GetByMessageID(ctx context.Context, consumerName, messageID string) (*ConsumerInbox, error) {
	return nil, ErrNotFound
}

// DeleteOlderThan always returns 0 deleted records.
func (r *NopInboxRepository) DeleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error) {
	return 0, nil
}
