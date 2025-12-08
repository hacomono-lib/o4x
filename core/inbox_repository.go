package core

import (
	"context"
	"errors"
	"time"
)

// ErrDuplicateMessage indicates the message has already been processed (idempotency violation)
var ErrDuplicateMessage = errors.New("duplicate message: already processed or in progress")

// InboxRepository defines the interface for Transactional Inbox (Idempotency Store).
// This is the recommended approach for ensuring exactly-once processing semantics
// in event-driven systems using the Inbox pattern.
//
// IMPORTANT - Scope and Use Cases:
//   - ✅ RECOMMENDED: Handlers with DB operations only (within transaction)
//   - ✅ SUPPORTED: Handlers with external API calls (if API supports idempotency keys)
//   - ❌ NOT RECOMMENDED: Handlers with non-idempotent external APIs
//
// TryStart() behavior (correctly handles retries):
//  1. Record doesn't exist → INSERT → returns true (first time)
//  2. Record exists with status=processing → returns true (retry scenario)
//  3. Record exists with status=completed → returns false (duplicate)
//
// For external API calls with idempotency key support:
//   - Pass message_id as idempotency key to the API
//   - Example:
//     ok, _ := inboxRepo.TryStart(ctx, "EmailHandler", msg.MessageID)
//     if !ok { return nil }
//     err := emailClient.Send(email, idempotencyKey: msg.MessageID)
//     if err != nil { return err } // Retry safe - API handles duplicates
//     inboxRepo.Complete(ctx, "EmailHandler", msg.MessageID)
//
// For non-idempotent external APIs:
//   - Use application-level idempotency instead
//   - message_id column in business data + ON CONFLICT DO NOTHING
//   - Redis cache with TTL for deduplication
//
// Purpose:
//   - Prevent duplicate message processing at the storage level
//   - Provide atomic "check-and-lock" semantics for idempotency
//   - Track message processing lifecycle (processing -> completed)
//
// Design:
//   - Primary key: (consumer_name, message_id)
//   - consumer_name: Identifies the handler/service processing the message
//   - message_id: Unique message identifier (e.g., SQS message ID)
//   - status: "processing" or "completed"
//
// Usage Pattern (DB operations only):
//
//	// In your consumer handler - DB OPERATIONS ONLY
//	tx := h.db.Begin()
//	defer tx.Rollback()
//
//	txInbox := h.inboxRepo.WithTx(tx)
//	ok, err := txInbox.TryStart(ctx, "OrderHandler", msg.MessageID)
//	if err != nil {
//	    return err
//	}
//	if !ok {
//	    return nil // Duplicate
//	}
//
//	// DB operations (no external API calls!)
//	if err := tx.Create(&order).Error; err != nil {
//	    return err // tx.Rollback() called, inbox record rolled back
//	}
//
//	// Mark as completed
//	if err := txInbox.Complete(ctx, "OrderHandler", msg.MessageID); err != nil {
//	    return err
//	}
//
//	return tx.Commit().Error
//
// Thread-Safety:
//   - TryStart uses INSERT ... ON CONFLICT (or equivalent) for atomic check-and-insert
//   - Race-safe: concurrent calls for same message_id will return false for duplicates
//   - No explicit locking required at application level
//
// Storage Implementations:
//   - contrib/gorm: PostgreSQL/MySQL using GORM
//   - contrib/pgx: PostgreSQL using pgx (planned)
//   - Custom: Redis, in-memory, or any other storage backend
type InboxRepository interface {
	// TryStart attempts to mark a message as "processing" in the inbox.
	//
	// This method provides atomic "check-and-insert" semantics to ensure
	// that each message is processed exactly once, even under concurrent conditions.
	//
	// Returns:
	//   - (true, nil): Message successfully marked as processing (first time)
	//   - (false, nil): Message already exists (duplicate, safe to skip)
	//   - (false, error): Storage error occurred
	//
	// Atomicity:
	//   - Must use INSERT ... ON CONFLICT DO NOTHING (or equivalent)
	//   - Concurrent calls for same (consumer_name, message_id) must be serialized
	//   - Only one caller should receive (true, nil)
	//
	// Example:
	//   ok, err := repo.TryStart(ctx, "EmailHandler", "msg-123")
	//   if err != nil {
	//       return fmt.Errorf("inbox check failed: %w", err)
	//   }
	//   if !ok {
	//       log.Info("duplicate message detected, skipping")
	//       return nil
	//   }
	//   // Proceed with message processing...
	TryStart(ctx context.Context, consumerName, messageID string) (bool, error)

	// Complete marks a message as "completed" in the inbox.
	//
	// This should be called after successful message processing.
	// If the message doesn't exist, this is a no-op (idempotent).
	//
	// Parameters:
	//   - consumerName: The name of the consumer/handler
	//   - messageID: The unique message identifier
	//
	// Returns:
	//   - nil: Success (or message doesn't exist)
	//   - error: Storage error occurred
	//
	// Example:
	//   if err := repo.Complete(ctx, "EmailHandler", "msg-123"); err != nil {
	//       return fmt.Errorf("failed to mark message as completed: %w", err)
	//   }
	Complete(ctx context.Context, consumerName, messageID string) error

	// GetByMessageID retrieves an inbox record by consumer name and message ID.
	// Returns ErrNotFound if the record doesn't exist.
	GetByMessageID(ctx context.Context, consumerName, messageID string) (*ConsumerInbox, error)
}

// InboxCleaner provides methods to clean up old inbox records.
// Implement this interface to prevent unbounded table growth.
type InboxCleaner interface {
	// DeleteOlderThan deletes inbox records with the given status
	// that are older than the specified duration.
	//
	// Recommendation:
	//   - Completed messages: 7-30 days retention (for audit trail)
	//   - Processing messages: Keep longer (30-90 days) for crash investigation
	//
	// Returns the number of deleted records.
	//
	// Example:
	//   // Delete completed messages older than 7 days
	//   deleted, err := repo.DeleteOlderThan(ctx, InboxStatusCompleted, 7*24*time.Hour)
	//   if err != nil {
	//       log.Error("cleanup failed", "error", err)
	//   } else {
	//       log.Info("cleanup completed", "deleted", deleted)
	//   }
	DeleteOlderThan(ctx context.Context, status InboxStatus, olderThan time.Duration) (int64, error)
}

// InboxStatus represents the processing state of a message in the inbox
type InboxStatus string

const (
	// InboxStatusProcessing indicates the message is currently being processed
	InboxStatusProcessing InboxStatus = "PROCESSING"

	// InboxStatusCompleted indicates the message has been successfully processed
	InboxStatusCompleted InboxStatus = "COMPLETED"
)

// String returns the string representation of InboxStatus
func (s InboxStatus) String() string {
	return string(s)
}

// ConsumerInbox represents a record in the consumer_inbox table
type ConsumerInbox struct {
	ConsumerName string
	MessageID    string
	Status       InboxStatus
	ReceivedAt   time.Time
	ProcessedAt  *time.Time
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
func (r *NopInboxRepository) DeleteOlderThan(ctx context.Context, status InboxStatus, olderThan time.Duration) (int64, error) {
	return 0, nil
}
