package gorm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/hacomono-lib/o4x/core"
)

// consumerInboxModel is the GORM model for consumer_inbox table.
//
// Design Notes:
//   - Primary Key: (consumer_name, event_id) provides natural idempotency
//   - No status field: existence = completed
//   - CompletedAt: When TryStart was first called (immutable)
//   - CRITICAL: EventID is Outbox ID, NOT SQS MessageID
//
// Thread-Safety:
//   - INSERT ON CONFLICT ensures atomic duplicate detection
//   - No locks needed - SQS visibility timeout handles concurrency
type consumerInboxModel struct {
	ConsumerName string    `gorm:"primaryKey;column:consumer_name;type:varchar(255);not null"`
	EventID      uuid.UUID `gorm:"primaryKey;column:event_id;type:uuid;not null"`
	CompletedAt  time.Time `gorm:"column:completed_at;type:timestamptz;not null;default:now()"`
}

// InboxRepository implements core.InboxRepository and core.InboxCleaner for GORM.
type InboxRepository struct {
	db        *gorm.DB
	tableName string
}

// NewInboxRepository creates a new GORM inbox repository.
//
// Options:
//   - WithInboxTableName: Customize table name (default: "consumer_inbox")
//
// Example:
//
//	repo := gorm.NewInboxRepository(db, gorm.WithInboxTableName("my_inbox"))
func NewInboxRepository(db *gorm.DB, opts ...Option) *InboxRepository {
	cfg := applyOptions(opts...)

	// Validate table name to prevent SQL injection
	if err := core.ValidateTableName(cfg.InboxTableName); err != nil {
		panic(fmt.Sprintf("invalid inbox table name %q: %v", cfg.InboxTableName, err))
	}

	return &InboxRepository{
		db:        db,
		tableName: cfg.InboxTableName,
	}
}

// WithTx returns a new InboxRepository that uses the given transaction.
// Use this to integrate inbox checking within application transactions.
//
// Example:
//
//	tx := db.Begin()
//	defer tx.Rollback()
//
//	txRepo := repo.WithTx(tx)
//	processed, _ := txRepo.IsProcessed(ctx, "OrderHandler", msg.EventID)
//	if processed {
//	    return nil
//	}
//
//	// Business logic
//	tx.Create(&order)
//
//	tx.Commit()
func (r *InboxRepository) WithTx(tx *gorm.DB) *InboxRepository {
	return &InboxRepository{
		db:        tx,
		tableName: r.tableName,
	}
}

// IsProcessed checks if this event has already been completed.
//
// **CRITICAL: Inbox Only Tracks Completion**
//
// IsProcessed does NOT provide:
//   - Exclusivity or mutual exclusion
//   - In-flight tracking or locking
//   - Retry or state management
//
// This is intentional and correct:
//   - Primary control: SQS visibility timeout prevents concurrent processing
//   - The inbox table represents **completed events only**
//   - In-flight processing is controlled by the message broker (SQS visibility timeout)
//   - The only definitive point is Complete()
//
// **CRITICAL: Uses event_id (Outbox ID), NOT SQS MessageID**
//
// Canonical Implementation:
//
//	SELECT EXISTS(
//	    SELECT 1 FROM consumer_inbox
//	    WHERE consumer_name = ? AND event_id = ?
//	)
//
// Behavior:
//   - Record EXISTS: return true (already completed, skip processing)
//   - Record NOT EXISTS: return false (not yet completed, proceed)
//
// No locking, no status checking, no retry logic.
// SQS visibility timeout handles concurrency.
// SQS redelivery handles retries.
//
// Returns:
//   - (true, nil): Already completed, skip processing
//   - (false, nil): Not yet completed, proceed with processing
//   - (_, error): Database error occurred
func (r *InboxRepository) IsProcessed(ctx context.Context, consumerName string, eventID uuid.UUID) (bool, error) {
	var count int64

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("consumer_name = ? AND event_id = ?", consumerName, eventID).
		Count(&count)

	if result.Error != nil {
		return false, fmt.Errorf("failed to check inbox record: %w", result.Error)
	}

	// Return true if EXISTS (already completed, skip)
	// Return false if NOT EXISTS (not yet completed, proceed)
	return count > 0, nil
}

// Complete records successful event processing.
//
// **CRITICAL: Uses event_id (Outbox ID), NOT SQS MessageID**
//
// Canonical Implementation:
//
//	INSERT INTO consumer_inbox (consumer_name, event_id, completed_at)
//	VALUES (?, ?, NOW())
//	ON CONFLICT (consumer_name, event_id) DO NOTHING
//
// Behavior:
//   - First call: INSERT succeeds, record created
//   - Subsequent calls: INSERT conflicts, no-op (idempotent)
//
// This method should be called after successful event processing.
// Idempotent design ensures safety even if multiple workers call it.
//
// Returns:
//   - nil: Success (record inserted or already exists)
//   - error: Database error occurred
func (r *InboxRepository) Complete(ctx context.Context, consumerName string, eventID uuid.UUID) error {
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, event_id, completed_at)
		VALUES (?, ?, NOW())
		ON CONFLICT (consumer_name, event_id) DO NOTHING
	`, r.tableName)

	result := r.db.WithContext(ctx).Exec(insertQuery, consumerName, eventID)
	if result.Error != nil {
		return fmt.Errorf("failed to insert inbox completion record: %w", result.Error)
	}

	return nil
}

// GetByEventID retrieves an inbox record by consumer name and event ID.
//
// **CRITICAL: Uses event_id (Outbox ID), NOT SQS MessageID**
//
// Returns:
//   - (*ConsumerInbox, nil): Record found (event completed)
//   - (nil, ErrNotFound): Record doesn't exist (event not yet completed)
//   - (nil, error): Database error occurred
func (r *InboxRepository) GetByEventID(ctx context.Context, consumerName string, eventID uuid.UUID) (*core.ConsumerInbox, error) {
	var model consumerInboxModel

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Select("consumer_name, event_id, completed_at").
		Where("consumer_name = ? AND event_id = ?", consumerName, eventID).
		First(&model)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, core.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get inbox record: %w", result.Error)
	}

	return r.modelToCore(&model), nil
}

// DeleteOlderThan deletes inbox records older than the specified duration.
//
// Cleanup Recommendations:
//   - Retention: 7-30 days (for audit trail and debugging)
//   - Run as scheduled job (daily or weekly)
//
// Returns the number of deleted records.
//
// Example:
//
//	// Daily cleanup job
//	deleted, err := repo.DeleteOlderThan(ctx, 7*24*time.Hour)
//	log.Info("cleanup completed", "deleted", deleted)
//
// SQL Example (PostgreSQL):
//
//	DELETE FROM consumer_inbox
//	WHERE completed_at < NOW() - INTERVAL '7 days';
func (r *InboxRepository) DeleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error) {
	// Convert Go duration to PostgreSQL interval format
	// This ensures timezone consistency by using PostgreSQL's NOW() function
	intervalStr := fmt.Sprintf("%d seconds", int64(olderThan.Seconds()))

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("completed_at < NOW() - ?::interval", intervalStr).
		Delete(&consumerInboxModel{})

	if result.Error != nil {
		return 0, fmt.Errorf("failed to delete old inbox records: %w", result.Error)
	}

	return result.RowsAffected, nil
}

// modelToCore converts GORM model to core.ConsumerInbox
func (r *InboxRepository) modelToCore(m *consumerInboxModel) *core.ConsumerInbox {
	return &core.ConsumerInbox{
		ConsumerName: m.ConsumerName,
		EventID:      m.EventID,
		CompletedAt:  m.CompletedAt,
	}
}
