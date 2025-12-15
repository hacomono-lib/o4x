package gorm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/hacomono-lib/o4x/core"
)

// consumerInboxModel is the GORM model for consumer_inbox table.
//
// Design Notes:
//   - Primary Key: (consumer_name, message_id) provides natural idempotency
//   - No status field: existence = completed
//   - CompletedAt: When TryStart was first called (immutable)
//
// Thread-Safety:
//   - INSERT ON CONFLICT ensures atomic duplicate detection
//   - No locks needed - SQS visibility timeout handles concurrency
type consumerInboxModel struct {
	ConsumerName string    `gorm:"primaryKey;column:consumer_name;type:varchar(255);not null"`
	MessageID    string    `gorm:"primaryKey;column:message_id;type:varchar(255);not null"`
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
//	ok, _ := txRepo.TryStart(ctx, "OrderHandler", msg.MessageID)
//	if !ok {
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

// TryStart checks if this message has already been processed.
//
// **CRITICAL: TryStart is NOT Exclusive**
//
// TryStart does NOT provide exclusivity or mutual exclusion semantics.
// Multiple consumer workers MAY pass TryStart() concurrently for the same message.
//
// This behavior is intentional and correct:
//   - Primary control: SQS visibility timeout prevents concurrent processing
//   - The inbox table represents **completed messages only**
//   - In-flight processing is controlled by the message broker (SQS visibility timeout)
//   - The only definitive point is Complete()
//
// Canonical Implementation:
//
//	SELECT EXISTS(
//	    SELECT 1 FROM consumer_inbox
//	    WHERE consumer_name = ? AND message_id = ?
//	)
//
// Behavior:
//   - Record NOT EXISTS: return true (proceed with processing)
//   - Record EXISTS: return false (already completed, skip processing)
//
// No locking, no status checking, no retry logic.
// SQS visibility timeout handles concurrency.
// SQS redelivery handles retries.
//
// Returns:
//   - (true, nil): Should proceed with processing
//   - (false, nil): Already completed, skip processing
//   - (false, error): Database error occurred
func (r *InboxRepository) TryStart(ctx context.Context, consumerName, messageID string) (bool, error) {
	var count int64

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("consumer_name = ? AND message_id = ?", consumerName, messageID).
		Count(&count)

	if result.Error != nil {
		return false, fmt.Errorf("failed to check inbox record: %w", result.Error)
	}

	// Return true if NOT exists (should proceed)
	// Return false if exists (already completed, skip)
	return count == 0, nil
}

// Complete records successful message processing.
//
// Canonical Implementation:
//
//	INSERT INTO consumer_inbox (consumer_name, message_id, completed_at)
//	VALUES (?, ?, NOW())
//	ON CONFLICT (consumer_name, message_id) DO NOTHING
//
// Behavior:
//   - First call: INSERT succeeds, record created
//   - Subsequent calls: INSERT conflicts, no-op (idempotent)
//
// This method should be called after successful message processing.
// Idempotent design ensures safety even if multiple workers call it.
//
// Returns:
//   - nil: Success (record inserted or already exists)
//   - error: Database error occurred
func (r *InboxRepository) Complete(ctx context.Context, consumerName, messageID string) error {
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, message_id, completed_at)
		VALUES (?, ?, NOW())
		ON CONFLICT (consumer_name, message_id) DO NOTHING
	`, r.tableName)

	result := r.db.WithContext(ctx).Exec(insertQuery, consumerName, messageID)
	if result.Error != nil {
		return fmt.Errorf("failed to insert inbox completion record: %w", result.Error)
	}

	return nil
}

// GetByMessageID retrieves an inbox record by consumer name and message ID.
//
// Returns:
//   - (*ConsumerInbox, nil): Record found (message completed)
//   - (nil, ErrNotFound): Record doesn't exist (message not yet completed)
//   - (nil, error): Database error occurred
func (r *InboxRepository) GetByMessageID(ctx context.Context, consumerName, messageID string) (*core.ConsumerInbox, error) {
	var model consumerInboxModel

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Select("consumer_name, message_id, completed_at").
		Where("consumer_name = ? AND message_id = ?", consumerName, messageID).
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
		MessageID:    m.MessageID,
		CompletedAt:  m.CompletedAt,
	}
}
