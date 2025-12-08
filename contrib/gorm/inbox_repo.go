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
//   - No separate UUID id needed - composite PK is sufficient
//   - Status tracks lifecycle: PROCESSING -> COMPLETED
//   - ReceivedAt: When TryStart was first called (immutable)
//   - ProcessedAt: When Complete was called (NULL until COMPLETED)
//
// Thread-Safety:
//   - INSERT ON CONFLICT ensures atomic duplicate detection
//   - No explicit locks needed at application level
type consumerInboxModel struct {
	ConsumerName string     `gorm:"primaryKey;type:varchar(255);not null"`
	MessageID    string     `gorm:"primaryKey;type:varchar(255);not null"`
	Status       string     `gorm:"type:varchar(20);not null;default:'PROCESSING'"`
	ReceivedAt   time.Time  `gorm:"type:timestamptz;not null;default:now()"`
	ProcessedAt  *time.Time `gorm:"type:timestamptz"`
}

// InboxRepository implements core.InboxRepository and core.InboxCleaner for GORM.
//
// Transactional Inbox Pattern:
//   - Ensures exactly-once message processing semantics
//   - Uses PostgreSQL's ON CONFLICT for atomic idempotency checking
//   - Safe for concurrent message processing
//
// Usage:
//
//	db, _ := gorm.Open(postgres.Open(dsn))
//	inboxRepo := gorm.NewInboxRepository(db)
//
//	// In consumer handler
//	ok, err := inboxRepo.TryStart(ctx, "OrderHandler", msg.MessageID)
//	if !ok {
//	    return nil // Duplicate message
//	}
//	// Process message...
//	inboxRepo.Complete(ctx, "OrderHandler", msg.MessageID)
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
//	// Check idempotency in transaction
//	txRepo := repo.WithTx(tx)
//	ok, _ := txRepo.TryStart(ctx, "OrderHandler", msg.MessageID)
//	if !ok {
//	    return nil
//	}
//
//	// Business logic
//	tx.Create(&order)
//
//	// Mark as completed
//	txRepo.Complete(ctx, "OrderHandler", msg.MessageID)
//	tx.Commit()
func (r *InboxRepository) WithTx(tx *gorm.DB) *InboxRepository {
	return &InboxRepository{
		db:        tx,
		tableName: r.tableName,
	}
}

// TryStart attempts to mark a message as "PROCESSING" in the inbox.
//
// Implementation:
//   - First checks if record exists
//   - If exists and status=COMPLETED -> returns (false, nil) - already processed
//   - If exists and status=PROCESSING -> returns (true, nil) - retry scenario
//   - If not exists -> INSERT -> returns (true, nil) - first time
//
// This correctly handles retry scenarios:
//  1. Initial call: INSERT success -> true
//  2. Handler fails (e.g., external API) -> error returned
//  3. SQS retry: Existing record with PROCESSING status -> true (process again)
//  4. Handler succeeds -> Complete() called -> status=COMPLETED
//  5. Next duplicate: Existing record with COMPLETED status -> false (skip)
//
// Returns:
//   - (true, nil): Should proceed with processing (first time OR retry)
//   - (false, nil): Already COMPLETED, safe to skip
//   - (false, error): Database error occurred
func (r *InboxRepository) TryStart(ctx context.Context, consumerName, messageID string) (bool, error) {
	// 1. Check if record already exists
	var existing consumerInboxModel
	err := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("consumer_name = ? AND message_id = ?", consumerName, messageID).
		First(&existing).Error

	if err == nil {
		// Record exists - check status
		if existing.Status == string(core.InboxStatusCompleted) {
			// Already completed - duplicate message
			return false, nil
		}
		// Still processing - retry scenario (handler failed previously)
		return true, nil
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, fmt.Errorf("failed to check existing inbox record: %w", err)
	}

	// 2. Record doesn't exist - create new one
	model := &consumerInboxModel{
		ConsumerName: consumerName,
		MessageID:    messageID,
		Status:       string(core.InboxStatusProcessing),
		ReceivedAt:   time.Now(),
	}

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Create(model)

	if result.Error != nil {
		// Check for unique constraint violation (race condition)
		// Another goroutine might have inserted the record between our check and insert
		var existing2 consumerInboxModel
		err := r.db.WithContext(ctx).
			Table(r.tableName).
			Where("consumer_name = ? AND message_id = ?", consumerName, messageID).
			First(&existing2).Error

		if err == nil {
			// Record was created by another goroutine - check its status
			return existing2.Status != string(core.InboxStatusCompleted), nil
		}

		return false, fmt.Errorf("failed to insert inbox record: %w", result.Error)
	}

	// Successfully created - first time processing
	return true, nil
}

// Complete marks a message as "COMPLETED" in the inbox.
//
// Implementation:
//   - Updates status to "COMPLETED" and sets processed_at timestamp
//   - If record doesn't exist, this is a no-op (returns nil)
//   - Idempotent: calling multiple times has no additional effect
//
// Returns:
//   - nil: Success (or record doesn't exist)
//   - error: Database error occurred
//
// SQL Example:
//
//	UPDATE consumer_inbox
//	SET status = 'COMPLETED', processed_at = NOW()
//	WHERE consumer_name = 'OrderHandler' AND message_id = 'msg-123';
func (r *InboxRepository) Complete(ctx context.Context, consumerName, messageID string) error {
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("consumer_name = ? AND message_id = ?", consumerName, messageID).
		Updates(map[string]interface{}{
			"status":       string(core.InboxStatusCompleted),
			"processed_at": gorm.Expr("NOW()"),
		})

	if result.Error != nil {
		return fmt.Errorf("failed to update inbox record to completed: %w", result.Error)
	}

	// Note: We don't check RowsAffected here because Complete is idempotent
	// If the record doesn't exist, that's fine (maybe it was cleaned up)
	return nil
}

// GetByMessageID retrieves an inbox record by consumer name and message ID.
//
// Returns:
//   - (*ConsumerInbox, nil): Record found
//   - (nil, ErrNotFound): Record doesn't exist
//   - (nil, error): Database error occurred
func (r *InboxRepository) GetByMessageID(ctx context.Context, consumerName, messageID string) (*core.ConsumerInbox, error) {
	var model consumerInboxModel

	result := r.db.WithContext(ctx).
		Table(r.tableName).
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

// DeleteOlderThan deletes inbox records with the given status older than the specified duration.
//
// Cleanup Recommendations:
//   - COMPLETED messages: 7-30 days retention (for audit trail)
//   - PROCESSING messages: Keep longer (30-90 days) for crash investigation
//
// Returns the number of deleted records.
//
// Example:
//
//	// Daily cleanup job
//	deleted, err := repo.DeleteOlderThan(ctx, core.InboxStatusCompleted, 7*24*time.Hour)
//	log.Info("cleanup completed", "deleted", deleted)
//
// SQL Example (PostgreSQL):
//
//	DELETE FROM consumer_inbox
//	WHERE status = 'COMPLETED'
//	  AND received_at < NOW() - INTERVAL '7 days';
func (r *InboxRepository) DeleteOlderThan(ctx context.Context, status core.InboxStatus, olderThan time.Duration) (int64, error) {
	// Convert Go duration to PostgreSQL interval format
	// This ensures timezone consistency by using PostgreSQL's NOW() function
	intervalStr := fmt.Sprintf("%d seconds", int64(olderThan.Seconds()))

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("status = ? AND received_at < NOW() - ?::interval", string(status), intervalStr).
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
		Status:       core.InboxStatus(m.Status),
		ReceivedAt:   m.ReceivedAt,
		ProcessedAt:  m.ProcessedAt,
	}
}
