package gorm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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
	db                  *gorm.DB
	tableName           string
	stuckInboxThreshold time.Duration
}

// NewInboxRepository creates a new GORM inbox repository.
//
// Options:
//   - WithInboxTableName: Customize table name (default: "consumer_inbox")
//   - WithStuckInboxThreshold: Set stuck message threshold (default: 2 minutes)
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
		db:                  db,
		tableName:           cfg.InboxTableName,
		stuckInboxThreshold: cfg.StuckInboxThreshold,
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
		db:                  tx,
		tableName:           r.tableName,
		stuckInboxThreshold: r.stuckInboxThreshold,
	}
}

// TryStart attempts to mark a message as "PROCESSING" in the inbox.
//
// Implementation Strategy:
//  1. Try INSERT first with ON CONFLICT DO NOTHING (optimistic path for new messages)
//  2. If INSERT conflicts, check existing record status
//
// Behavior:
//   - First message: INSERT succeeds -> returns (true, nil)
//   - Duplicate (COMPLETED): Returns (false, nil) - skip processing
//   - Duplicate (PROCESSING): Returns (true, nil) - allow retry
//
// Returns:
//   - (true, nil): Should proceed with processing
//   - (false, nil): Already completed (duplicate message)
//   - (false, error): Database error occurred
func (r *InboxRepository) TryStart(ctx context.Context, consumerName, messageID string) (bool, error) {
	// Optimistic path: Try INSERT first (most messages are new)
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, message_id, status, received_at)
		VALUES (?, ?, 'PROCESSING', NOW())
		ON CONFLICT (consumer_name, message_id) DO NOTHING
	`, r.tableName)

	result := r.db.WithContext(ctx).Exec(insertQuery, consumerName, messageID)
	if result.Error != nil {
		return false, fmt.Errorf("failed to insert inbox record: %w", result.Error)
	}

	// INSERT succeeded (1 row affected) - first time processing
	if result.RowsAffected == 1 {
		return true, nil
	}

	// INSERT conflicted (0 rows affected) - record already exists
	// Check its status with exclusive lock to prevent concurrent processing
	// Defense in depth: SQS visibility timeout is primary control, this is secondary
	lockQuery := fmt.Sprintf(`
		SELECT consumer_name, message_id, status, received_at, processed_at
		FROM %s
		WHERE consumer_name = ? AND message_id = ?
		FOR UPDATE NOWAIT
	`, r.tableName)

	var existing consumerInboxModel
	err := r.db.WithContext(ctx).Raw(lockQuery, consumerName, messageID).Scan(&existing).Error

	// Lock acquired successfully
	if err == nil {
		// Check status
		if existing.Status == string(core.InboxStatusCompleted) {
			// Already completed - duplicate message
			return false, nil
		}

		// PROCESSING state - check if stuck (crashed handler)
		if existing.Status == string(core.InboxStatusProcessing) {
			age := time.Since(existing.ReceivedAt)
			if age > r.stuckInboxThreshold {
				// Stuck message (handler likely crashed) - allow retry
				// The lock ensures only one worker will retry at a time
				return true, nil
			}
			// Recent PROCESSING - another worker is processing
			// Reject to be safe (shouldn't happen with correct visibility timeout)
			return false, nil
		}

		// Unknown status - allow processing
		return true, nil
	}

	// Check for lock timeout error (concurrent processing)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55P03" { // lock_not_available
		// Another worker is currently processing this message
		// Return false to skip (the other worker will handle it)
		return false, nil
	}

	// Check for context cancellation or deadline
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false, nil
	}

	// Check for record not found (shouldn't happen, but handle gracefully)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Race condition: record was deleted between INSERT and SELECT
		// This is very rare but possible. Return false to skip.
		return false, nil
	}

	// Unexpected error
	return false, fmt.Errorf("failed to lock inbox record: %w", err)
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
