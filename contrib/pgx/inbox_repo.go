package pgx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/core"
)

// SQL queries for consumer_inbox table
const (
	queryInboxCheckExists = `
		SELECT EXISTS(
			SELECT 1 FROM %s
			WHERE consumer_name = $1 AND message_id = $2
		)`

	queryInboxComplete = `
		INSERT INTO %s (consumer_name, message_id, completed_at)
		VALUES ($1, $2, now())
		ON CONFLICT (consumer_name, message_id) DO NOTHING`

	queryInboxGetByMessageID = `
		SELECT consumer_name, message_id, completed_at
		FROM %s
		WHERE consumer_name = $1 AND message_id = $2`

	queryInboxDeleteOlderThan = `
		DELETE FROM %s
		WHERE completed_at < now() - $1::interval`
)

// InboxRepository implements core.InboxRepository and core.InboxCleaner for pgx.
type InboxRepository struct {
	pool      *pgxpool.Pool
	tx        pgx.Tx // nil if not in transaction
	tableName string
}

// NewInboxRepository creates a new pgx inbox repository.
func NewInboxRepository(pool *pgxpool.Pool, opts ...Option) *InboxRepository {
	cfg := applyOptions(opts...)

	// Validate table name to prevent SQL injection
	if err := core.ValidateTableName(cfg.InboxTableName); err != nil {
		panic(fmt.Sprintf("invalid inbox table name %q: %v", cfg.InboxTableName, err))
	}

	return &InboxRepository{
		pool:      pool,
		tableName: cfg.InboxTableName,
	}
}

// WithTx returns a new InboxRepository that uses the given transaction.
// Use this to integrate inbox checking within application transactions.
//
// Example:
//
//	tx, _ := pool.Begin(ctx)
//	defer tx.Rollback(ctx)
//
//	txRepo := repo.WithTx(tx)
//	ok, _ := txRepo.TryStart(ctx, "OrderHandler", msg.MessageID)
//	if !ok { return nil }
//
//	// Business logic
//	tx.Exec(ctx, "INSERT INTO orders ...")
//
//	tx.Commit(ctx)
func (r *InboxRepository) WithTx(tx pgx.Tx) *InboxRepository {
	return &InboxRepository{
		pool:      r.pool,
		tx:        tx,
		tableName: r.tableName,
	}
}

// TryStart checks if this message has already been processed.
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
	query := fmt.Sprintf(queryInboxCheckExists, r.tableName)

	var exists bool
	var err error

	if r.tx != nil {
		err = r.tx.QueryRow(ctx, query, consumerName, messageID).Scan(&exists)
	} else {
		err = r.pool.QueryRow(ctx, query, consumerName, messageID).Scan(&exists)
	}

	if err != nil {
		return false, fmt.Errorf("failed to check inbox record: %w", err)
	}

	// Return true if NOT exists (should proceed)
	// Return false if exists (already completed, skip)
	return !exists, nil
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
	query := fmt.Sprintf(queryInboxComplete, r.tableName)

	var err error
	if r.tx != nil {
		_, err = r.tx.Exec(ctx, query, consumerName, messageID)
	} else {
		_, err = r.pool.Exec(ctx, query, consumerName, messageID)
	}

	if err != nil {
		return fmt.Errorf("failed to insert inbox completion record: %w", err)
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
	query := fmt.Sprintf(queryInboxGetByMessageID, r.tableName)

	var inbox core.ConsumerInbox

	var row pgx.Row
	if r.tx != nil {
		row = r.tx.QueryRow(ctx, query, consumerName, messageID)
	} else {
		row = r.pool.QueryRow(ctx, query, consumerName, messageID)
	}

	err := row.Scan(&inbox.ConsumerName, &inbox.MessageID, &inbox.CompletedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, core.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get inbox record: %w", err)
	}

	return &inbox, nil
}

// DeleteOlderThan deletes inbox records older than the specified duration.
//
// Cleanup Recommendations:
//   - Retention: 7-30 days (for audit trail and debugging)
//   - Run as scheduled job (daily or weekly)
//
// Returns the number of deleted records.
func (r *InboxRepository) DeleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error) {
	query := fmt.Sprintf(queryInboxDeleteOlderThan, r.tableName)

	// Convert Go duration to PostgreSQL interval format
	intervalStr := fmt.Sprintf("%d seconds", int64(olderThan.Seconds()))

	var tag pgconn.CommandTag
	var err error

	if r.tx != nil {
		tag, err = r.tx.Exec(ctx, query, intervalStr)
	} else {
		tag, err = r.pool.Exec(ctx, query, intervalStr)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to delete old inbox records: %w", err)
	}

	return tag.RowsAffected(), nil
}
