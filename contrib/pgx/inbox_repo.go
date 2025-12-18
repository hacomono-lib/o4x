package pgx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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
			WHERE consumer_name = $1 AND event_id = $2
		)`

	queryInboxComplete = `
		INSERT INTO %s (consumer_name, event_id, completed_at)
		VALUES ($1, $2, now())
		ON CONFLICT (consumer_name, event_id) DO NOTHING`

	queryInboxGetByEventID = `
		SELECT consumer_name, event_id, completed_at
		FROM %s
		WHERE consumer_name = $1 AND event_id = $2`

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
//	processed, _ := txRepo.IsProcessed(ctx, "OrderHandler", msg.EventID)
//	if processed { return nil }
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
	query := fmt.Sprintf(queryInboxCheckExists, r.tableName)

	var exists bool
	var err error

	if r.tx != nil {
		err = r.tx.QueryRow(ctx, query, consumerName, eventID).Scan(&exists)
	} else {
		err = r.pool.QueryRow(ctx, query, consumerName, eventID).Scan(&exists)
	}

	if err != nil {
		return false, fmt.Errorf("failed to check inbox record: %w", err)
	}

	// Return true if EXISTS (already completed, skip)
	// Return false if NOT EXISTS (not yet completed, proceed)
	return exists, nil
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
	query := fmt.Sprintf(queryInboxComplete, r.tableName)

	var err error
	if r.tx != nil {
		_, err = r.tx.Exec(ctx, query, consumerName, eventID)
	} else {
		_, err = r.pool.Exec(ctx, query, consumerName, eventID)
	}

	if err != nil {
		return fmt.Errorf("failed to insert inbox completion record: %w", err)
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
	query := fmt.Sprintf(queryInboxGetByEventID, r.tableName)

	var inbox core.ConsumerInbox

	var row pgx.Row
	if r.tx != nil {
		row = r.tx.QueryRow(ctx, query, consumerName, eventID)
	} else {
		row = r.pool.QueryRow(ctx, query, consumerName, eventID)
	}

	err := row.Scan(&inbox.ConsumerName, &inbox.EventID, &inbox.CompletedAt)

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
