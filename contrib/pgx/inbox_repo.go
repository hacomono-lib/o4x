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
	queryInboxTryStart = `
		INSERT INTO %s (consumer_name, message_id, status, received_at)
		VALUES ($1, $2, 'PROCESSING', now())
		ON CONFLICT (consumer_name, message_id) DO NOTHING
		RETURNING consumer_name, message_id, status, received_at, processed_at`

	queryInboxComplete = `
		UPDATE %s
		SET status = 'COMPLETED', processed_at = now()
		WHERE consumer_name = $1 AND message_id = $2`

	queryInboxGetByMessageID = `
		SELECT consumer_name, message_id, status, received_at, processed_at
		FROM %s
		WHERE consumer_name = $1 AND message_id = $2`

	queryInboxDeleteOlderThan = `
		DELETE FROM %s
		WHERE status = $1 AND received_at < now() - $2::interval`
)

// InboxRepository implements core.InboxRepository and core.InboxCleaner for pgx.
type InboxRepository struct {
	pool                *pgxpool.Pool
	tx                  pgx.Tx // nil if not in transaction
	tableName           string
	stuckInboxThreshold time.Duration
}

// NewInboxRepository creates a new pgx inbox repository.
func NewInboxRepository(pool *pgxpool.Pool, opts ...Option) *InboxRepository {
	cfg := applyOptions(opts...)

	// Validate table name to prevent SQL injection
	if err := core.ValidateTableName(cfg.InboxTableName); err != nil {
		panic(fmt.Sprintf("invalid inbox table name %q: %v", cfg.InboxTableName, err))
	}

	return &InboxRepository{
		pool:                pool,
		tableName:           cfg.InboxTableName,
		stuckInboxThreshold: cfg.StuckInboxThreshold,
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
//	txRepo.Complete(ctx, "OrderHandler", msg.MessageID)
//	tx.Commit(ctx)
func (r *InboxRepository) WithTx(tx pgx.Tx) *InboxRepository {
	return &InboxRepository{
		pool:                r.pool,
		tx:                  tx,
		tableName:           r.tableName,
		stuckInboxThreshold: r.stuckInboxThreshold,
	}
}

// TryStart attempts to mark a message as "PROCESSING" in the inbox.
//
// Implementation Strategy (ATOMIC with Deadlock Prevention):
//  1. Try INSERT first (optimistic path for new messages)
//  2. If INSERT conflicts, try SELECT FOR UPDATE NOWAIT to check existing record
//  3. FOR UPDATE NOWAIT prevents concurrent processing and deadlocks
//
// Behavior:
//   - First message: INSERT succeeds -> returns (true, nil)
//   - Duplicate (COMPLETED): Returns (false, nil) - skip processing
//   - Duplicate (PROCESSING, recent): Returns (false, nil) - another worker is processing
//   - Duplicate (PROCESSING, stuck): Returns (true, nil) - allow crash recovery
//   - Concurrent requests: First gets lock, others get lock error -> returns (false, nil)
//
// Crash Recovery:
//   - If a record is stuck in PROCESSING state longer than StuckInboxThreshold,
//     it's considered crashed and retry is allowed.
//   - Default: 2 minutes (should be 2x your maximum handler processing time)
//
// Returns:
//   - (true, nil): Should proceed with processing
//   - (false, nil): Duplicate or currently being processed, safe to skip
//   - (false, error): Database error occurred
func (r *InboxRepository) TryStart(ctx context.Context, consumerName, messageID string) (bool, error) {
	// Optimistic path: Try INSERT first (most messages are new)
	insertQuery := fmt.Sprintf(queryInboxTryStart, r.tableName)

	var inserted core.ConsumerInbox
	var processedAt *time.Time
	var err error

	if r.tx != nil {
		err = r.tx.QueryRow(ctx, insertQuery, consumerName, messageID).
			Scan(&inserted.ConsumerName, &inserted.MessageID, &inserted.Status, &inserted.ReceivedAt, &processedAt)
	} else {
		err = r.pool.QueryRow(ctx, insertQuery, consumerName, messageID).
			Scan(&inserted.ConsumerName, &inserted.MessageID, &inserted.Status, &inserted.ReceivedAt, &processedAt)
	}

	// INSERT succeeded - first time processing
	if err == nil {
		return true, nil
	}

	// INSERT failed due to conflict (pgx.ErrNoRows = no RETURNING data from ON CONFLICT DO NOTHING)
	if errors.Is(err, pgx.ErrNoRows) {
		// Record already exists - check its status with exclusive lock to prevent concurrent processing
		lockQuery := fmt.Sprintf(`
			SELECT consumer_name, message_id, status, received_at, processed_at
			FROM %s
			WHERE consumer_name = $1 AND message_id = $2
			FOR UPDATE NOWAIT
		`, r.tableName)

		var existing core.ConsumerInbox

		var row pgx.Row
		if r.tx != nil {
			row = r.tx.QueryRow(ctx, lockQuery, consumerName, messageID)
		} else {
			row = r.pool.QueryRow(ctx, lockQuery, consumerName, messageID)
		}

		err2 := row.Scan(&existing.ConsumerName, &existing.MessageID, &existing.Status, &existing.ReceivedAt, &processedAt)

		// Lock acquired successfully
		if err2 == nil {
			// Check status
			if existing.Status == core.InboxStatusCompleted {
				// Already completed - duplicate message
				return false, nil
			}

			// PROCESSING state - check if stuck (crashed handler)
			if existing.Status == core.InboxStatusProcessing {
				age := time.Since(existing.ReceivedAt)
				if age > r.stuckInboxThreshold {
					// Stuck message (handler likely crashed) - allow retry
					// The lock ensures only one worker will retry at a time
					return true, nil
				}
				// Recent PROCESSING - this shouldn't happen if visibility timeout is set correctly
				// but another worker might have just picked it up. Reject to be safe.
				return false, nil
			}

			// Unknown status - allow processing
			return true, nil
		}

		// Check for lock timeout error (concurrent processing)
		var pgErr *pgconn.PgError
		if errors.As(err2, &pgErr) && pgErr.Code == "55P03" { // lock_not_available
			// Another worker is currently processing this message
			// Return false to skip (the other worker will handle it)
			return false, nil
		}

		// Check for record not found (shouldn't happen, but handle gracefully)
		if errors.Is(err2, pgx.ErrNoRows) {
			// Race condition: record was deleted between INSERT and SELECT
			// This is very rare but possible. Return false to skip.
			return false, nil
		}

		// Unexpected error during lock acquisition
		return false, fmt.Errorf("failed to lock existing inbox record: %w", err2)
	}

	// Unexpected error during INSERT
	return false, fmt.Errorf("failed to insert inbox record: %w", err)
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
func (r *InboxRepository) Complete(ctx context.Context, consumerName, messageID string) error {
	query := fmt.Sprintf(queryInboxComplete, r.tableName)

	var tag pgconn.CommandTag
	var err error

	if r.tx != nil {
		tag, err = r.tx.Exec(ctx, query, consumerName, messageID)
	} else {
		tag, err = r.pool.Exec(ctx, query, consumerName, messageID)
	}

	if err != nil {
		return fmt.Errorf("failed to update inbox record to completed: %w", err)
	}

	// Note: We don't check RowsAffected here because Complete is idempotent
	// If the record doesn't exist, that's fine (maybe it was cleaned up)
	_ = tag.RowsAffected()

	return nil
}

// GetByMessageID retrieves an inbox record by consumer name and message ID.
//
// Returns:
//   - (*ConsumerInbox, nil): Record found
//   - (nil, ErrNotFound): Record doesn't exist
//   - (nil, error): Database error occurred
func (r *InboxRepository) GetByMessageID(ctx context.Context, consumerName, messageID string) (*core.ConsumerInbox, error) {
	query := fmt.Sprintf(queryInboxGetByMessageID, r.tableName)

	var inbox core.ConsumerInbox
	var processedAt *time.Time

	var row pgx.Row
	if r.tx != nil {
		row = r.tx.QueryRow(ctx, query, consumerName, messageID)
	} else {
		row = r.pool.QueryRow(ctx, query, consumerName, messageID)
	}

	err := row.Scan(&inbox.ConsumerName, &inbox.MessageID, &inbox.Status, &inbox.ReceivedAt, &processedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, core.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get inbox record: %w", err)
	}

	inbox.ProcessedAt = processedAt
	return &inbox, nil
}

// DeleteOlderThan deletes inbox records with the given status older than the specified duration.
//
// Cleanup Recommendations:
//   - COMPLETED messages: 7-30 days retention (for audit trail)
//   - PROCESSING messages: Keep longer (30-90 days) for crash investigation
//
// Returns the number of deleted records.
func (r *InboxRepository) DeleteOlderThan(ctx context.Context, status core.InboxStatus, olderThan time.Duration) (int64, error) {
	query := fmt.Sprintf(queryInboxDeleteOlderThan, r.tableName)

	// Convert Go duration to PostgreSQL interval format
	intervalStr := fmt.Sprintf("%d seconds", int64(olderThan.Seconds()))

	var tag pgconn.CommandTag
	var err error

	if r.tx != nil {
		tag, err = r.tx.Exec(ctx, query, string(status), intervalStr)
	} else {
		tag, err = r.pool.Exec(ctx, query, string(status), intervalStr)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to delete old inbox records: %w", err)
	}

	return tag.RowsAffected(), nil
}
