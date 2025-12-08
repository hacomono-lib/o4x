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
	queryInboxCheckExisting = `
		SELECT consumer_name, message_id, status, received_at, processed_at
		FROM %s
		WHERE consumer_name = $1 AND message_id = $2`

	queryInboxInsert = `
		INSERT INTO %s (consumer_name, message_id, status, received_at)
		VALUES ($1, $2, 'PROCESSING', now())
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
//	txRepo.Complete(ctx, "OrderHandler", msg.MessageID)
//	tx.Commit(ctx)
func (r *InboxRepository) WithTx(tx pgx.Tx) *InboxRepository {
	return &InboxRepository{
		pool:      r.pool,
		tx:        tx,
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
	query := fmt.Sprintf(queryInboxCheckExisting, r.tableName)

	var existing core.ConsumerInbox
	var processedAt *time.Time

	// 1. Check if record already exists
	var row pgx.Row
	if r.tx != nil {
		row = r.tx.QueryRow(ctx, query, consumerName, messageID)
	} else {
		row = r.pool.QueryRow(ctx, query, consumerName, messageID)
	}

	err := row.Scan(&existing.ConsumerName, &existing.MessageID, &existing.Status, &existing.ReceivedAt, &processedAt)

	if err == nil {
		// Record exists - check status
		if existing.Status == core.InboxStatusCompleted {
			// Already completed - duplicate message
			return false, nil
		}
		// Still processing - retry scenario (handler failed previously)
		return true, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("failed to check existing inbox record: %w", err)
	}

	// 2. Record doesn't exist - create new one
	insertQuery := fmt.Sprintf(queryInboxInsert, r.tableName)

	if r.tx != nil {
		err = r.tx.QueryRow(ctx, insertQuery, consumerName, messageID).
			Scan(&existing.ConsumerName, &existing.MessageID, &existing.Status, &existing.ReceivedAt, &processedAt)
	} else {
		err = r.pool.QueryRow(ctx, insertQuery, consumerName, messageID).
			Scan(&existing.ConsumerName, &existing.MessageID, &existing.Status, &existing.ReceivedAt, &processedAt)
	}

	if err != nil {
		// Check for unique constraint violation (race condition)
		// Another goroutine might have inserted the record between our check and insert
		var row2 pgx.Row
		if r.tx != nil {
			row2 = r.tx.QueryRow(ctx, query, consumerName, messageID)
		} else {
			row2 = r.pool.QueryRow(ctx, query, consumerName, messageID)
		}

		var existing2 core.ConsumerInbox
		err2 := row2.Scan(&existing2.ConsumerName, &existing2.MessageID, &existing2.Status, &existing2.ReceivedAt, &processedAt)

		if err2 == nil {
			// Record was created by another goroutine - check its status
			return existing2.Status != core.InboxStatusCompleted, nil
		}

		return false, fmt.Errorf("failed to insert inbox record: %w", err)
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
