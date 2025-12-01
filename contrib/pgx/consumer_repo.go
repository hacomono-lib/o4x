package pgx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
	"github.com/hacomono-lib/o4x/core"
)

// ConsumerRepository implements consumer.Repository for PostgreSQL using pgx
type ConsumerRepository struct {
	pool      *pgxpool.Pool
	tableName string
}

// NewConsumerRepository creates a new PostgreSQL consumer repository
func NewConsumerRepository(pool *pgxpool.Pool, opts ...Option) *ConsumerRepository {
	cfg := applyOptions(opts...)

	// Validate table name to prevent SQL injection
	if err := core.ValidateTableName(cfg.ConsumerMessagesTableName); err != nil {
		panic(fmt.Sprintf("invalid consumer_messages table name %q: %v", cfg.ConsumerMessagesTableName, err))
	}

	return &ConsumerRepository{
		pool:      pool,
		tableName: cfg.ConsumerMessagesTableName,
	}
}

// InsertOrUpdate records a message receipt with CONSUMING status
func (r *ConsumerRepository) InsertOrUpdate(ctx context.Context, params consumer.ConsumerMessageInsertParams) (*consumer.ConsumerMessage, error) {
	id := consumer.GenerateID()

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, outbox_id, message_id, receipt_handle, receive_count,
			queue_url, status, max_retries
		)
		VALUES ($1, $2, $3, $4, $5, $6, 'CONSUMING', $7)
		ON CONFLICT (message_id) DO UPDATE
		SET receipt_handle = EXCLUDED.receipt_handle,
		    receive_count = EXCLUDED.receive_count,
		    status = 'CONSUMING',
		    updated_at = now()
		RETURNING id, outbox_id, message_id, receipt_handle, receive_count,
		          queue_url, status, error_message, last_error_at,
		          max_retries, processed_at, created_at, updated_at
	`, r.tableName)

	return r.scanOne(ctx, query,
		id,
		params.OutboxID,
		params.MessageID,
		params.ReceiptHandle,
		params.ReceiveCount,
		params.QueueURL,
		params.MaxRetries,
	)
}

// UpdateToConsumed marks the message as CONSUMED
// Only updates messages in CONSUMING state to prevent invalid state transitions
func (r *ConsumerRepository) UpdateToConsumed(ctx context.Context, id string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = 'CONSUMED',
		    processed_at = now(),
		    updated_at = now()
		WHERE id = $1 AND status = 'CONSUMING'
	`, r.tableName)
	result, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected CONSUMING status for consumer message %s", consumer.ErrInvalidStatus, id)
	}
	return nil
}

// UpdateToFailed marks the message as FAILED
// Only updates messages in CONSUMING state to prevent invalid state transitions
func (r *ConsumerRepository) UpdateToFailed(ctx context.Context, id, errMsg string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = 'FAILED',
		    error_message = $2,
		    last_error_at = now(),
		    updated_at = now()
		WHERE id = $1 AND status = 'CONSUMING'
	`, r.tableName)
	result, err := r.pool.Exec(ctx, query, id, errMsg)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected CONSUMING status for consumer message %s", consumer.ErrInvalidStatus, id)
	}
	return nil
}

// UpdateToDead marks the message as DEAD
// Only updates messages in CONSUMING state to prevent invalid state transitions
func (r *ConsumerRepository) UpdateToDead(ctx context.Context, id, errMsg string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = 'DEAD',
		    error_message = $2,
		    last_error_at = now(),
		    updated_at = now()
		WHERE id = $1 AND status = 'CONSUMING'
	`, r.tableName)
	result, err := r.pool.Exec(ctx, query, id, errMsg)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected CONSUMING status for consumer message %s", consumer.ErrInvalidStatus, id)
	}
	return nil
}

// GetByMessageID retrieves a consumer message by SQS message ID
func (r *ConsumerRepository) GetByMessageID(ctx context.Context, messageID string) (*consumer.ConsumerMessage, error) {
	query := fmt.Sprintf(`
		SELECT id, outbox_id, message_id, receipt_handle, receive_count,
		       queue_url, status, error_message, last_error_at,
		       max_retries, processed_at, created_at, updated_at
		FROM %s
		WHERE message_id = $1
	`, r.tableName)
	return r.scanOne(ctx, query, messageID)
}

// ReviveStuckConsuming recovers messages stuck in CONSUMING state.
// This should be called once at startup to recover from crashes.
// CONSUMING -> FAILED (will be retried via SQS visibility timeout)
//
// Only revives messages that have been in CONSUMING state for more than 5 minutes,
// preventing recovery of messages actively being processed.
func (r *ConsumerRepository) ReviveStuckConsuming(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = 'FAILED',
		    error_message = 'revived from CONSUMING (crash recovery)',
		    last_error_at = now(),
		    updated_at = now()
		WHERE status = 'CONSUMING'
		  AND updated_at < now() - interval '5 minutes'
	`, r.tableName)
	result, err := r.pool.Exec(ctx, query)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// scanOne scans a single row into a ConsumerMessage
func (r *ConsumerRepository) scanOne(ctx context.Context, query string, args ...any) (*consumer.ConsumerMessage, error) {
	var msg consumer.ConsumerMessage
	var outboxID sql.NullString
	var errMsg sql.NullString
	var lastErrorAt sql.NullTime
	var processedAt sql.NullTime

	err := r.pool.QueryRow(ctx, query, args...).Scan(
		&msg.ID,
		&outboxID,
		&msg.MessageID,
		&msg.ReceiptHandle,
		&msg.ReceiveCount,
		&msg.QueueURL,
		&msg.Status,
		&errMsg,
		&lastErrorAt,
		&msg.MaxRetries,
		&processedAt,
		&msg.CreatedAt,
		&msg.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, consumer.ErrNotFound
		}
		return nil, err
	}

	if outboxID.Valid {
		msg.OutboxID = &outboxID.String
	}
	if errMsg.Valid {
		msg.ErrorMessage = &errMsg.String
	}
	if lastErrorAt.Valid {
		msg.LastErrorAt = &lastErrorAt.Time
	}
	if processedAt.Valid {
		msg.ProcessedAt = &processedAt.Time
	}

	return &msg, nil
}

// DeleteOlderThan deletes consumer messages with the given status older than the specified duration.
// Returns the number of deleted records.
func (r *ConsumerRepository) DeleteOlderThan(ctx context.Context, status consumer.ConsumerStatus, olderThan time.Duration) (int64, error) {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE status = $1
		  AND updated_at < now() - $2::interval
	`, r.tableName)

	// Convert Go duration to PostgreSQL interval format (e.g., "3600 seconds")
	intervalStr := fmt.Sprintf("%d seconds", int64(olderThan.Seconds()))

	result, err := r.pool.Exec(ctx, query, string(status), intervalStr)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
