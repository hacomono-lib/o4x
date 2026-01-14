package pgx

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/core"
)

// SQL query templates
// %s placeholder is used for table name (validated during repository initialization)
const (
	queryInsert = `
		INSERT INTO %s (id, event_type, payload, metadata, idempotency_key, status, max_attempts)
		VALUES ($1, $2, $3, $4, $5, 'ENQUEUED', $6)
		RETURNING id, event_type, payload, metadata, idempotency_key, status, error_message,
		          attempt_count, max_attempts, created_at, updated_at`

	queryFetchAndLockToPublishing = `
		WITH locked AS (
			SELECT id
			FROM %s
			WHERE status = 'ENQUEUED'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE %s
			SET status = 'PUBLISHING', updated_at = clock_timestamp()
			FROM locked
			WHERE %s.id = locked.id
			RETURNING %s.id, %s.event_type, %s.payload, %s.metadata, %s.idempotency_key,
			          %s.status, %s.error_message, %s.attempt_count,
			          %s.max_attempts, %s.created_at, %s.updated_at
		)
		SELECT * FROM updated`

	queryUpdateToPublished = `
		UPDATE %s
		SET status = 'PUBLISHED', updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'PUBLISHING'`

	queryUpdateToFailed = `
		UPDATE %s
		SET status = 'FAILED',
		    error_message = $2,
		    attempt_count = attempt_count + 1,
		    next_retry_at = clock_timestamp() + (
		        LEAST(
		            ($3 * interval '1 second') * POWER(2, attempt_count),
		            ($4 * interval '1 second')
		        ) * (0.5 + random() * 0.5)
		    ),
		    updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'PUBLISHING'`

	queryUpdateToDead = `
		UPDATE %s
		SET status = 'DEAD',
		    error_message = $2,
		    attempt_count = LEAST(attempt_count + 1, max_attempts),
		    updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'PUBLISHING'`

	queryRequeueFailed = `
		UPDATE %s
		SET status = 'ENQUEUED', updated_at = clock_timestamp()
		WHERE status = 'FAILED'
		  AND attempt_count < max_attempts
		  AND next_retry_at IS NOT NULL
		  AND next_retry_at <= clock_timestamp()`

	queryGetByID = `
		SELECT id, event_type, payload, metadata, idempotency_key, status, error_message,
		       attempt_count, max_attempts, next_retry_at, created_at, updated_at
		FROM %s
		WHERE id = $1`

	queryGetByIdempotencyKey = `
		SELECT id, event_type, payload, metadata, idempotency_key, status, error_message,
		       attempt_count, max_attempts, next_retry_at, created_at, updated_at
		FROM %s
		WHERE event_type = $1 AND idempotency_key = $2`

	queryReviveStuckPublishing = `
		UPDATE %s
		SET status = CASE 
		        WHEN attempt_count + 1 >= max_attempts THEN 'DEAD'::%s
		        ELSE 'FAILED'::%s
		    END,
		    error_message = CASE
		        WHEN attempt_count + 1 >= max_attempts 
		        THEN 'revived from PUBLISHING (crash recovery) - max attempts reached'
		        ELSE 'revived from PUBLISHING (crash recovery)'
		    END,
		    attempt_count = LEAST(attempt_count + 1, max_attempts),
		    next_retry_at = CASE
		        WHEN attempt_count + 1 >= max_attempts THEN NULL
		        ELSE clock_timestamp() + (
		            LEAST(
		                ($1 * interval '1 second') * POWER(2, attempt_count),
		                ($2 * interval '1 second')
		            ) * (0.5 + random() * 0.5)
		        )
		    END,
		    updated_at = clock_timestamp()
		WHERE status = 'PUBLISHING'::%s
		  AND updated_at < clock_timestamp() - $3::interval`

	queryFetchLockAndMarkPublishing = `
		WITH locked AS (
			SELECT id
			FROM %s
			WHERE status = 'ENQUEUED'
			ORDER BY created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE %s
			SET status = 'PUBLISHING', updated_at = clock_timestamp()
			FROM locked
			WHERE %s.id = locked.id
			RETURNING %s.id, %s.event_type, %s.payload, %s.metadata, %s.idempotency_key,
			          %s.status, %s.error_message, %s.attempt_count,
			          %s.max_attempts, %s.created_at, %s.updated_at
		)
		SELECT * FROM updated`

	queryUpdateBatchToPublished = `
		UPDATE %s
		SET status = 'PUBLISHED', updated_at = clock_timestamp()
		WHERE id = ANY($1) AND status = 'PUBLISHING'`

	queryDeleteOlderThan = `
		DELETE FROM %s
		WHERE status = ANY($1)
		  AND updated_at < clock_timestamp() - $2::interval`
)

// querier is an interface that both pgxpool.Pool and pgx.Tx implement
type querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// OutboxRepository implements core.OutboxRepository for PostgreSQL using pgx
type OutboxRepository struct {
	pool                     *pgxpool.Pool
	q                        querier // either pool or tx
	tableName                string
	backoffBase              time.Duration
	backoffMax               time.Duration
	stuckPublishingThreshold time.Duration
}

// NewOutboxRepository creates a new PostgreSQL outbox repository
func NewOutboxRepository(pool *pgxpool.Pool, opts ...Option) *OutboxRepository {
	cfg := applyOptions(opts...)

	// Validate table name to prevent SQL injection
	if err := core.ValidateTableName(cfg.OutboxTableName); err != nil {
		panic(fmt.Sprintf("invalid outbox table name %q: %v", cfg.OutboxTableName, err))
	}

	return &OutboxRepository{
		pool:                     pool,
		q:                        pool,
		tableName:                cfg.OutboxTableName,
		backoffBase:              cfg.RequeueBackoffBase,
		backoffMax:               cfg.RequeueBackoffMax,
		stuckPublishingThreshold: cfg.StuckPublishingThreshold,
	}
}

// WithTx returns a new OutboxRepository that uses the given transaction.
// Use this to insert outbox messages within an application transaction.
//
// Example:
//
//	tx, _ := pool.Begin(ctx)
//	defer tx.Rollback(ctx)
//
//	// Business logic
//	_, err := tx.Exec(ctx, "INSERT INTO orders ...")
//
//	// Insert outbox in same transaction
//	txRepo := repo.WithTx(tx)
//	txRepo.Insert(ctx, params)
//
//	tx.Commit(ctx)
func (r *OutboxRepository) WithTx(tx pgx.Tx) *OutboxRepository {
	return &OutboxRepository{
		pool:                     r.pool,
		q:                        tx,
		tableName:                r.tableName,
		backoffBase:              r.backoffBase,
		backoffMax:               r.backoffMax,
		stuckPublishingThreshold: r.stuckPublishingThreshold,
	}
}

// Insert adds a new message to the outbox with ENQUEUED status
func (r *OutboxRepository) Insert(ctx context.Context, params core.OutboxInsertParams) (*core.Outbox, error) {
	id := core.GenerateID()
	query := fmt.Sprintf(queryInsert, r.tableName)

	var outbox core.Outbox
	var errMsg sql.NullString

	err := r.q.QueryRow(ctx, query,
		id,
		params.EventType,
		params.Payload,
		params.Metadata,
		params.IdempotencyKey,
		params.MaxAttempts,
	).Scan(
		&outbox.ID,
		&outbox.EventType,
		&outbox.Payload,
		&outbox.Metadata,
		&outbox.IdempotencyKey,
		&outbox.Status,
		&errMsg,
		&outbox.AttemptCount,
		&outbox.MaxAttempts,
		&outbox.CreatedAt,
		&outbox.UpdatedAt,
	)
	if err != nil {
		// Check for unique constraint violation (idempotency_key duplicate)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return nil, core.ErrAlreadyExists
		}
		return nil, err
	}

	if errMsg.Valid {
		outbox.ErrorMessage = &errMsg.String
	}

	return &outbox, nil
}

// FetchAndLockToPublishing atomically fetches one ENQUEUED message,
// locks it, and updates its status to PUBLISHING in a single SQL statement.
// Uses SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1 with atomic CTE update
// to ensure the operation is atomic and prevents race conditions.
func (r *OutboxRepository) FetchAndLockToPublishing(ctx context.Context) (*core.Outbox, error) {
	query := fmt.Sprintf(queryFetchAndLockToPublishing,
		r.tableName, r.tableName, r.tableName, r.tableName, r.tableName,
		r.tableName, r.tableName, r.tableName, r.tableName, r.tableName,
		r.tableName, r.tableName, r.tableName, r.tableName)

	var outbox core.Outbox
	var errMsg sql.NullString

	err := r.q.QueryRow(ctx, query).Scan(
		&outbox.ID,
		&outbox.EventType,
		&outbox.Payload,
		&outbox.Metadata,
		&outbox.IdempotencyKey,
		&outbox.Status,
		&errMsg,
		&outbox.AttemptCount,
		&outbox.MaxAttempts,
		&outbox.CreatedAt,
		&outbox.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, core.ErrNoMessage
		}
		return nil, err
	}

	if errMsg.Valid {
		outbox.ErrorMessage = &errMsg.String
	}

	return &outbox, nil
}

// UpdateToPublished marks the message as PUBLISHED
// Only updates messages in PUBLISHING state to prevent invalid state transitions
func (r *OutboxRepository) UpdateToPublished(ctx context.Context, id string) error {
	query := fmt.Sprintf(queryUpdateToPublished, r.tableName)
	result, err := r.q.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected PUBLISHING status for message %s", core.ErrInvalidStatus, id)
	}
	return nil
}

// UpdateToFailed marks the message as FAILED with error info
// Only updates messages in PUBLISHING state to prevent invalid state transitions
// Increments attempt_count and sets next_retry_at based on exponential backoff
// FIXED: Calculates next_retry_at in PostgreSQL to avoid data race
func (r *OutboxRepository) UpdateToFailed(ctx context.Context, id, errMsg string) error {
	// Calculate backoff in PostgreSQL for atomicity
	// Formula: now() + (backoffBase * 2^attempt_count), capped at backoffMax
	// Note: attempt_count is used directly (already incremented in the query)
	query := fmt.Sprintf(queryUpdateToFailed, r.tableName)

	result, err := r.q.Exec(ctx, query, id, errMsg, r.backoffBase.Seconds(), r.backoffMax.Seconds())
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected PUBLISHING status for message %s", core.ErrInvalidStatus, id)
	}
	return nil
}

// UpdateToDead marks the message as DEAD
// Only updates messages in PUBLISHING state to prevent invalid state transitions
func (r *OutboxRepository) UpdateToDead(ctx context.Context, id, errMsg string) error {
	query := fmt.Sprintf(queryUpdateToDead, r.tableName)
	result, err := r.q.Exec(ctx, query, id, errMsg)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected PUBLISHING status for message %s", core.ErrInvalidStatus, id)
	}
	return nil
}

// RequeueFailed moves FAILED messages back to ENQUEUED.
// Only messages whose next_retry_at has elapsed are requeued.
// Returns the number of messages requeued.
// FIXED: Removed unnecessary SKIP LOCKED - no lock contention with FetchAndLockToPublishing
// since it only targets ENQUEUED messages, not FAILED ones.
func (r *OutboxRepository) RequeueFailed(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(queryRequeueFailed, r.tableName)

	result, err := r.q.Exec(ctx, query)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// GetByID retrieves an outbox message by ID
func (r *OutboxRepository) GetByID(ctx context.Context, id string) (*core.Outbox, error) {
	query := fmt.Sprintf(queryGetByID, r.tableName)

	var outbox core.Outbox
	var errMsg sql.NullString
	var nextRetryAt sql.NullTime

	err := r.q.QueryRow(ctx, query, id).Scan(
		&outbox.ID,
		&outbox.EventType,
		&outbox.Payload,
		&outbox.Metadata,
		&outbox.IdempotencyKey,
		&outbox.Status,
		&errMsg,
		&outbox.AttemptCount,
		&outbox.MaxAttempts,
		&nextRetryAt,
		&outbox.CreatedAt,
		&outbox.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, core.ErrNotFound
		}
		return nil, err
	}

	if errMsg.Valid {
		outbox.ErrorMessage = &errMsg.String
	}
	if nextRetryAt.Valid {
		outbox.NextRetryAt = &nextRetryAt.Time
	}

	return &outbox, nil
}

// GetByIdempotencyKey retrieves an outbox message by event_type and idempotency key
func (r *OutboxRepository) GetByIdempotencyKey(ctx context.Context, eventType, idempotencyKey string) (*core.Outbox, error) {
	query := fmt.Sprintf(queryGetByIdempotencyKey, r.tableName)

	var outbox core.Outbox
	var errMsg sql.NullString
	var nextRetryAt sql.NullTime

	err := r.q.QueryRow(ctx, query, eventType, idempotencyKey).Scan(
		&outbox.ID,
		&outbox.EventType,
		&outbox.Payload,
		&outbox.Metadata,
		&outbox.IdempotencyKey,
		&outbox.Status,
		&errMsg,
		&outbox.AttemptCount,
		&outbox.MaxAttempts,
		&nextRetryAt,
		&outbox.CreatedAt,
		&outbox.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, core.ErrNotFound
		}
		return nil, err
	}

	if errMsg.Valid {
		outbox.ErrorMessage = &errMsg.String
	}
	if nextRetryAt.Valid {
		outbox.NextRetryAt = &nextRetryAt.Time
	}

	return &outbox, nil
}

// InsertOutboxJSON is a helper to insert with a Go struct as payload
func (r *OutboxRepository) InsertOutboxJSON(ctx context.Context, eventType string, payload any, idempotencyKey string, maxRetries int) (*core.Outbox, error) {
	return r.InsertOutboxJSONWithMetadata(ctx, eventType, payload, nil, idempotencyKey, maxRetries)
}

// InsertOutboxJSONWithMetadata is a helper to insert with a Go struct as payload and optional metadata
func (r *OutboxRepository) InsertOutboxJSONWithMetadata(ctx context.Context, eventType string, payload, metadata any, idempotencyKey string, maxRetries int) (*core.Outbox, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var metadataBytes []byte
	if metadata != nil {
		metadataBytes, err = json.Marshal(metadata)
		if err != nil {
			return nil, err
		}
	}

	return r.Insert(ctx, core.OutboxInsertParams{
		EventType:      eventType,
		Payload:        data,
		Metadata:       metadataBytes,
		IdempotencyKey: idempotencyKey,
		MaxAttempts:    maxRetries,
	})
}

// ReviveStuckPublishing recovers messages stuck in PUBLISHING state.
// This should be called once at startup to recover from crashes.
// PUBLISHING -> FAILED (will be retried by RequeueFailed) or DEAD (if max_attempts reached)
//
// Only revives messages that have been in PUBLISHING state for more than the configured
// threshold (default: 5 minutes), preventing recovery of messages actively being processed.
//
// Note: attempt_count is incremented to ensure max_attempts limit is enforced.
// If incrementing would reach max_attempts, the message is marked as DEAD directly,
// ensuring consistency with handlePublishFailure behavior.
func (r *OutboxRepository) ReviveStuckPublishing(ctx context.Context) (int64, error) {
	// ENUM type name follows schema convention: {tableName}_status
	enumName := r.tableName + "_status"
	
	query := fmt.Sprintf(queryReviveStuckPublishing, 
		r.tableName,  // UPDATE table
		enumName,     // DEAD cast
		enumName,     // FAILED cast
		enumName)     // PUBLISHING cast in WHERE
	intervalStr := fmt.Sprintf("%d seconds", int64(r.stuckPublishingThreshold.Seconds()))
	result, err := r.q.Exec(ctx, query, r.backoffBase.Seconds(), r.backoffMax.Seconds(), intervalStr)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// FetchLockAndMarkPublishing atomically fetches ENQUEUED messages,
// locks them, and updates their status to PUBLISHING in a single SQL statement.
// This uses a CTE to ensure the operation is atomic and prevents race conditions.
// Implements core.BatchOutboxRepository
func (r *OutboxRepository) FetchLockAndMarkPublishing(ctx context.Context, limit int) ([]*core.Outbox, error) {
	// Use CTE to atomically lock and update in a single query
	query := fmt.Sprintf(queryFetchLockAndMarkPublishing,
		r.tableName, r.tableName, r.tableName, r.tableName, r.tableName,
		r.tableName, r.tableName, r.tableName, r.tableName, r.tableName,
		r.tableName, r.tableName, r.tableName, r.tableName)

	rows, err := r.q.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*core.Outbox
	for rows.Next() {
		var outbox core.Outbox
		var errMsg sql.NullString

		err := rows.Scan(
			&outbox.ID,
			&outbox.EventType,
			&outbox.Payload,
			&outbox.Metadata,
			&outbox.IdempotencyKey,
			&outbox.Status,
			&errMsg,
			&outbox.AttemptCount,
			&outbox.MaxAttempts,
			&outbox.CreatedAt,
			&outbox.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		if errMsg.Valid {
			outbox.ErrorMessage = &errMsg.String
		}

		results = append(results, &outbox)
	}

	return results, rows.Err()
}

// UpdateBatchToPublished marks multiple messages as PUBLISHED
// Implements core.BatchOutboxRepository
// Returns the number of successfully updated messages.
// Partial success is allowed - only messages in PUBLISHING state will be updated.
func (r *OutboxRepository) UpdateBatchToPublished(ctx context.Context, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	query := fmt.Sprintf(queryUpdateBatchToPublished, r.tableName)

	result, err := r.q.Exec(ctx, query, ids)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected(), nil
}

// DeleteOlderThan deletes outbox records with the given statuses older than the specified duration.
// Implements core.OutboxCleaner
// Can accept one or more statuses to delete multiple statuses in a single call.
func (r *OutboxRepository) DeleteOlderThan(ctx context.Context, statuses []core.OutboxStatus, olderThan time.Duration) (int64, error) {
	if len(statuses) == 0 {
		return 0, fmt.Errorf("at least one status must be specified")
	}

	query := fmt.Sprintf(queryDeleteOlderThan, r.tableName)

	// Convert Go duration to PostgreSQL interval format using microseconds
	// to avoid truncation of sub-second durations (e.g., 50ms would become 0 seconds)
	intervalStr := fmt.Sprintf("%d microseconds", olderThan.Microseconds())

	// Convert statuses to []string for PostgreSQL ANY() operator
	statusStrings := make([]string, len(statuses))
	for i, status := range statuses {
		statusStrings[i] = string(status)
	}

	result, err := r.q.Exec(ctx, query, statusStrings, intervalStr)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
