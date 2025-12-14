package gorm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"

	"github.com/hacomono-lib/o4x/core"
)

// outboxModel is the GORM model for outbox table
type outboxModel struct {
	ID             string     `gorm:"primaryKey;type:uuid"`
	EventType      string     `gorm:"type:varchar(255);not null"`
	Payload        []byte     `gorm:"type:jsonb;not null"`
	Metadata       []byte     `gorm:"type:jsonb"`
	IdempotencyKey string     `gorm:"type:varchar(255);not null"`
	Status         string     `gorm:"type:varchar(20);not null;default:'ENQUEUED'"`
	ErrorMessage   *string    `gorm:"type:text"`
	RetryCount     int        `gorm:"not null;default:0"`
	MaxRetries     int        `gorm:"not null;default:10"`
	NextRetryAt    *time.Time `gorm:"type:timestamptz"`
	CreatedAt      time.Time  `gorm:"autoCreateTime"`
	UpdatedAt      time.Time  `gorm:"autoUpdateTime"`
}

// OutboxRepository implements core.OutboxRepository for GORM
type OutboxRepository struct {
	db                       *gorm.DB
	tableName                string
	backoffBase              time.Duration
	backoffMax               time.Duration
	stuckPublishingThreshold time.Duration
}

// NewOutboxRepository creates a new GORM outbox repository
func NewOutboxRepository(db *gorm.DB, opts ...Option) *OutboxRepository {
	cfg := applyOptions(opts...)

	// Validate table name to prevent SQL injection
	if err := core.ValidateTableName(cfg.OutboxTableName); err != nil {
		panic(fmt.Sprintf("invalid outbox table name %q: %v", cfg.OutboxTableName, err))
	}

	return &OutboxRepository{
		db:                       db,
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
//	tx := db.Begin()
//	defer tx.Rollback()
//
//	// Business logic
//	tx.Create(&order)
//
//	// Insert outbox in same transaction
//	txRepo := repo.WithTx(tx)
//	txRepo.Insert(ctx, params)
//
//	tx.Commit()
func (r *OutboxRepository) WithTx(tx *gorm.DB) *OutboxRepository {
	return &OutboxRepository{
		db:                       tx,
		tableName:                r.tableName,
		backoffBase:              r.backoffBase,
		backoffMax:               r.backoffMax,
		stuckPublishingThreshold: r.stuckPublishingThreshold,
	}
}

// Insert adds a new message to the outbox with ENQUEUED status
func (r *OutboxRepository) Insert(ctx context.Context, params core.OutboxInsertParams) (*core.Outbox, error) {
	id := core.GenerateID()

	model := &outboxModel{
		ID:             id,
		EventType:      params.EventType,
		Payload:        params.Payload,
		Metadata:       params.Metadata,
		IdempotencyKey: params.IdempotencyKey,
		Status:         string(core.OutboxStatusEnqueued),
		MaxRetries:     params.MaxRetries,
	}

	result := r.db.WithContext(ctx).Table(r.tableName).Create(model)
	if result.Error != nil {
		// Check for unique constraint violation (idempotency_key duplicate)
		var pgErr *pgconn.PgError
		if errors.As(result.Error, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return nil, core.ErrAlreadyExists
		}
		return nil, result.Error
	}

	return r.modelToCore(model), nil
}

// FetchAndLockToPublishing atomically fetches one ENQUEUED message,
// locks it, and updates its status to PUBLISHING in a single SQL statement.
// Uses SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1 with atomic CTE update
// to ensure the operation is atomic and prevents race conditions.
func (r *OutboxRepository) FetchAndLockToPublishing(ctx context.Context) (*core.Outbox, error) {
	// Use raw SQL with CTE for atomic operation
	query := `
		WITH locked AS (
			SELECT id
			FROM ` + r.tableName + `
			WHERE status = 'ENQUEUED'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE ` + r.tableName + `
			SET status = 'PUBLISHING', updated_at = NOW()
			FROM locked
			WHERE ` + r.tableName + `.id = locked.id
			RETURNING ` + r.tableName + `.id, ` + r.tableName + `.event_type, ` + r.tableName + `.payload,
			          ` + r.tableName + `.metadata, ` + r.tableName + `.idempotency_key, ` + r.tableName + `.status,
			          ` + r.tableName + `.error_message, ` + r.tableName + `.retry_count,
			          ` + r.tableName + `.max_retries, ` + r.tableName + `.created_at, ` + r.tableName + `.updated_at
		)
		SELECT * FROM updated
	`

	var model outboxModel
	result := r.db.WithContext(ctx).Raw(query).Scan(&model)
	if result.Error != nil {
		return nil, result.Error
	}

	// Check if no rows were returned
	if result.RowsAffected == 0 {
		return nil, core.ErrNoMessage
	}

	return r.modelToCore(&model), nil
}

// UpdateToPublished marks the message as PUBLISHED
// Only updates messages in PUBLISHING state to prevent invalid state transitions
func (r *OutboxRepository) UpdateToPublished(ctx context.Context, id string) error {
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("id = ? AND status = ?", id, string(core.OutboxStatusPublishing)).
		Updates(map[string]interface{}{
			"status":     string(core.OutboxStatusPublished),
			"updated_at": gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("%w: expected PUBLISHING status for message %s", core.ErrInvalidStatus, id)
	}
	return nil
}

// UpdateToFailed marks the message as FAILED with error info
// Only updates messages in PUBLISHING state to prevent invalid state transitions
// Increments retry_count and sets next_retry_at based on exponential backoff with jitter
// FIXED: Calculates next_retry_at in PostgreSQL to avoid data race
func (r *OutboxRepository) UpdateToFailed(ctx context.Context, id, errMsg string) error {
	// Calculate backoff in PostgreSQL for atomicity
	// Formula: now() + (backoffBase * 2^(retry_count + 1) * jitter), capped at backoffMax
	// Jitter is (0.5 + random() * 0.5) to prevent thundering herd problem
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("id = ? AND status = ?", id, string(core.OutboxStatusPublishing)).
		Updates(map[string]interface{}{
			"status":        string(core.OutboxStatusFailed),
			"error_message": errMsg,
			"retry_count":   gorm.Expr("retry_count + 1"),
			"next_retry_at": gorm.Expr("NOW() + (LEAST(?::interval * POWER(2, retry_count + 1), ?::interval) * (0.5 + random() * 0.5))", r.backoffBase, r.backoffMax),
			"updated_at":    gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("%w: expected PUBLISHING status for message %s", core.ErrInvalidStatus, id)
	}
	return nil
}

// UpdateToDead marks the message as DEAD
// Only updates messages in PUBLISHING state to prevent invalid state transitions
func (r *OutboxRepository) UpdateToDead(ctx context.Context, id, errMsg string) error {
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("id = ? AND status = ?", id, string(core.OutboxStatusPublishing)).
		Updates(map[string]interface{}{
			"status":        string(core.OutboxStatusDead),
			"error_message": errMsg,
			"updated_at":    gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
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
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("status = ? AND retry_count < max_retries AND next_retry_at IS NOT NULL AND next_retry_at <= NOW()",
			string(core.OutboxStatusFailed)).
		Updates(map[string]interface{}{
			"status":     string(core.OutboxStatusEnqueued),
			"updated_at": gorm.Expr("NOW()"),
		})

	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

// GetByID retrieves an outbox message by ID
func (r *OutboxRepository) GetByID(ctx context.Context, id string) (*core.Outbox, error) {
	var model outboxModel

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("id = ?", id).
		First(&model)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, core.ErrNotFound
		}
		return nil, result.Error
	}

	return r.modelToCore(&model), nil
}

// GetByIdempotencyKey retrieves an outbox message by event_type and idempotency key
func (r *OutboxRepository) GetByIdempotencyKey(ctx context.Context, eventType, idempotencyKey string) (*core.Outbox, error) {
	var model outboxModel

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("event_type = ? AND idempotency_key = ?", eventType, idempotencyKey).
		First(&model)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, core.ErrNotFound
		}
		return nil, result.Error
	}

	return r.modelToCore(&model), nil
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
		MaxRetries:     maxRetries,
	})
}

// ReviveStuckPublishing recovers messages stuck in PUBLISHING state.
// This should be called once at startup to recover from crashes.
// PUBLISHING -> FAILED (will be retried by RequeueFailed)
//
// Only revives messages that have been in PUBLISHING state for more than 5 minutes,
// preventing recovery of messages actively being processed.
//
// Note: retry_count is incremented to ensure max_retries limit is enforced.
// This prevents infinite retries for messages that consistently fail.
// Messages exceeding max_retries will be moved to DEAD on next retry attempt.
func (r *OutboxRepository) ReviveStuckPublishing(ctx context.Context) (int64, error) {
	threshold := time.Now().Add(-r.stuckPublishingThreshold)
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("status = ? AND updated_at < ?", string(core.OutboxStatusPublishing), threshold).
		Updates(map[string]interface{}{
			"status":        string(core.OutboxStatusFailed),
			"error_message": "revived from PUBLISHING (crash recovery)",
			"retry_count":   gorm.Expr("retry_count + 1"),
			"next_retry_at": gorm.Expr("NOW() + (LEAST(?::interval * POWER(2, retry_count + 1), ?::interval) * (0.5 + random() * 0.5))", r.backoffBase, r.backoffMax),
			"updated_at":    gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

// modelToCore converts GORM model to core.Outbox
func (r *OutboxRepository) modelToCore(m *outboxModel) *core.Outbox {
	return &core.Outbox{
		ID:             m.ID,
		EventType:      m.EventType,
		Payload:        m.Payload,
		Metadata:       m.Metadata,
		IdempotencyKey: m.IdempotencyKey,
		Status:         core.OutboxStatus(m.Status),
		ErrorMessage:   m.ErrorMessage,
		RetryCount:     m.RetryCount,
		MaxRetries:     m.MaxRetries,
		NextRetryAt:    m.NextRetryAt,
		CreatedAt:      m.CreatedAt,
		UpdatedAt:      m.UpdatedAt,
	}
}

// FetchLockAndMarkPublishing atomically fetches ENQUEUED messages,
// locks them, and updates their status to PUBLISHING in a single transaction.
// This uses a CTE to ensure the operation is atomic and prevents race conditions.
// Implements core.BatchOutboxRepository
func (r *OutboxRepository) FetchLockAndMarkPublishing(ctx context.Context, limit int) ([]*core.Outbox, error) {
	// Use raw SQL with CTE for atomic operation
	query := `
		WITH locked AS (
			SELECT id
			FROM ` + r.tableName + `
			WHERE status = 'ENQUEUED'
			ORDER BY created_at ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE ` + r.tableName + `
			SET status = 'PUBLISHING', updated_at = NOW()
			FROM locked
			WHERE ` + r.tableName + `.id = locked.id
			RETURNING ` + r.tableName + `.id, ` + r.tableName + `.event_type, ` + r.tableName + `.payload,
			          ` + r.tableName + `.metadata, ` + r.tableName + `.idempotency_key, ` + r.tableName + `.status,
			          ` + r.tableName + `.error_message, ` + r.tableName + `.retry_count,
			          ` + r.tableName + `.max_retries, ` + r.tableName + `.created_at, ` + r.tableName + `.updated_at
		)
		SELECT * FROM updated
	`

	var models []outboxModel
	result := r.db.WithContext(ctx).Raw(query, limit).Scan(&models)
	if result.Error != nil {
		return nil, result.Error
	}

	results := make([]*core.Outbox, len(models))
	for i := range models {
		results[i] = r.modelToCore(&models[i])
	}

	return results, nil
}

// UpdateBatchToPublished marks multiple messages as PUBLISHED
// Implements core.BatchOutboxRepository
// Returns the number of successfully updated messages.
// Partial success is allowed - only messages in PUBLISHING state will be updated.
func (r *OutboxRepository) UpdateBatchToPublished(ctx context.Context, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("id IN ? AND status = ?", ids, string(core.OutboxStatusPublishing)).
		Updates(map[string]interface{}{
			"status":     string(core.OutboxStatusPublished),
			"updated_at": gorm.Expr("NOW()"),
		})

	if result.Error != nil {
		return 0, result.Error
	}

	return result.RowsAffected, nil
}

// DeleteOlderThan deletes outbox records with the given status older than the specified duration.
// Implements core.OutboxCleaner
func (r *OutboxRepository) DeleteOlderThan(ctx context.Context, status core.OutboxStatus, olderThan time.Duration) (int64, error) {
	// Convert Go duration to PostgreSQL interval format (e.g., "3600 seconds")
	// This ensures timezone consistency by using PostgreSQL's NOW() function
	// instead of Go's time.Now() which may use different timezone settings.
	intervalStr := fmt.Sprintf("%d seconds", int64(olderThan.Seconds()))

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("status = ? AND updated_at < NOW() - ?::interval", string(status), intervalStr).
		Delete(&outboxModel{})

	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}
