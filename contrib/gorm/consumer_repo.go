package gorm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
	"github.com/hacomono-lib/o4x/core"
)

// consumerMessageModel is the GORM model for consumer_messages table
type consumerMessageModel struct {
	ID            string     `gorm:"primaryKey;type:uuid"`
	OutboxID      *string    `gorm:"type:uuid;index"`
	MessageID     string     `gorm:"type:varchar(255);uniqueIndex;not null"`
	ReceiptHandle string     `gorm:"type:text;not null"`
	ReceiveCount  int        `gorm:"not null;default:0"`
	QueueURL      string     `gorm:"type:varchar(1024);not null"`
	Status        string     `gorm:"type:varchar(20);not null;default:'CONSUMING'"`
	ErrorMessage  *string    `gorm:"type:text"`
	LastErrorAt   *time.Time `gorm:""`
	MaxRetries    int        `gorm:"not null;default:5"`
	ProcessedAt   *time.Time `gorm:""`
	CreatedAt     time.Time  `gorm:"autoCreateTime"`
	UpdatedAt     time.Time  `gorm:"autoUpdateTime"`
}

// ConsumerRepository implements consumer.Repository for GORM
type ConsumerRepository struct {
	db        *gorm.DB
	tableName string
}

// NewConsumerRepository creates a new GORM consumer repository
func NewConsumerRepository(db *gorm.DB, opts ...Option) *ConsumerRepository {
	cfg := applyOptions(opts...)

	// Validate table name to prevent SQL injection
	if err := core.ValidateTableName(cfg.ConsumerMessagesTableName); err != nil {
		panic(fmt.Sprintf("invalid consumer_messages table name %q: %v", cfg.ConsumerMessagesTableName, err))
	}

	return &ConsumerRepository{
		db:        db,
		tableName: cfg.ConsumerMessagesTableName,
	}
}

// InsertOrUpdate records a message receipt with CONSUMING status
func (r *ConsumerRepository) InsertOrUpdate(ctx context.Context, params consumer.ConsumerMessageInsertParams) (*consumer.ConsumerMessage, error) {
	model := &consumerMessageModel{
		ID:            consumer.GenerateID(),
		OutboxID:      params.OutboxID,
		MessageID:     params.MessageID,
		ReceiptHandle: params.ReceiptHandle,
		ReceiveCount:  params.ReceiveCount,
		QueueURL:      params.QueueURL,
		Status:        string(consumer.ConsumerStatusConsuming),
		MaxRetries:    params.MaxRetries,
	}

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "message_id"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"receipt_handle": model.ReceiptHandle,
				"receive_count":  model.ReceiveCount,
				"status":         string(consumer.ConsumerStatusConsuming),
				"updated_at":     time.Now(),
			}),
		}).
		Create(model)

	if result.Error != nil {
		return nil, result.Error
	}

	// Fetch the record to get all fields (including auto-generated ones)
	var fetched consumerMessageModel
	if err := r.db.WithContext(ctx).Table(r.tableName).Where("message_id = ?", params.MessageID).First(&fetched).Error; err != nil {
		return nil, err
	}

	return r.modelToCore(&fetched), nil
}

// UpdateToConsumed marks the message as CONSUMED
// Only updates messages in CONSUMING state to prevent invalid state transitions
func (r *ConsumerRepository) UpdateToConsumed(ctx context.Context, id string) error {
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("id = ? AND status = ?", id, string(consumer.ConsumerStatusConsuming)).
		Updates(map[string]interface{}{
			"status":       string(consumer.ConsumerStatusConsumed),
			"processed_at": gorm.Expr("NOW()"),
			"updated_at":   gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("%w: expected CONSUMING status for consumer message %s", consumer.ErrInvalidStatus, id)
	}
	return nil
}

// UpdateToFailed marks the message as FAILED
// Only updates messages in CONSUMING state to prevent invalid state transitions
func (r *ConsumerRepository) UpdateToFailed(ctx context.Context, id, errMsg string) error {
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("id = ? AND status = ?", id, string(consumer.ConsumerStatusConsuming)).
		Updates(map[string]interface{}{
			"status":        string(consumer.ConsumerStatusFailed),
			"error_message": errMsg,
			"last_error_at": gorm.Expr("NOW()"),
			"updated_at":    gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("%w: expected CONSUMING status for consumer message %s", consumer.ErrInvalidStatus, id)
	}
	return nil
}

// UpdateToDead marks the message as DEAD
// Only updates messages in CONSUMING state to prevent invalid state transitions
func (r *ConsumerRepository) UpdateToDead(ctx context.Context, id, errMsg string) error {
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("id = ? AND status = ?", id, string(consumer.ConsumerStatusConsuming)).
		Updates(map[string]interface{}{
			"status":        string(consumer.ConsumerStatusDead),
			"error_message": errMsg,
			"last_error_at": gorm.Expr("NOW()"),
			"updated_at":    gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("%w: expected CONSUMING status for consumer message %s", consumer.ErrInvalidStatus, id)
	}
	return nil
}

// GetByMessageID retrieves a consumer message by SQS message ID
func (r *ConsumerRepository) GetByMessageID(ctx context.Context, messageID string) (*consumer.ConsumerMessage, error) {
	var model consumerMessageModel

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("message_id = ?", messageID).
		First(&model)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, consumer.ErrNotFound
		}
		return nil, result.Error
	}

	return r.modelToCore(&model), nil
}

// ReviveStuckConsuming recovers messages stuck in CONSUMING state.
// This should be called once at startup to recover from crashes.
// CONSUMING -> FAILED (will be retried via SQS visibility timeout)
//
// Only revives messages that have been in CONSUMING state for more than 5 minutes,
// preventing recovery of messages actively being processed.
func (r *ConsumerRepository) ReviveStuckConsuming(ctx context.Context) (int64, error) {
	threshold := time.Now().Add(-5 * time.Minute)
	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("status = ? AND updated_at < ?", string(consumer.ConsumerStatusConsuming), threshold).
		Updates(map[string]interface{}{
			"status":        string(consumer.ConsumerStatusFailed),
			"error_message": "revived from CONSUMING (crash recovery)",
			"last_error_at": gorm.Expr("NOW()"),
			"updated_at":    gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

// modelToCore converts GORM model to consumer.ConsumerMessage
func (r *ConsumerRepository) modelToCore(m *consumerMessageModel) *consumer.ConsumerMessage {
	return &consumer.ConsumerMessage{
		ID:            m.ID,
		OutboxID:      m.OutboxID,
		MessageID:     m.MessageID,
		ReceiptHandle: m.ReceiptHandle,
		ReceiveCount:  m.ReceiveCount,
		QueueURL:      m.QueueURL,
		Status:        consumer.ConsumerStatus(m.Status),
		ErrorMessage:  m.ErrorMessage,
		LastErrorAt:   m.LastErrorAt,
		MaxRetries:    m.MaxRetries,
		ProcessedAt:   m.ProcessedAt,
		CreatedAt:     m.CreatedAt,
		UpdatedAt:     m.UpdatedAt,
	}
}

// DeleteOlderThan deletes consumer messages with the given status older than the specified duration.
// Returns the number of deleted records.
func (r *ConsumerRepository) DeleteOlderThan(ctx context.Context, status consumer.ConsumerStatus, olderThan time.Duration) (int64, error) {
	// Convert Go duration to PostgreSQL interval format (e.g., "3600 seconds")
	// This ensures timezone consistency by using PostgreSQL's NOW() function
	// instead of Go's time.Now() which may use different timezone settings.
	intervalStr := fmt.Sprintf("%d seconds", int64(olderThan.Seconds()))

	result := r.db.WithContext(ctx).
		Table(r.tableName).
		Where("status = ? AND updated_at < NOW() - ?::interval", string(status), intervalStr).
		Delete(&consumerMessageModel{})

	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}
