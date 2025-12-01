package consumer

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound indicates the requested resource was not found
var ErrNotFound = errors.New("consumer message not found")

// ErrInvalidStatus indicates an invalid status transition was attempted
var ErrInvalidStatus = errors.New("invalid consumer message status")

// Repository defines the interface for consumer message persistence
// NOTE: This is COMPLETELY SEPARATE from core.OutboxRepository
// Consumer NEVER updates outbox_status
//
// Repository is OPTIONAL. If nil is passed to NewService, the consumer
// will still function but won't persist message state to a database.
// This is useful when you only need SQS visibility timeout + DLQ for retry handling.
//
// Implementations are provided in:
//   - contrib/pgx: PostgreSQL using pgx
//   - contrib/gorm: PostgreSQL/MySQL using GORM
type Repository interface {
	// InsertOrUpdate records a message receipt with CONSUMING status
	// Uses ON CONFLICT to handle redeliveries
	InsertOrUpdate(ctx context.Context, params ConsumerMessageInsertParams) (*ConsumerMessage, error)

	// UpdateToConsumed marks the message as CONSUMED after successful processing
	UpdateToConsumed(ctx context.Context, id string) error

	// UpdateToFailed marks the message as FAILED on handler error
	UpdateToFailed(ctx context.Context, id string, errMsg string) error

	// UpdateToDead marks the message as DEAD when max retries exceeded
	UpdateToDead(ctx context.Context, id string, errMsg string) error

	// GetByMessageID retrieves a consumer message by SQS message ID
	GetByMessageID(ctx context.Context, messageID string) (*ConsumerMessage, error)

	// DeleteOlderThan deletes consumer messages with the given status older than the specified duration
	// Returns the number of deleted records
	DeleteOlderThan(ctx context.Context, status ConsumerStatus, olderThan time.Duration) (int64, error)
}

// NopRepository is a no-op implementation of Repository.
// It implements the Null Object Pattern, allowing the consumer service to function
// without a database by providing a repository that does nothing.
//
// This is useful when you want to rely solely on SQS visibility timeout and DLQ
// for retry handling, without persisting message processing state to a database.
//
// All methods return success without performing any operations.
type NopRepository struct{}

// NewNopRepository creates a new no-op repository.
func NewNopRepository() *NopRepository {
	return &NopRepository{}
}

// InsertOrUpdate always returns a dummy ConsumerMessage without persisting anything.
func (r *NopRepository) InsertOrUpdate(ctx context.Context, params ConsumerMessageInsertParams) (*ConsumerMessage, error) {
	return &ConsumerMessage{
		ID:            "noop",
		MessageID:     params.MessageID,
		ReceiptHandle: params.ReceiptHandle,
		ReceiveCount:  params.ReceiveCount,
		QueueURL:      params.QueueURL,
		Status:        ConsumerStatusConsuming,
		MaxRetries:    params.MaxRetries,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}, nil
}

// UpdateToConsumed always succeeds without persisting anything.
func (r *NopRepository) UpdateToConsumed(ctx context.Context, id string) error {
	return nil
}

// UpdateToFailed always succeeds without persisting anything.
func (r *NopRepository) UpdateToFailed(ctx context.Context, id, errMsg string) error {
	return nil
}

// UpdateToDead always succeeds without persisting anything.
func (r *NopRepository) UpdateToDead(ctx context.Context, id, errMsg string) error {
	return nil
}

// GetByMessageID always returns ErrNotFound since no messages are persisted.
func (r *NopRepository) GetByMessageID(ctx context.Context, messageID string) (*ConsumerMessage, error) {
	return nil, ErrNotFound
}

// DeleteOlderThan always returns 0 deleted records since no messages are persisted.
func (r *NopRepository) DeleteOlderThan(ctx context.Context, status ConsumerStatus, olderThan time.Duration) (int64, error) {
	return 0, nil
}
