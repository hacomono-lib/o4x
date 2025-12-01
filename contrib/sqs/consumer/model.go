package consumer

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ConsumerStatus represents the state of a consumer message
// 4 states: CONSUMING, CONSUMED, FAILED, DEAD
// NOTE: This is COMPLETELY SEPARATE from outbox_status (Publisher side)
type ConsumerStatus string

const (
	ConsumerStatusConsuming ConsumerStatus = "CONSUMING" // Handler executing
	ConsumerStatusConsumed  ConsumerStatus = "CONSUMED"  // Handler completed, message deleted from SQS
	ConsumerStatusFailed    ConsumerStatus = "FAILED"    // Handler error (retrying via SQS visibility timeout)
	ConsumerStatusDead      ConsumerStatus = "DEAD"      // Retry limit exceeded
)

// ConsumerMessage represents a message being processed by the consumer
type ConsumerMessage struct {
	ID            string         `json:"id"`                  // UUID v7 for global uniqueness
	OutboxID      *string        `json:"outbox_id,omitempty"` // Optional reference to source outbox (UUID v7)
	MessageID     string         `json:"message_id"`          // SQS Message ID
	ReceiptHandle string         `json:"receipt_handle"`      // SQS Receipt Handle for deletion
	ReceiveCount  int            `json:"receive_count"`       // Number of times message was received
	QueueURL      string         `json:"queue_url"`
	Status        ConsumerStatus `json:"status"`
	ErrorMessage  *string        `json:"error_message,omitempty"`
	LastErrorAt   *time.Time     `json:"last_error_at,omitempty"`
	MaxRetries    int            `json:"max_retries"`
	ProcessedAt   *time.Time     `json:"processed_at,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// ConsumerMessageInsertParams contains parameters for inserting a consumer message
type ConsumerMessageInsertParams struct {
	OutboxID      *string
	MessageID     string
	ReceiptHandle string
	ReceiveCount  int
	QueueURL      string
	MaxRetries    int
}

// ShouldMarkDead returns true if the message should be marked as DEAD
func (m *ConsumerMessage) ShouldMarkDead() bool {
	return m.ReceiveCount >= m.MaxRetries
}

// SQSMessage represents an incoming SQS message with parsed attributes
type SQSMessage struct {
	MessageID      string
	ReceiptHandle  string
	Body           json.RawMessage
	Topic          string
	OutboxID       *string
	IdempotencyKey string
	ReceiveCount   int
}

// GenerateID generates a new UUID v7 for consumer messages
// UUID v7 is time-ordered, which provides better index performance
func GenerateID() string {
	return uuid.Must(uuid.NewV7()).String()
}
