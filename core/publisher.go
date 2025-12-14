package core

import "context"

// Publisher defines the interface for publishing messages to a message queue
// Implementations: SQS, Kafka, etc.
type Publisher interface {
	// Publish sends a message to the message queue
	// For SQS FIFO:
	//   - MessageGroupId = event_type
	//   - MessageDeduplicationId = idempotencyKey
	Publish(ctx context.Context, msg *Outbox) error
}

// BatchPublisher extends Publisher with batch publishing capability
// Implement this interface for better throughput with transports that support batching
// (e.g., SQS SendMessageBatch, Kafka batch produce)
type BatchPublisher interface {
	Publisher

	// PublishBatch sends multiple messages in a single batch operation
	// Returns a result for each message in the same order as the input
	// Partial failures are possible - check each result's Error field
	PublishBatch(ctx context.Context, msgs []*Outbox) []PublishResult

	// MaxBatchSize returns the maximum number of messages per batch
	// For SQS this is 10, for Kafka it depends on configuration
	MaxBatchSize() int
}

// PublishResult contains the result of a publish operation
type PublishResult struct {
	// OutboxID is the ID of the outbox message (UUID v7)
	OutboxID string
	// MessageID is the transport-specific message ID (e.g., SQS MessageId)
	MessageID string
	// Success indicates whether the publish succeeded
	Success bool
	// Error contains the error if Success is false
	Error error
}
