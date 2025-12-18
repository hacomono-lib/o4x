package consumer

import (
	"encoding/json"

	"github.com/google/uuid"
)

// SQSMessage represents an incoming SQS message with parsed attributes.
// Handlers receive this struct to process messages from SQS.
//
// CRITICAL: EventID is the Outbox event ID (logical event identity), NOT SQS MessageID.
// Use EventID for idempotency checks with InboxRepository.
type SQSMessage struct {
	MessageID      string          // SQS MessageID (changes on every redelivery)
	ReceiptHandle  string          // SQS receipt handle for deletion
	Body           json.RawMessage // Event payload
	EventType      string          // Event type from MessageAttributes
	EventID        uuid.UUID       // Outbox event ID (CRITICAL: Use this for idempotency, NOT MessageID)
	IdempotencyKey string          // Outbox idempotency key
	Metadata       json.RawMessage // Optional metadata (trace_id, span_id, custom headers, etc.)
	ReceiveCount   int             // Number of times this message has been received
}
