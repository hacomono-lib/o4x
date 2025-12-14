package consumer

import (
	"encoding/json"
)

// SQSMessage represents an incoming SQS message with parsed attributes.
// Handlers receive this struct to process messages from SQS.
type SQSMessage struct {
	MessageID      string
	ReceiptHandle  string
	Body           json.RawMessage
	EventType      string
	OutboxID       *string
	IdempotencyKey string
	ReceiveCount   int
}
