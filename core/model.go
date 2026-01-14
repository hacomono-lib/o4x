package core

import (
	"encoding/json"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// validTableNamePattern matches valid PostgreSQL table names.
// Only alphanumeric characters, underscores, and schema-qualified names (schema.table) are allowed.
var validTableNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

// OutboxStatus represents the state of an outbox message (Publisher side)
// 5 states: ENQUEUED, PUBLISHING, PUBLISHED, FAILED, DEAD
type OutboxStatus string

const (
	OutboxStatusEnqueued   OutboxStatus = "ENQUEUED"   // Application inserted into outbox
	OutboxStatusPublishing OutboxStatus = "PUBLISHING" // Dispatcher locked and publishing
	OutboxStatusPublished  OutboxStatus = "PUBLISHED"  // Publish succeeded
	OutboxStatusFailed     OutboxStatus = "FAILED"     // Publish failed (retryable)
	OutboxStatusDead       OutboxStatus = "DEAD"       // Retry limit exceeded
)

// Outbox represents a message in the transactional outbox table
type Outbox struct {
	ID             string          `json:"id"` // UUID v7
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	Metadata       json.RawMessage `json:"metadata,omitempty"` // Optional metadata (trace context, custom headers, etc.)
	IdempotencyKey string          `json:"idempotency_key"`
	Status         OutboxStatus    `json:"status"`
	ErrorMessage   *string         `json:"error_message,omitempty"`
	AttemptCount   int             `json:"attempt_count"`
	MaxAttempts    int             `json:"max_attempts"`
	NextRetryAt    *time.Time      `json:"next_retry_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// OutboxInsertParams contains parameters for inserting a new outbox message
type OutboxInsertParams struct {
	EventType      string
	Payload        json.RawMessage
	Metadata       json.RawMessage // Optional metadata (trace_id, span_id, custom headers, etc.)
	IdempotencyKey string
	MaxAttempts    int
}

// CanRetry returns true if the outbox message can be retried
// This checks the CURRENT attempt count against max attempts.
// Note: This is primarily for querying existing message state.
func (o *Outbox) CanRetry() bool {
	return o.AttemptCount < o.MaxAttempts
}

// ShouldMarkDead returns true if the message should be marked as DEAD
// This checks if the CURRENT attempt count has reached or exceeded max attempts.
// Note: This is primarily for querying existing message state.
func (o *Outbox) ShouldMarkDead() bool {
	return o.AttemptCount >= o.MaxAttempts
}

// WillExceedMaxAttemptsAfterFailure returns true if marking this message as FAILED
// would result in attempt_count reaching max_attempts.
// This is used in handlePublishFailure to decide between FAILED and DEAD.
func (o *Outbox) WillExceedMaxAttemptsAfterFailure() bool {
	return o.AttemptCount+1 >= o.MaxAttempts
}

// GenerateID generates a new UUID v7 for outbox messages
// UUID v7 is time-ordered, which provides better index performance
func GenerateID() string {
	return uuid.Must(uuid.NewV7()).String()
}

// CalculateNextRetryAt calculates the next retry time with exponential backoff and jitter.
// Formula: now + (baseInterval * 2^attempt_count * jitter), capped at maxInterval.
// Jitter is a random value between 0.5 and 1.0 to prevent thundering herd problem.
// This is used by UpdateToFailed to pre-calculate next_retry_at.
//
// Note: This Go implementation is provided for reference and testing.
// The actual production implementation uses PostgreSQL's random() function
// for atomicity and to avoid race conditions.
func CalculateNextRetryAt(now time.Time, attemptCount int, baseInterval, maxInterval time.Duration, jitter float64) time.Time {
	// Exponential backoff: 2^attempt_count
	multiplier := 1 << uint(attemptCount) // bit shift for power of 2
	backoff := baseInterval * time.Duration(multiplier)

	// Cap at maxInterval before applying jitter
	if backoff > maxInterval {
		backoff = maxInterval
	}

	// Apply jitter (0.5 to 1.0) to prevent thundering herd
	backoff = time.Duration(float64(backoff) * jitter)

	return now.Add(backoff)
}

// ValidateTableName validates a PostgreSQL table name.
// Valid names contain only alphanumeric characters, underscores, and optionally a schema prefix.
// Examples: "outbox", "my_outbox", "public.outbox"
// Returns ErrInvalidTableName if the name is invalid.
func ValidateTableName(name string) error {
	if name == "" || !validTableNamePattern.MatchString(name) {
		return ErrInvalidTableName
	}
	return nil
}
