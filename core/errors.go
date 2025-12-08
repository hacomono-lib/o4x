package core

import (
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"
)

var (
	// ErrNoMessage indicates no message is available for processing
	ErrNoMessage = errors.New("no message available")

	// ErrAlreadyExists indicates the message already exists (idempotency violation)
	ErrAlreadyExists = errors.New("message already exists")

	// ErrPublishFailed indicates the publish operation failed
	ErrPublishFailed = errors.New("publish failed")

	// ErrMaxRetriesExceeded indicates the maximum retry count has been exceeded
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")

	// ErrInvalidStatus indicates an invalid status transition was attempted
	ErrInvalidStatus = errors.New("invalid status")

	// ErrNotFound indicates the requested resource was not found
	ErrNotFound = errors.New("not found")

	// ErrAlreadyRunning is returned when attempting to start an already running dispatcher
	ErrAlreadyRunning = errors.New("dispatcher already running")

	// ErrInvalidTableName indicates the table name contains invalid characters
	ErrInvalidTableName = errors.New("invalid table name: must contain only alphanumeric characters and underscores")

	// ErrPayloadTooLarge indicates the message payload exceeds the maximum allowed size
	ErrPayloadTooLarge = errors.New("payload too large")

	// ErrInvalidConfig indicates an invalid configuration was provided
	ErrInvalidConfig = errors.New("invalid configuration")
)

// RetryableError is an interface that errors can implement to indicate
// whether they should be retried or immediately marked as DEAD.
//
// If an error does not implement this interface, it is treated as retryable
// (default behavior: retry until max_retries is exceeded).
//
// Example usage:
//
//	type ValidationError struct {
//	    Field string
//	    Reason string
//	}
//
//	func (e *ValidationError) Error() string {
//	    return fmt.Sprintf("validation failed: %s: %s", e.Field, e.Reason)
//	}
//
//	func (e *ValidationError) IsRetryable() bool {
//	    return false // validation errors should not be retried
//	}
type RetryableError interface {
	error
	// IsRetryable returns true if the error is transient and the operation
	// should be retried, false if it's a permanent failure.
	IsRetryable() bool
}

// IsRetryable checks if an error is retryable.
// If the error implements RetryableError, it returns the result of IsRetryable().
// Otherwise, it returns true (errors are retryable by default).
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	var retryable RetryableError
	if errors.As(err, &retryable) {
		return retryable.IsRetryable()
	}

	// Default: errors are retryable
	return true
}

// PermanentError wraps an error to indicate it should not be retried.
// Use this to wrap errors that should immediately be marked as DEAD.
//
// Example:
//
//	if err := validate(msg); err != nil {
//	    return core.NewPermanentError(err)
//	}
type PermanentError struct {
	Cause error
}

// NewPermanentError creates a new PermanentError wrapping the given error.
func NewPermanentError(cause error) *PermanentError {
	return &PermanentError{Cause: cause}
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent error: %v", e.Cause)
}

func (e *PermanentError) Unwrap() error {
	return e.Cause
}

func (e *PermanentError) IsRetryable() bool {
	return false
}

// TransientError wraps an error to explicitly indicate it should be retried.
// This is useful when you want to be explicit about retryability.
type TransientError struct {
	Cause error
}

// NewTransientError creates a new TransientError wrapping the given error.
func NewTransientError(cause error) *TransientError {
	return &TransientError{Cause: cause}
}

func (e *TransientError) Error() string {
	return fmt.Sprintf("transient error: %v", e.Cause)
}

func (e *TransientError) Unwrap() error {
	return e.Cause
}

func (e *TransientError) IsRetryable() bool {
	return true
}

// PublishError wraps a publish failure with additional context
type PublishError struct {
	OutboxID string
	Topic    string
	Cause    error
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("publish error for outbox %s topic %s: %v", e.OutboxID, e.Topic, e.Cause)
}

func (e *PublishError) Unwrap() error {
	return e.Cause
}

// IsRetryable returns whether the underlying cause is retryable.
func (e *PublishError) IsRetryable() bool {
	return IsRetryable(e.Cause)
}

// MaxErrorMessageLength is the maximum byte length for error messages stored in the database.
// Messages longer than this will be truncated.
// FIXED: Changed from character count to byte count to handle UTF-8 correctly
const MaxErrorMessageLength = 4000

// sensitivePatterns contains regular expressions that match sensitive information
// that should be redacted from error messages to prevent information leakage.
var sensitivePatterns = []*regexp.Regexp{
	// API keys, secret keys, passwords, tokens (case-insensitive)
	// Matches: "api_key=xxx", "password: xxx", "token='xxx'", etc.
	regexp.MustCompile(`(?i)(api[_-]?key|secret[_-]?key|password|token|auth[_-]?token|access[_-]?token|bearer)\s*[:=]\s*['"]?([^\s'",}]+)`),

	// AWS Secret Access Key pattern (40 characters)
	regexp.MustCompile(`(?i)(aws[_-]?secret[_-]?access[_-]?key|secret[_-]?access[_-]?key)\s*[:=]\s*['"]?([A-Za-z0-9/+=]{40})`),

	// JWT tokens (xxx.yyy.zzz format)
	regexp.MustCompile(`\b(eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)`),

	// Database connection strings with passwords
	// postgres://user:password@host:port/db
	regexp.MustCompile(`(?i)(postgres|mysql|mongodb)://([^:]+):([^@]+)@`),

	// Bearer tokens in Authorization header
	regexp.MustCompile(`(?i)authorization:\s*bearer\s+(\S+)`),
}

// sanitizeSensitiveInfo redacts sensitive information from error messages.
// This prevents accidental leakage of credentials, tokens, and passwords in logs and database.
//
// Redacted patterns:
//   - API keys, secret keys, passwords, tokens
//   - AWS credentials
//   - JWT tokens
//   - Database connection strings with passwords
//   - Authorization headers with bearer tokens
//
// Example:
//
//	"error: api_key=sk_live_abcd1234" → "error: api_key=***REDACTED***"
func sanitizeSensitiveInfo(msg string) string {
	result := msg
	for _, pattern := range sensitivePatterns {
		if pattern.NumSubexp() == 2 {
			// Pattern with 2 capture groups: prefix and sensitive value
			result = pattern.ReplaceAllString(result, "${1}=***REDACTED***")
		} else {
			// Pattern with 1 capture group: entire sensitive value
			result = pattern.ReplaceAllString(result, "***REDACTED***")
		}
	}
	return result
}

// TruncateErrorMessage truncates an error message to MaxErrorMessageLength bytes
// and redacts sensitive information such as API keys, passwords, and tokens.
// If the message is longer, it appends "... (truncated)" to indicate truncation.
// Uses utf8.ValidString for correct UTF-8 boundary detection.
func TruncateErrorMessage(msg string) string {
	// First, sanitize sensitive information
	msg = sanitizeSensitiveInfo(msg)
	const suffix = "... (truncated)"
	suffixLen := len(suffix)

	// If message fits within limit, return as-is
	if len(msg) <= MaxErrorMessageLength {
		return msg
	}

	// Calculate max content bytes (total limit - suffix length)
	maxContentBytes := MaxErrorMessageLength - suffixLen

	// Truncate to max content bytes
	truncated := msg[:maxContentBytes]

	// Ensure we don't split a UTF-8 character by removing bytes from the end
	// until we have a valid UTF-8 string. This is O(k) where k <= 4 (max UTF-8 byte length).
	for truncated != "" && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}

	return truncated + suffix
}
