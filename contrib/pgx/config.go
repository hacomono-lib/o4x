package pgx

import (
	"time"

	"github.com/hacomono-lib/o4x/core"
)

// Config holds configuration for pgx repositories
type Config struct {
	// OutboxTableName is the name of the outbox table (default: "outbox")
	OutboxTableName string
	// InboxTableName is the name of the consumer inbox table (default: "consumer_inbox")
	InboxTableName string
	// RequeueBackoffBase is the base interval for exponential backoff (default: 1 second)
	RequeueBackoffBase time.Duration
	// RequeueBackoffMax is the maximum backoff interval (default: 1 hour)
	RequeueBackoffMax time.Duration
	// StuckPublishingThreshold is the time duration after which messages stuck in PUBLISHING state
	// are considered crashed and will be recovered to FAILED state by ReviveStuckPublishing.
	// Default: 5 minutes. Lower values (e.g., 30 seconds) provide faster recovery but may cause
	// false positives during slow network operations. Higher values (e.g., 10 minutes) reduce
	// false positives but slow down crash recovery.
	StuckPublishingThreshold time.Duration
	// StuckInboxThreshold is the time duration after which consumer inbox records stuck in PROCESSING state
	// are considered crashed and will be allowed to retry by TryStart.
	// Default: 2 minutes (typically 2-4x the SQS visibility timeout).
	// This should be set to at least 2x your maximum handler processing time to prevent
	// false positives during normal processing.
	StuckInboxThreshold time.Duration
}

// Option is a function that modifies Config
type Option func(*Config)

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		OutboxTableName:          "outbox",
		InboxTableName:           "consumer_inbox",
		RequeueBackoffBase:       1 * time.Second,
		RequeueBackoffMax:        1 * time.Hour,
		StuckPublishingThreshold: 5 * time.Minute,
		StuckInboxThreshold:      2 * time.Minute,
	}
}

// WithOutboxTableName sets the outbox table name.
// The name must contain only alphanumeric characters, underscores, and optionally a schema prefix.
// Panics if the name is invalid to prevent SQL injection.
func WithOutboxTableName(name string) Option {
	if err := core.ValidateTableName(name); err != nil {
		panic(err)
	}
	return func(c *Config) {
		c.OutboxTableName = name
	}
}

// WithInboxTableName sets the consumer inbox table name.
// The name must contain only alphanumeric characters, underscores, and optionally a schema prefix.
// Panics if the name is invalid to prevent SQL injection.
func WithInboxTableName(name string) Option {
	if err := core.ValidateTableName(name); err != nil {
		panic(err)
	}
	return func(c *Config) {
		c.InboxTableName = name
	}
}

// WithStuckPublishingThreshold sets the threshold for detecting stuck messages in PUBLISHING state.
// Messages that remain in PUBLISHING state longer than this duration will be recovered to FAILED
// by ReviveStuckPublishing.
//
// Recommended values:
//   - Fast recovery (30s-1m): For high-SLA environments where quick recovery is critical
//   - Balanced (5m): Default, suitable for most use cases
//   - Safe (10m-15m): For environments with slow network or large message payloads
func WithStuckPublishingThreshold(threshold time.Duration) Option {
	return func(c *Config) {
		c.StuckPublishingThreshold = threshold
	}
}

// WithStuckInboxThreshold sets the threshold for detecting stuck messages in consumer inbox PROCESSING state.
// Messages that remain in PROCESSING state longer than this duration will be allowed to retry by TryStart.
//
// IMPORTANT: Set this to at least 2x your maximum handler processing time to prevent duplicate processing.
//
// Recommended values:
//   - High throughput (1m): For fast handlers with 30s SQS visibility timeout
//   - Balanced (2m): Default, suitable for most use cases (30s visibility timeout)
//   - Long processing (5-10m): For handlers with long-running operations
//
// Formula: StuckInboxThreshold >= 2 * (maximum handler processing time)
// Example: If handler can take up to 60s, set this to at least 120s (2 minutes)
func WithStuckInboxThreshold(threshold time.Duration) Option {
	return func(c *Config) {
		c.StuckInboxThreshold = threshold
	}
}

// applyOptions applies options to a config
func applyOptions(opts ...Option) *Config {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
