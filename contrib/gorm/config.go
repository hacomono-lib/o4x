package gorm

import (
	"time"

	"github.com/hacomono-lib/o4x/core"
)

// Config holds configuration for GORM repositories
type Config struct {
	// OutboxTableName is the name of the outbox table (default: "outbox")
	OutboxTableName string
	// InboxTableName is the name of the consumer inbox table (default: "consumer_inbox")
	InboxTableName string
	// RequeueBackoffBase is the base interval for exponential backoff (default: 1 second)
	RequeueBackoffBase time.Duration
	// RequeueBackoffMax is the maximum backoff interval (default: 1 hour)
	RequeueBackoffMax time.Duration
}

// Option is a function that modifies Config
type Option func(*Config)

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		OutboxTableName:    "outbox",
		InboxTableName:     "consumer_inbox",
		RequeueBackoffBase: 1 * time.Second,
		RequeueBackoffMax:  1 * time.Hour,
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

// applyOptions applies options to a config
func applyOptions(opts ...Option) *Config {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
