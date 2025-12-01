package gorm

import (
	"time"

	"github.com/hacomono-lib/o4x/core"
)

// Config holds configuration for GORM repositories
type Config struct {
	// OutboxTableName is the name of the outbox table (default: "outbox")
	OutboxTableName string
	// ConsumerMessagesTableName is the name of the consumer messages table (default: "consumer_messages")
	ConsumerMessagesTableName string
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
		OutboxTableName:           "outbox",
		ConsumerMessagesTableName: "consumer_messages",
		RequeueBackoffBase:        1 * time.Second,
		RequeueBackoffMax:         1 * time.Hour,
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

// WithConsumerMessagesTableName sets the consumer messages table name.
// The name must contain only alphanumeric characters, underscores, and optionally a schema prefix.
// Panics if the name is invalid to prevent SQL injection.
func WithConsumerMessagesTableName(name string) Option {
	if err := core.ValidateTableName(name); err != nil {
		panic(err)
	}
	return func(c *Config) {
		c.ConsumerMessagesTableName = name
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
