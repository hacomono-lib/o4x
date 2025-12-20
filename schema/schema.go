// Package schema provides DDL generation helpers for o4x tables.
// These helpers allow users to generate DDL with custom table names for PostgreSQL.
package schema

import (
	"fmt"
)

// OutboxDDL generates the DDL for the outbox table with the given table name.
// The ENUM type name will be derived from the table name (e.g., "my_outbox" -> "my_outbox_status").
func OutboxDDL(tableName string) string {
	enumName := tableName + "_status"
	return fmt.Sprintf(`-- Outbox Schema for Publisher (Transactional Outbox Pattern)
-- 5 states: ENQUEUED, PUBLISHING, PUBLISHED, FAILED, DEAD

CREATE TYPE %s AS ENUM (
  'ENQUEUED',    -- Application inserted into outbox
  'PUBLISHING',  -- Dispatcher locked and publishing
  'PUBLISHED',   -- Publish succeeded
  'FAILED',      -- Publish failed (retryable)
  'DEAD'         -- Retry limit exceeded
);

CREATE TABLE %s (
  id               UUID PRIMARY KEY,
  event_type       TEXT NOT NULL,
  payload          JSONB NOT NULL,
  metadata         JSONB,
  idempotency_key  TEXT NOT NULL,
  status           %s NOT NULL DEFAULT 'ENQUEUED',
  error_message    TEXT,
  retry_count      INT NOT NULL DEFAULT 0,
  max_retries      INT NOT NULL DEFAULT 10,
  next_retry_at    TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Column comments for outbox table
COMMENT ON COLUMN %s.id IS 'Unique message identifier (UUID v7 for time-ordered)';
COMMENT ON COLUMN %s.event_type IS 'Event type identifier for routing (e.g. order.created)';
COMMENT ON COLUMN %s.payload IS 'Message payload as JSON';
COMMENT ON COLUMN %s.metadata IS 'Optional metadata (trace context, custom headers, etc.)';
COMMENT ON COLUMN %s.idempotency_key IS 'Unique key per event_type to prevent duplicate insertions';
COMMENT ON COLUMN %s.status IS 'Current message state in the outbox pattern';
COMMENT ON COLUMN %s.error_message IS 'Last error message (truncated to 4000 bytes, sanitized)';
COMMENT ON COLUMN %s.retry_count IS 'Number of failed publish attempts';
COMMENT ON COLUMN %s.max_retries IS 'Maximum retries before marking as DEAD';
COMMENT ON COLUMN %s.next_retry_at IS 'Scheduled time for next retry attempt (with exponential backoff)';
COMMENT ON COLUMN %s.created_at IS 'Timestamp when message was inserted into outbox';
COMMENT ON COLUMN %s.updated_at IS 'Timestamp of last status change';

-- Index for efficient polling by dispatcher
CREATE INDEX idx_%s_status_created_at
  ON %s (status, created_at);

-- Index for efficient RequeueFailed with next_retry_at
-- Note: This partial index covers the WHERE clause of RequeueFailed query:
--   WHERE status = 'FAILED' AND next_retry_at IS NOT NULL AND next_retry_at <= now()
-- The retry_count column is not included to keep index size small, as most FAILED
-- messages have retry_count < max_retries. If you have many FAILED messages near
-- max_retries, consider adding retry_count to this index.
CREATE INDEX idx_%s_status_next_retry_at
  ON %s (status, next_retry_at)
  WHERE status = 'FAILED' AND next_retry_at IS NOT NULL;

-- Ensure idempotency per event_type
ALTER TABLE %s
  ADD CONSTRAINT uq_%s_event_type_idempotency
    UNIQUE (event_type, idempotency_key);
`, enumName, tableName, enumName,
		tableName, tableName, tableName, tableName, tableName, tableName, tableName, tableName, tableName, tableName, tableName, tableName, // 12x for COMMENT statements
		tableName, tableName, tableName, tableName, tableName, tableName) // 6x for indexes and constraints
}

// DropOutboxDDL generates the DDL to drop the outbox table and its ENUM type.
func DropOutboxDDL(tableName string) string {
	enumName := tableName + "_status"
	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
DROP TYPE IF EXISTS %s;
`, tableName, enumName)
}

// ConsumerInboxDDL generates the DDL for the consumer_inbox table (Transactional Inbox pattern).
//
// Purpose:
//   - Idempotency Store: Ensures exactly-once event processing
//   - Atomic duplicate detection via composite primary key
//   - CRITICAL: Inbox is NOT a message broker - it only tracks "has this EVENT been COMPLETED?"
//
// Design Philosophy:
//   - Primary Key: (consumer_name, event_id) - Natural idempotency via INSERT conflict
//   - NO status field: Only tracks completion via completed_at timestamp
//   - NO locking: SQS visibility timeout handles concurrency control
//   - NO retry logic: Broker handles redelivery
//   - NO stuck detection: Use observability (logs/metrics) instead
//
// CRITICAL: Idempotency Key is event_id (Outbox ID), NOT SQS MessageID
//
// Why event_id, not SQS MessageID:
//   - SQS MessageID changes on EVERY redelivery (Outbox republish, visibility timeout, DLQ requeue)
//   - event_id is the LOGICAL event identity from Outbox
//   - Same event = same event_id, regardless of how many times it's sent to SQS
//   - This ensures true exactly-once processing at the event level
//
// Semantic Model:
//   - Record exists: Event has been COMPLETED (skip forever)
//   - Record missing: Event not yet completed (proceed with processing)
//
// Usage:
//
//	// In consumer handler
//	processed, err := inboxRepo.IsProcessed(ctx, "OrderHandler", msg.EventID)
//	if processed {
//	    return nil // Already completed, skip
//	}
//	// Process event (idempotent logic)...
//	inboxRepo.Complete(ctx, "OrderHandler", msg.EventID)
func ConsumerInboxDDL(tableName string) string {
	return fmt.Sprintf(`-- Consumer Inbox Schema (Transactional Inbox / Idempotency Store)
-- Purpose: Track COMPLETED events only for exactly-once semantics
-- Design: Composite PK (consumer_name, event_id) for atomic duplicate detection
-- Philosophy: Inbox is NOT a broker - it only answers "has this EVENT been completed?"
-- CRITICAL: event_id is Outbox ID (logical event identity), NOT SQS MessageID

CREATE TABLE %s (
  consumer_name    TEXT NOT NULL,
  event_id         UUID NOT NULL,
  completed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);

-- Column comments for consumer_inbox table
COMMENT ON COLUMN %s.consumer_name IS 'Logical consumer service identity (e.g., order-service, notification-service)';
COMMENT ON COLUMN %s.event_id IS 'Outbox event ID (logical event identity). CRITICAL: NOT SQS MessageID. Same event = same event_id across all redeliveries.';
COMMENT ON COLUMN %s.completed_at IS 'Timestamp when event processing was completed successfully';

-- Index for cleanup queries (DELETE WHERE completed_at < ...)
CREATE INDEX idx_%s_completed_at
  ON %s (completed_at);

-- Index for event_id lookups across consumers
CREATE INDEX idx_%s_event_id
  ON %s (event_id);
`, tableName, tableName, tableName, tableName, tableName, tableName, tableName, tableName)
}

// DropConsumerInboxDDL generates the DDL to drop the consumer_inbox table.
func DropConsumerInboxDDL(tableName string) string {
	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
`, tableName)
}
