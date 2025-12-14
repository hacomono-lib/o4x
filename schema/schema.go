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
`, enumName, tableName, enumName, tableName, tableName, tableName, tableName, tableName, tableName)
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
//   - Idempotency Store: Ensures exactly-once message processing
//   - Atomic duplicate detection via composite primary key
//   - CRITICAL: Inbox is NOT a message broker - it only tracks "has this been COMPLETED?"
//
// Design Philosophy:
//   - Primary Key: (consumer_name, message_id) - Natural idempotency via INSERT conflict
//   - NO status field: Only tracks completion via completed_at timestamp
//   - NO locking: SQS visibility timeout handles concurrency control
//   - NO retry logic: Broker handles redelivery
//   - NO stuck detection: Use observability (logs/metrics) instead
//
// Semantic Model:
//   - Record exists: Message has been COMPLETED (skip forever)
//   - Record missing: Message not yet completed (proceed with processing)
//
// Usage:
//
//	// In consumer handler
//	ok, err := inboxRepo.TryStart(ctx, "OrderHandler", msg.MessageID)
//	if !ok {
//	    return nil // Already completed, skip
//	}
//	// Process message (idempotent logic)...
//	inboxRepo.Complete(ctx, "OrderHandler", msg.MessageID)
func ConsumerInboxDDL(tableName string) string {
	return fmt.Sprintf(`-- Consumer Inbox Schema (Transactional Inbox / Idempotency Store)
-- Purpose: Track COMPLETED messages only for exactly-once semantics
-- Design: Composite PK (consumer_name, message_id) for atomic duplicate detection
-- Philosophy: Inbox is NOT a broker - it only answers "has this been completed?"

CREATE TABLE %s (
  consumer_name    TEXT NOT NULL,
  message_id       TEXT NOT NULL,
  completed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, message_id)
);

-- Index for cleanup queries (DELETE WHERE completed_at < ...)
CREATE INDEX idx_%s_completed_at
  ON %s (completed_at);
`, tableName, tableName, tableName)
}

// DropConsumerInboxDDL generates the DDL to drop the consumer_inbox table.
func DropConsumerInboxDDL(tableName string) string {
	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
`, tableName)
}
