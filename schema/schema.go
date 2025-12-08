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
  topic            TEXT NOT NULL,
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

-- Ensure idempotency per topic
ALTER TABLE %s
  ADD CONSTRAINT uq_%s_topic_idempotency
    UNIQUE (topic, idempotency_key);
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
//   - Simpler design than consumer_messages (2 statuses: PROCESSING, COMPLETED)
//
// Design Differences from consumer_messages:
//   - Primary Key: (consumer_name, message_id) - Natural idempotency
//   - Status: ENUM type (consistent with outbox_status pattern)
//   - No outbox_id, receipt_handle, receive_count, etc.
//   - Focus: Idempotency checking, not SQS state tracking
//
// Usage:
//
//	// In consumer handler
//	ok, err := inboxRepo.TryStart(ctx, "OrderHandler", msg.MessageID)
//	if !ok {
//	    return nil // Duplicate
//	}
//	// Process message...
//	inboxRepo.Complete(ctx, "OrderHandler", msg.MessageID)
func ConsumerInboxDDL(tableName string) string {
	enumName := tableName + "_status"
	return fmt.Sprintf(`-- Consumer Inbox Schema (Transactional Inbox / Idempotency Store)
-- Purpose: Ensure exactly-once message processing semantics
-- Design: Composite PK (consumer_name, message_id) for atomic duplicate detection
-- 2 states: PROCESSING, COMPLETED

CREATE TYPE %s AS ENUM (
  'PROCESSING',  -- Message currently being processed
  'COMPLETED'    -- Message successfully processed
);

CREATE TABLE %s (
  consumer_name    TEXT NOT NULL,
  message_id       TEXT NOT NULL,
  status           %s NOT NULL DEFAULT 'PROCESSING',
  received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at     TIMESTAMPTZ,
  PRIMARY KEY (consumer_name, message_id)
);

-- Index for cleanup queries (DELETE WHERE status = 'COMPLETED' AND received_at < ...)
CREATE INDEX idx_%s_status_received_at
  ON %s (status, received_at);
`, enumName, tableName, enumName, tableName, tableName)
}

// DropConsumerInboxDDL generates the DDL to drop the consumer_inbox table and its ENUM type.
func DropConsumerInboxDDL(tableName string) string {
	enumName := tableName + "_status"
	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
DROP TYPE IF EXISTS %s;
`, tableName, enumName)
}
