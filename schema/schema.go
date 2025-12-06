// Package schema provides DDL generation helpers for o4x tables.
// These helpers allow users to generate DDL with custom table names for PostgreSQL.
package schema

import (
	"fmt"
	"strings"
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

// ConsumerMessagesDDL generates the DDL for the consumer_messages table with the given table name.
// The ENUM type name will be derived from the table name.
//
// Note: outbox_id does NOT have a foreign key constraint. Reasons:
//  1. Consumer messages table is optional (can be enabled later)
//  2. Outbox messages are periodically deleted (PUBLISHED cleanup)
//  3. Consumer messages should be kept longer for audit trail
//  4. The two tables have independent lifecycles (Publisher vs Consumer side)
func ConsumerMessagesDDL(tableName string) string {
	enumName := tableName + "_status"

	return fmt.Sprintf(`-- Consumer Schema for SQS Message Processing
-- 4 states: CONSUMING, CONSUMED, FAILED, DEAD
-- NOTE: This is completely separate from outbox_status

CREATE TYPE %s AS ENUM (
  'CONSUMING',   -- Handler executing
  'CONSUMED',    -- Handler completed, message deleted from SQS
  'FAILED',      -- Handler error (retrying via SQS visibility timeout)
  'DEAD'         -- Retry limit exceeded
);

CREATE TABLE %s (
  id               UUID PRIMARY KEY,
  outbox_id        UUID,
  message_id       TEXT NOT NULL,
  receipt_handle   TEXT NOT NULL,
  receive_count    INT NOT NULL,
  queue_url        TEXT NOT NULL,
  status           %s NOT NULL DEFAULT 'CONSUMING',
  error_message    TEXT,
  last_error_at    TIMESTAMPTZ,
  max_retries      INT NOT NULL DEFAULT 5,
  processed_at     TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ensure each SQS message is processed only once
ALTER TABLE %s
  ADD CONSTRAINT uq_%s_message_id UNIQUE (message_id);

-- Index for querying by status
CREATE INDEX idx_%s_status ON %s (status);

-- Index for outbox_id (for correlation queries, no FK constraint)
CREATE INDEX idx_%s_outbox_id ON %s (outbox_id)
  WHERE outbox_id IS NOT NULL;
`, enumName, tableName, enumName, tableName, tableName, tableName, tableName, tableName, tableName)
}

// DropOutboxDDL generates the DDL to drop the outbox table and its ENUM type.
func DropOutboxDDL(tableName string) string {
	enumName := tableName + "_status"
	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
DROP TYPE IF EXISTS %s;
`, tableName, enumName)
}

// DropConsumerMessagesDDL generates the DDL to drop the consumer_messages table and its ENUM type.
func DropConsumerMessagesDDL(tableName string) string {
	enumName := tableName + "_status"
	return fmt.Sprintf(`DROP TABLE IF EXISTS %s;
DROP TYPE IF EXISTS %s;
`, tableName, enumName)
}

// MigrationSQL generates a complete migration SQL with both outbox and consumer tables.
// Use this for a full setup with both tables.
func MigrationSQL(outboxTableName, consumerTableName string) string {
	var sb strings.Builder
	sb.WriteString("-- o4x Migration: Outbox + Consumer Tables\n")
	sb.WriteString("-- Generated by github.com/hacomono-lib/o4x/schema\n\n")
	sb.WriteString("BEGIN;\n\n")
	sb.WriteString(OutboxDDL(outboxTableName))
	sb.WriteString("\n")
	sb.WriteString(ConsumerMessagesDDL(consumerTableName))
	sb.WriteString("\nCOMMIT;\n")
	return sb.String()
}

// RollbackSQL generates the DDL to drop both outbox and consumer tables.
func RollbackSQL(outboxTableName, consumerTableName string) string {
	var sb strings.Builder
	sb.WriteString("-- o4x Rollback: Drop Outbox + Consumer Tables\n")
	sb.WriteString("-- Generated by github.com/hacomono-lib/o4x/schema\n\n")
	sb.WriteString("BEGIN;\n\n")
	// Order doesn't matter since there's no FK dependency
	sb.WriteString(DropConsumerMessagesDDL(consumerTableName))
	sb.WriteString("\n")
	sb.WriteString(DropOutboxDDL(outboxTableName))
	sb.WriteString("\nCOMMIT;\n")
	return sb.String()
}
