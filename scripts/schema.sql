-- Outbox Schema for Publisher (Transactional Outbox Pattern)
-- 5 states: ENQUEUED, PUBLISHING, PUBLISHED, FAILED, DEAD

CREATE TYPE outbox_status AS ENUM (
  'ENQUEUED',    -- Application inserted into outbox
  'PUBLISHING',  -- Dispatcher locked and publishing
  'PUBLISHED',   -- Publish succeeded
  'FAILED',      -- Publish failed (retryable)
  'DEAD'         -- Retry limit exceeded
);

CREATE TABLE outbox (
  id               UUID PRIMARY KEY,
  event_type       TEXT NOT NULL,
  payload          JSONB NOT NULL,
  metadata         JSONB,
  idempotency_key  TEXT NOT NULL,
  status           outbox_status NOT NULL DEFAULT 'ENQUEUED',
  error_message    TEXT,
  retry_count      INT NOT NULL DEFAULT 0,
  max_retries      INT NOT NULL DEFAULT 10,
  next_retry_at    TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Column comments for outbox table
COMMENT ON COLUMN outbox.id IS 'Unique message identifier (UUID v7 for time-ordered)';
COMMENT ON COLUMN outbox.event_type IS 'Event type identifier for routing (e.g. order.created)';
COMMENT ON COLUMN outbox.payload IS 'Message payload as JSON';
COMMENT ON COLUMN outbox.metadata IS 'Optional metadata (trace context, custom headers, etc.)';
COMMENT ON COLUMN outbox.idempotency_key IS 'Unique key per event_type to prevent duplicate insertions';
COMMENT ON COLUMN outbox.status IS 'Current message state in the outbox pattern';
COMMENT ON COLUMN outbox.error_message IS 'Last error message (truncated to 4000 bytes, sanitized)';
COMMENT ON COLUMN outbox.retry_count IS 'Number of failed publish attempts';
COMMENT ON COLUMN outbox.max_retries IS 'Maximum retries before marking as DEAD';
COMMENT ON COLUMN outbox.next_retry_at IS 'Scheduled time for next retry attempt (with exponential backoff)';
COMMENT ON COLUMN outbox.created_at IS 'Timestamp when message was inserted into outbox';
COMMENT ON COLUMN outbox.updated_at IS 'Timestamp of last status change';

-- Index for efficient polling by dispatcher
CREATE INDEX idx_outbox_status_created_at
  ON outbox (status, created_at);

-- Index for efficient RequeueFailed with next_retry_at
-- Note: This partial index covers the WHERE clause of RequeueFailed query:
--   WHERE status = 'FAILED' AND next_retry_at IS NOT NULL AND next_retry_at <= now()
-- The retry_count column is not included to keep index size small, as most FAILED
-- messages have retry_count < max_retries. If you have many FAILED messages near
-- max_retries, consider adding retry_count to this index.
CREATE INDEX idx_outbox_status_next_retry_at
  ON outbox (status, next_retry_at)
  WHERE status = 'FAILED' AND next_retry_at IS NOT NULL;

-- Ensure idempotency per event_type
ALTER TABLE outbox
  ADD CONSTRAINT uq_outbox_event_type_idempotency
    UNIQUE (event_type, idempotency_key);

-- Consumer Inbox Schema (Transactional Inbox / Idempotency Store)
-- Purpose: Track COMPLETED events only for exactly-once semantics
-- Design: Composite PK (consumer_name, event_id) for atomic duplicate detection
-- Philosophy: Inbox is NOT a broker - it only answers "has this EVENT been completed?"
-- CRITICAL: event_id is Outbox ID (logical event identity), NOT SQS MessageID

CREATE TABLE consumer_inbox (
  consumer_name    TEXT NOT NULL,
  event_id         UUID NOT NULL,
  completed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);

-- Column comments for consumer_inbox table
COMMENT ON COLUMN consumer_inbox.consumer_name IS 'Logical consumer service identity (e.g., order-service, notification-service)';
COMMENT ON COLUMN consumer_inbox.event_id IS 'Outbox event ID (logical event identity). CRITICAL: NOT SQS MessageID. Same event = same event_id across all redeliveries.';
COMMENT ON COLUMN consumer_inbox.completed_at IS 'Timestamp when event processing was completed successfully';

-- Index for cleanup queries (DELETE WHERE completed_at < ...)
CREATE INDEX idx_consumer_inbox_completed_at
  ON consumer_inbox (completed_at);
