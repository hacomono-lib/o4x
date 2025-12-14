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
-- Purpose: Track COMPLETED messages only for exactly-once semantics
-- Design: Composite PK (consumer_name, message_id) for atomic duplicate detection
-- Philosophy: Inbox is NOT a broker - it only answers "has this been completed?"

CREATE TABLE consumer_inbox (
  consumer_name    TEXT NOT NULL,
  message_id       TEXT NOT NULL,
  completed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, message_id)
);

-- Index for cleanup queries (DELETE WHERE completed_at < ...)
CREATE INDEX idx_consumer_inbox_completed_at
  ON consumer_inbox (completed_at);
