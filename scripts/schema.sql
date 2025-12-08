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
  topic            TEXT NOT NULL,
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

-- Ensure idempotency per topic
ALTER TABLE outbox
  ADD CONSTRAINT uq_outbox_topic_idempotency
    UNIQUE (topic, idempotency_key);

-- Consumer Inbox Schema (Transactional Inbox / Idempotency Store)
-- Purpose: Ensure exactly-once message processing semantics
-- Design: Composite PK (consumer_name, message_id) for atomic duplicate detection
-- 2 states: PROCESSING, COMPLETED

CREATE TYPE consumer_inbox_status AS ENUM (
  'PROCESSING',  -- Message currently being processed
  'COMPLETED'    -- Message successfully processed
);

CREATE TABLE consumer_inbox (
  consumer_name    TEXT NOT NULL,
  message_id       TEXT NOT NULL,
  status           consumer_inbox_status NOT NULL DEFAULT 'PROCESSING',
  received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at     TIMESTAMPTZ,
  PRIMARY KEY (consumer_name, message_id)
);

-- Index for cleanup queries (DELETE WHERE status = 'COMPLETED' AND received_at < ...)
CREATE INDEX idx_consumer_inbox_status_received_at
  ON consumer_inbox (status, received_at);
