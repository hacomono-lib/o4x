#!/bin/bash
set -e

# Create o4x_test database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE o4x_test;
EOSQL

# Initialize o4x database schema
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "o4x" <<-'EOSQL'
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
CREATE INDEX idx_outbox_status_next_retry_at
  ON outbox (status, next_retry_at)
  WHERE status = 'FAILED' AND next_retry_at IS NOT NULL;

-- Ensure idempotency per topic
ALTER TABLE outbox
  ADD CONSTRAINT uq_outbox_topic_idempotency
    UNIQUE (topic, idempotency_key);

-- Consumer Schema for SQS Message Processing
-- 4 states: CONSUMING, CONSUMED, FAILED, DEAD

CREATE TYPE consumer_messages_status AS ENUM (
  'CONSUMING',   -- Handler executing
  'CONSUMED',    -- Handler completed, message deleted from SQS
  'FAILED',      -- Handler error (retrying via SQS visibility timeout)
  'DEAD'         -- Retry limit exceeded
);

CREATE TABLE consumer_messages (
  id               UUID PRIMARY KEY,
  outbox_id        UUID,
  message_id       TEXT NOT NULL,
  receipt_handle   TEXT NOT NULL,
  receive_count    INT NOT NULL,
  queue_url        TEXT NOT NULL,
  status           consumer_messages_status NOT NULL DEFAULT 'CONSUMING',
  error_message    TEXT,
  last_error_at    TIMESTAMPTZ,
  max_retries      INT NOT NULL DEFAULT 5,
  processed_at     TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ensure each SQS message is processed only once
ALTER TABLE consumer_messages
  ADD CONSTRAINT uq_consumer_messages_message_id UNIQUE (message_id);

-- Index for querying by status
CREATE INDEX idx_consumer_messages_status ON consumer_messages (status);

-- Index for outbox_id (for correlation queries, no FK constraint)
CREATE INDEX idx_consumer_messages_outbox_id ON consumer_messages (outbox_id)
  WHERE outbox_id IS NOT NULL;
EOSQL

# Initialize o4x_test database schema (same as o4x)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "o4x_test" <<-'EOSQL'
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
CREATE INDEX idx_outbox_status_next_retry_at
  ON outbox (status, next_retry_at)
  WHERE status = 'FAILED' AND next_retry_at IS NOT NULL;

-- Ensure idempotency per topic
ALTER TABLE outbox
  ADD CONSTRAINT uq_outbox_topic_idempotency
    UNIQUE (topic, idempotency_key);

-- Consumer Schema for SQS Message Processing
-- 4 states: CONSUMING, CONSUMED, FAILED, DEAD

CREATE TYPE consumer_messages_status AS ENUM (
  'CONSUMING',   -- Handler executing
  'CONSUMED',    -- Handler completed, message deleted from SQS
  'FAILED',      -- Handler error (retrying via SQS visibility timeout)
  'DEAD'         -- Retry limit exceeded
);

CREATE TABLE consumer_messages (
  id               UUID PRIMARY KEY,
  outbox_id        UUID,
  message_id       TEXT NOT NULL,
  receipt_handle   TEXT NOT NULL,
  receive_count    INT NOT NULL,
  queue_url        TEXT NOT NULL,
  status           consumer_messages_status NOT NULL DEFAULT 'CONSUMING',
  error_message    TEXT,
  last_error_at    TIMESTAMPTZ,
  max_retries      INT NOT NULL DEFAULT 5,
  processed_at     TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ensure each SQS message is processed only once
ALTER TABLE consumer_messages
  ADD CONSTRAINT uq_consumer_messages_message_id UNIQUE (message_id);

-- Index for querying by status
CREATE INDEX idx_consumer_messages_status ON consumer_messages (status);

-- Index for outbox_id (for correlation queries, no FK constraint)
CREATE INDEX idx_consumer_messages_outbox_id ON consumer_messages (outbox_id)
  WHERE outbox_id IS NOT NULL;
EOSQL

echo "Initialized o4x and o4x_test databases"
