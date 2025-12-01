-- Seed data for local development
-- Insert sample outbox messages for testing

-- Enable pgcrypto extension for gen_random_bytes
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Temporary UUID v7 generation function for seeding
-- This is only used for test data and won't affect production schema
CREATE OR REPLACE FUNCTION uuid_generate_v7_temp()
RETURNS uuid AS $$
DECLARE
  unix_ts_ms BIGINT;
  uuid_bytes BYTEA;
BEGIN
  unix_ts_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
  uuid_bytes := 
    substring(int8send(unix_ts_ms) from 3 for 6) ||
    substring(gen_random_bytes(10) from 1 for 2) ||
    gen_random_bytes(8);
  uuid_bytes := set_byte(uuid_bytes, 6, (get_byte(uuid_bytes, 6) & 15) | 112);
  uuid_bytes := set_byte(uuid_bytes, 8, (get_byte(uuid_bytes, 8) & 63) | 128);
  RETURN encode(uuid_bytes, 'hex')::uuid;
END;
$$ LANGUAGE plpgsql VOLATILE;

INSERT INTO outbox (id, topic, payload, idempotency_key, status, max_retries)
VALUES
  (uuid_generate_v7_temp(), 'order.created', '{"order_id": "ord-001", "user_id": "usr-001", "total": 99.99}', 'order-001-v1', 'ENQUEUED', 10),
  (uuid_generate_v7_temp(), 'order.created', '{"order_id": "ord-002", "user_id": "usr-002", "total": 149.99}', 'order-002-v1', 'ENQUEUED', 10),
  (uuid_generate_v7_temp(), 'user.registered', '{"user_id": "usr-003", "email": "test@example.com", "name": "Test User"}', 'user-003-v1', 'ENQUEUED', 10)
ON CONFLICT DO NOTHING;

-- Drop the temporary function after use
DROP FUNCTION uuid_generate_v7_temp();
