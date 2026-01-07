# Database Schema and Index Optimization Guide

This document explains the o4x database schema, indexes, and how to verify query performance.

## Table of Contents

- [Schema Overview](#schema-overview)
- [Indexes Explained](#indexes-explained)
- [Query Performance Verification](#query-performance-verification)
- [Index Tuning](#index-tuning)
- [Maintenance](#maintenance)

## Schema Overview

### Outbox Table

The outbox table stores messages for the transactional outbox pattern.

```sql
CREATE TABLE outbox (
  id               UUID PRIMARY KEY,
  event_type       TEXT NOT NULL,
  payload          JSONB NOT NULL,
  metadata         JSONB,                                         -- Optional trace context, custom headers
  idempotency_key  TEXT NOT NULL,
  status           outbox_status NOT NULL DEFAULT 'ENQUEUED',
  error_message    TEXT,
  attempt_count      INT NOT NULL DEFAULT 0,
  max_attempts      INT NOT NULL DEFAULT 10,
  next_retry_at    TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_outbox_event_type_idempotency UNIQUE (event_type, idempotency_key)
);
```

### Consumer Inbox Table (Optional)

The consumer_inbox table provides idempotency checking for SQS message handlers.

```sql
CREATE TABLE consumer_inbox (
  consumer_name  TEXT NOT NULL,
  event_id       UUID NOT NULL,
  completed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);

-- Indexes
CREATE INDEX idx_consumer_inbox_completed_at ON consumer_inbox (completed_at);
CREATE INDEX idx_consumer_inbox_event_id ON consumer_inbox (event_id);
```

## Indexes Explained

### 1. `idx_outbox_status_created_at`

**Purpose**: Efficient polling by Dispatcher workers

**Query pattern**:
```sql
SELECT id FROM outbox
WHERE status = 'ENQUEUED'
ORDER BY created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

**Why this index works**:
- Filters by `status = 'ENQUEUED'` first (highly selective)
- Sorts by `created_at` using index (no separate sort step)
- `LIMIT 1` stops scanning after finding first match

**Performance characteristics**:
- Best case: O(1) when messages are available
- Worst case: O(n) when scanning for non-locked rows with SKIP LOCKED
- Expected: < 1ms for tables with < 1M rows

### 2. `idx_outbox_status_next_retry_at` (Partial Index)

**Purpose**: Efficient retry of FAILED messages

**Query pattern**:
```sql
UPDATE outbox
SET status = 'ENQUEUED', updated_at = now()
WHERE status = 'FAILED'
  AND attempt_count < max_attempts
  AND next_retry_at IS NOT NULL
  AND next_retry_at <= now();
```

**Why partial index**:
```sql
CREATE INDEX idx_outbox_status_next_retry_at
  ON outbox (status, next_retry_at)
  WHERE status = 'FAILED' AND next_retry_at IS NOT NULL;
```

- Only indexes FAILED messages (smaller index size)
- Covers the `status = 'FAILED' AND next_retry_at IS NOT NULL` conditions
- Does NOT include `attempt_count` to keep index small

**Trade-off**:
- ✅ Smaller index size (only FAILED messages)
- ✅ Faster index updates
- ⚠️ May scan extra rows if many FAILED messages have `attempt_count >= max_attempts`

**When to add `attempt_count` to index**:
If you frequently have >10,000 FAILED messages with varying attempt_count, consider:
```sql
CREATE INDEX idx_outbox_retry_failed
  ON outbox (status, attempt_count, next_retry_at)
  WHERE status = 'FAILED' AND next_retry_at IS NOT NULL;
```

### 3. `uq_outbox_topic_idempotency` (Unique Constraint)

**Purpose**: Prevent duplicate message insertion

**Query pattern**:
```sql
INSERT INTO outbox (id, event_type, payload, idempotency_key, ...)
VALUES (...);
```

**Behavior**:
- Throws `unique_violation` error (SQL code 23505) on duplicate
- Repository catches this and returns `core.ErrAlreadyExists`
- Application can handle duplicates gracefully

**Performance**:
- Adds overhead to INSERT operations (unique check)
- B-tree index scan: O(log n)
- Negligible for < 10M rows

### Consumer Inbox Indexes

#### Primary Key `(consumer_name, event_id)`

**Purpose**: Prevent duplicate event processing

**Query pattern**:
```sql
SELECT consumer_name, event_id, completed_at
FROM consumer_inbox
WHERE consumer_name = $1 AND event_id = $2;
```

**Performance**:
- B-tree index scan: O(log n)
- Fast lookup for idempotency checking
- Prevents duplicate INSERT via unique constraint

#### `idx_consumer_inbox_completed_at`

**Purpose**: Efficient cleanup of old completed events

**Query pattern**:
```sql
DELETE FROM consumer_inbox
WHERE completed_at < NOW() - INTERVAL '7 days';
```

**Performance**:
- Index scan for range queries on completed_at
- Enables efficient batch deletion of old records
- Critical for preventing table bloat

#### `idx_consumer_inbox_event_id`

**Purpose**: Event ID lookups across multiple consumers

**Query pattern**:
```sql
SELECT consumer_name, event_id, completed_at
FROM consumer_inbox
WHERE event_id = $1;
```

**Use cases**:
- Debugging: Find which consumers processed a specific event
- Monitoring: Track event processing across services
- Auditing: Verify event delivery to multiple consumers

**Performance**:
- B-tree index scan: O(log n)
- Enables efficient cross-consumer event lookups
- Useful when event_id is known but consumer_name is not

## Query Performance Verification

### Using EXPLAIN ANALYZE

Check actual query performance and index usage:

```sql
-- Dispatcher polling (should use idx_outbox_status_created_at)
EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM outbox
WHERE status = 'ENQUEUED'
ORDER BY created_at ASC
LIMIT 1;
```

**Expected output**:
```
Limit  (cost=0.42..0.48 rows=1 width=16) (actual time=0.015..0.016 rows=1 loops=1)
  ->  Index Scan using idx_outbox_status_created_at on outbox  (cost=0.42..10.45 rows=100 width=16) (actual time=0.014..0.014 rows=1 loops=1)
        Index Cond: (status = 'ENQUEUED'::outbox_status)
```

**Good signs**:
- ✅ `Index Scan` (not `Seq Scan`)
- ✅ `actual time` < 1ms
- ✅ `rows=1` (stopped after finding first match)

**Bad signs**:
- ❌ `Seq Scan` (index not being used)
- ❌ `actual time` > 10ms
- ❌ `rows` >> 1 (scanned many rows)

### RequeueFailed Query

```sql
EXPLAIN (ANALYZE, BUFFERS)
UPDATE outbox
SET status = 'ENQUEUED', updated_at = now()
WHERE status = 'FAILED'
  AND attempt_count < max_attempts
  AND next_retry_at IS NOT NULL
  AND next_retry_at <= now();
```

**Expected output**:
```
Update on outbox  (cost=8.17..20.35 rows=10 width=100) (actual time=0.123..0.123 rows=0 loops=1)
  ->  Index Scan using idx_outbox_status_next_retry_at on outbox  (cost=0.42..20.35 rows=10 width=100) (actual time=0.120..0.120 rows=0 loops=1)
        Index Cond: ((status = 'FAILED'::outbox_status) AND (next_retry_at IS NOT NULL) AND (next_retry_at <= now()))
        Filter: (attempt_count < max_attempts)
```

**Good signs**:
- ✅ `Index Scan` on `idx_outbox_status_next_retry_at`
- ✅ `Filter: (attempt_count < max_attempts)` appears AFTER index scan
- ✅ Low `rows` count in Index Scan

**Potential issue**:
If `rows` in Index Scan >> `rows` in Update (e.g., index scans 10000 rows but updates only 10), it means many FAILED messages have `attempt_count >= max_attempts`. Consider:
1. Running periodic cleanup: `DELETE FROM outbox WHERE status = 'DEAD' AND updated_at < now() - interval '30 days'`
2. Adding `attempt_count` to the index (see "Index Tuning" below)

## Index Tuning

### Scenario 1: High FAILED Message Count

**Symptom**: RequeueFailed is slow (>100ms)

**Diagnosis**:
```sql
SELECT status, COUNT(*), AVG(attempt_count), MAX(attempt_count)
FROM outbox
GROUP BY status;
```

**If you see**:
```
 status  | count  | avg | max
---------+--------+-----+-----
 FAILED  | 100000 |  8  |  10
```

**Solution**: Add attempt_count to index
```sql
DROP INDEX idx_outbox_status_next_retry_at;

CREATE INDEX idx_outbox_retry_failed
  ON outbox (status, attempt_count, next_retry_at)
  WHERE status = 'FAILED' AND next_retry_at IS NOT NULL;
```

**Trade-off**:
- ✅ Faster RequeueFailed (filters out near-max-retries messages)
- ❌ Larger index size (~1.5x)
- ❌ Slower UPDATE (index maintenance on attempt_count change)

### Scenario 2: Table Bloat

**Symptom**: Queries slow despite correct indexes

**Diagnosis**:
```sql
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
  n_live_tup,
  n_dead_tup,
  round(n_dead_tup::numeric * 100 / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct
FROM pg_stat_user_tables
WHERE tablename IN ('outbox', 'consumer_inbox');
```

**If dead_pct > 20%**, run VACUUM:
```sql
VACUUM ANALYZE outbox;
VACUUM ANALYZE consumer_inbox;
```

**For heavy bloat (dead_pct > 50%)**, run VACUUM FULL (requires table lock):
```sql
VACUUM FULL outbox;
```

### Scenario 3: Index Bloat

**Diagnosis**:
```sql
SELECT
  schemaname,
  tablename,
  indexname,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
  idx_scan,
  idx_tup_read,
  idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename IN ('outbox', 'consumer_inbox')
ORDER BY pg_relation_size(indexrelid) DESC;
```

**If idx_scan = 0** (unused index):
```sql
DROP INDEX unused_index_name;
```

**If index_size is very large**, rebuild:
```sql
REINDEX INDEX idx_outbox_status_created_at;
```

## Maintenance

### Periodic Cleanup

Clean up old PUBLISHED and DEAD messages to prevent table bloat:

```go
import "github.com/hacomono-lib/o4x/contrib/pgx"

repo := pgx.NewOutboxRepository(pool)

// Delete PUBLISHED messages older than 7 days
count, err := repo.DeleteOlderThan(ctx, []core.OutboxStatus{core.OutboxStatusPublished}, 7*24*time.Hour)
log.Printf("Deleted %d PUBLISHED messages", count)

// Delete DEAD messages older than 30 days
count, err = repo.DeleteOlderThan(ctx, []core.OutboxStatus{core.OutboxStatusDead}, 30*24*time.Hour)
log.Printf("Deleted %d DEAD messages", count)

// Or delete both PUBLISHED and DEAD messages in a single call
count, err = repo.DeleteOlderThan(ctx, []core.OutboxStatus{core.OutboxStatusPublished, core.OutboxStatusDead}, 7*24*time.Hour)
log.Printf("Deleted %d messages", count)
```

**Recommended schedule**:
- PUBLISHED: Daily cleanup (keep 7 days)
- DEAD: Weekly cleanup (keep 30 days for investigation)

### Monitoring Queries

**Check message distribution by status**:
```sql
SELECT
  status,
  COUNT(*) as count,
  MIN(created_at) as oldest,
  MAX(created_at) as newest,
  pg_size_pretty(pg_total_relation_size('outbox')) as table_size
FROM outbox
GROUP BY status
ORDER BY count DESC;
```

**Check stuck PUBLISHING messages**:
```sql
SELECT id, event_type, attempt_count, max_attempts,
       updated_at, NOW() - updated_at as stuck_duration, error_message
FROM outbox
WHERE status = 'PUBLISHING'
  AND updated_at < NOW() - INTERVAL '5 minutes'
ORDER BY updated_at ASC;
```

**Check high retry count messages**:
```sql
SELECT id, event_type, status, attempt_count, max_attempts, error_message
FROM outbox
WHERE attempt_count >= max_attempts - 2
  AND status IN ('FAILED', 'DEAD')
ORDER BY created_at DESC
LIMIT 20;
```

### Performance Baselines

Expected query performance for well-configured systems:

| Query | Table Size | Expected Time |
|-------|-----------|---------------|
| FetchAndLockToPublishing (single) | < 1M rows | < 1ms |
| FetchLockAndMarkPublishing (batch 10) | < 1M rows | < 5ms |
| RequeueFailed (10 messages) | < 1M rows | < 10ms |
| UpdateToPublished | any | < 1ms |
| UpdateToFailed | any | < 2ms |

**If queries exceed these baselines**:
1. Run EXPLAIN ANALYZE to verify index usage
2. Check for table bloat (run VACUUM)
3. Consider adding attempt_count to index (if many FAILED messages)
4. Verify connection pool is not exhausted

### Database Monitoring Checklist

✅ **Weekly**:
- Check table bloat (dead_pct < 20%)
- Verify index usage (idx_scan > 0 for critical indexes)
- Monitor DEAD message count

✅ **Monthly**:
- VACUUM ANALYZE on outbox and consumer_inbox tables
- Review slow query logs
- Check if index rebuild is needed (compare index_size trend)

✅ **Quarterly**:
- Review and optimize RequeueFailed performance
- Consider archiving old DEAD messages to separate table
- Evaluate if attempt_count index is needed
