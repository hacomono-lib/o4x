# Broker Migration Strategy

When migrating from one message broker to another (e.g., SQS to Kafka, Kafka to Pub/Sub), you need a strategy that minimizes downtime and risk. This guide covers various approaches with their trade-offs and provides a recommended migration path.

## Table of Contents

- [Migration Approaches](#migration-approaches)
  - [Approach 1: Dual Outbox Tables](#approach-1-dual-outbox-tables-zero-downtime-migration)
  - [Approach 2: Single Outbox + Multi-Publisher](#approach-2-single-outbox--multi-publisher)
  - [Approach 3: CDC (Change Data Capture)](#approach-3-cdc-change-data-capture)
  - [Approach 4: Strangler Fig Pattern (Recommended)](#approach-4-strangler-fig-pattern-recommended)
- [Consumer-Side Idempotency](#consumer-side-idempotency-critical-for-all-approaches)
- [Recommended Migration Steps](#recommended-migration-steps)
- [Migration Checklist](#migration-checklist)
- [Monitoring During Migration](#monitoring-during-migration)

## Migration Approaches

### Approach 1: Dual Outbox Tables (Zero-Downtime Migration)

Create separate outbox tables for each broker and write to both during migration.

**Implementation:**

```go
// Migration period: Dual write to both outbox tables
type DualOutboxRepository struct {
    sqsRepo   core.OutboxRepository  // Existing SQS outbox
    kafkaRepo core.OutboxRepository  // New Kafka outbox
}

func (r *DualOutboxRepository) Insert(ctx context.Context, params core.OutboxInsertParams) (*core.Outbox, error) {
    // Write to both outboxes within the same transaction
    msg1, err := r.sqsRepo.Insert(ctx, params)
    if err != nil {
        return nil, err
    }

    msg2, err := r.kafkaRepo.Insert(ctx, params)
    if err != nil {
        return nil, err
    }

    return msg1, nil
}
```

**Architecture Diagram:**

```
Application Transaction:
┌─────────────────────────────────────────┐
│ Business Logic (INSERT INTO orders)    │
│         ↓                               │
│ DualOutboxRepository.Insert()           │
│    ├──→ outbox_sqs   (INSERT)          │
│    └──→ outbox_kafka (INSERT)          │
│                                         │
│ COMMIT                                  │
└─────────────────────────────────────────┘

Publishing (Separate Processes):
┌─────────────────────┐    ┌─────────────────────┐
│ SQS Dispatcher      │    │ Kafka Dispatcher    │
│   ↓                 │    │   ↓                 │
│ SELECT FROM         │    │ SELECT FROM         │
│ outbox_sqs          │    │ outbox_kafka        │
│   ↓                 │    │   ↓                 │
│ Publish to SQS      │    │ Publish to Kafka    │
└─────────────────────┘    └─────────────────────┘
```

**Pros:**
- ✅ Zero-downtime migration
- ✅ Easy rollback (just stop Kafka Dispatcher)
- ✅ Gradual validation possible
- ✅ Both brokers can run independently
- ✅ Clear separation of concerns

**Cons:**
- ❌ 2x storage during migration period
- ❌ Application code changes required (DualOutboxRepository)
- ❌ Must run both Dispatchers
- ❌ Need to manage two sets of PUBLISHED/DEAD messages

**When to Use:**
- Long migration period expected
- Need high confidence before committing to new broker
- Resources (storage, compute) are not constrained
- Want to run comprehensive production testing in parallel

---

### Approach 2: Single Outbox + Multi-Publisher

Keep one outbox table but publish to multiple brokers.

**Implementation:**

```go
// Single outbox, dual publishers
type DualPublisher struct {
    sqsPublisher   core.Publisher
    kafkaPublisher core.Publisher
    migrationMode  bool  // Feature flag
}

func (p *DualPublisher) Publish(ctx context.Context, msg *core.Outbox) error {
    var sqsErr, kafkaErr error

    // Publish to both brokers
    sqsErr = p.sqsPublisher.Publish(ctx, msg)
    kafkaErr = p.kafkaPublisher.Publish(ctx, msg)

    if p.migrationMode {
        // During migration: prioritize SQS success
        return sqsErr
    }

    // After migration: prioritize Kafka success
    if kafkaErr != nil {
        return kafkaErr
    }
    return nil
}
```

**Architecture Diagram:**

```
Application Transaction:
┌─────────────────────────────────────────┐
│ Business Logic (INSERT INTO orders)    │
│         ↓                               │
│ OutboxRepository.Insert()               │
│         ↓                               │
│ outbox (single table)                   │
│                                         │
│ COMMIT                                  │
└─────────────────────────────────────────┘

Publishing:
┌─────────────────────────────────────────┐
│ Dispatcher (single process)             │
│   ↓                                     │
│ SELECT FROM outbox                      │
│   ↓                                     │
│ DualPublisher.Publish()                 │
│   ├──→ Publish to SQS                  │
│   └──→ Publish to Kafka                │
│         ↓                               │
│ Update outbox status based on result   │
└─────────────────────────────────────────┘
```

**Pros:**
- ✅ Single outbox table (no storage overhead)
- ✅ No application code changes (Insert layer)
- ✅ Publisher swap only
- ✅ Single Dispatcher process

**Cons:**
- ❌ Complex failure handling (what if only one broker succeeds?)
- ❌ Both brokers may receive retries
- ❌ Status tracking becomes ambiguous (PUBLISHED to which broker?)
- ❌ Difficult to monitor per-broker success rates

**When to Use:**
- Short migration window expected
- Storage constraints exist
- Want to minimize application layer changes
- Comfortable with more complex Publisher logic

---

### Approach 3: CDC (Change Data Capture)

Use CDC tools to replicate outbox changes to new broker.

**Architecture Diagram:**

```
Existing System:
┌─────────────────────────────────────────┐
│ Application                             │
│         ↓                               │
│ outbox_sqs (PostgreSQL)                 │
│         ↓                               │
│ SQS Dispatcher                          │
│         ↓                               │
│ Amazon SQS                              │
└─────────────────────────────────────────┘

Add CDC Layer:
┌─────────────────────────────────────────┐
│ outbox_sqs (PostgreSQL)                 │
│    ├──→ SQS Dispatcher → Amazon SQS    │
│    └──→ CDC (Debezium) → Kafka         │
└─────────────────────────────────────────┘

After Migration:
┌─────────────────────────────────────────┐
│ Application                             │
│         ↓                               │
│ outbox_kafka (PostgreSQL)               │
│         ↓                               │
│ Kafka Dispatcher                        │
│         ↓                               │
│ Apache Kafka                            │
└─────────────────────────────────────────┘
```

**CDC Configuration Example (Debezium):**

```json
{
  "name": "outbox-sqs-to-kafka-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres.example.com",
    "database.port": "5432",
    "database.user": "debezium",
    "database.dbname": "o4x",
    "table.include.list": "public.outbox_sqs",
    "topic.prefix": "o4x",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.topic.replacement": "${routedByValue}",
    "transforms.outbox.table.field.event.key": "idempotency_key",
    "transforms.outbox.table.field.event.payload": "payload"
  }
}
```

**Pros:**
- ✅ Minimal application code changes
- ✅ Low impact on existing system
- ✅ Mature tooling (Debezium, Maxwell, etc.)
- ✅ Can filter specific tables/columns

**Cons:**
- ❌ CDC infrastructure setup required (Kafka Connect, etc.)
- ❌ Increased operational complexity
- ❌ Replication lag possible
- ❌ Need to understand CDC tool internals for troubleshooting
- ❌ Additional infrastructure costs

**When to Use:**
- Already using CDC in your infrastructure
- Want to isolate migration risk from application layer
- Need to migrate very large volumes (CDC is highly optimized)
- Have CDC expertise in the team

---

### Approach 4: Strangler Fig Pattern (Recommended)

Gradually migrate topic-by-topic using feature flags.

**Phase 1: New features use Kafka**

```go
// Topic-based gradual migration
type HybridRepository struct {
    sqsRepo   core.OutboxRepository
    kafkaRepo core.OutboxRepository
    migrationTopics map[string]bool  // "order.created" -> true
}

func (r *HybridRepository) Insert(ctx context.Context, params core.OutboxInsertParams) (*core.Outbox, error) {
    if r.migrationTopics[params.Topic] {
        return r.kafkaRepo.Insert(ctx, params)
    }
    return r.sqsRepo.Insert(ctx, params)
}
```

**Phase 2: Migrate existing topics**

Use feature flags to gradually switch topics:

```go
// Environment variable or config file
migrationTopics := map[string]bool{
    "order.created":       true,  // Migrated
    "order.updated":       true,  // Migrated
    "notification.email":  false, // Still on SQS
    "payment.processed":   false, // Still on SQS
}
```

**Phase 3: Decommission SQS**

- All topics migrated to Kafka
- Stop SQS Dispatcher
- Archive or drop `outbox_sqs` table

**Architecture Evolution:**

```
Phase 1: Hybrid (New → Kafka, Existing → SQS)
┌─────────────────────────────────────────┐
│ Application                             │
│         ↓                               │
│ HybridRepository                        │
│    ├──→ outbox_sqs   (old topics)      │
│    └──→ outbox_kafka (new topics)      │
│         ↓                               │
│ SQS Dispatcher + Kafka Dispatcher       │
└─────────────────────────────────────────┘

Phase 2: Gradual Migration
┌─────────────────────────────────────────┐
│ Feature Flags:                          │
│   order.created → kafka (✅)            │
│   notification.* → sqs (⏳)             │
│   payment.* → sqs (⏳)                  │
└─────────────────────────────────────────┘

Phase 3: Complete
┌─────────────────────────────────────────┐
│ Application                             │
│         ↓                               │
│ OutboxRepository (kafka only)           │
│         ↓                               │
│ outbox_kafka                            │
│         ↓                               │
│ Kafka Dispatcher                        │
└─────────────────────────────────────────┘
```

**Pros:**
- ✅ Lowest risk - incremental rollout
- ✅ Immediate rollback per topic
- ✅ Can validate Kafka with low-risk topics first
- ✅ Clear migration progress tracking
- ✅ Production traffic validation at each step

**Cons:**
- ❌ Longer overall migration timeline
- ❌ Need feature flag infrastructure
- ❌ Temporary code complexity during migration

**When to Use (Recommended for Most Cases):**
- Production system with high uptime requirements
- Want to minimize blast radius of issues
- Can tolerate longer migration timeline
- Have feature flag infrastructure (or willing to build it)

---

## Consumer-Side Idempotency (Critical for All Approaches)

**IMPORTANT**: During migration, the same message may arrive from both brokers (duplicate delivery). Consumer handlers MUST be idempotent.

### Why Duplicates Occur During Migration

1. **Crash Recovery Scenario:**
   - Message is in PUBLISHING state in `outbox_sqs`
   - Process crashes before updating to PUBLISHED
   - `ReviveStuckPublishing()` moves it to FAILED, will retry
   - Meanwhile, message already reached SQS and was processed
   - Result: Duplicate processing possible

2. **Dual Publishing Scenario (Approach 1/2):**
   - Same message written to both `outbox_sqs` and `outbox_kafka`
   - Both Dispatchers publish successfully
   - Consumer receives message from both SQS and Kafka
   - Result: Guaranteed duplicate

### Idempotency Implementation

**Strategy 1: message_id Uniqueness (Recommended)**

```go
func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.Message) error {
    // Use message_id (same as idempotency_key from outbox) as deduplication key
    query := `
        INSERT INTO orders (id, customer_id, message_id, broker_type)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (message_id) DO NOTHING
    `
    result, err := h.db.ExecContext(ctx, query,
        event.OrderID, event.CustomerID, msg.MessageID, msg.BrokerType)
    if err != nil {
        return err
    }

    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        // Already processed (from SQS or Kafka)
        log.Info("Duplicate message detected, skipping", "message_id", msg.MessageID)
        return nil
    }

    // Process new order
    return h.processNewOrder(ctx, event)
}
```

**Database Schema:**

```sql
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    message_id TEXT NOT NULL UNIQUE,  -- Idempotency key
    broker_type TEXT,                 -- 'sqs' or 'kafka' (for debugging)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_message_id ON orders(message_id);
```

**Strategy 2: Redis Cache (for non-persistent operations)**

```go
type NotificationHandler struct {
    redis *redis.Client
}

func (h *NotificationHandler) Handle(ctx context.Context, msg *consumer.Message) error {
    // Try to acquire lock with message_id
    key := fmt.Sprintf("processed:notification:%s", msg.MessageID)
    acquired, err := h.redis.SetNX(ctx, key, "1", 10*time.Minute).Result()
    if err != nil {
        return err
    }

    if !acquired {
        log.Info("Duplicate notification detected, skipping", "message_id", msg.MessageID)
        return nil
    }

    // Send notification (idempotent - email providers handle duplicates)
    return h.sendEmail(ctx, msg.Payload)
}
```

### Testing Idempotency

```go
func TestOrderHandler_Idempotency(t *testing.T) {
    handler := NewOrderHandler(db)
    msg := &consumer.Message{
        MessageID: "unique-msg-123",
        Body:      `{"order_id":"order-456","customer_id":"cust-789"}`,
    }

    // First call - should create order
    err := handler.Handle(ctx, msg)
    assert.NoError(t, err)

    var count int
    db.QueryRow("SELECT COUNT(*) FROM orders WHERE message_id = $1", msg.MessageID).Scan(&count)
    assert.Equal(t, 1, count)

    // Second call - should be idempotent
    err = handler.Handle(ctx, msg)
    assert.NoError(t, err)

    db.QueryRow("SELECT COUNT(*) FROM orders WHERE message_id = $1", msg.MessageID).Scan(&count)
    assert.Equal(t, 1, count, "Handler should be idempotent")
}
```

---

## Recommended Migration Steps

### 1. Preparation Phase

**Tasks:**
- [ ] Implement idempotency checks in Consumer handlers (message_id UNIQUE constraint)
- [ ] Create Kafka outbox table (`outbox_kafka`) with indexes
- [ ] Implement Kafka Publisher/Dispatcher
- [ ] Set up Kafka topics with appropriate partitioning
- [ ] Configure monitoring and alerting for Kafka
- [ ] Write rollback runbook

**Estimated Duration:** 1-2 weeks

**Code Changes:**

```go
// 1. Add message_id column to business tables
ALTER TABLE orders ADD COLUMN message_id TEXT UNIQUE;

// 2. Update handlers to use message_id for idempotency
func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.Message) error {
    query := `
        INSERT INTO orders (id, customer_id, message_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (message_id) DO NOTHING
    `
    // ... (implementation as shown above)
}

// 3. Create Kafka outbox table
_, err = db.Exec(`
    CREATE TABLE outbox_kafka (
        -- Same schema as outbox_sqs
        id UUID PRIMARY KEY,
        topic TEXT NOT NULL,
        payload JSONB NOT NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL,
        -- ... (full schema)
    )
`)
```

---

### 2. Parallel Operation Phase

**Tasks:**
- [ ] Deploy HybridRepository with empty migrationTopics
- [ ] Start Kafka Dispatcher (will be idle initially)
- [ ] Verify both Dispatchers are healthy
- [ ] Create new feature topics in Kafka only

**Estimated Duration:** 1 week

**Configuration:**

```go
// Initial hybrid repository - all traffic still on SQS
hybrid := &HybridRepository{
    sqsRepo:         pgx.NewOutboxRepository(pool, "outbox_sqs"),
    kafkaRepo:       pgx.NewOutboxRepository(pool, "outbox_kafka"),
    migrationTopics: map[string]bool{},  // Empty - all to SQS
}

// For new features, route to Kafka immediately
hybrid.migrationTopics["order.v2.created"] = true
```

---

### 3. Gradual Migration Phase

**Tasks:**
- [ ] Select pilot topic (low traffic, non-critical)
- [ ] Enable feature flag for pilot topic
- [ ] Monitor for 24-48 hours
- [ ] Gradually add more topics (10-20% at a time)
- [ ] Monitor duplicate rate (should be very low)

**Estimated Duration:** 2-4 weeks (depending on topic count)

**Migration Order (Recommended):**

```go
// Week 1: Low-risk topics
migrationTopics["analytics.event"] = true
migrationTopics["notification.email"] = true

// Week 2: Medium-risk topics
migrationTopics["order.created"] = true
migrationTopics["inventory.updated"] = true

// Week 3-4: High-risk topics
migrationTopics["payment.processed"] = true
migrationTopics["user.registered"] = true
```

**Monitoring Commands:**

```sql
-- Check message volume per broker
SELECT 'SQS' AS broker, COUNT(*) FROM outbox_sqs WHERE created_at > NOW() - INTERVAL '1 hour'
UNION ALL
SELECT 'Kafka' AS broker, COUNT(*) FROM outbox_kafka WHERE created_at > NOW() - INTERVAL '1 hour';

-- Check failure rates
SELECT
    'SQS' AS broker,
    status,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM outbox_sqs
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY status;
```

---

### 4. Completion Phase

**Tasks:**
- [ ] All topics migrated to Kafka
- [ ] Monitor Kafka for 1 week (no issues)
- [ ] Stop SQS Dispatcher gracefully
- [ ] Delete PUBLISHED messages from `outbox_sqs` (keep DEAD for audit)
- [ ] Archive or drop `outbox_sqs` table after retention period
- [ ] Decommission SQS queues
- [ ] Remove HybridRepository, use KafkaRepository directly

**Estimated Duration:** 1-2 weeks

**Cleanup:**

```sql
-- Archive old SQS messages before deleting
CREATE TABLE outbox_sqs_archive AS
SELECT * FROM outbox_sqs WHERE status IN ('PUBLISHED', 'DEAD');

-- Delete PUBLISHED (keep DEAD for audit)
DELETE FROM outbox_sqs WHERE status = 'PUBLISHED' AND created_at < NOW() - INTERVAL '30 days';

-- After 90 days, drop entire table
DROP TABLE outbox_sqs;
```

---

## Migration Checklist

**Pre-Migration:**
- [ ] Consumer handlers implement idempotency (message_id UNIQUE constraint)
- [ ] Kafka infrastructure provisioned (brokers, topics, monitoring)
- [ ] Kafka outbox table created with proper indexes
- [ ] Kafka Publisher/Dispatcher implemented and tested
- [ ] Load testing completed for Kafka path
- [ ] Feature flags configured for topic-level migration
- [ ] Monitoring/alerting set up for both brokers
- [ ] Rollback plan documented and tested
- [ ] Team trained on Kafka operations

**During Migration:**
- [ ] Gradual traffic shifting plan (topic selection order)
- [ ] Consumer duplicate rate monitoring enabled
- [ ] Daily status updates to stakeholders
- [ ] Incident response plan ready
- [ ] Regular backups of outbox tables

**Post-Migration:**
- [ ] All topics migrated successfully
- [ ] SQS Dispatcher stopped
- [ ] SQS resources decommissioned
- [ ] Code cleanup (remove HybridRepository)
- [ ] Documentation updated
- [ ] Post-migration retrospective completed
- [ ] Cleanup plan for old outbox table (archive/delete)

---

## Monitoring During Migration

### Key Metrics to Track

**1. Message Volume Distribution**

```sql
-- Check message distribution across brokers
SELECT
    'SQS' AS broker,
    status,
    COUNT(*) AS count
FROM outbox_sqs
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY status
UNION ALL
SELECT
    'Kafka' AS broker,
    status,
    COUNT(*) AS count
FROM outbox_kafka
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY status;
```

**Expected Output:**

```
 broker | status     | count
--------|------------|-------
 SQS    | ENQUEUED   |   120
 SQS    | PUBLISHED  |  4850
 Kafka  | ENQUEUED   |    80
 Kafka  | PUBLISHED  |  3200
```

**2. Failure Rates**

```sql
-- Compare failure rates between brokers
SELECT
    broker,
    total_messages,
    failed_messages,
    ROUND(100.0 * failed_messages / total_messages, 2) AS failure_rate_pct
FROM (
    SELECT
        'SQS' AS broker,
        COUNT(*) AS total_messages,
        COUNT(*) FILTER (WHERE status IN ('FAILED', 'DEAD')) AS failed_messages
    FROM outbox_sqs
    WHERE created_at > NOW() - INTERVAL '24 hours'
    UNION ALL
    SELECT
        'Kafka' AS broker,
        COUNT(*) AS total_messages,
        COUNT(*) FILTER (WHERE status IN ('FAILED', 'DEAD')) AS failed_messages
    FROM outbox_kafka
    WHERE created_at > NOW() - INTERVAL '24 hours'
) AS stats;
```

**Alert if:** Kafka failure rate > 1% or > 2x SQS failure rate

**3. Consumer Duplicate Processing**

```sql
-- Check for duplicate processing via inbox
SELECT
    DATE_TRUNC('hour', received_at) AS hour,
    COUNT(*) AS total_messages,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
    COUNT(*) FILTER (WHERE status = 'processing') AS processing
FROM consumer_inbox
WHERE received_at > NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1 DESC;
```

**Expected Output:**

```
       hour         | total_messages | completed | processing
--------------------|----------------|-----------|------------
 2025-12-01 09:00   |           5420 |      5408 |         12
 2025-12-01 08:00   |           5380 |      5365 |         15
```

**Alert if:** processing count is high (indicates stuck messages or handler errors)

**4. Publishing Latency**

```sql
-- Check publishing latency (time from ENQUEUED to PUBLISHED)
SELECT
    broker,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_seconds) AS p50,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_seconds) AS p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_seconds) AS p99
FROM (
    SELECT
        'SQS' AS broker,
        EXTRACT(EPOCH FROM (updated_at - created_at)) AS latency_seconds
    FROM outbox_sqs
    WHERE status = 'PUBLISHED' AND created_at > NOW() - INTERVAL '1 hour'
    UNION ALL
    SELECT
        'Kafka' AS broker,
        EXTRACT(EPOCH FROM (updated_at - created_at)) AS latency_seconds
    FROM outbox_kafka
    WHERE status = 'PUBLISHED' AND created_at > NOW() - INTERVAL '1 hour'
) AS latencies
GROUP BY broker;
```

**5. Topic Migration Progress**

```sql
-- Track which topics are on which broker
SELECT
    topic,
    COUNT(*) AS message_count,
    'SQS' AS broker
FROM outbox_sqs
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY topic
UNION ALL
SELECT
    topic,
    COUNT(*) AS message_count,
    'Kafka' AS broker
FROM outbox_kafka
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY topic
ORDER BY topic, broker;
```

### Grafana Dashboard Example

```json
{
  "dashboard": {
    "title": "o4x Broker Migration Dashboard",
    "panels": [
      {
        "title": "Message Volume by Broker",
        "targets": [
          {
            "rawSql": "SELECT $__time(created_at), 'SQS' AS broker, COUNT(*) FROM outbox_sqs WHERE $__timeFilter(created_at) GROUP BY 1"
          },
          {
            "rawSql": "SELECT $__time(created_at), 'Kafka' AS broker, COUNT(*) FROM outbox_kafka WHERE $__timeFilter(created_at) GROUP BY 1"
          }
        ]
      },
      {
        "title": "Failure Rate by Broker",
        "targets": [
          {
            "rawSql": "SELECT $__time(created_at), 'SQS' AS broker, COUNT(*) FILTER (WHERE status IN ('FAILED', 'DEAD'))::float / COUNT(*) * 100 AS failure_rate FROM outbox_sqs WHERE $__timeFilter(created_at) GROUP BY 1"
          }
        ]
      },
      {
        "title": "Consumer Processing Status",
        "targets": [
          {
            "rawSql": "SELECT $__time(received_at), status, COUNT(*) FROM consumer_inbox WHERE $__timeFilter(received_at) GROUP BY 1, 2"
          }
        ]
      }
    ]
  }
}
```

---

## Troubleshooting

### Issue: High Duplicate Rate During Migration

**Symptoms:**
- Duplicate rate > 10%
- Consumer handlers frequently skipping messages

**Diagnosis:**

```sql
-- Find messages being processed multiple times
SELECT
    i.consumer_name,
    i.message_id,
    i.status,
    i.received_at,
    i.processed_at
FROM consumer_inbox i
WHERE i.status = 'processing'
  AND i.received_at < NOW() - INTERVAL '5 minutes'
ORDER BY i.received_at ASC
LIMIT 100;
```

**Root Cause:**
- Same message exists in both `outbox_sqs` and `outbox_kafka`
- Dual publishing approach (expected behavior)

**Resolution:**
- This is normal during Approach 1/2 migration
- Ensure idempotency is working correctly
- If duplicate rate is too high, consider switching to Approach 4 (Strangler Fig)

---

### Issue: Kafka Publishing Failures

**Symptoms:**
- High FAILED count in `outbox_kafka`
- Error logs showing Kafka connection issues

**Diagnosis:**

```sql
-- Check Kafka failure reasons
SELECT
    error_message,
    COUNT(*) AS occurrences
FROM outbox_kafka
WHERE status = 'FAILED'
  AND created_at > NOW() - INTERVAL '1 hour'
GROUP BY error_message
ORDER BY occurrences DESC;
```

**Common Root Causes:**
1. **Network issues:** Check Kafka broker connectivity
2. **Authentication:** Verify Kafka credentials
3. **Topic misconfiguration:** Ensure topics exist and have correct permissions
4. **Payload size:** Kafka default max.message.bytes is 1MB

**Resolution:**

```bash
# Check Kafka broker health
kafka-broker-api-versions --bootstrap-server localhost:9092

# Verify topic exists
kafka-topics --bootstrap-server localhost:9092 --list | grep order.created

# Check topic config
kafka-topics --bootstrap-server localhost:9092 --describe --topic order.created
```

---

### Issue: SQS Messages Stuck in PUBLISHING

**Symptoms:**
- Messages stuck in PUBLISHING state for > 5 minutes
- `ReviveStuckPublishing()` not recovering them

**Diagnosis:**

```sql
-- Find stuck messages
SELECT id, topic, status, created_at, updated_at
FROM outbox_sqs
WHERE status = 'PUBLISHING'
  AND updated_at < NOW() - INTERVAL '5 minutes'
ORDER BY updated_at ASC
LIMIT 10;
```

**Root Cause:**
- SQS Dispatcher crashed during publish
- `ReviveStuckPublishing()` not running on startup

**Resolution:**

```go
// Ensure ReviveStuckPublishing is called on startup
func main() {
    repo := pgx.NewOutboxRepository(pool, "outbox_sqs")

    // CRITICAL: Call this on startup
    count, err := repo.ReviveStuckPublishing(ctx)
    if err != nil {
        log.Fatal("Failed to revive stuck messages", "error", err)
    }
    log.Info("Revived stuck messages", "count", count)

    dispatcher := core.NewBatchDispatcher(repo, publisher, config)
    dispatcher.Start(ctx)
}
```

---

## Summary

**Recommended Approach for Most Teams:** **Strangler Fig Pattern (Approach 4)**

**Rationale:**
- ✅ Lowest risk - incremental rollout
- ✅ Immediate rollback capability per topic
- ✅ Production validation at each step
- ✅ Clear progress tracking

**Timeline:** 4-8 weeks for complete migration

**Critical Success Factors:**
1. **Idempotent handlers** - Non-negotiable for safe migration
2. **Comprehensive monitoring** - Know what's happening at all times
3. **Gradual rollout** - Don't rush, validate each step
4. **Clear rollback plan** - Be ready to revert at any stage

**Next Steps:**
1. Review your current system against the preparation checklist
2. Implement idempotency in consumer handlers
3. Set up monitoring infrastructure
4. Start with a pilot topic migration
5. Document your learnings and adjust the plan as needed

Good luck with your migration! 🚀
