# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Table of Contents

1. [What is o4x](#what-is-o4x)
2. [Quick Start](#quick-start)
   - [Build and Run Commands](#build-and-run-commands)
   - [Environment Variables](#environment-variables)
3. [Core Architecture](#core-architecture)
   - [Layered Usage (3 Tiers)](#layered-usage-3-tiers)
   - [State Machines: Outbox vs Inbox](#state-machines-outbox-vs-inbox)
   - [Key Components](#key-components)
   - [Database Tables](#database-tables)
4. [SQS Integration](#sqs-integration)
   - [Multi-Queue Routing](#multi-queue-routing)
   - [Topic-based Routing vs Fan-Out](#topic-based-routing-vs-fan-out)
   - [Standard vs FIFO Queues](#standard-vs-fifo-queues)
5. [Consumer Patterns](#consumer-patterns)
   - [Idempotency Strategy](#idempotency-strategy)
   - [MessageConcurrency](#messageconcurrency)
6. [Operational Guide](#operational-guide)
   - [Startup Recovery](#startup-recovery)
   - [Health Checks](#health-checks)
   - [Testing and Linting](#testing-and-linting)
   - [Constraints and Limits](#constraints-and-limits)

---

## What is o4x

o4x is a Transactional Outbox + SQS event delivery platform for Go. It provides reliable message delivery from PostgreSQL to SQS using the outbox pattern. The consumer component is SQS-specific and optional.

## Quick Start

### Build and Run Commands

**IMPORTANT**: After making any code changes, always run `make lint` to ensure code quality and catch potential issues before committing.

**Using Makefile (recommended)**:

```bash
make help             # Show all available commands
make build            # Build all packages
make test             # Run all tests with race detector (requires DB - run 'make up' first)
make test-short       # Run unit tests only (no DB required)
make test-coverage    # Generate coverage report (coverage.html)
make lint             # Run golangci-lint (REQUIRED after code changes)
make up               # Start local infrastructure (PostgreSQL + LocalStack)
make down             # Stop local infrastructure
make schema           # Generate outbox schema SQL
make schema-inbox     # Generate schema SQL with consumer_inbox table
```

**Direct Commands**:

```bash
# Run single test
go test -run TestName ./path/to/package

# Generate schema with options
go run cmd/o4x-schema/main.go                     # Outbox only
go run cmd/o4x-schema/main.go --with-inbox        # Outbox + consumer_inbox (recommended)
go run cmd/o4x-schema/main.go --outbox my_outbox  # Custom table name
go run cmd/o4x-schema/main.go --rollback          # Generate rollback SQL

# Run examples (requires local infrastructure)
go run examples/app/cmd/dispatcher/main.go
go run examples/app/cmd/consumer/main.go
go run examples/app/cmd/api/main.go
```

**Workflow**: Code → `make lint` → fix errors → `make test-short` → commit

### Environment Variables

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable
SQS_ENDPOINT=http://localhost:14566
AWS_REGION=us-east-1
SQS_QUEUE_URL=http://localhost:14566/000000000000/o4x-events      # Standard
SQS_QUEUE_URL=http://localhost:14566/000000000000/o4x-events.fifo # FIFO
```

## Core Architecture

### Layered Usage (3 Tiers)

o4x is designed for flexible adoption with three usage tiers:

| Tier | Use Case | What to Import |
|------|----------|----------------|
| **1. Outbox Core Only** | Insert messages to outbox within business transactions. External system polls/publishes. | `contrib/pgx` or `contrib/gorm` (repository only) |
| **2. Core + Publisher** | Full outbox pattern with built-in Dispatcher polling and publishing to SQS. | Tier 1 + `core` (Dispatcher) + `contrib/sqs` |
| **3. Core + Publisher + Consumer** | Complete event-driven system with SQS consumer. | Tier 2 + `contrib/sqs/consumer` |

**Tier 1: Outbox Core Only**
```go
repo := pgx.NewOutboxRepository(pool)
tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)
tx.Exec(ctx, "INSERT INTO orders ...") // Business logic
repo.WithTx(tx).Insert(ctx, core.OutboxInsertParams{...}) // Outbox in same tx
tx.Commit(ctx)
```

**Tier 2: Core + Publisher**
```go
repo := pgx.NewOutboxRepository(pool)
publisher := sqs.NewBatchPublisher(sqsClient, queueURL)
dispatcher := core.NewBatchDispatcher(repo, publisher, config)
dispatcher.Start(ctx)
```

**Tier 3: Core + Publisher + Consumer**
```go
service := consumer.NewService(sqsClient, handler, config)
service.Start(ctx)

// For idempotency, use InboxRepository in your handler
inboxRepo := pgx.NewInboxRepository(pool)
```

### State Machines: Outbox vs Inbox

**Critical**: Outbox (Publisher) and Inbox (Consumer) have independent state machines. Never mix them.

**Outbox Status (5 states)** - Publisher/Dispatcher side:
- `ENQUEUED` → `PUBLISHING` → `PUBLISHED`
- `ENQUEUED` → `PUBLISHING` → `FAILED` → (retry) → `ENQUEUED`
- `FAILED` → `DEAD` (when max_retries exceeded)

**Key scenarios:**
1. **Normal**: ENQUEUED → PUBLISHING → PUBLISHED
2. **Retry**: ENQUEUED → PUBLISHING → FAILED → (RequeueFailed) → ENQUEUED
3. **Max retries exceeded**: FAILED → DEAD (OnMessageDead hook called)
4. **Crash recovery**: PUBLISHING → (ReviveStuckPublishing) → FAILED
5. **Oversized message**: ENQUEUED → PUBLISHING → DEAD (PermanentError, no retry)

**Inbox Status (2 states)** - Consumer side (idempotency):
- `processing` → `completed`
- Failed handlers → SQS retry via visibility timeout (status stays `processing`)

**Operational actions for FAILED/DEAD messages:**

**Outbox FAILED**: Monitor alerts, auto-recovery via RequeueFailed, query `error_message` and `retry_count`. Common causes: network issues, AWS credentials, rate limiting. Manual reset: `UPDATE outbox SET retry_count = 0`.

**Outbox DEAD**: Alert immediately via OnMessageDead hook. Common causes: payload > 256KB, malformed data, invalid routing. Recovery: fix and re-enqueue, manual publish, or archive. Add validation before Insert.

**Consumer Handler Failures**: SQS handles retries via visibility timeout. If handler returns error, SQS redelivers message. Use `InboxRepository` to prevent duplicate processing. Configure SQS Dead Letter Queue (DLQ) for max retries handling.

### Key Components

- **core/**: Dispatcher and Worker that poll outbox table and publish to message broker
  - `Dispatcher` / `BatchDispatcher` - Poll and publish messages
  - `Publisher` / `BatchPublisher` interfaces
  - `OutboxRepository` / `BatchOutboxRepository` interfaces
  - Worker uses `SELECT ... FOR UPDATE SKIP LOCKED`

- **contrib/sqs/**: SQS publisher implementations
  - Supports both Standard and FIFO queues
  - `Publisher` / `BatchPublisher` - Single queue
  - `MultiQueuePublisher` / `MultiBatchPublisher` - Topic-based routing
  - `TopicQueueMap` - Thread-safe routing with sync.RWMutex
  - FIFO: `MessageGroupId` = topic, `MessageDeduplicationId` = idempotency_key
  - Standard: Higher throughput, no ordering, handler must be idempotent

- **contrib/sqs/consumer/**: SQS message consumer (optional)
  - `Handler` interface with `TopicRouter` and `TypedHandler[T]` helpers
  - Handler must be idempotent (use `InboxRepository` or application-level idempotency)
  - **Point to Point design**: 1 Queue → 1 Consumer Service

- **contrib/pgx/**: PostgreSQL repository using pgx (includes BatchOutboxRepository and InboxRepository)
- **contrib/gorm/**: PostgreSQL repository using GORM (includes BatchOutboxRepository and InboxRepository)
- **schema/**: DDL generation helpers

### Database Tables

**Outbox Table** (Publisher side):
- `id` (UUID v7), `topic`, `payload` (JSONB), `metadata` (JSONB), `idempotency_key`
- `status` (ENUM), `error_message`, `retry_count`, `max_retries`
- `next_retry_at` (TIMESTAMPTZ), `created_at`, `updated_at`
- Indexes: `idx_outbox_status_created_at`, `idx_outbox_status_next_retry_at`

**Consumer Inbox Table** (Consumer side, **RECOMMENDED** for idempotency):
- **Primary Key**: `(consumer_name, message_id)` - Ensures exactly-once processing
- `status` (TEXT: "processing", "completed"), `received_at`, `processed_at`
- Index: `idx_consumer_inbox_status_received_at` (for cleanup queries)
- Purpose: Atomic duplicate detection via composite primary key
- Use `InboxRepository.TryStart()` and `Complete()` in handlers
- Generate DDL: `make schema-inbox`

## SQS Integration

### Multi-Queue Routing

```go
router := sqs.NewTopicQueueMap("https://sqs.../default-queue")
router.Register("order.created", "https://sqs.../orders.fifo") // FIFO
router.RegisterPrefix("notification.", "https://sqs.../notifications") // Standard
publisher := sqs.NewMultiBatchPublisher(sqsClient, router)
```

### Standard vs FIFO Queues

**Standard Queue**: Higher throughput, lower cost, no ordering, possible duplicates. Use for: notifications, logs, analytics.

**FIFO Queue (*.fifo)**: Ordering guarantee (per topic), 5-min deduplication, lower throughput, higher cost. Use for: state transitions, inventory, payments.

**Decision**: Need ordered processing of same entity? YES → FIFO, NO → Standard

**Message Ordering:**
- FIFO: Messages with same MessageGroupId (= topic) delivered in order
- Multi-Queue FIFO: Each queue has independent ordering
- Standard: Best-effort only, use timestamps if ordering needed

**Idempotency:**
- FIFO: SQS provides 5-min deduplication window
- Standard: No SQS deduplication, handler MUST be idempotent
- Both: Application handlers must be idempotent for at-least-once delivery

### Topic-based Routing vs Fan-Out

**IMPORTANT**: `TopicRouter` is for **Topic-based Routing**, NOT Fan-Out.

**Topic-based Routing** (current `TopicRouter` feature):
```go
// 1 Queue receives DIFFERENT message types (different topics)
router.Register("order.created", OrderHandler)       // topic A → Handler A
router.Register("user.registered", UserHandler)      // topic B → Handler B
router.Register("notification.email", EmailHandler)  // topic C → Handler C
```
- Purpose: Route different event types to appropriate handlers
- 1 topic → 1 Handler

**Fan-Out** (same message → multiple handlers):
```
Same message "order.created" processed by multiple handlers:
→ EmailHandler (send email)
→ SlackHandler (notify Slack)
→ MetricsHandler (track analytics)
```
- Purpose: Process the same message in multiple ways
- 1 message → N Handlers

**SQS Constraint**: Point to Point delivery (1 message → 1 consumer only). **Fan-Out is physically impossible** within a single SQS queue.

**Recommended Fan-Out Architecture** (SNS + Multiple SQS Queues):
```
Publisher → SNS Topic "order.created"
             ↓
             ├→ SQS Queue "email-queue" → Consumer Service 1
             ├→ SQS Queue "slack-queue" → Consumer Service 2
             └→ SQS Queue "metrics-queue" → Consumer Service 3
```

**Key Points**:
1. 1 Queue → 1 Consumer Service (separate processes)
2. Each service tracks idempotency independently using `InboxRepository`
3. Individual success/failure tracking per handler
4. Failure in one consumer doesn't affect others

**Alternative**: Kinesis/Kafka (native Fan-Out, out of o4x scope)

**Rule of Thumb**:
- Topic-based Routing (different events) → Use `TopicRouter` with 1 SQS queue
- Fan-Out (same event, multiple handlers) → Use SNS + multiple SQS queues OR Kinesis/Kafka

## Consumer Patterns

### Idempotency Strategy

SQS provides at-least-once delivery, so handlers must be idempotent. Choose an approach:

#### InboxRepository (Recommended)

Use `consumer_inbox` table for database-level idempotency checking.

**When to use**:
- ✅ DB operations only (within transaction)
- ✅ External API calls with idempotency key support
- ❌ External APIs without idempotency support → **Don't use async messaging**

**Transaction Pattern** (Recommended):

```go
func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    tx := h.db.Begin()
    defer tx.Rollback()

    // 1. Check idempotency (within transaction)
    inboxTx := h.inboxRepo.WithTx(tx)
    ok, err := inboxTx.TryStart(ctx, "OrderHandler", msg.MessageID)
    if err != nil {
        return err
    }
    if !ok {
        return nil // Already completed (duplicate)
    }

    // 2. Business logic (same transaction)
    var event OrderCreatedEvent
    json.Unmarshal(msg.Body, &event)

    if err := tx.Create(&Order{
        ID:         event.OrderID,
        CustomerID: event.CustomerID,
        Amount:     event.Amount,
    }).Error; err != nil {
        return err
    }

    // 3. Mark completed
    if err := inboxTx.Complete(ctx, "OrderHandler", msg.MessageID); err != nil {
        return err
    }

    // 4. Commit (all or nothing)
    return tx.Commit().Error
}
```

**Why transaction is recommended**:
- ✅ Protects non-idempotent business logic (e.g., `UPDATE counters SET count = count + 1`)
- ✅ Multiple DB operations are atomic with inbox state
- ✅ Crash during processing → full rollback → safe to retry

**Auto-commit Pattern** (Only when fully idempotent):

Use when ALL of these conditions are met:
1. ✅ Business logic uses `ON CONFLICT DO NOTHING`
2. ✅ External APIs support idempotency keys
3. ✅ No multi-step DB operations requiring atomicity

```go
func (h *NotificationHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // 1. Check idempotency (auto-commit)
    ok, err := h.inboxRepo.TryStart(ctx, "NotificationHandler", msg.MessageID)
    if err != nil {
        return err
    }
    if !ok {
        return nil // Already completed
    }

    // 2. Idempotent DB operation (auto-commit)
    h.db.Exec(`INSERT INTO notifications (user_id, message_id, content)
               VALUES ($1, $2, $3) ON CONFLICT (message_id) DO NOTHING`,
               event.UserID, msg.MessageID, event.Content)

    // 3. Call external API with idempotency key
    if err := h.emailClient.Send(EmailRequest{
        To:             event.Email,
        IdempotencyKey: msg.MessageID, // ← API handles duplicates
    }); err != nil {
        return err
    }

    // 4. Mark completed (auto-commit)
    return h.inboxRepo.Complete(ctx, "NotificationHandler", msg.MessageID)
}
```

**Retry scenarios comparison**:

| Scenario | Transaction | Auto-commit |
|----------|-------------|-------------|
| Crash after TryStart, before business logic | Rollback → Retry safe | Inbox record exists → TryStart returns true → Retry safe |
| Crash after business logic, before Complete | Rollback → Retry safe | **Depends on business logic idempotency** |
| Business logic fails | Rollback → Retry safe | **May leave inbox in 'processing' state** |

**Recommendation**: Use transaction pattern unless you have specific performance requirements and can guarantee full idempotency.

#### Application-Level Idempotency

Use when InboxRepository overhead is not justified (e.g., simple analytics, logging).

**Strategies**:
1. **DB Unique Constraint**: `ON CONFLICT (message_id) DO NOTHING`
2. **Redis Cache**: `SetNX` with TTL for fast deduplication
3. **Business Data Check**: Query if operation already completed

**Example**:
```go
func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    tx := h.db.Begin()
    defer tx.Rollback()

    // Idempotent insert
    result := tx.Exec(`INSERT INTO orders (id, customer_id, message_id)
                       VALUES ($1, $2, $3) ON CONFLICT (message_id) DO NOTHING`,
                       event.OrderID, event.CustomerID, msg.MessageID)

    if result.RowsAffected == 0 {
        return nil // Already processed
    }

    // Process new order
    if err := h.processNewOrder(tx, event); err != nil {
        return err
    }

    return tx.Commit()
}
```

#### External APIs Without Idempotency Support

**Don't use asynchronous message processing**. Creating a send queue table defeats the purpose of using o4x.

**Your options**:
1. **Switch to an idempotent API** ✅ (Recommended) - Stripe, Twilio, SendGrid, AWS SES
2. **Accept duplicate calls** ⚠️ - If duplicates are tolerable (e.g., notification emails)
3. **Call API synchronously** ⚠️ - Within application transaction (loses async benefits)

**Bottom line**: Non-idempotent APIs are fundamentally incompatible with at-least-once delivery semantics. Fix the API, not the pattern.

#### Decision Tree

1. **Does your handler involve external API calls?**
   - YES → Does the API support idempotency keys?
     - YES → Use **InboxRepository** + pass message_id as idempotency key ✅
     - NO → **Don't use async messaging** ⛔
   - NO → Continue to step 2

2. **Is your business logic fully idempotent?** (ON CONFLICT DO NOTHING, etc.)
   - YES → Use **InboxRepository with auto-commit** OR **Application-Level** ✅
   - NO → Use **InboxRepository with transaction** ✅ (safest)

#### Comparison Table

| Approach | Idempotency | Audit Trail | Complexity | Recommended For |
|----------|-------------|-------------|------------|-----------------|
| **InboxRepository (Transaction)** | ✅ Built-in | ✅ Processing status | Low | **DB operations** + **API calls with idempotency keys** |
| **InboxRepository (Auto-commit)** | ✅ Built-in | ✅ Processing status | Low | **Idempotent business logic** + **API with idempotency keys** |
| **Application-Level** | ✅ Custom logic | ⚠️ Custom | Medium | **Pure DB operations** (when InboxRepository overhead not justified) |

#### Best Practices

1. **DB operations only**: Use `InboxRepository` with transaction ✅
2. **External API with idempotency key support**: Use `InboxRepository` ✅
3. **External API without idempotency support**: **Don't use async messaging** ⛔
4. **Prefer transaction pattern**: Safer default unless performance is critical
5. Set cleanup TTLs via `InboxCleaner.DeleteOlderThan()` (completed: 7-30d, processing: 30-90d)
6. Monitor stuck messages in 'processing' status (indicates handler crashes)
7. **Important**: Return `nil` on duplicates, not an error

### MessageConcurrency

Parallel message processing within workers. Only for Standard queues (NOT FIFO).

```go
service := consumer.NewService(sqsClient, handler, consumer.ServiceConfig{
    QueueURL:           queueURL,
    WorkerCount:        5,
    MessageConcurrency: 10, // Process 10 messages concurrently per worker
})
```

**Key Concepts**:
- **Sequential** (MessageConcurrency=1): Default, safe for all queue types
- **Parallel** (MessageConcurrency>1): Only for Standard queues
- **Total parallelism**: `WorkerCount * MessageConcurrency` (e.g., 5 * 10 = 50 concurrent messages)
- **FIFO validation**: Service.Start() returns error if MessageConcurrency>1 with FIFO queue

**When to Use**:
- Standard queues only (FIFO requires MessageConcurrency=1)
- Fast handlers (<100ms processing time)
- High throughput requirements (>1000 msg/sec)
- I/O-bound operations (DB queries, HTTP calls)

**Important Considerations**:
1. **Database Connections**: Ensure `maxConns >= WorkerCount * MessageConcurrency + margin`
2. **Memory**: Each goroutine consumes memory
3. **Handler Idempotency**: Critical with parallel processing
4. **Monitoring**: Track processing time, error rates, DB connection pool usage

**Performance Tuning**:
```go
// Low throughput, ordered processing (FIFO)
WorkerCount: 2, MessageConcurrency: 1

// Moderate throughput (Standard)
WorkerCount: 5, MessageConcurrency: 5  // 25 concurrent messages

// High throughput (Standard)
WorkerCount: 10, MessageConcurrency: 10 // 100 concurrent messages
```

## Operational Guide

### Startup Recovery

Call once at startup:
- `OutboxRepository.ReviveStuckPublishing()` - PUBLISHING → FAILED (increments retry_count)

Consumer side:
- No manual recovery needed
- Messages with status=`processing` in `consumer_inbox` naturally retry when SQS redelivers

### Health Checks

o4x provides health status APIs for containerized environments:

```go
// Dispatcher/BatchDispatcher/consumer.Service
status := dispatcher.HealthStatus()
status.IsHealthy()                // running && !pending_shutdown
status.IsStale(5 * time.Minute)  // no messages processed in 5min
```

**Implementation**:
- `/health` endpoint - liveness probe (restart trigger)
- `/ready` endpoint - readiness probe (traffic routing)
- See `examples/app/cmd/dispatcher/main.go` for full example

### Testing and Linting

- `make test-short` - Unit tests (no DB)
- `make test` - Full tests with DB (requires `make up`)
- `make test-coverage` - Generate coverage.html
- `make lint` - **REQUIRED** after code changes

### Constraints and Limits

**SQS Message Size**: 256 KB hard limit. Oversized messages → DEAD (no retry).

**BatchDispatcher Configuration**:
- RequeueInterval: Default 10s (0 = no auto-retry)
- Exponential backoff: `baseInterval * 2^retry_count`, capped at maxInterval

**Graceful Shutdown**: Context cancellation respected, 10s timeout for DB cleanup.

**At-least-once Delivery**: Duplicates possible. Handlers MUST be idempotent.

**Batch Operations**: `UpdateBatchToPublished` returns success count. Partial success allowed.

**Database Cleanup**: Use `OutboxCleaner.DeleteOlderThan()` and `InboxCleaner.DeleteOlderThan()` periodically (PUBLISHED > 7d, DEAD > 30d, completed > 7d, processing > 30d).
