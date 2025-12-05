# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is o4x

o4x is a Transactional Outbox + SQS event delivery platform for Go. It provides reliable message delivery from PostgreSQL to SQS using the outbox pattern. The consumer component is SQS-specific and optional.

## Build and Run Commands

**IMPORTANT**: After making any code changes, always run `make lint` to ensure code quality and catch potential issues before committing.

### Using Makefile (recommended)

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
make schema-consumer  # Generate schema SQL with consumer tables
```

### Direct Commands

```bash
# Run single test
go test -run TestName ./path/to/package

# Generate schema with options
go run cmd/o4x-schema/main.go                     # Outbox only
go run cmd/o4x-schema/main.go --with-consumer     # Outbox + consumer tables
go run cmd/o4x-schema/main.go --outbox my_outbox  # Custom table name
go run cmd/o4x-schema/main.go --rollback          # Generate rollback SQL

# Run examples (requires local infrastructure)
go run examples/app/cmd/dispatcher/main.go
go run examples/app/cmd/consumer/main.go
go run examples/app/cmd/api/main.go
```

## Architecture

### Design Concept: Layered Usage

o4x is designed for flexible adoption with three usage tiers:

| Tier | Use Case | What to Import |
|------|----------|----------------|
| **1. Outbox Core Only** | Insert messages to outbox within business transactions. External system polls/publishes. | `contrib/pgx` or `contrib/gorm` (repository only) |
| **2. Core + Publisher** | Full outbox pattern with built-in Dispatcher polling and publishing to SQS/Kafka. | Tier 1 + `core` (Dispatcher) + `contrib/sqs` |
| **3. Core + Publisher + Consumer** | Complete event-driven system with SQS consumer tracking. | Tier 2 + `contrib/sqs/consumer` |

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
consumerRepo := pgx.NewConsumerRepository(pool)  // optional, can be nil
service := consumer.NewService(sqsClient, queueURL, handler, consumerRepo, config)
service.Start(ctx)
```

### Two Completely Separate State Machines

**Critical**: Outbox (Publisher) and Consumer have independent state machines. Never mix them.

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

**Consumer Status (4 states)** - Consumer side only:
- `CONSUMING` → `CONSUMED`
- `CONSUMING` → `FAILED` → (retry via SQS visibility timeout)
- `FAILED` → `DEAD` (when max_retries exceeded)

**Key scenarios:**
1. **Normal**: CONSUMING → CONSUMED → SQS message deleted
2. **Retry**: CONSUMING → FAILED → (SQS visibility timeout) → retry
3. **Max retries**: FAILED → DEAD → SQS message deleted (NOT moved to DLQ)
4. **Crash recovery**: CONSUMING → (ReviveStuckConsuming) → FAILED

**Operational actions for FAILED/DEAD messages:**

**Outbox FAILED**: Monitor alerts, auto-recovery via RequeueFailed, query `error_message` and `retry_count`. Common causes: network issues, AWS credentials, rate limiting. Manual reset: `UPDATE outbox SET retry_count = 0`.

**Outbox DEAD**: Alert immediately via OnMessageDead hook. Common causes: payload > 256KB, malformed data, invalid routing. Recovery: fix and re-enqueue, manual publish, or archive. Add validation before Insert.

**Consumer FAILED**: Monitor alerts, auto-recovery via SQS visibility timeout. Query `error_message` and `receive_count`. Common causes: API timeout, DB issues, handler errors.

**Consumer DEAD**: Alert via OnMessageDead hook. SQS message already deleted. Implement payload preservation in OnMessageDead for recovery. Options: manual re-publish, manual processing, or archive.

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
  - `Repository` is optional (nil = no DB tracking)
  - Repository purpose: Audit log and observability, NOT execution control
  - Handler must be idempotent regardless of Repository usage
  - **Point to Point design**: 1 Queue → 1 Consumer Service (see Fan-Out Pattern section for multi-handler scenarios)

- **contrib/pgx/**: PostgreSQL repository using pgx (includes BatchOutboxRepository)
- **contrib/gorm/**: PostgreSQL repository using GORM (includes BatchOutboxRepository)
- **schema/**: DDL generation helpers

### Database Tables

**Outbox Table** (Publisher side):
- `id` (UUID v7), `topic`, `payload` (JSONB), `idempotency_key`
- `status` (ENUM), `error_message`, `retry_count`, `max_retries`
- `next_retry_at` (TIMESTAMPTZ), `created_at`, `updated_at`
- Indexes: `idx_outbox_status_created_at`, `idx_outbox_status_next_retry_at`

**Consumer Messages Table** (Consumer side, optional):
- `id` (UUID v7), `outbox_id`, `message_id` (UNIQUE), `receipt_handle`
- `status` (ENUM), `error_message`, `receive_count`, `max_retries`
- `queue_url`, `processed_at`, `created_at`, `updated_at`
- Tracks consumption state only, never updated by Publisher
- **UNIQUE constraint on `message_id`**: 1 SQS Message = 1 Record (Point to Point design)

### Startup Recovery

Call once at startup:
- `OutboxRepository.ReviveStuckPublishing()` - PUBLISHING → FAILED (increments retry_count)
- `ConsumerRepository.ReviveStuckConsuming()` - CONSUMING → FAILED

### Multi-Queue Routing (SQS)

```go
router := sqs.NewTopicQueueMap("https://sqs.../default-queue")
router.Register("order.created", "https://sqs.../orders.fifo") // FIFO
router.RegisterPrefix("notification.", "https://sqs.../notifications") // Standard
publisher := sqs.NewMultiBatchPublisher(sqsClient, router)
```

**SQS Queue Type Selection:**

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

### Fan-Out Pattern (1 Message → N Handlers)

**IMPORTANT**: `consumer_messages` table is designed for **Point to Point** (1 Queue → 1 Consumer Service).

**Why Composite Handler is NOT recommended with Repository**:
```go
// ❌ AVOID: Composite Handler with consumer_messages
composite := &CompositeHandler{
    handlers: []Handler{&EmailHandler{}, &SlackHandler{}, &MetricsHandler{}},
}
```

**Problem**: `consumer_messages.status` is a single state (CONSUMING/CONSUMED/FAILED/DEAD).
- If EmailHandler succeeds but SlackHandler fails → What status should be recorded?
- CONSUMED → Loses SlackHandler failure information
- FAILED → Ignores EmailHandler success
- **Partial failures cannot be tracked accurately**

**Recommended Fan-Out Architecture** (SNS + Multiple SQS Queues):
```
Publisher → SNS Topic "order.created"
             ↓
             ├→ SQS Queue "email-queue" → Consumer Service 1 (consumer_messages_email)
             ├→ SQS Queue "slack-queue" → Consumer Service 2 (consumer_messages_slack)
             └→ SQS Queue "metrics-queue" → Consumer Service 3 (consumer_messages_metrics)
```

**Key Points**:
1. **1 Queue → 1 Consumer Service** (separate processes)
2. Each service has independent `consumer_messages` tracking (or separate tables)
3. Individual success/failure tracking per handler
4. Failure in one consumer doesn't affect others
5. Each consumer can have different retry policies and max_retries

**Alternative without Repository**:
If you don't need `consumer_messages` tracking, Composite Handler is acceptable:
```go
// ✅ OK: Composite Handler without Repository
service := consumer.NewService(sqsClient, nil, compositeHandler, config)
```
However, you lose audit trail and observability for individual handler failures.

**Rule of Thumb**: Need Fan-Out? Use SNS + multiple SQS queues with separate Consumer services.

### Environment Variables

```
DATABASE_URL=postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable
SQS_ENDPOINT=http://localhost:14566
AWS_REGION=us-east-1
SQS_QUEUE_URL=http://localhost:14566/000000000000/o4x-events      # Standard
SQS_QUEUE_URL=http://localhost:14566/000000000000/o4x-events.fifo # FIFO
```

### Important Constraints and Limits

**SQS Message Size**: 256 KB hard limit. Oversized messages → DEAD (no retry).

**BatchDispatcher Configuration**:
- RequeueInterval: Default 10s (0 = no auto-retry)
- Exponential backoff: `baseInterval * 2^retry_count`, capped at maxInterval

**Graceful Shutdown**: Context cancellation respected, 10s timeout for DB cleanup.

**At-least-once Delivery**: Duplicates possible. Handlers MUST be idempotent.

**Batch Operations**: `UpdateBatchToPublished` returns success count. Partial success allowed.

**Database Cleanup**: Use `OutboxCleaner.DeleteOlderThan()` periodically (PUBLISHED > 7d, DEAD > 30d).

### Testing and Linting

- `make test-short` - Unit tests (no DB)
- `make test` - Full tests with DB (requires `make up`)
- `make test-coverage` - Generate coverage.html
- `make lint` - **REQUIRED** after code changes

**Workflow**: Code → `make lint` → fix errors → `make test-short` → commit

### Health Check Endpoints

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

### Idempotency Implementation

Consumer `Repository` is **optional**. Two approaches:

#### Approach 1: With Consumer Repository (DB-Tracked)

```go
consumerRepo := pgx.NewConsumerRepository(pool)
service := consumer.NewService(sqsClient, consumerRepo, handler, config)
```

**How it works**: Tracks all messages in `consumer_messages` table with status transitions.

**Benefits**: Audit trail, failure investigation, crash recovery, metrics.

**Important**:
- Handler still called on duplicates - must be idempotent
- Handler and Consumer use SEPARATE transactions
- Handler manages its own transaction for business data

**Example with Business Idempotency**:
```go
func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    tx, _ := h.db.Begin(ctx)
    defer tx.Rollback(ctx)

    // Idempotent insert
    query := `INSERT INTO orders (id, customer_id, message_id)
              VALUES ($1, $2, $3) ON CONFLICT (message_id) DO NOTHING`
    result, _ := tx.ExecContext(ctx, query, event.OrderID, event.CustomerID, msg.MessageID)

    if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
        return nil // Already processed
    }

    // Process new order
    if err := h.processNewOrder(ctx, tx, event); err != nil {
        return err
    }

    return tx.Commit(ctx)
}
```

#### Approach 2: Without Consumer Repository (Application-Level)

```go
service := consumer.NewService(sqsClient, nil, handler, config)
```

**Strategies**:

1. **DB Unique Constraint** (Recommended): `ON CONFLICT (message_id) DO NOTHING`
2. **Redis Cache**: `SetNX` with TTL for fast deduplication
3. **Business Data Check**: Query if operation already completed
4. **Hybrid**: Redis for fast check + DB for permanent record

**Strategy Selection**:
- Financial transactions → DB Unique Constraint (+ Repository for audit)
- Notifications/emails → Redis Cache
- Analytics → Business Data Check
- Compliance-critical → Repository + DB Unique Constraint

**Best Practices**:
1. Always implement handler-level idempotency (at-least-once delivery)
2. Use Repository for audit trail and monitoring needs
3. Set appropriate TTLs (Redis: 10-30min, cleanup: CONSUMED 7d, DEAD 30d)
4. Use DB transactions for multi-step operations
5. Monitor duplicate rates and alert on anomalies

### MessageConcurrency (Parallel Processing within Workers)

The consumer supports parallel message processing within each worker via `MessageConcurrency` config:

```go
service := consumer.NewService(sqsClient, repo, handler, consumer.ServiceConfig{
    QueueURL:           queueURL,
    WorkerCount:        5,
    MessageConcurrency: 10, // Process 10 messages concurrently per worker
})
```

**Key Concepts**:
- **Sequential** (MessageConcurrency=1): Default, safe for all queue types
- **Parallel** (MessageConcurrency>1): Only for Standard queues, NOT FIFO
- **Total parallelism**: `WorkerCount * MessageConcurrency` (e.g., 5 * 10 = 50 concurrent messages)
- **FIFO validation**: Service.Start() returns error if MessageConcurrency>1 with FIFO queue (detected via `.fifo` suffix)

**Implementation Details**:
- Sequential path: Simple for loop over messages (existing behavior)
- Parallel path: Semaphore (buffered channel) + sync.WaitGroup pattern
- Graceful shutdown: Context cancellation checked before starting each goroutine
- Labeled break: Properly exits loop when context cancelled during semaphore acquisition

**When to Use**:
- **Standard queues only** (FIFO requires MessageConcurrency=1 for ordering)
- Fast handlers (<100ms processing time)
- High throughput requirements (>1000 msg/sec)
- I/O-bound operations (DB queries, HTTP calls)

**Performance Tuning**:
```go
// Low throughput, ordered processing (FIFO)
WorkerCount:        2
MessageConcurrency: 1  // Must be 1 for FIFO

// Moderate throughput (Standard)
WorkerCount:        5
MessageConcurrency: 5  // 25 concurrent messages

// High throughput (Standard)
WorkerCount:        10
MessageConcurrency: 10 // 100 concurrent messages
```

**Important Considerations**:
1. **Database Connections**: Ensure pgxpool has sufficient connections: `maxConns >= WorkerCount * MessageConcurrency + margin`
2. **Memory**: Each goroutine consumes memory (handler state + message payload)
3. **Handler Idempotency**: Critical with parallel processing (potential out-of-order execution)
4. **Monitoring**: Track processing time, error rates, and DB connection pool usage

**Example** (see examples/app/cmd/consumer/main.go):
```bash
# Standard queue with parallel processing
go run cmd/consumer/main.go --workers 5 --message-concurrency 10

# FIFO queue (MessageConcurrency must be 1)
go run cmd/consumer/main.go --fifo --workers 2 --message-concurrency 1
```
