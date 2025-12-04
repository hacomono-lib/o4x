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
go run examples/local/cmd/dispatcher/main.go
go run examples/local/cmd/consumer/main.go
go run examples/local/cmd/enqueue/main.go -topic order.created -key order-123 -payload '{"order_id":"123"}'
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
- Tracks consumption state only, never updated by Publisher

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
- See `examples/local/cmd/dispatcher/main.go` for full example

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
