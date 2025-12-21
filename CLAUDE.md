# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation Structure

This CLAUDE.md file provides developer guidance and operational details for working with the o4x codebase.

**For complete documentation:**
- **[README.md](README.md)** - User-facing documentation, Quick Start, basic usage
- **[docs/idempotency.md](docs/idempotency.md)** - Detailed idempotency patterns, decision tree, and implementation examples
- **[docs/deployment.md](docs/deployment.md)** - Schema evolution, Expand-Contract pattern, deployment strategies

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

### Documentation Sync

**CRITICAL**: When modifying Config structures, update README.md configuration tables simultaneously.

- `DispatcherConfig` → README "Dispatcher Config" section
- `BatchDispatcherConfig` → README "BatchDispatcher Config" section
- `ServiceConfig` → README "Consumer Config" section

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
repo.WithTx(tx).Insert(ctx, core.OutboxInsertParams{
    EventType: "order.created",
    Payload: payload,
    IdempotencyKey: "order-123",
    MaxAttempts: 10,
}) // Outbox in same tx
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

- **Outbox**: ENQUEUED → PUBLISHING → PUBLISHED/FAILED/DEAD (5 states, publisher side)
- **Inbox**: processing → completed (2 states, consumer idempotency)

→ **For detailed state machine diagrams and operational guidance**, see [README.md Architecture section](README.md#1-outbox-publisher-side)

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
  - FIFO: `MessageGroupId` = event_type, `MessageDeduplicationId` = idempotency_key
  - Standard: Higher throughput, no ordering, handler must be idempotent

- **contrib/sqs/consumer/**: SQS message consumer (optional)
  - `Handler` interface with `EventTypeRouter` and `TypedHandler[T]` helpers
  - Handler must be idempotent (use `InboxRepository` or application-level idempotency)
  - **Point to Point design**: 1 Queue → 1 Consumer Service

- **contrib/pgx/**: PostgreSQL repository using pgx (includes BatchOutboxRepository and InboxRepository)
- **contrib/gorm/**: PostgreSQL repository using GORM (includes BatchOutboxRepository and InboxRepository)
- **schema/**: DDL generation helpers

### Database Tables

**Outbox Table** (Publisher side):
- `id` (UUID v7), `event_type`, `payload` (JSONB), `metadata` (JSONB), `idempotency_key`
- `status` (ENUM), `error_message`, `attempt_count`, `max_attempts`
- `next_retry_at` (TIMESTAMPTZ), `created_at`, `updated_at`
- Indexes: `idx_outbox_status_created_at`, `idx_outbox_status_next_retry_at`

**Consumer Inbox Table** (Consumer side, **RECOMMENDED** for idempotency):
- **Primary Key**: `(consumer_name, event_id)` - Ensures exactly-once processing
- **CRITICAL**: `event_id` is the Outbox event ID (logical identity), NOT SQS MessageID
- `completed_at` (TIMESTAMPTZ) - Only tracks completion timestamp
- Indexes: `idx_consumer_inbox_completed_at` (for cleanup queries), `idx_consumer_inbox_event_id` (for event_id lookups across consumers)
- Purpose: Atomic duplicate detection via composite primary key
- Design: NO status field, NO locking - only tracks "completed or not"
- Use `InboxRepository.IsProcessed()` and `Complete()` in handlers
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

**FIFO Queue (*.fifo)**: Ordering guarantee (per event_type), 5-min deduplication, lower throughput, higher cost. Use for: state transitions, inventory, payments.

**Decision**: Need ordered processing of same entity? YES → FIFO, NO → Standard

**Message Ordering:**
- FIFO: Messages with same MessageGroupId (= event_type) delivered in order
- Multi-Queue FIFO: Each queue has independent ordering
- Standard: Best-effort only, use timestamps if ordering needed

**Idempotency:**
- FIFO: SQS provides 5-min deduplication window
- Standard: No SQS deduplication, handler MUST be idempotent
- Both: Application handlers must be idempotent for at-least-once delivery

### Event-Type-based Routing vs Fan-Out

**Critical Distinction**: `EventTypeRouter` is for **Event-Type-based Routing**, NOT Fan-Out.

- **Event-Type-based Routing**: Different event types → different handlers (1 event_type → 1 handler)
- **Fan-Out**: Same event → multiple handlers (1 message → N handlers)

**SQS Constraint**: Point-to-Point delivery (1 message → 1 consumer only). **Fan-Out is impossible** within a single SQS queue.

**Rule of Thumb**:
- Different events → Use `EventTypeRouter` with 1 SQS queue
- Same event, multiple handlers → Use SNS + multiple SQS queues OR Kinesis/Kafka

→ **For detailed examples and architecture diagrams**, see [README.md Multiple Queues section](README.md#multiple-queues-topic-based-routing)

## Consumer Patterns

### Idempotency Strategy

SQS provides at-least-once delivery, so handlers must be idempotent.

**CRITICAL: External APIs Without Idempotency Support - MANDATORY REQUIREMENT**

If your consumer handler calls an external API, that API **MUST** support idempotency keys.

This is **NOT** a recommendation. This is a **REQUIREMENT**.

**Rule: No idempotency support = No async messaging**

At-least-once delivery guarantees mean:
- Handlers MAY crash after calling the API but before acknowledgment
- Messages WILL be delivered more than once
- The same API call WILL execute multiple times

Without idempotency keys, async processing **MUST NOT** be used.

**What will happen without idempotency keys:**
- ❌ Duplicate payment charges (financial loss)
- ❌ Duplicate email sends (customer complaints)
- ❌ Duplicate state changes (data corruption)
- ❌ Duplicate resource creation (inconsistent state)

**Required API Capabilities:**
- ✅ API accepts idempotency key header/parameter
- ✅ API deduplicates requests with same key
- ✅ API returns same response for duplicate requests

**Example: Passing event_id as idempotency key**

```go
handler := consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
    var event PaymentEvent
    json.Unmarshal(msg.Body, &event)

    // REQUIRED: Pass event_id (Outbox ID) as idempotency key to external API
    // CRITICAL: Use msg.EventID (stable logical identity), NOT msg.MessageID (changes on redelivery)
    params := &stripe.ChargeParams{
        Amount:   stripe.Int64(event.Amount),
        Currency: stripe.String("usd"),
    }
    params.SetIdempotencyKey(msg.EventID.String()) // CRITICAL: Use msg.EventID

    charge, err := client.Charges.New(params)
    if err != nil {
        return err // Will retry
    }

    return nil
})
```

#### Decision Tree

1. **Does your handler involve external API calls?**
   - YES → Does the API support idempotency keys?
     - YES → Use **InboxRepository** + pass event_id as idempotency key ✅
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
2. **External API with idempotency key support**: Use `InboxRepository` + pass event_id as idempotency key ✅
3. **External API without idempotency support**: **Don't use async messaging - handle synchronously instead** ⛔
4. **Prefer transaction pattern**: Safer default unless performance is critical
5. Set cleanup TTLs via `InboxCleaner.DeleteOlderThan()` (completed: 7-30d)
6. **Important**: Return `nil` on duplicates, not an error
7. **CRITICAL**: Always use `msg.EventID` (Outbox ID) for idempotency, NEVER `msg.MessageID` (changes on redelivery)

**consumer_name Definition**

The `consumer_name` parameter in `InboxRepository.IsProcessed()` is a **logical consumer identity** at the service or consumer-group level.

**What consumer_name IS:**
- Logical service name (e.g., "order-service", "notification-service")
- Deployment unit identifier (e.g., "payment-processor-v2")
- Consumer group identity shared across all instances

**What consumer_name is NOT:**
- ❌ Handler function name
- ❌ Event type name
- ❌ Struct name
- ❌ Per-handler identifier

**Key Rules:**
- Use the same `consumer_name` for all instances of the same service
- Do NOT change `consumer_name` casually - it affects idempotency tracking
- Changing `consumer_name` causes messages to be treated as new (duplicate processing)

**Example:**

```go
// CORRECT: All instances use same consumer_name
processed, _ := inboxRepo.IsProcessed(ctx, "order-service", msg.EventID)

// WRONG: Different consumer_name per handler
processed, _ := inboxRepo.IsProcessed(ctx, "OrderCreatedHandler", msg.EventID) // ❌
processed, _ := inboxRepo.IsProcessed(ctx, msg.EventType, msg.EventID)         // ❌
```

#### InboxRepository Implementation Notes

**CRITICAL: IsProcessed is NOT Exclusive**

`IsProcessed()` does NOT provide exclusivity or mutual exclusion semantics.

Multiple consumer workers MAY pass `IsProcessed()` concurrently for the same event.

This behavior is intentional and correct. Handlers MUST be safe to run multiple times.

**Why This Design:**
- Primary control: SQS visibility timeout prevents concurrent processing
- The inbox table represents **completed events only**
- In-flight processing is controlled by the message broker (e.g. SQS visibility timeout)
- The only definitive point is `Complete()`

```text
IsProcessed : optimistic check (may pass concurrently)
Complete    : final commit (single source of truth)
```

**Implementation:**
- `IsProcessed()`: Simple `SELECT EXISTS` check (no locking)
  - Returns `true` if EXISTS (already completed, skip)
  - Returns `false` if NOT EXISTS (not yet completed, proceed)
- `Complete()`: `INSERT ... ON CONFLICT DO NOTHING` (idempotent)
- Both `pgx` and `gorm` implementations use identical logic for consistency

**Note:** The inbox table intentionally records completed events only.

**Design Rationale:**
- **event_id vs message_id**: Uses Outbox event_id (stable logical identity), NOT SQS MessageID (changes on redelivery)
- **IsProcessed vs TryStart**: IsProcessed accurately reflects implementation (check completion), TryStart falsely implied locking/starting
- **No status field**: Inbox only tracks completion, not in-flight state (SQS handles that)

→ **For complete idempotency patterns, code examples, and decision tree**, see [docs/idempotency.md](docs/idempotency.md)

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
- `OutboxRepository.ReviveStuckPublishing()` - PUBLISHING → FAILED (increments attempt_count)

Consumer side:
- No manual recovery needed
- In-flight events (not yet in `consumer_inbox`) naturally retry when SQS redelivers
- Completed events (in `consumer_inbox`) are skipped via `IsProcessed()` check

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
- Exponential backoff: `baseInterval * 2^attempt_count`, capped at maxInterval

**Graceful Shutdown**: Context cancellation respected, 10s timeout for DB cleanup.

**At-least-once Delivery**: Duplicates possible. Handlers MUST be idempotent.

**Batch Operations**: `UpdateBatchToPublished` returns success count. Partial success allowed.

**Database Cleanup**: Use `OutboxCleaner.DeleteOlderThan()` and `InboxCleaner.DeleteOlderThan()` periodically (PUBLISHED > 7d, DEAD > 30d, completed > 7-30d).
