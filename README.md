# o4x

<table style="border: none; border-collapse: collapse;">
<tr>
<td width="200" style="border: none;">
<img src="docs/o4x_logo.png" alt="o4x logo" width="180" />
</td>
<td style="border: none;">

**Transactional Outbox + SQS event delivery platform for Go.**

*o4x is pronounced "outbox"*

o4x provides reliable message delivery from PostgreSQL to SQS using the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html). It ensures **at-least-once** delivery by storing events in the same database transaction as your business logic, then reliably delivering them to SQS. Application handlers must be idempotent.

</td>
</tr>
</table>

## Features

- **Transactional Outbox Pattern** - Atomic writes with your business data
- **SQS Support (Standard & FIFO)** - Choose between high throughput or ordered delivery
- **Batch Processing** - High throughput with `SendMessageBatch`
- **Graceful Shutdown** - Context-aware shutdown with proper cleanup
- **Pluggable Repository** - pgx and GORM adapters included
- **Concurrent Workers** - Configurable parallelism for both dispatcher and consumer
- **Thread-Safe** - All components safe for concurrent use with proper synchronization

## Installation

```bash
go get github.com/hacomono-lib/o4x
```

## Architecture

o4x consists of two independent components:

```mermaid
flowchart LR
    subgraph Publisher Side
        App[Application]
        DB[(PostgreSQL<br/>outbox table)]
        Dispatcher[Dispatcher]
    end

    subgraph Message Broker
        SQS[SQS]
    end

    subgraph Consumer Side
        Consumer[Consumer Service]
        CDB[(PostgreSQL<br/>consumer_messages)]
        Handler[Your Handler]
    end

    App -->|"INSERT (same tx)"| DB
    Dispatcher -->|poll| DB
    Dispatcher -->|publish| SQS
    SQS -->|receive| Consumer
    Consumer -.->|"track (optional)"| CDB
    Consumer -->|dispatch| Handler
```

### 1. Outbox (Publisher Side)

The core outbox pattern implementation. Your application inserts messages into the `outbox` table within the same database transaction as your business logic. The Dispatcher polls for pending messages and publishes them to SQS.

#### Outbox Status Flow (5 states)

```mermaid
stateDiagram-v2
    [*] --> ENQUEUED: Insert
    ENQUEUED --> PUBLISHING: Worker picks up
    PUBLISHING --> PUBLISHED: Success
    PUBLISHING --> FAILED: Error
    FAILED --> ENQUEUED: Retry (retry_count < max_retries)
    FAILED --> DEAD: Max retries exceeded
    PUBLISHED --> [*]
    DEAD --> [*]
```

**Common scenarios:**
- **Normal flow**: App inserts → Dispatcher polls → SQS publish succeeds → PUBLISHED
- **Temporary failure**: Network error during publish → FAILED → RequeueFailed retries → ENQUEUED → retry
- **Permanent failure**: 5 consecutive failures → DEAD (no more retries, OnMessageDead hook called)
- **Crash during publishing**: Process killed while PUBLISHING → ReviveStuckPublishing on restart → FAILED → retry
- **Oversized message**: 300KB payload exceeds SQS 256KB limit → immediately DEAD (PermanentError)

**Operational actions:**
- **FAILED**: Usually auto-recovers via RequeueFailed. Check `error_message` for network/auth issues. Reset retry count if needed: `UPDATE outbox SET retry_count = 0 WHERE id = '...'`
- **DEAD**: Alert immediately. Query cause: `SELECT id, topic, error_message, payload FROM outbox WHERE status = 'DEAD'`. Options: (1) Fix payload and re-enqueue, (2) Manual publish to SQS, (3) Archive/delete if invalid. See CLAUDE.md for detailed recovery procedures.

### 2. Consumer (SQS-specific, Optional)

An optional component for processing SQS messages. The Consumer Repository (`consumer_messages` table) is **optional** and serves as an **audit log and observability tool**.

**What the Repository does:**
- ✅ **Records** all message processing attempts (status, errors, retry counts)
- ✅ **Enables** detailed failure investigation and metrics collection
- ✅ **Provides** crash recovery visibility via `ReviveStuckConsuming()`
- ❌ **Does NOT prevent** duplicate Handler execution (Handler must be idempotent)
- ❌ **Does NOT skip** Handler calls for duplicate messages

**When to use the Repository:**
- Compliance requirements (audit trail for processed messages)
- Complex failure investigation (preserve error messages and context)
- Detailed metrics (processing time, failure rates, duplicate detection)
- Standard Queue usage (track duplicate delivery via `receive_count`)

**When to skip the Repository:**
- Simple, non-critical workloads (notifications, logs)
- FIFO Queue with low failure rates (less duplicate delivery)
- High-throughput scenarios (avoid DB write overhead)
- Use `consumer.NewService(sqsClient, nil, handler, config)` with `nil` repository

**Note:** The consumer is SQS-specific and located at `contrib/sqs/consumer`. If you use Kafka or other message brokers, they typically manage consumption state internally (e.g., Kafka offsets).

#### Consumer Status Flow (4 states)

```mermaid
stateDiagram-v2
    [*] --> CONSUMING: Receive from SQS
    CONSUMING --> CONSUMED: Handler success
    CONSUMING --> FAILED: Handler error
    FAILED --> CONSUMING: Retry (via SQS visibility timeout)
    FAILED --> DEAD: Max retries exceeded
    CONSUMED --> [*]
    DEAD --> [*]
```

**Common scenarios:**
- **Normal flow**: Receive from SQS → Handler succeeds → CONSUMED → SQS message deleted
- **Temporary failure**: Handler error (e.g., downstream API timeout) → FAILED → SQS visibility timeout expires → retry
- **Permanent failure**: MaxRetries exceeded (default: 5) → DEAD → SQS message deleted (NOT moved to DLQ)
- **Crash during consuming**: Process killed while handler running → ReviveStuckConsuming on restart → FAILED → SQS retries

**Operational actions:**
- **FAILED**: Usually auto-recovers via SQS visibility timeout. Check handler logs and `error_message`: `SELECT id, receive_count, error_message FROM consumer_messages WHERE status = 'FAILED'`. Fix handler bugs if persistent.
- **DEAD**: Message deleted from SQS, preserved in `consumer_messages` table. Query: `SELECT id, message_id, error_message FROM consumer_messages WHERE status = 'DEAD'`. Use `OnMessageDead` hook to preserve payloads. Options: (1) Extract payload and re-insert to outbox table, (2) Manual processing, (3) Archive/delete if invalid. See CLAUDE.md for detailed recovery procedures.

**Important:** Outbox and Consumer have completely separate state machines. The consumer never updates the outbox table.

## Quick Start

### 1. Create Database Tables

Use the schema generator to create the required tables:

```go
import "github.com/hacomono-lib/o4x/schema"

// Generate migration SQL
sql := schema.MigrationSQL("outbox", "consumer_messages")
// Execute the SQL against your database
```

Or use the CLI tool:

```bash
go run github.com/hacomono-lib/o4x/cmd/o4x-schema@latest
```

### 2. Enqueue Messages

Insert messages into the outbox table within your business transaction:

```go
import (
    "github.com/hacomono-lib/o4x/core"
    "github.com/hacomono-lib/o4x/contrib/pgx"
)

// Create repository
repo := pgx.NewOutboxRepository(pool)

// Insert message (do this within your business transaction)
_, err := repo.Insert(ctx, core.OutboxInsertParams{
    Topic:          "user.created",
    Payload:        json.RawMessage(`{"user_id": "123"}`),
    IdempotencyKey: "user-123-created",
    MaxRetries:     10,
})
```

#### Metadata (Optional)

The `Metadata` field (JSONB) can store additional context that travels with the message. Common use cases:

- **Distributed Tracing** - `trace_id`, `span_id` for OpenTelemetry/Datadog/Jaeger
- **Custom Headers** - Values to map to SQS MessageAttributes
- **Routing Hints** - Priority, tenant ID, or other routing metadata
- **Audit Context** - User ID, request ID, or correlation ID

### 3. Run the Dispatcher

**Standard Dispatcher (1 message at a time):**

```go
import (
    "github.com/hacomono-lib/o4x/core"
    "github.com/hacomono-lib/o4x/contrib/pgx"
    "github.com/hacomono-lib/o4x/contrib/sqs"
)

// Create publisher
publisher := sqs.NewPublisher(sqsClient, queueURL)

// Create repository
repo := pgx.NewOutboxRepository(pool)

// Create and start dispatcher
dispatcher := core.NewDispatcher(repo, publisher, core.DispatcherConfig{
    PollInterval: 100 * time.Millisecond,
    WorkerCount:  4,
})

if err := dispatcher.Start(ctx); err != nil {
    log.Fatal(err)
}

// Graceful shutdown
dispatcher.Stop()
```

**Batch Dispatcher (high throughput):**

```go
// Create batch publisher
publisher := sqs.NewBatchPublisher(sqsClient, queueURL)

// Create repository (implements BatchOutboxRepository)
repo := pgx.NewOutboxRepository(pool)

// Create batch dispatcher
dispatcher := core.NewBatchDispatcher(repo, publisher, core.BatchDispatcherConfig{
    PollInterval:    100 * time.Millisecond,
    BatchSize:       10, // SQS max is 10
    WorkerCount:     4,
    RequeueInterval: 30 * time.Second, // Auto-requeue FAILED messages
})

dispatcher.Start(ctx)
```

### 4. Run the Consumer (Optional)

If you need to track message consumption:

```go
import (
    "github.com/hacomono-lib/o4x/contrib/pgx"
    "github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

// Create repository (optional - can be nil)
repo := pgx.NewConsumerRepository(pool)

// Create handler (see "Handler Patterns" section for more options)
handler := consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
    log.Printf("Received: topic=%s, body=%s", msg.Topic, msg.Body)
    return nil
})

// Create and start service with the handler
// Pass nil for repo if you don't need DB tracking
svc := consumer.NewService(sqsClient, repo, handler, consumer.ServiceConfig{
    QueueURL:    queueURL,
    WorkerCount: 4,
    MaxRetries:  5,
})

if err := svc.Start(ctx); err != nil {
    log.Fatal(err)
}

// Graceful shutdown
svc.Stop()
```

## Handler Patterns

o4x provides several ways to define message handlers.

### Simple Handler Function

```go
handler := consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
    log.Printf("topic=%s body=%s", msg.Topic, string(msg.Body))
    return nil
})
```

### Topic Router

Route messages to different handlers based on topic:

```go
router := consumer.NewTopicRouter()

// Register handlers for specific topics
router.RegisterFunc("order.created", func(ctx context.Context, msg *consumer.SQSMessage) error {
    // Handle order.created events
    return nil
})

router.RegisterFunc("user.registered", func(ctx context.Context, msg *consumer.SQSMessage) error {
    // Handle user.registered events
    return nil
})

// Optional: Set fallback for unknown topics
router.SetFallback(consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
    log.Printf("unhandled topic: %s", msg.Topic)
    return nil  // Return nil to acknowledge, error to retry
}))

// Use router as the handler
svc := consumer.NewService(sqsClient, repo, router, config)
```

### Typed Handler (Generics)

Automatically unmarshal JSON payload to a specific type:

```go
type OrderCreatedEvent struct {
    OrderID   string `json:"order_id"`
    UserID    string `json:"user_id"`
    Amount    int64  `json:"amount"`
}

// Create typed handler
orderHandler := consumer.NewTypedHandler(func(ctx context.Context, topic string, event OrderCreatedEvent) error {
    log.Printf("Order %s created for user %s, amount=%d", event.OrderID, event.UserID, event.Amount)
    return nil
})

// Register with router
router := consumer.NewTopicRouter()
router.Register("order.created", orderHandler)
```

### Custom Handler Implementation

Implement the `Handler` interface for complex scenarios:

```go
type MyHandler struct {
    db     *sql.DB
    cache  *redis.Client
    logger *slog.Logger
}

func (h *MyHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // Access injected dependencies
    h.logger.Info("processing message", "topic", msg.Topic)

    // Implement idempotency check
    if h.cache.Exists(ctx, msg.IdempotencyKey).Val() {
        return nil // Already processed
    }

    // Process message...

    return nil
}

// Use custom handler
handler := &MyHandler{db: db, cache: cache, logger: logger}
svc := consumer.NewService(sqsClient, repo, handler, config)
```

## Idempotency

### Publisher Side (Outbox)

o4x provides **at-least-once delivery** with strong consistency guarantees:
- Transactional writes (message + business logic in same transaction)
- Idempotency keys prevent duplicate insertions within the outbox table
- Status transitions track publishing state (ENQUEUED → PUBLISHING → PUBLISHED)
- **Note**: Due to crash recovery edge cases, messages may be published more than once to SQS

### Consumer Side

**IMPORTANT:** You must implement idempotency in your message handlers!

Both o4x and SQS guarantee **at-least-once delivery**. This means your consumer may receive the same message multiple times in these scenarios:

- Message processing takes longer than visibility timeout
- Consumer crashes after processing but before ACK
- Network issues during message deletion
- SQS internal retries

#### Recommended Approaches

**1. Use IdempotencyKey with Cache/Database**

```go
type IdempotentHandler struct {
    cache  *redis.Client
    logger *slog.Logger
}

func (h *IdempotentHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // Check if already processed
    key := fmt.Sprintf("processed:%s", msg.IdempotencyKey)
    exists, _ := h.cache.Exists(ctx, key).Result()
    if exists > 0 {
        h.logger.Info("message already processed, skipping", "key", msg.IdempotencyKey)
        return nil // Return success to ACK the message
    }

    // Process message
    if err := h.processMessage(ctx, msg); err != nil {
        return err // Will retry
    }

    // Mark as processed (with TTL to auto-cleanup)
    h.cache.Set(ctx, key, "1", 7*24*time.Hour)
    return nil
}
```

**2. Use Database Unique Constraint**

```go
func (h *Handler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // Try to insert with unique constraint on idempotency_key
    _, err := h.db.Exec(ctx,
        "INSERT INTO processed_messages (idempotency_key, processed_at) VALUES ($1, NOW())",
        msg.IdempotencyKey,
    )
    
    if isDuplicateKeyError(err) {
        // Already processed
        return nil
    }
    if err != nil {
        return err
    }

    // Process message...
    return h.processMessage(ctx, msg)
}
```

**3. Use Consumer Repository (Built-in Tracking)**

```go
// Consumer repository automatically tracks message status
// Note: Repository records processing attempts but does NOT prevent duplicate Handler execution
// Your handler must still be idempotent
repo := pgx.NewConsumerRepository(pool)
svc := consumer.NewService(sqsClient, repo, handler, config)

// Benefits:
// - Audit trail: Full history of processing attempts
// - Failure investigation: Preserve error messages
// - Metrics: Query receive_count, processing times, failure rates
// - Crash recovery: ReviveStuckConsuming() identifies stuck messages
```

**4. Make Operations Idempotent**

Design your business logic to be naturally idempotent:

```go
// Instead of: balance += amount (NOT idempotent)
// Use: UPDATE accounts SET balance = $1 WHERE id = $2 (idempotent with final state)

// Instead of: INSERT INTO ... (may fail on duplicate)
// Use: INSERT INTO ... ON CONFLICT DO NOTHING (idempotent)
```

#### Best Practices

- **Always check IdempotencyKey** before processing
- **Use TTL for cleanup** - Processed keys don't need to live forever (7-30 days is typical)
- **Return nil for duplicates** - To ACK the message and remove it from the queue
- **Log duplicate detections** - For monitoring and debugging
- **Test duplicate scenarios** - Simulate message redelivery in your tests

## Maintenance

### Cleanup Old Messages

Over time, your outbox and consumer tables will accumulate old messages. Use `DeleteOlderThan` to clean them up periodically.

#### Cleanup Script Example

```go
import (
    "time"
    "github.com/hacomono-lib/o4x/core"
    "github.com/hacomono-lib/o4x/contrib/pgx"
)

func CleanupOldMessages(ctx context.Context) error {
    repo := pgx.NewOutboxRepository(pool)

    // Delete PUBLISHED messages older than 7 days
    // Note: DeleteOlderThan uses PostgreSQL interval format internally for reliable cleanup
    publishedCount, err := repo.DeleteOlderThan(ctx, core.OutboxStatusPublished, 7*24*time.Hour)
    if err != nil {
        return fmt.Errorf("failed to delete PUBLISHED messages: %w", err)
    }
    log.Printf("Deleted %d PUBLISHED messages", publishedCount)

    // Delete DEAD messages older than 30 days
    deadCount, err := repo.DeleteOlderThan(ctx, core.OutboxStatusDead, 30*24*time.Hour)
    if err != nil {
        return fmt.Errorf("failed to delete DEAD messages: %w", err)
    }
    log.Printf("Deleted %d DEAD messages", deadCount)

    return nil
}
```

#### Automated Cleanup with Cron

```go
import (
    "github.com/robfig/cron/v3"
)

func StartCleanupScheduler(ctx context.Context) {
    c := cron.New()
    
    // Run cleanup daily at 2 AM
    c.AddFunc("0 2 * * *", func() {
        if err := CleanupOldMessages(ctx); err != nil {
            log.Printf("cleanup failed: %v", err)
        }
    })
    
    c.Start()
}
```

#### Recommended Retention Policies

| Status | Recommended Retention | Reason |
|--------|----------------------|--------|
| `PUBLISHED` | 7-30 days | For audit/debugging, can be deleted after verification period |
| `DEAD` | 30-90 days | Keep longer for investigation, these are failed messages |
| `ENQUEUED` | N/A | Don't delete, these are pending messages |
| `PUBLISHING` | N/A | Should be transient, use `ReviveStuckPublishing()` instead |
| `FAILED` | N/A | Should be auto-retried or moved to DEAD |

#### Consumer Messages Cleanup

```go
consumerRepo := pgx.NewConsumerRepository(pool)

// Delete CONSUMED messages older than 7 days
consumedCount, err := consumerRepo.DeleteOlderThan(ctx, consumer.StatusConsumed, 7*24*time.Hour)
log.Printf("Deleted %d CONSUMED messages", consumedCount)

// Delete DEAD consumer messages older than 30 days
deadCount, err := consumerRepo.DeleteOlderThan(ctx, consumer.StatusDead, 30*24*time.Hour)
log.Printf("Deleted %d DEAD consumer messages", deadCount)
```

#### Monitoring Cleanup

```go
func CleanupWithMetrics(ctx context.Context) error {
    repo := pgx.NewOutboxRepository(pool)
    
    // Count before cleanup
    var beforeCount int64
    db.QueryRow("SELECT COUNT(*) FROM outbox WHERE status = 'PUBLISHED'").Scan(&beforeCount)
    
    // Cleanup
    deletedCount, err := repo.DeleteOlderThan(ctx, core.OutboxStatusPublished, 7*24*time.Hour)
    if err != nil {
        return err
    }
    
    // Report metrics
    metrics.RecordOutboxCleanup("PUBLISHED", deletedCount)
    log.Printf("Cleaned up %d/%d PUBLISHED messages", deletedCount, beforeCount)
    
    return nil
}
```

## Configuration

### Dispatcher Config

| Option | Default | Description |
|--------|---------|-------------|
| `PollInterval` | 100ms | How often to poll for new messages |
| `WorkerCount` | 1 | Number of concurrent workers |
| `ShutdownTimeout` | 30s | Time to wait for graceful shutdown (context respected) |
| `ForceTimeout` | 60s | Hard limit before forceful exit |

**Graceful Shutdown**: Worker and BatchDispatcher respect context cancellation. Cleanup operations (UpdateToPublished, UpdateToFailed) use derived context with 10s timeout to allow DB updates while respecting cancellation signals.

### BatchDispatcher Config

| Option | Default | Description |
|--------|---------|-------------|
| `PollInterval` | 100ms | How often to poll for new messages |
| `BatchSize` | 10 | Messages per batch (max 10 for SQS) |
| `WorkerCount` | 1 | Number of concurrent batch workers |
| `RequeueInterval` | **10s** | Interval for auto-requeue FAILED→ENQUEUED. Important: If 0, FAILED messages will not retry automatically |
| `RequeueBackoffBase` | 1s | Base interval for exponential backoff on retry |
| `RequeueBackoffMax` | 1h | Maximum backoff interval (caps exponential growth) |
| `ShutdownTimeout` | 30s | Time to wait for graceful shutdown |
| `ForceTimeout` | 60s | Hard limit before forceful exit |

**Important Notes**:
- **RequeueInterval default changed from 0 to 10s** - Previously FAILED messages never retried automatically
- Exponential backoff formula: `RequeueBackoffBase * 2^retry_count`, capped at `RequeueBackoffMax`
- For high-priority messages, use `RequeueInterval: 1*time.Second`
- For low-priority/cost-sensitive workloads, use `RequeueInterval: 60*time.Second`

### Consumer Config

| Option | Default | Description |
|--------|---------|-------------|
| `QueueURL` | (required) | SQS queue URL |
| `MaxNumberOfMessages` | 10 | Messages per poll |
| `WaitTimeSeconds` | 20 | Long polling wait time |
| `VisibilityTimeout` | 30 | SQS visibility timeout |
| `MaxRetries` | 5 | Max processing attempts |
| `WorkerCount` | 1 | Number of concurrent workers |
| `MessageConcurrency` | 1 | Messages processed concurrently per worker (>1 only for Standard queues) |

**MessageConcurrency** (new feature):
- Controls parallel message processing within each worker
- **Standard queues**: Can use any value (e.g., 10 for 10x parallelism)
- **FIFO queues**: Must be 1 (parallel processing breaks ordering guarantees)
- **Total parallelism**: `WorkerCount * MessageConcurrency`
- **Example**: `WorkerCount=5, MessageConcurrency=10` → 50 messages processed simultaneously
- **Use when**: Fast handlers (<100ms), high throughput needs, I/O-bound operations
- **Performance tip**: Start with 5-10, monitor DB connection pool, increase based on metrics

**Note**: Consumer service checks context cancellation before each polling cycle. SQS long polling (up to 20s) may delay shutdown, but context is respected during message processing.

## Repository Adapters

o4x provides adapters for popular database libraries:

- **[pgx](docs/pgx.md)** - `github.com/hacomono-lib/o4x/contrib/pgx` - High-performance PostgreSQL driver
- **[GORM](docs/gorm.md)** - `github.com/hacomono-lib/o4x/contrib/gorm` - Popular ORM with PostgreSQL support

See the detailed documentation for each adapter:
- [pgx Adapter Documentation](docs/pgx.md) - Transaction support, performance tips, and examples
- [GORM Adapter Documentation](docs/gorm.md) - ORM integration, hooks, and migration guide

## Environment Variables

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/mydb?sslmode=disable
SQS_ENDPOINT=http://localhost:4566  # For LocalStack
AWS_REGION=us-east-1

# Standard Queue (high throughput, no ordering guarantee)
SQS_QUEUE_URL=http://localhost:4566/000000000000/my-queue

# FIFO Queue (ordered delivery, deduplication)
SQS_QUEUE_URL=http://localhost:4566/000000000000/my-queue.fifo
```

## SQS Queue Types

o4x supports both Standard and FIFO SQS queues. Choose based on your requirements:

### Standard Queue (Recommended for most use cases)

```go
// No .fifo suffix
publisher := sqs.NewBatchPublisher(sqsClient, "https://sqs.../my-queue")
```

- ✅ Higher throughput (nearly unlimited)
- ✅ Lower cost ($0.40 per million requests)
- ❌ No ordering guarantee
- ❌ Possible duplicate delivery
- **Use for:** Independent events, notifications, logs, analytics

### FIFO Queue (Use when ordering matters)

```go
// Must end with .fifo
publisher := sqs.NewBatchPublisher(sqsClient, "https://sqs.../my-queue.fifo")
```

- ✅ Ordering guarantee (per MessageGroupId = topic)
- ✅ Deduplication (5-minute window via MessageDeduplicationId = idempotency_key)
- ❌ Lower throughput (300 msg/sec per queue, 3,000 with high throughput mode)
- ❌ Higher cost ($0.50 per million requests)
- **Use for:** Order workflows, payment processing, inventory updates

### When to use which?

| Scenario | Recommended Queue Type |
|----------|----------------------|
| Email notifications | **Standard** - Independent, high volume |
| User registration events | **Standard** - One-time, no ordering needed |
| Access logs | **Standard** - Timestamp-based, very high volume |
| Order processing (created→paid→shipped) | **FIFO** - State transitions must be ordered |
| Payment flows (authorize→capture→refund) | **FIFO** - Operations must follow sequence |
| Inventory updates (+10, -5, +3) | **FIFO** - Math operations are order-sensitive |

**Important**: Regardless of queue type, your consumer handlers must be idempotent. See the [Idempotency](#idempotency) section.

### Message Size Limits

**SQS enforces a hard limit of 256 KB per message.**

o4x automatically validates message sizes before publishing:
- Messages exceeding 256 KB are **immediately marked as DEAD** (PermanentError)
- No automatic retry for oversized messages (would fail again)
- Validation happens at the Publisher layer to prevent wasted SQS API calls

**Best Practices**:
- Keep payloads small - store large data in S3/database, send references in messages
- If you need >256 KB, use [SQS Extended Client](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html) pattern
- Monitor `ErrPayloadTooLarge` errors to detect oversized messages

### Single Queue (Default)

By default, all topics are published to a single queue:

```go
publisher := sqs.NewPublisher(sqsClient, queueURL)
// or for batch
publisher := sqs.NewBatchPublisher(sqsClient, queueURL)
```

### Multiple Queues

For advanced use cases, route topics to different queues. You can mix Standard and FIFO queues:

```go
import "github.com/hacomono-lib/o4x/contrib/sqs"

// Create topic-to-queue router with a default queue (Standard or FIFO)
// TopicQueueMap is thread-safe and can be registered concurrently
router := sqs.NewTopicQueueMap("https://sqs.../default-queue") // Standard

// Route order topics to FIFO queue (ordering required)
router.Register("order.created", "https://sqs.../orders-queue.fifo")
router.Register("order.updated", "https://sqs.../orders-queue.fifo")
router.Register("payment.completed", "https://sqs.../payments-queue.fifo")

// Route notification topics to Standard queue (high throughput, order doesn't matter)
router.RegisterPrefix("notification.", "https://sqs.../notifications-queue") // Standard
router.RegisterPrefix("log.", "https://sqs.../logs-queue") // Standard

// Create multi-queue publisher
publisher := sqs.NewMultiQueuePublisher(sqsClient, router)
// or for batch
publisher := sqs.NewMultiBatchPublisher(sqsClient, router)
```

**Thread Safety**: `TopicQueueMap` is safe for concurrent use. You can call `Register()`, `RegisterPrefix()`, and `QueueURL()` from multiple goroutines without external synchronization.

**When to use multiple queues:**
- Different topics have different ordering requirements (FIFO vs Standard)
- Different topics have different throughput requirements
- Different teams own different topic consumers
- Topics need different retry policies (VisibilityTimeout, MaxRetries)
- Isolation between critical and non-critical events

**Example routing strategy:**
```
Critical workflows   → FIFO queues   (orders, payments, inventory)
High-volume logs     → Standard      (access logs, analytics)
Notifications        → Standard      (emails, push notifications)
```

**Custom Router:**

Implement `TopicQueueRouter` for dynamic routing (e.g., from database or config service):

```go
type TopicQueueRouter interface {
    QueueURL(topic string) string
}
```

## Observability

o4x is designed to work with your existing observability stack. All external clients (SQS, database) are injected from outside, allowing you to wrap them with tracing middleware.

### Hooks

o4x provides hooks for metrics collection and monitoring. **All hooks have built-in panic recovery** - panics in user code are logged but won't crash workers.

```go
import "github.com/hacomono-lib/o4x/core"

hooks := &core.Hooks{
    OnPublishStart: func(ctx context.Context, msg *core.Outbox) {
        metrics.IncrCounter("outbox.publish.start", "topic", msg.Topic)
    },
    OnPublishSuccess: func(ctx context.Context, msg *core.Outbox, duration time.Duration) {
        metrics.RecordLatency("outbox.publish.latency", duration, "topic", msg.Topic)
        metrics.IncrCounter("outbox.publish.success", "topic", msg.Topic)
    },
    OnPublishFailure: func(ctx context.Context, msg *core.Outbox, err error, duration time.Duration, retryable bool) {
        metrics.IncrCounter("outbox.publish.failure", "topic", msg.Topic, "retryable", retryable)
        if !retryable {
            // Alert on permanent failures
            alerting.Send("Permanent publish failure", msg)
        }
    },
    OnMessageDead: func(ctx context.Context, msg *core.Outbox, err error) {
        metrics.IncrCounter("outbox.message.dead", "topic", msg.Topic)
        // Alert ops team, log to monitoring system, etc.
    },
}

dispatcher := core.NewBatchDispatcher(repo, publisher, core.BatchDispatcherConfig{
    Hooks: hooks,
    // ... other config
})
```

**Available Hooks**:
- `OnPublishStart` - Before publishing a message
- `OnPublishSuccess` - After successful publish
- `OnPublishFailure` - On publish error (includes retryability flag)
- `OnMessageDead` - When message exceeds max retries
- `OnBatchPublishStart` - Before publishing a batch
- `OnBatchPublishComplete` - After batch publish (includes success/failure counts)

### Datadog APM Integration

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    sqstrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/aws/aws-sdk-go-v2/aws"
)

// Load AWS config with Datadog tracing
cfg, err := config.LoadDefaultConfig(ctx)
if err != nil {
    log.Fatal(err)
}

// Wrap with Datadog tracer
sqstrace.AppendMiddleware(&cfg)

// Create traced SQS client
sqsClient := sqs.NewFromConfig(cfg)

// Use with o4x - all SQS operations will be traced
publisher := sqspub.NewPublisher(sqsClient, queueURL)
consumerSvc := consumer.NewService(sqsClient, repo, handler, config)
```

### OpenTelemetry Integration

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    "go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
)

// Load AWS config
cfg, err := config.LoadDefaultConfig(ctx)
if err != nil {
    log.Fatal(err)
}

// Instrument with OpenTelemetry
otelaws.AppendMiddleware(&cfg)

// Create traced SQS client
sqsClient := sqs.NewFromConfig(cfg)

// Use with o4x
publisher := sqspub.NewPublisher(sqsClient, queueURL)
```

## Operational Considerations

### Partial Batch Success

When using `BatchDispatcher`, the `UpdateBatchToPublished` method may return fewer updated rows than the number of IDs provided:

```go
// BatchDispatcher publishes 10 messages, but only 7 are updated to PUBLISHED
updatedCount, err := repo.UpdateBatchToPublished(ctx, successIDs)
// updatedCount might be 7 instead of 10
```

**Why this happens:**
- Messages in states other than `PUBLISHING` cannot be updated
- During crash recovery, some messages may have already been processed
- This is normal behavior and handled gracefully by the system

**What happens to the remaining messages:**
- They remain in `PUBLISHING` state temporarily
- `ReviveStuckPublishing()` will move them to `FAILED` on the next startup (5+ minutes old)
- They will be retried through the normal retry mechanism

**When to investigate:**
- If partial success occurs frequently (>5% of batches), check:
  - Database connection stability
  - Network reliability between application and database
  - Database performance (slow queries, high load)

### At-Least-Once Delivery Guarantees

o4x guarantees **at-least-once delivery**, not exactly-once. Messages may be delivered multiple times in these scenarios:

**Duplicate Delivery Scenarios:**

1. **Publisher crash during state transition**
   ```
   1. Message published to SQS successfully
   2. Process crashes before UpdateToPublished completes
   3. On restart, ReviveStuckPublishing moves message to FAILED
   4. Message is retried → Published again to SQS (duplicate)
   ```

2. **Database transaction rollback**
   ```
   1. UpdateToPublished transaction starts
   2. Transaction rolls back due to database error
   3. Message remains in PUBLISHING state
   4. Eventually retried → Published again (duplicate)
   ```

3. **Consumer processing timeout**
   ```
   1. Handler processes message successfully
   2. Processing takes longer than SQS VisibilityTimeout
   3. Message becomes visible in SQS again
   4. Another consumer receives the same message (duplicate)
   ```

**Mitigation Strategies:**

✅ **Required: Idempotent Handlers**
```go
func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // Use database unique constraint
    result, err := h.db.Exec(ctx,
        `INSERT INTO orders (id, customer_id, message_id, created_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (message_id) DO NOTHING`,
        orderID, customerID, msg.MessageID)

    if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
        // Already processed - return success
        return nil
    }

    // Process new order...
}
```

✅ **Recommended: Use IdempotencyKey**
- Publisher: Set unique `IdempotencyKey` when inserting to outbox
- Consumer: Use `msg.MessageID` or `msg.IdempotencyKey` for deduplication
- FIFO queues provide 5-minute deduplication via MessageDeduplicationId

✅ **Optional: Track Duplicates**
```go
hooks := &core.Hooks{
    OnPublishSuccess: func(ctx context.Context, msg *core.Outbox, duration time.Duration) {
        // Check if this message was published before
        var count int
        db.QueryRow("SELECT COUNT(*) FROM published_messages WHERE outbox_id = $1", msg.ID).Scan(&count)
        if count > 1 {
            metrics.IncrCounter("outbox.duplicates", "topic", msg.Topic)
        }
    },
}
```

### Performance Tuning

**BatchDispatcher Configuration:**

| Workload | BatchSize | WorkerCount | RequeueInterval | Expected Throughput |
|----------|-----------|-------------|-----------------|---------------------|
| Low volume (<100 msg/min) | 5 | 1 | 30s | ~100 msg/min |
| Medium volume (100-1000 msg/min) | 10 | 2-4 | 10s | ~1,000-5,000 msg/min |
| High volume (>1000 msg/min) | 10 | 8-16 | 5s | ~5,000-20,000 msg/min |

**Database Connection Pool:**
```go
// For BatchDispatcher with 10 workers
maxConns := WorkerCount * 2 + 5  // ~25 connections
pool, _ := pgxpool.New(ctx, fmt.Sprintf("%s&pool_max_conns=%d", dbURL, maxConns))
```

**Worker Count Guidelines:**
- **Too few workers**: Messages pile up in ENQUEUED state, increased latency
- **Too many workers**: Database connection pool exhaustion, lock contention
- **Rule of thumb**: Start with `WorkerCount = 2 * CPU cores`, adjust based on metrics

**PollInterval Tuning:**
- **100ms (default)**: Balanced latency and database load
- **10ms**: Ultra-low latency (<100ms P99), higher database load
- **500ms**: Reduce database queries by 80%, acceptable for non-real-time workloads

**Monitoring Metrics:**
```go
hooks := &core.Hooks{
    OnBatchPublishComplete: func(ctx context.Context, successCount, failCount int, duration time.Duration) {
        metrics.RecordHistogram("batch.publish.duration", duration)
        metrics.RecordGauge("batch.publish.success_rate", float64(successCount)/(successCount+failCount))
    },
}
```

**Database Optimization:**
```sql
-- Ensure critical indexes exist
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_status_created_at
ON outbox(status, created_at) WHERE status IN ('ENQUEUED', 'FAILED');

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_status_next_retry_at
ON outbox(status, next_retry_at) WHERE status = 'FAILED';

-- Monitor table bloat
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE tablename = 'outbox';
```

## License

MIT
