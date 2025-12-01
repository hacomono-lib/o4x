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
import "github.com/hacomono-lib/o4x/contrib/pgx"

repo := pgx.NewOutboxRepository(pool)

// Insert within your business transaction using WithTx
tx, _ := pool.Begin(ctx)
defer tx.Rollback(ctx)

tx.Exec(ctx, "INSERT INTO orders ...") // Business logic
repo.WithTx(tx).Insert(ctx, core.OutboxInsertParams{...}) // Outbox in same tx

tx.Commit(ctx)
// External CDC tool (Debezium) or custom poller handles publishing
```

**Tier 2: Core + Publisher**
```go
import (
    "github.com/hacomono-lib/o4x/core"
    "github.com/hacomono-lib/o4x/contrib/pgx"
    "github.com/hacomono-lib/o4x/contrib/sqs"
)

repo := pgx.NewOutboxRepository(pool)
publisher := sqs.NewBatchPublisher(sqsClient, queueURL)
dispatcher := core.NewBatchDispatcher(repo, publisher, config)
dispatcher.Start(ctx)
```

**Tier 3: Core + Publisher + Consumer**
```go
import "github.com/hacomono-lib/o4x/contrib/sqs/consumer"

// Add to Tier 2
consumerRepo := pgx.NewConsumerRepository(pool)  // optional, can be nil
service := consumer.NewService(sqsClient, queueURL, handler, consumerRepo, config)
service.Start(ctx)
```

Each tier is independent - you only import what you need. The `core` package contains both model/repository interfaces AND Dispatcher, but using only repository methods incurs no Dispatcher dependencies.

### Two Completely Separate State Machines

**Critical**: Outbox (Publisher) and Consumer have independent state machines. Never mix them.

**Outbox Status (5 states)** - Publisher/Dispatcher side:
- `ENQUEUED` → `PUBLISHING` → `PUBLISHED`
- `ENQUEUED` → `PUBLISHING` → `FAILED` → (retry) → `ENQUEUED`
- `FAILED` → `DEAD` (when max_retries exceeded)

**Concrete scenarios for Outbox status transitions:**

1. **Normal flow (ENQUEUED → PUBLISHING → PUBLISHED)**
   - Application inserts message to outbox table with status=ENQUEUED
   - Dispatcher fetches the message and updates status to PUBLISHING
   - SQS publish succeeds, status updates to PUBLISHED

2. **Temporary failure with retry (ENQUEUED → PUBLISHING → FAILED → ENQUEUED)**
   - Dispatcher tries to publish to SQS but gets network timeout error
   - Status updates to FAILED, retry_count increments, next_retry_at is calculated
   - After RequeueInterval, RequeueFailed moves message back to ENQUEUED
   - Dispatcher retries publishing (repeats until success or max_retries exceeded)

3. **Permanent failure (FAILED → DEAD)**
   - Message fails publishing 5 times (max_retries=5)
   - On next retry attempt, status updates to DEAD instead of ENQUEUED
   - No further retry attempts, OnMessageDead hook is called for alerting

4. **Crash during publishing**
   - Dispatcher updates status to PUBLISHING and starts SQS publish
   - Process crashes (SIGKILL, OOM, etc.) before UpdateToPublished completes
   - On next startup, ReviveStuckPublishing finds stuck PUBLISHING messages (>5min old)
   - Message moves to FAILED with incremented retry_count, will retry via RequeueFailed

5. **Oversized message (ENQUEUED → PUBLISHING → DEAD)**
   - Dispatcher tries to publish 300KB message to SQS (limit: 256KB)
   - Publisher returns PermanentError (IsRetryable=false)
   - Status immediately updates to DEAD without retry

**Operational actions for FAILED/DEAD messages (Outbox):**

**When message is FAILED:**
- **Monitor**: Set up alerts for high FAILED count or messages stuck in FAILED
- **Auto-recovery**: RequeueFailed automatically retries (check RequeueInterval config)
- **Investigation**: Query `error_message` and `retry_count` columns
  ```sql
  SELECT id, topic, retry_count, error_message, next_retry_at, payload
  FROM outbox WHERE status = 'FAILED' ORDER BY created_at DESC LIMIT 10;
  ```
- **Common causes**:
  - Network issues (temporary) → usually auto-recovers
  - Invalid AWS credentials → fix credentials and restart Dispatcher
  - SQS endpoint unreachable → check SQS_ENDPOINT config
  - Rate limiting → adjust WorkerCount or RequeueInterval
- **Manual intervention**: If needed, update `retry_count = 0` to reset retry attempts

**When message is DEAD:**
- **Alert immediately**: Configure OnMessageDead hook to alert ops team
- **Investigation**: Query DEAD messages to identify root cause
  ```sql
  SELECT id, topic, error_message, retry_count, payload
  FROM outbox WHERE status = 'DEAD' ORDER BY created_at DESC LIMIT 10;
  ```
- **Common causes**:
  - Payload exceeds 256 KB → Refactor to use S3 reference pattern
  - Malformed payload → Fix application code generating the payload
  - Invalid topic routing → Check topic name and queue configuration
  - Persistent infrastructure issues → Resolve infrastructure problems first
- **Recovery options**:
  1. **Fix and re-enqueue**: If fixable (e.g., reduce payload size)
     ```sql
     UPDATE outbox SET status = 'ENQUEUED', retry_count = 0, error_message = NULL
     WHERE id = 'problematic-message-id';
     ```
  2. **Manual publish**: Extract payload and publish manually to SQS
  3. **Archive and ignore**: If message is no longer relevant, delete or archive
     ```sql
     DELETE FROM outbox WHERE status = 'DEAD' AND created_at < NOW() - INTERVAL '30 days';
     ```
- **Preventive measures**:
  - Add payload size validation before Insert
  - Implement OnPublishFailure hook to detect patterns
  - Monitor DEAD message rate and set up alerts

**Consumer Status (4 states)** - Consumer side only:
- `CONSUMING` → `CONSUMED`
- `CONSUMING` → `FAILED` → (retry via SQS visibility timeout)
- `FAILED` → `DEAD` (when max_retries exceeded)

**Concrete scenarios for Consumer status transitions:**

1. **Normal flow (CONSUMING → CONSUMED)**
   - Consumer receives message from SQS with receipt handle
   - Consumer repository inserts/updates record with status=CONSUMING
   - Handler processes message successfully
   - Status updates to CONSUMED, SQS message is deleted via receipt handle

2. **Temporary failure with retry (CONSUMING → FAILED → retry)**
   - Handler throws error during processing (e.g., downstream API timeout)
   - Status updates to FAILED with error_message
   - SQS message is NOT deleted (receipt handle expires)
   - After SQS visibility timeout (e.g., 30s), message becomes visible again
   - Consumer receives same message again with incremented receive_count
   - Status updates back to CONSUMING, handler retries

3. **Permanent failure (FAILED → DEAD)**
   - Message fails processing repeatedly until receive_count >= max_retries (default: 5)
   - o4x marks status as DEAD and deletes message from SQS
   - Message is NOT moved to DLQ (o4x handles DEAD messages via DB + Hooks)
   - No further processing attempts

4. **Crash during consuming**
   - Consumer updates status to CONSUMING and starts handler processing
   - Process crashes (SIGKILL, OOM, etc.) before UpdateToConsumed completes
   - On next startup, ReviveStuckConsuming finds stuck CONSUMING messages (>5min old)
   - Message moves to FAILED
   - SQS message becomes visible again after visibility timeout expires
   - Consumer retries processing

**Operational actions for FAILED/DEAD messages (Consumer):**

**When message is FAILED:**
- **Monitor**: Set up alerts for high FAILED count or long processing times
- **Auto-recovery**: SQS visibility timeout automatically retries (check VisibilityTimeout config)
- **Investigation**: Query `error_message` and `receive_count` columns
  ```sql
  SELECT id, message_id, receive_count, error_message, last_error_at, queue_url
  FROM consumer_messages WHERE status = 'FAILED' ORDER BY created_at DESC LIMIT 10;
  ```
- **Common causes**:
  - Downstream API timeout (temporary) → usually auto-recovers via SQS retry
  - Database connection issues → check DB connectivity and pool settings
  - Handler logic error → review error_message and fix handler code
  - Idempotency key collision → check for duplicate processing logic
- **Debugging**:
  - Check handler logs around `last_error_at` timestamp
  - Verify SQS message is still in queue (not expired)
  - Test handler with payload locally
- **Manual intervention**: If message is stuck, can manually move to CONSUMING to force retry
  ```sql
  UPDATE consumer_messages SET status = 'CONSUMING', receive_count = 0
  WHERE id = 'stuck-message-id';
  ```

**When message is DEAD:**
- **Alert immediately**: Message exceeded max retries, likely requires manual intervention
  - Use `OnMessageDead` hook to send alerts to Slack/PagerDuty
  - SQS message is already deleted (not in DLQ)
- **Investigation**: Query `consumer_messages` table for processing history
  ```sql
  SELECT id, message_id, receive_count, error_message, last_error_at, created_at
  FROM consumer_messages WHERE status = 'DEAD' ORDER BY created_at DESC LIMIT 10;
  ```
  - If using Repository: Message payload may be preserved via custom logging in OnMessageDead hook
  - If not using Repository: Must implement custom payload persistence in handler
- **Common causes**:
  - Persistent handler bug → Fix handler logic and redeploy
  - Invalid message payload → Sender produced malformed data
  - Required downstream service permanently down → Fix infrastructure
  - Business rule violation → Payload doesn't meet validation requirements
- **Recovery options**:
  1. **Manual re-publishing** (if payload preserved):
     - Extract payload from your logging system or DB
     - Re-insert to outbox table manually: `INSERT INTO outbox (topic, payload, ...) VALUES (...)`
     - Dispatcher will pick it up and publish to SQS again
  2. **Manual processing**:
     - Extract payload from logs or DB
     - Process manually (e.g., database insert, API call)
     - Mark as resolved in tracking system
  3. **Ignore and archive**: If message is invalid or no longer relevant
     ```sql
     DELETE FROM consumer_messages WHERE status = 'DEAD' AND created_at < NOW() - INTERVAL '30 days';
     ```
- **Payload preservation strategy** (implement in OnMessageDead hook):
  ```go
  hooks := &consumer.Hooks{
      OnMessageDead: func(ctx context.Context, msg *consumer.SQSMessage, err error) {
          // Save to dead messages table for investigation
          db.Exec(ctx, `INSERT INTO dead_messages (message_id, topic, payload, error)
                        VALUES ($1, $2, $3, $4)`,
              msg.MessageID, msg.Topic, msg.Body, err.Error())

          // Alert ops team
          slackClient.SendAlert(ctx, fmt.Sprintf("DEAD: %s - %v", msg.Topic, err))
      },
  }
  ```
- **Preventive measures**:
  - Add input validation in handler before processing
  - Implement comprehensive error handling with specific error types
  - Set up monitoring for DEAD message rate (via OnMessageDead hook)
  - Preserve payloads in OnMessageDead hook for recovery
  - Regular cleanup: `DELETE FROM consumer_messages WHERE status = 'DEAD' AND created_at < NOW() - INTERVAL '30 days'`

### Key Components

- **core/**: Dispatcher and Worker that poll outbox table and publish to message broker
  - `Dispatcher` - Standard 1-message-at-a-time processing
  - `BatchDispatcher` - High throughput batch processing with `RequeueInterval` for auto-retry
  - Worker uses `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` (or LIMIT N for batch)
  - `Publisher` interface allows different backends (SQS, Kafka, etc.)
  - `BatchPublisher` extends Publisher with batch operations
  - `OutboxRepository` interface for outbox persistence
  - `BatchOutboxRepository` extends with batch operations

- **contrib/sqs/**: SQS publisher implementations
  - `Publisher` / `BatchPublisher` - Single queue
  - `MultiQueuePublisher` / `MultiBatchPublisher` - Topic-based queue routing
  - `TopicQueueRouter` interface for custom routing logic
  - `TopicQueueMap` - Thread-safe implementation with sync.RWMutex (safe for concurrent Register/RegisterPrefix/QueueURL calls)
  - `MessageGroupId` = topic (ordering)
  - `MessageDeduplicationId` = idempotency_key

- **contrib/sqs/consumer/**: SQS message consumer (optional)
  - Only needed for SQS (Kafka manages offsets internally)
  - Consumer never updates outbox table
  - `Handler` interface with `TopicRouter` and `TypedHandler[T]` helpers
  - `Repository` interface is optional (can be nil for no DB tracking)

- **contrib/pgx/**: PostgreSQL repository implementations using pgx (includes BatchOutboxRepository)
  - `WithTx(tx pgx.Tx)` - Returns repository that uses the given transaction
- **contrib/gorm/**: PostgreSQL repository implementations using GORM (includes BatchOutboxRepository)
  - `WithTx(tx *gorm.DB)` - Returns repository that uses the given transaction

- **schema/**: DDL generation helpers (`OutboxDDL`, `ConsumerMessagesDDL`, `MigrationSQL`)

### Key Interfaces

```go
// core/publisher.go - Implement for new message brokers
type Publisher interface {
    Publish(ctx context.Context, msg *Outbox) error
}

type BatchPublisher interface {
    Publisher
    PublishBatch(ctx context.Context, msgs []*Outbox) []PublishResult
    MaxBatchSize() int
}

// core/repository.go - Implement for new databases (outbox side)
type OutboxRepository interface {
    Insert, FetchAndLock, UpdateToPublishing, UpdateToPublished, UpdateToFailed, UpdateToDead, RequeueFailed, GetByID, GetByIdempotencyKey
}

type BatchOutboxRepository interface {
    OutboxRepository
    FetchLockAndMarkPublishing(ctx context.Context, limit int) ([]*Outbox, error)
    UpdateBatchToPublished(ctx context.Context, ids []string) (int64, error)  // Returns success count
}

// core/repository.go - Table cleanup
type OutboxCleaner interface {
    DeleteOlderThan(ctx context.Context, status OutboxStatus, olderThan time.Duration) (int64, error)
}

// core/errors.go - Error classification for retry behavior
type RetryableError interface {
    error
    IsRetryable() bool  // false = mark as DEAD immediately
}

// core/hooks.go - Observability hooks
type Hooks struct {
    OnPublishStart, OnPublishSuccess, OnPublishFailure, OnMessageDead
    OnBatchPublishStart, OnBatchPublishComplete
}

// contrib/sqs/consumer/handler.go - Message processing
type Handler interface {
    Handle(ctx context.Context, msg *SQSMessage) error
}

// contrib/sqs/consumer/repository.go - Implement for new databases (consumer side, optional)
type Repository interface {
    InsertOrUpdate, UpdateToConsumed, UpdateToFailed, UpdateToDead, GetByMessageID
}
```

### Database Tables

**Outbox Table Schema** (Publisher side):
- `id` (UUID, PK) - UUID v7 for time-ordered identifiers
- `topic`, `payload` (JSONB), `idempotency_key`
- `status` (ENUM) - ENQUEUED, PUBLISHING, PUBLISHED, FAILED, DEAD
- `error_message`, `retry_count`, `max_retries`
- `next_retry_at` (TIMESTAMPTZ) - Pre-calculated next retry time for efficient RequeueFailed
- `created_at`, `updated_at`

**Indexes**:
- `idx_outbox_status_created_at` - For Dispatcher polling
- `idx_outbox_status_next_retry_at` - For efficient RequeueFailed (WHERE status='FAILED')

**Consumer Messages Table** (Consumer side, optional):
- Used by Consumer only, tracks consumption state
- Never updated by Dispatcher/Publisher

### Startup Recovery

Call these methods once at startup to recover from crashes:
- `OutboxRepository.ReviveStuckPublishing()` - Moves PUBLISHING → FAILED
  - Increments `retry_count` to enforce `max_retries` limit
  - Messages exceeding `max_retries` will be moved to DEAD on next retry attempt
  - Only affects messages stuck in PUBLISHING for more than 5 minutes
  - Uses exponential backoff for `next_retry_at` calculation
  - Rationale: While a message may have been successfully published before the crash, incrementing the counter prevents infinite retries for consistently failing messages. This trade-off favors system stability over potential duplicate detection.
- `ConsumerRepository.ReviveStuckConsuming()` - Moves CONSUMING → FAILED

### Outbox ID

Outbox ID uses UUID v7 (string type) for time-ordered unique identifiers.

### Multi-Queue Routing (SQS)

Route different topics to different SQS queues:

```go
import "github.com/hacomono-lib/o4x/contrib/sqs"

router := sqs.NewTopicQueueMap("https://sqs.../default-queue.fifo")
router.Register("order.created", "https://sqs.../orders-queue.fifo")
router.RegisterPrefix("notification.", "https://sqs.../notifications-queue.fifo")

publisher := sqs.NewMultiBatchPublisher(sqsClient, router)
```

**Message Ordering Guarantees:**

o4x provides different ordering guarantees depending on your SQS queue configuration:

1. **FIFO Queue (*.fifo)**: Messages with the same `MessageGroupId` are delivered in order
   - o4x sets `MessageGroupId = topic` by default
   - All messages for the same topic (e.g., "order.created") are processed in insertion order
   - Order is guaranteed per topic within a single queue
   - Example: order.created-1, order.created-2, order.created-3 will be consumed in this exact order

2. **Multi-Queue with FIFO**: Each queue maintains separate ordering
   - Messages routed to different queues have independent ordering
   - Example:
     - Queue A: "order.created" messages (ordered within this queue)
     - Queue B: "notification.sent" messages (ordered within this queue)
     - No ordering guarantee between Queue A and Queue B

3. **Standard Queue (non-FIFO)**: Best-effort ordering only
   - SQS does not guarantee message order
   - Messages may be delivered out of insertion order
   - Use FIFO queues if ordering is critical

**Idempotency and Deduplication:**

- `MessageDeduplicationId = idempotency_key` ensures exactly-once publishing within SQS's 5-minute deduplication window
- Application handlers must still be idempotent for at-least-once delivery guarantee
- See "Idempotency Implementation Without Repository" example below for handler-level idempotency

**Batch Publishing and Ordering:**

- `BatchDispatcher` fetches messages ordered by `created_at` (UUID v7 time-ordered)
- Within a single batch, messages maintain insertion order before being sent to SQS
- SQS FIFO queues preserve this order for messages with the same MessageGroupId

### Environment Variables

```
DATABASE_URL=postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable
SQS_ENDPOINT=http://localhost:14566
SQS_QUEUE_URL=http://localhost:14566/000000000000/o4x-events.fifo
AWS_REGION=us-east-1
```

### Important Constraints and Limits

**SQS Message Size**:
- **Hard limit: 256 KB** per message (SQS restriction)
- Messages exceeding this limit are automatically marked as DEAD (PermanentError)
- Validation happens at Publisher layer before sending to SQS
- No automatic retry for oversized messages

**BatchDispatcher Configuration**:
- **RequeueInterval**: Default is 10 seconds
  - Important: If set to 0, FAILED messages will not be retried automatically
  - Recommended: 10s for normal workloads, 1s for high-priority messages
- **RequeueBackoffBase**: Default 1 second
- **RequeueBackoffMax**: Default 1 hour
- Exponential backoff formula: `baseInterval * 2^retry_count`, capped at maxInterval

**Hook Safety**:
- All hooks have panic recovery built-in
- Panics in user-defined hooks are logged but do NOT crash workers
- Use hooks for metrics/observability without worrying about stability

**Graceful Shutdown**:
- Worker and BatchDispatcher respect context cancellation during shutdown
- Cleanup operations (UpdateToPublished, UpdateToFailed) use context with timeout to respect cancellation while allowing time for DB updates
- Consumer service checks context before each polling cycle for responsive shutdown
- Extended timeout (10s) allows slow DB operations to complete during shutdown while respecting cancellation signals

**At-least-once Delivery Guarantee**:
- o4x guarantees **at-least-once** delivery, NOT exactly-once
- Duplicate messages are possible in edge cases (crash during state transition)
- **Application handlers MUST be idempotent**
- Use `idempotency_key` to detect duplicates if needed

**Batch Operations**:
- `UpdateBatchToPublished` returns the number of successfully updated messages (int64)
- Partial success is allowed - only messages in PUBLISHING state will be updated
- If returned count < len(ids), some messages were not in PUBLISHING state
- This can occur during crash recovery when messages are already processed
- BatchDispatcher logs warnings for partial success but continues operation
- Messages not updated will be recovered via `ReviveStuckPublishing` on next startup

**Database Cleanup**:
- PUBLISHED and DEAD messages accumulate over time
- Use `OutboxCleaner.DeleteOlderThan()` periodically to prevent table bloat
- Recommended: Delete PUBLISHED > 7 days, DEAD > 30 days
- Implementation note: Uses PostgreSQL interval format internally (e.g., "3600 seconds") for reliable cleanup
- Both pgx and GORM implementations support this correctly

### Testing

- **Unit Tests**: Run `make test-short` for unit tests without database (fast, no DB required)
- **Integration Tests**: Run `make test` for full test suite with PostgreSQL (requires `make up` first)
- **Race Detector**: All test commands include `-race` flag to detect concurrency issues
- **E2E Tests**: Comprehensive E2E tests for DeleteOlderThan verify behavior across multiple statuses (PUBLISHED, DEAD, ENQUEUED) and time ranges
- **Test Coverage**: Run `make test-coverage` to generate HTML coverage report

### Linting

**CRITICAL**: Always run `make lint` after making code changes and before committing.

Uses golangci-lint with: errcheck, govet, staticcheck, unused, misspell, gocritic, revive.
Formatting: gofmt + goimports with local-prefix `github.com/hacomono-lib/o4x`.

**Workflow**:
1. Make code changes
2. Run `make lint` to catch issues
3. Fix any linting errors
4. Run `make test-short` to verify tests pass
5. Commit changes

If `make lint` fails, you MUST fix all reported issues before committing. Linting errors indicate potential bugs, style violations, or code quality issues that should be addressed.

### Health Check Endpoints for Containerized Deployments

o4x provides health status APIs for implementing health check endpoints in containerized environments (ECS, Kubernetes, etc.).

**Available APIs:**
- `Dispatcher.HealthStatus() core.HealthStatus`
- `BatchDispatcher.HealthStatus() core.HealthStatus`
- `consumer.Service.HealthStatus() core.HealthStatus`

**HealthStatus fields:**
- `Running bool` - Whether the component is currently running
- `LastProcessedAt *time.Time` - Timestamp of last successful message processing (nil if no messages processed yet)
- `WorkerCount int` - Number of active workers
- `PendingShutdown bool` - Whether shutdown has been initiated

**Helper methods:**
- `IsHealthy() bool` - Returns true if running and not pending shutdown
- `IsStale(maxAge time.Duration) bool` - Returns true if no messages processed within maxAge

**Example implementation (Dispatcher):**
```go
// /health endpoint (liveness probe)
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    status := dispatcher.HealthStatus()

    if !status.IsHealthy() {
        w.WriteHeader(http.StatusServiceUnavailable)
        return
    }

    // Optional: Detect stuck workers
    if status.IsStale(5 * time.Minute) {
        w.WriteHeader(http.StatusServiceUnavailable)
        return
    }

    w.WriteHeader(http.StatusOK)
})

// /ready endpoint (readiness probe)
http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
    if !dispatcher.IsRunning() {
        w.WriteHeader(http.StatusServiceUnavailable)
        return
    }

    // Check database connectivity
    if err := pool.Ping(r.Context()); err != nil {
        w.WriteHeader(http.StatusServiceUnavailable)
        return
    }

    w.WriteHeader(http.StatusOK)
})
```

**ECS Task Definition Example:**
```json
{
  "containerDefinitions": [{
    "name": "dispatcher",
    "image": "your-registry/o4x-dispatcher:latest",
    "portMappings": [{
      "containerPort": 8080,
      "protocol": "tcp"
    }],
    "healthCheck": {
      "command": ["CMD-SHELL", "curl -f http://localhost:8080/health || exit 1"],
      "interval": 30,
      "timeout": 5,
      "retries": 3,
      "startPeriod": 60
    },
    "environment": [
      {"name": "DATABASE_URL", "value": "postgres://..."},
      {"name": "SQS_QUEUE_URL", "value": "https://sqs..."},
      {"name": "HEALTH_PORT", "value": "8080"}
    ]
  }]
}
```

**ALB Target Group Health Check:**
- Path: `/ready`
- Port: 8080
- Healthy threshold: 2
- Unhealthy threshold: 3
- Timeout: 5s
- Interval: 30s

**Best Practices:**
- Use `/health` for **liveness** probe (container restart trigger)
- Use `/ready` for **readiness** probe (traffic routing decision)
- Set `IsStale()` threshold based on expected message frequency
- For Consumer: Port 8081 (to avoid conflict with Dispatcher)
- Enable graceful shutdown with sufficient timeout (default: 30s shutdown, 60s force)

See `examples/local/cmd/dispatcher/main.go` and `examples/local/cmd/consumer/main.go` for full implementation examples.

### Idempotency Implementation With and Without Repository

The Consumer `Repository` is **optional**. There are two approaches to idempotency:

#### Approach 1: With Consumer Repository (DB-Tracked)

o4x provides a built-in `consumer_messages` table to track message processing state:

```go
import (
    "github.com/hacomono-lib/o4x/contrib/sqs/consumer"
    "github.com/hacomono-lib/o4x/contrib/pgx"
)

// Create consumer repository (tracks message state in DB)
consumerRepo := pgx.NewConsumerRepository(pool)

// Create consumer service with repository
service := consumer.NewService(sqsClient, consumerRepo, handler, config)
service.Start(ctx)
```

**How it works:**

1. **Message arrives from SQS** → Service calls `InsertOrUpdate()`:
   ```sql
   INSERT INTO consumer_messages (id, message_id, status, ...)
   VALUES (...)
   ON CONFLICT (message_id) DO UPDATE
   SET receipt_handle = EXCLUDED.receipt_handle,
       receive_count = EXCLUDED.receive_count,
       status = 'CONSUMING',
       updated_at = now()
   ```

2. **If message is a duplicate** (same `message_id` already exists):
   - Existing record is updated with new `receipt_handle` and `receive_count`
   - Status becomes `CONSUMING` again (allows retry after crashes)
   - Handler is still called (must be idempotent!)

3. **After successful processing** → `UpdateToConsumed()`:
   ```sql
   UPDATE consumer_messages
   SET status = 'CONSUMED', processed_at = now()
   WHERE id = $1 AND status = 'CONSUMING'
   ```

4. **On failure** → `UpdateToFailed()` or `UpdateToDead()`

**Benefits:**
- ✅ Audit trail: Full history of all message processing attempts
- ✅ Monitoring: Query DB for FAILED/DEAD messages
- ✅ Crash recovery: `ReviveStuckConsuming()` handles stuck CONSUMING messages
- ✅ Metrics: Track processing times, failure rates, receive counts

**Database Schema:**
```sql
CREATE TABLE consumer_messages (
    id UUID PRIMARY KEY,
    outbox_id UUID,
    message_id TEXT NOT NULL UNIQUE,  -- SQS MessageId (idempotency key)
    receipt_handle TEXT NOT NULL,
    receive_count INT NOT NULL,
    queue_url TEXT NOT NULL,
    status TEXT NOT NULL,  -- CONSUMING, CONSUMED, FAILED, DEAD
    error_message TEXT,
    last_error_at TIMESTAMPTZ,
    max_retries INT NOT NULL,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Important Notes:**
- ⚠️ **Handler must still be idempotent** - Repository tracks SQS message state, but your business logic may execute multiple times
- ⚠️ The repository does NOT prevent handler execution on duplicates - it only records the processing state
- For true deduplication at handler level, combine with "Strategy 1" below (DB unique constraint in your business table)

**Transaction Isolation (Critical):**
- ⚠️ **Handler and Consumer use SEPARATE transactions** - This is by design and critical for correctness
- Consumer's transaction manages `consumer_messages` state (CONSUMING → CONSUMED/FAILED)
- Handler's transaction manages your business data (orders, inventory, etc.)
- **Why separation is necessary:**
  - If Handler fails, Consumer MUST still record the failure in `consumer_messages` (status=FAILED)
  - If they shared a transaction, Handler failure → rollback → Consumer state lost
  - Result: No failure history, incorrect `receive_count`, broken monitoring
- **Handler responsibility:** Manage your own transaction for business operations
  ```go
  func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
      // Handler manages its own transaction
      tx, _ := h.db.Begin(ctx)
      defer tx.Rollback(ctx)

      // Business operations in transaction
      tx.Exec(ctx, "INSERT INTO orders ...")
      tx.Exec(ctx, "UPDATE inventory ...")

      // Commit if all succeeded
      return tx.Commit(ctx)

      // If error returned, Consumer records FAILED status (in separate transaction)
  }
  ```

**Example with Repository + Business Idempotency:**
```go
type OrderHandler struct {
    db *sql.DB
}

func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // Consumer repository tracks this message in consumer_messages table (separate transaction)
    // Handler manages its own transaction for business data

    var event OrderCreatedEvent
    json.Unmarshal(msg.Body, &event)

    // Start handler's own transaction
    tx, _ := h.db.Begin(ctx)
    defer tx.Rollback(ctx)  // Rollback if not committed

    // Idempotent insert: prevents duplicate orders even if handler runs twice
    query := `
        INSERT INTO orders (id, customer_id, message_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (message_id) DO NOTHING
    `
    result, _ := tx.ExecContext(ctx, query, event.OrderID, event.CustomerID, msg.MessageID)

    if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
        // Already processed - consumer_messages shows CONSUMED, order already exists
        return nil  // No commit needed, just return success
    }

    // Process new order (all in same transaction)
    if err := h.processNewOrder(ctx, tx, event); err != nil {
        return err  // Transaction rolls back, Consumer records FAILED
    }

    // Commit business transaction
    return tx.Commit(ctx)  // If commit succeeds, Consumer records CONSUMED
}

func (h *OrderHandler) processNewOrder(ctx context.Context, tx *sql.Tx, event OrderCreatedEvent) error {
    // Update inventory
    _, err := tx.ExecContext(ctx, "UPDATE inventory SET stock = stock - 1 WHERE product_id = $1", event.ProductID)
    if err != nil {
        return err
    }

    // Other business operations...
    return nil
}
```

#### Approach 2: Without Consumer Repository (Application-Level Idempotency)

If you don't need message processing audit trail, you can skip the repository and implement idempotency at the application level:

```go
// No repository - relies on SQS visibility timeout + application idempotency
// DEAD messages are deleted from SQS (not moved to DLQ)
service := consumer.NewService(sqsClient, nil, handler, config)
service.Start(ctx)
```

You can implement idempotency at the application level using various strategies:

**Strategy 1: Database Unique Constraint (Recommended)**

Use your existing business table with a unique constraint on the message ID or idempotency key:

```go
import (
    "github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

type OrderHandler struct {
    db *sql.DB
}

func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // Parse message
    var event OrderCreatedEvent
    if err := json.Unmarshal(msg.Body, &event); err != nil {
        return err
    }

    // Idempotent insert using ON CONFLICT (PostgreSQL)
    // The message_id column has a UNIQUE constraint
    query := `
        INSERT INTO orders (id, customer_id, amount, message_id, created_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (message_id) DO NOTHING
    `
    result, err := h.db.ExecContext(ctx, query,
        event.OrderID, event.CustomerID, event.Amount, msg.MessageID)
    if err != nil {
        return err
    }

    // Check if row was actually inserted (not a duplicate)
    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        // Duplicate message - already processed, return success
        return nil
    }

    // Process the new order
    return h.processNewOrder(ctx, event)
}
```

**Strategy 2: Redis Cache for Recent Messages**

Use Redis with TTL for short-term deduplication (complement to SQS's 5-minute window):

```go
import (
    "github.com/redis/go-redis/v9"
    "github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

type NotificationHandler struct {
    redis *redis.Client
    db    *sql.DB
}

func (h *NotificationHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // Try to set message ID with NX (set if not exists) and 10-minute expiration
    key := fmt.Sprintf("processed:notification:%s", msg.MessageID)
    set, err := h.redis.SetNX(ctx, key, "1", 10*time.Minute).Result()
    if err != nil {
        return err
    }

    if !set {
        // Message already processed recently
        return nil
    }

    // Parse and process message
    var event NotificationEvent
    if err := json.Unmarshal(msg.Body, &event); err != nil {
        return err
    }

    return h.sendNotification(ctx, event)
}
```

**Strategy 3: Business Data Check**

Check if the business operation was already completed:

```go
type PaymentHandler struct {
    db *sql.DB
}

func (h *PaymentHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    var event PaymentRequestedEvent
    if err := json.Unmarshal(msg.Body, &event); err != nil {
        return err
    }

    // Check if payment already exists (idempotent check)
    var exists bool
    err := h.db.QueryRowContext(ctx,
        "SELECT EXISTS(SELECT 1 FROM payments WHERE external_id = $1)",
        event.PaymentID).Scan(&exists)
    if err != nil {
        return err
    }

    if exists {
        // Payment already processed
        return nil
    }

    // Process payment
    return h.processPayment(ctx, event)
}
```

**Strategy 4: Hybrid Approach (Database + Redis)**

Combine database for permanent record with Redis for fast duplicate detection:

```go
type OrderHandler struct {
    db    *sql.DB
    redis *redis.Client
}

func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    // Fast path: Check Redis cache first
    cacheKey := fmt.Sprintf("msg:%s", msg.MessageID)
    exists, _ := h.redis.Exists(ctx, cacheKey).Result()
    if exists > 0 {
        return nil // Already processed
    }

    // Slow path: Insert with unique constraint
    var event OrderCreatedEvent
    json.Unmarshal(msg.Body, &event)

    tx, err := h.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // Insert order (message_id has UNIQUE constraint)
    _, err = tx.ExecContext(ctx, `
        INSERT INTO orders (id, customer_id, message_id, created_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (message_id) DO NOTHING
    `, event.OrderID, event.CustomerID, msg.MessageID)
    if err != nil {
        return err
    }

    // Process business logic
    if err := h.processOrder(ctx, tx, event); err != nil {
        return err
    }

    if err := tx.Commit(); err != nil {
        return err
    }

    // Cache successful processing (fire-and-forget)
    h.redis.Set(ctx, cacheKey, "1", 10*time.Minute)

    return nil
}
```

**Comparison:**

| Approach/Strategy | Pros | Cons | Use Case |
|----------|------|------|----------|
| **Consumer Repository** | Audit trail, monitoring, crash recovery, no app code needed | Extra DB table, handler still needs idempotency | When you need processing history and metrics |
| **Repository + Business Constraint** | Best of both worlds: audit + deduplication | Two tables to manage | Production systems with compliance requirements |
| **DB Unique Constraint** | Simple, permanent, no external deps | Requires schema change | Critical operations requiring audit trail |
| **Redis Cache** | Fast, simple, no schema change | Temporary (TTL), extra dependency | High-throughput non-critical operations |
| **Business Data Check** | No schema change, uses existing data | May have false negatives | When business entities are naturally unique |
| **Hybrid (Redis + DB)** | Fast + permanent | Complex, multiple dependencies | High-throughput critical operations |

**Best Practices:**

1. **Always implement handler-level idempotency** - o4x guarantees at-least-once delivery, not exactly-once
   - Consumer Repository tracks message state, but doesn't prevent duplicate handler execution
   - Combine Repository with business-level idempotency (unique constraints, Redis, etc.)

2. **Choose approach based on requirements:**
   - **Use Consumer Repository when:**
     - You need audit trail of all message processing attempts
     - Monitoring FAILED/DEAD messages is important
     - Compliance requires processing history
     - You want built-in crash recovery (`ReviveStuckConsuming`)
   - **Skip Repository when:**
     - Simple use case with no audit requirements
     - Handler-level idempotency + OnMessageDead hook is sufficient
     - Minimizing DB tables is a priority

3. **Choose handler idempotency strategy:**
   - Financial transactions → DB Unique Constraint (+ Repository for audit)
   - Notifications/emails → Redis Cache (+ Repository optional)
   - Analytics events → Business Data Check
   - Compliance-critical → Repository + DB Unique Constraint

4. **Set appropriate TTLs:**
   - Redis: 10-30 minutes (longer than SQS visibility timeout)
   - Consumer messages cleanup: `DeleteOlderThan(CONSUMED, 7 days)`, `DeleteOlderThan(DEAD, 30 days)`

5. **Handle partial failures gracefully:**
   - Use database transactions for multi-step operations
   - Implement compensating transactions if needed
   - With Repository: DB state may show CONSUMED even if handler partially failed (handler should be atomic)

6. **Monitor duplicate rates:**
   - Track `rowsAffected == 0` or cache hits in handlers
   - With Repository: Query `consumer_messages` for receive_count > 1
   - Alert if duplicate rate is unusually high (may indicate retry storms or configuration issues)
