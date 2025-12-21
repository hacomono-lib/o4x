# o4x Performance Tuning Guide

This guide helps you optimize o4x for different workloads and throughput requirements.

## ⚠️ Important Notice

This document provides **general tuning guidelines** based on configuration parameters and their trade-offs.

**For benchmark results**, see [docs/benchmarks/](./benchmarks/) directory. Note that benchmark results are environment-specific and should not be extrapolated to production without validation.

## Table of Contents

- [Configuration Parameters](#configuration-parameters)
- [Database Connection Pool](#database-connection-pool)
- [Dispatcher Tuning](#dispatcher-tuning)
- [Batch Dispatcher Tuning](#batch-dispatcher-tuning)
- [Consumer Tuning](#consumer-tuning)
- [Consumer Scaling](#consumer-scaling)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Common Scenarios](#common-scenarios)
- [Example App Benchmarks](#example-app-benchmarks)

## Configuration Parameters

### Core Concepts

- **WorkerCount**: Number of concurrent workers processing messages
- **PollInterval**: Base interval for polling the database
- **BatchSize**: Number of messages to fetch/publish in a single batch
- **RequeueInterval**: How often to move FAILED messages back to ENQUEUED

### Trade-offs

| Parameter | Higher Value | Lower Value |
|-----------|--------------|-------------|
| WorkerCount | Higher throughput, more DB connections | Lower resource usage |
| PollInterval | Lower CPU usage | Lower latency |
| BatchSize | Higher throughput, fewer DB round-trips | Lower memory usage |
| RequeueInterval | Less DB load | Faster retry |

## Database Connection Pool

### Recommended Settings

```go
import "github.com/jackc/pgx/v5/pgxpool"

config, _ := pgxpool.ParseConfig(databaseURL)

// Formula: WorkerCount (Dispatcher) + WorkerCount (Consumer) + Safety Margin
// Example: 10 dispatcher workers + 5 consumer workers + 5 margin = 20
config.MaxConns = 20

// Keep some connections always ready
config.MinConns = 5

// Recycle connections periodically to prevent stale connections
config.MaxConnLifetime = time.Hour
config.MaxConnIdleTime = 30 * time.Minute

// Connection timeout
config.ConnConfig.ConnectTimeout = 5 * time.Second

pool, err := pgxpool.NewWithConfig(context.Background(), config)
```

### Connection Pool Sizing

**Formula**: `MaxConns = (Dispatcher Workers × Dispatcher Count) + (Consumer Workers × Consumer Count) + Safety Margin`

**Example configurations**:

| Workload | Dispatchers | Consumers | Total Workers | Recommended MaxConns |
|----------|-------------|-----------|---------------|---------------------|
| Small | 1×3 workers | 1×2 workers | 5 | 10 |
| Medium | 2×5 workers | 2×5 workers | 20 | 30 |
| Large | 4×10 workers | 4×10 workers | 80 | 100 |

**Warning**: Setting `MaxConns` too low causes workers to wait for available connections, reducing throughput.

## Dispatcher Tuning

### Low Latency (Real-time Messages)

**Goal**: Minimize time from `Insert()` to delivery

```go
config := core.DefaultDispatcherConfig()
config.WorkerCount = 5           // Moderate parallelism
config.PollInterval = 50 * time.Millisecond   // Fast polling
config.MaxPollInterval = 500 * time.Millisecond
```

**Expected performance**: < 100ms latency, ~100 messages/second per worker

### High Throughput (Batch Processing)

**Goal**: Maximize messages per second

```go
config := core.DefaultBatchDispatcherConfig()
config.WorkerCount = 20          // High parallelism
config.BatchSize = 10            // SQS max batch size
config.PollInterval = 100 * time.Millisecond
config.RequeueInterval = 10 * time.Second
```

**Expected performance**: ~2,000 messages/second (20 workers × 10 batch size × 10 batches/sec)

### Low Resource (Background Jobs)

**Goal**: Minimize CPU and database load

```go
config := core.DefaultDispatcherConfig()
config.WorkerCount = 1           // Single worker
config.PollInterval = 1 * time.Second
config.MaxPollInterval = 30 * time.Second
```

**Expected performance**: ~10 messages/second, very low overhead

## Batch Dispatcher Tuning

### Optimal Batch Size

**SQS**: Always use `BatchSize = 10` (SQS limit)

```go
config := core.DefaultBatchDispatcherConfig()
config.BatchSize = 10  // SQS maximum
```

**Kafka**: Depends on message size and latency requirements

```go
// For small messages (< 1KB)
config.BatchSize = 100

// For large messages (> 10KB)
config.BatchSize = 10
```

### RequeueInterval Tuning

**Fast Retry (High Priority Messages)**:
```go
config.RequeueInterval = 1 * time.Second
config.RequeueBackoffBase = 1 * time.Second
config.RequeueBackoffMax = 5 * time.Minute
```

**Balanced (Normal Workload)**:
```go
config.RequeueInterval = 10 * time.Second
config.RequeueBackoffBase = 1 * time.Second
config.RequeueBackoffMax = 1 * time.Hour
```

**Slow Retry (Background Jobs)**:
```go
config.RequeueInterval = 1 * time.Minute
config.RequeueBackoffBase = 10 * time.Second
config.RequeueBackoffMax = 24 * time.Hour
```

## Consumer Tuning

### SQS Consumer Configuration

```go
config := consumer.DefaultServiceConfig(queueURL)

// Long polling for efficiency (reduces API calls)
config.WaitTimeSeconds = 20  // Max is 20 seconds

// Visibility timeout should be > max processing time
config.VisibilityTimeout = 60  // 1 minute for slow handlers

// Batch size for receiving messages
config.MaxNumberOfMessages = 10  // SQS max is 10

// Concurrency
config.WorkerCount = 5  // Parallel message processing
```

### Worker Count Guidelines

**CPU-bound handlers** (e.g., data transformation):
- `WorkerCount = Number of CPU cores`

**I/O-bound handlers** (e.g., API calls, database queries):
- `WorkerCount = 2-4x Number of CPU cores`

**Mixed workload**:
- Start with `WorkerCount = 2x CPU cores`, monitor, and adjust

### Visibility Timeout

**Formula**: `VisibilityTimeout = (Average Handler Duration × 3) + Safety Margin`

**Example**:
- Average handler duration: 5 seconds
- Formula: `5s × 3 + 5s = 20s`
- Set `VisibilityTimeout = 20`

**Too short**: Message redelivered while still processing (duplicate work)
**Too long**: Slow retry on failures

## Monitoring and Metrics

### Key Metrics to Track

**Dispatcher Metrics (via Hooks)**:
```go
hooks := &core.Hooks{
    OnPublishSuccess: func(ctx context.Context, msg *core.Outbox, duration time.Duration) {
        metrics.PublishDuration.Observe(duration.Seconds())
        metrics.PublishedCounter.Inc()
    },
    OnPublishFailure: func(ctx context.Context, msg *core.Outbox, err error, duration time.Duration, retryable bool) {
        metrics.FailureCounter.Inc()
        if !retryable {
            metrics.DeadCounter.Inc()
        }
    },
    OnBatchPublishComplete: func(ctx context.Context, successCount, failureCount int, duration time.Duration) {
        metrics.BatchSize.Observe(float64(successCount + failureCount))
        metrics.BatchDuration.Observe(duration.Seconds())
    },
    OnPartialBatchSuccess: func(ctx context.Context, expectedCount, actualCount int, duration time.Duration) {
        metrics.PartialBatchCounter.Inc()
        metrics.PartialBatchMissing.Observe(float64(expectedCount - actualCount))
    },
}
```

**Consumer Metrics**:
```go
hooks := &consumer.Hooks{
    OnConsumeSuccess: func(ctx context.Context, msg *consumer.SQSMessage, duration time.Duration) {
        metrics.ConsumeSuccessCounter.Inc()
        metrics.ConsumeDuration.Observe(duration.Seconds())
    },
    OnConsumeFailure: func(ctx context.Context, msg *consumer.SQSMessage, err error, duration time.Duration, retryable bool) {
        metrics.ConsumeFailureCounter.Inc()
    },
    OnMessageDead: func(ctx context.Context, msg *consumer.SQSMessage, err error) {
        metrics.DeadMessageCounter.Inc()
        // Alert ops team!
    },
}
```

### Health Checks

```go
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    status := dispatcher.HealthStatus()

    if !status.IsHealthy() {
        w.WriteHeader(http.StatusServiceUnavailable)
        json.NewEncoder(w).Encode(map[string]string{
            "status": "unhealthy",
            "reason": "dispatcher not running or pending shutdown",
        })
        return
    }

    // Check for stale processing (no messages processed in 5 minutes)
    if status.IsStale(5 * time.Minute) {
        w.WriteHeader(http.StatusServiceUnavailable)
        json.NewEncoder(w).Encode(map[string]string{
            "status": "unhealthy",
            "reason": "no messages processed recently",
        })
        return
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]interface{}{
        "status": "healthy",
        "running": status.Running,
        "worker_count": status.WorkerCount,
        "last_processed_at": status.LastProcessedAt,
    })
})
```

### Database Monitoring

```sql
-- Check outbox table size
SELECT status, COUNT(*) as count,
       pg_size_pretty(pg_total_relation_size('outbox')) as table_size
FROM outbox
GROUP BY status;

-- Check stuck messages
SELECT id, event_type, status, attempt_count, created_at, updated_at, error_message
FROM outbox
WHERE status = 'PUBLISHING' AND updated_at < NOW() - INTERVAL '5 minutes'
LIMIT 10;

-- Check failed messages
SELECT id, event_type, attempt_count, max_attempts, next_retry_at, error_message
FROM outbox
WHERE status = 'FAILED'
ORDER BY created_at DESC
LIMIT 10;
```

## Common Scenarios

### Scenario 1: E-commerce Order Events

**Requirements**: Low latency (<1s), moderate throughput (~1000 orders/hour)

```go
// Dispatcher
config := core.DefaultBatchDispatcherConfig()
config.WorkerCount = 3
config.BatchSize = 10
config.PollInterval = 100 * time.Millisecond
config.RequeueInterval = 5 * time.Second

// Database pool
poolConfig.MaxConns = 10  // 3 workers + margin

// Consumer
consumerConfig.WorkerCount = 2
consumerConfig.VisibilityTimeout = 30
```

**Expected performance**: 500ms p99 latency, 1500 orders/hour capacity

### Scenario 2: Analytics Event Stream

**Requirements**: High throughput (>10K events/second), can tolerate latency (~10s)

```go
// Batch Dispatcher
config := core.DefaultBatchDispatcherConfig()
config.WorkerCount = 50
config.BatchSize = 10
config.PollInterval = 50 * time.Millisecond
config.RequeueInterval = 30 * time.Second

// Database pool
poolConfig.MaxConns = 60  // 50 workers + margin
poolConfig.MinConns = 20

// Consumer
consumerConfig.WorkerCount = 20
consumerConfig.MaxNumberOfMessages = 10
```

**Expected performance**: 15,000 events/second throughput, 5-10s latency

### Scenario 3: Scheduled Notifications

**Requirements**: Low resource usage, background processing

```go
// Standard Dispatcher (not batch)
config := core.DefaultDispatcherConfig()
config.WorkerCount = 2
config.PollInterval = 5 * time.Second
config.MaxPollInterval = 1 * time.Minute

// Database pool
poolConfig.MaxConns = 5
poolConfig.MinConns = 2

// Consumer
consumerConfig.WorkerCount = 2
consumerConfig.VisibilityTimeout = 120
```

**Expected performance**: 20-50 messages/second, minimal resource usage

## Consumer Scaling

### Horizontal Scaling

Multiple consumer instances can process messages from the same queue concurrently.

**Benefits:**
- Increased throughput capacity
- Fault tolerance (if one instance fails, others continue)
- Shared idempotency tracking via `consumer_name`

**Constraints:**
- **Standard Queue**: Supports horizontal scaling (no ordering guarantees)
- **FIFO Queue**: Order processing requires `MessageConcurrency=1` per instance
- All instances must use the same `consumer_name` for proper idempotency tracking

### Scaling Strategies

**Standard Queue (No Ordering Required)**:
```go
// Configuration per instance
config := consumer.ServiceConfig{
    QueueURL:           queueURL,
    WorkerCount:        5,
    MessageConcurrency: 10,  // Parallel processing
}

// Deploy multiple instances for increased capacity
// Actual scaling characteristics depend on your specific environment
```

**FIFO Queue (Ordering Required)**:
```go
// Configuration per instance
config := consumer.ServiceConfig{
    QueueURL:           queueURL,
    WorkerCount:        5,
    MessageConcurrency: 1,  // REQUIRED: Sequential processing
}

// Vertical scaling often preferred (increase resources per instance)
// Horizontal scaling effectiveness depends on message distribution patterns
```

### Idempotency with Multiple Instances

When scaling consumers horizontally, all instances must share the same `consumer_name` to ensure exactly-once processing:

```go
// CORRECT: All instances use the same consumer_name
// CRITICAL: Use msg.EventID (Outbox ID), NOT msg.MessageID (changes on redelivery)
inboxRepo := pgx.NewInboxRepository(pool)
processed, err := inboxRepo.IsProcessed(ctx, "notification-service", msg.EventID)
```

**How it works:**
- `consumer_inbox` table uses composite primary key: `(consumer_name, event_id)`
- Same `event_id` can only be processed once per `consumer_name`
- Different instances of the same service share the `consumer_name`
- `event_id` is the Outbox event ID (stable logical identity), NOT SQS MessageID (changes on redelivery)

### Scaling Considerations

**Factors Affecting Scaling Effectiveness:**
- Database connection pool size
- Message broker throughput and characteristics
- Handler processing time
- Network latency and bandwidth
- Queue type (Standard vs FIFO)
- Message distribution patterns

**Recommendation**: Always validate scaling behavior in production-like environments with realistic workloads.

→ **For example benchmark results**, see [Benchmark Results](./benchmarks/) (note: environment-specific)

## Troubleshooting

### High CPU Usage

**Symptom**: CPU usage >80% constantly

**Possible causes**:
1. PollInterval too low → Increase to 200ms-500ms
2. Too many workers → Reduce WorkerCount
3. MaxPollInterval not set → Set to 5-10s

**Solution**:
```go
config.PollInterval = 500 * time.Millisecond
config.MaxPollInterval = 10 * time.Second
config.WorkerCount = min(WorkerCount, NumCPU * 2)
```

### High Database Connections

**Symptom**: "too many connections" errors

**Possible causes**:
1. MaxConns too high
2. Multiple dispatcher instances without load balancer
3. Connection leaks

**Solution**:
```go
// Reduce MaxConns
poolConfig.MaxConns = WorkerCount + 5

// Enable connection logging
poolConfig.BeforeConnect = func(ctx context.Context, config *pgx.ConnConfig) error {
    log.Printf("Creating new connection")
    return nil
}
```

### Messages Stuck in FAILED

**Symptom**: Messages not being retried

**Possible causes**:
1. RequeueInterval = 0 (automatic retry disabled)
2. next_retry_at far in the future
3. attempt_count >= max_attempts

**Solution**:
```sql
-- Check next_retry_at
SELECT id, attempt_count, max_attempts, next_retry_at, NOW() as current_time
FROM outbox
WHERE status = 'FAILED'
LIMIT 10;

-- Reset retry count if needed
UPDATE outbox
SET attempt_count = 0, next_retry_at = NOW()
WHERE status = 'FAILED' AND id = 'problematic-id';
```

```go
// Ensure RequeueInterval is set
config.RequeueInterval = 10 * time.Second  // NOT 0!
```

### Low Throughput

**Symptom**: Processing <100 messages/second with BatchDispatcher

**Possible causes**:
1. WorkerCount too low
2. BatchSize too small
3. Database slow queries

**Solution**:
1. Increase WorkerCount to 10-20
2. Use BatchSize = 10 for SQS
3. Check database indexes:
```sql
-- Verify indexes exist
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE tablename = 'outbox';
```

Expected indexes:
- `idx_outbox_status_created_at` (for polling)
- `idx_outbox_status_next_retry_at` (for RequeueFailed)

## Example App Benchmarks

The `examples/app` directory contains a complete working example with benchmark scripts and performance testing utilities.

### Running Benchmarks

```bash
cd examples/app

# Start all services
./scripts/start.sh

# Run benchmark with default settings (200 requests, concurrency 20)
./scripts/benchmark.sh

# Scale consumers and re-test (Standard queues only)
docker-compose up -d --scale consumer-notification=5
./scripts/benchmark.sh

# Return to single instance
docker-compose down
./scripts/start.sh
```

### Important Notes

- **Environment-Specific**: Benchmarks run in LocalStack + docker-compose environment
- **FIFO Queue Caution**: Do NOT scale the order consumer (FIFO queue) with `--scale`
- For FIFO queues, vertical scaling (increase resources) is often preferred
- Standard queues (notification, user) support horizontal scaling

→ **For benchmark results and detailed analysis**, see [docs/benchmarks/localstack-benchmark-results.md](./benchmarks/localstack-benchmark-results.md)

→ **For operational procedures**, see [examples/app/scripts/README.md](../examples/app/scripts/README.md)
