# pgx Adapter

pgx adapter provides PostgreSQL implementation for o4x using [pgx/v5](https://github.com/jackc/pgx).

## Installation

```bash
go get github.com/hacomono-lib/o4x/contrib/pgx
```

## Features

- **Connection Pooling** - Uses `pgxpool.Pool` for efficient connection management
- **Transaction Support** - Seamless integration with pgx transactions via `WithTx()`
- **Batch Operations** - Full support for `BatchDispatcher` with atomic CTE queries
- **Performance** - Optimized for PostgreSQL with native pgx driver

## Basic Usage

### Create Repository

```go
import (
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/hacomono-lib/o4x/contrib/pgx"
)

// Create connection pool
pool, err := pgxpool.New(ctx, databaseURL)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

// Create repository
repo := pgx.NewOutboxRepository(pool)
```

### Custom Table Names

```go
// Use custom table names
repo := pgx.NewOutboxRepository(pool,
    pgx.WithOutboxTableName("my_outbox"),
    pgx.WithConsumerMessagesTableName("my_consumer_messages"),
)
```

## Transactional Outbox Pattern

The key to the outbox pattern is inserting messages within the same transaction as your business logic.

### Example: Order Creation

```go
import (
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/hacomono-lib/o4x/core"
    "github.com/hacomono-lib/o4x/contrib/pgx"
)

func CreateOrder(ctx context.Context, pool *pgxpool.Pool, order Order) error {
    // Start transaction
    tx, err := pool.Begin(ctx)
    if err != nil {
        return err
    }
    defer tx.Rollback(ctx)

    // 1. Insert order (business logic)
    _, err = tx.Exec(ctx,
        "INSERT INTO orders (id, user_id, amount) VALUES ($1, $2, $3)",
        order.ID, order.UserID, order.Amount,
    )
    if err != nil {
        return err
    }

    // 2. Insert outbox message in same transaction
    repo := pgx.NewOutboxRepository(pool)
    txRepo := repo.WithTx(tx) // Use transaction

    payload, _ := json.Marshal(order)
    _, err = txRepo.Insert(ctx, core.OutboxInsertParams{
        Topic:          "order.created",
        Payload:        payload,
        IdempotencyKey: fmt.Sprintf("order-%s", order.ID),
        MaxRetries:     10,
    })
    if err != nil {
        return err
    }

    // 3. Commit both together (atomic!)
    return tx.Commit(ctx)
}
```

**Important:** Both the order and the outbox message are committed in a single transaction. If either fails, both are rolled back.

## Repository Methods

### Outbox Operations

```go
// Insert message
repo.Insert(ctx, core.OutboxInsertParams{
    Topic:          "user.created",
    Payload:        json.RawMessage(`{"user_id": "123"}`),
    IdempotencyKey: "user-123-created",
    MaxRetries:     10,
})

// Insert with JSON marshaling
repo.InsertOutboxJSON(ctx, "user.created", userEvent, "user-123-created", 10)

// Fetch and lock message (used by dispatcher)
msg, err := repo.FetchAndLock(ctx)

// Update status
repo.UpdateToPublishing(ctx, id)
repo.UpdateToPublished(ctx, id)
repo.UpdateToFailed(ctx, id, "error message")
repo.UpdateToDead(ctx, id, "max retries exceeded")

// Requeue failed messages
repo.RequeueFailed(ctx)

// Revive stuck messages (call at startup)
count, err := repo.ReviveStuckPublishing(ctx)

// Get message
msg, err := repo.GetByID(ctx, id)
msg, err := repo.GetByIdempotencyKey(ctx, topic, key)

// Cleanup old messages
count, err := repo.DeleteOlderThan(ctx, core.OutboxStatusPublished, 7*24*time.Hour)
```

### Batch Operations

```go
// Atomically fetch and mark as publishing
messages, err := repo.FetchLockAndMarkPublishing(ctx, 10)

// Batch update to published
ids := []string{"id1", "id2", "id3"}
err := repo.UpdateBatchToPublished(ctx, ids)
```

## Consumer Repository

```go
import "github.com/hacomono-lib/o4x/contrib/pgx"

// Create consumer repository
consumerRepo := pgx.NewConsumerRepository(pool)

// Use with consumer service
svc := consumer.NewService(sqsClient, consumerRepo, handler, config)
```

### Custom Table Names

```go
consumerRepo := pgx.NewConsumerRepository(pool,
    pgx.WithConsumerMessagesTableName("my_consumer_messages"),
)
```

## Performance Optimization

### Connection Pool Configuration

```go
poolConfig, err := pgxpool.ParseConfig(databaseURL)
if err != nil {
    log.Fatal(err)
}

// Tune pool settings
poolConfig.MaxConns = 20
poolConfig.MinConns = 5
poolConfig.MaxConnLifetime = time.Hour
poolConfig.MaxConnIdleTime = 30 * time.Minute
poolConfig.HealthCheckPeriod = time.Minute

pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
```

### Database Indexes

Create these indexes for optimal performance:

```sql
-- Outbox table
CREATE INDEX idx_outbox_status_created ON outbox(status, created_at) 
    WHERE status = 'ENQUEUED';

CREATE INDEX idx_outbox_idempotency ON outbox(topic, idempotency_key);

-- Consumer messages table
CREATE INDEX idx_consumer_status_received ON consumer_messages(status, received_at) 
    WHERE status = 'CONSUMING';
```

## Best Practices

1. **Always use transactions** - Use `WithTx()` to ensure atomicity with your business logic
2. **Set appropriate MaxRetries** - Based on your use case (typically 5-10)
3. **Use meaningful idempotency keys** - Make them unique per event (e.g., `order-{id}-created`)
4. **Tune connection pool** - Based on your dispatcher worker count and load
5. **Create indexes** - Especially on `(status, created_at)` for fast polling
6. **Monitor stuck messages** - Call `ReviveStuckPublishing()` at startup
7. **Clean up old messages** - Periodically delete old PUBLISHED/DEAD messages

## Common Patterns

### Idempotent Message Publishing

```go
func PublishUserEvent(ctx context.Context, tx pgx.Tx, userID string, eventType string) error {
    repo := pgx.NewOutboxRepository(pool).WithTx(tx)
    
    idempotencyKey := fmt.Sprintf("user-%s-%s-%d", userID, eventType, time.Now().Unix())
    
    _, err := repo.Insert(ctx, core.OutboxInsertParams{
        Topic:          fmt.Sprintf("user.%s", eventType),
        Payload:        json.RawMessage(`{"user_id": "` + userID + `"}`),
        IdempotencyKey: idempotencyKey,
        MaxRetries:     10,
    })
    return err
}
```

### Retry Logic with Custom Error Handling

```go
// Check if message should be marked as DEAD
msg, _ := repo.GetByID(ctx, id)
if msg.RetryCount >= msg.MaxRetries {
    repo.UpdateToDead(ctx, id, "max retries exceeded")
} else {
    repo.UpdateToFailed(ctx, id, "temporary error")
}
```

## Troubleshooting

### Messages stuck in PUBLISHING

This happens when the dispatcher crashes. Call `ReviveStuckPublishing()` at startup:

```go
count, err := repo.ReviveStuckPublishing(ctx)
if err != nil {
    log.Fatal(err)
}
log.Printf("Revived %d stuck messages", count)
```

### Slow polling

Add index on outbox table:

```sql
CREATE INDEX idx_outbox_enqueued ON outbox(status, created_at) 
    WHERE status = 'ENQUEUED';
```

### Connection pool exhaustion

Increase pool size or reduce worker count:

```go
poolConfig.MaxConns = 50 // Increase from default 20
```

## Migration from database/sql

If you're using `database/sql`, consider migrating to pgx for better performance:

```go
// Before (database/sql)
db, _ := sql.Open("postgres", connString)

// After (pgx)
pool, _ := pgxpool.New(ctx, connString)
```

pgx is 2-3x faster than database/sql for PostgreSQL workloads.

