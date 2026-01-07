# GORM Adapter

GORM adapter provides PostgreSQL implementation for o4x using [GORM v2](https://gorm.io/).

## Installation

```bash
go get github.com/hacomono-lib/o4x/contrib/gorm
```

## Features

- **ORM Integration** - Seamless integration with GORM models and queries
- **Transaction Support** - Works with GORM transactions via `WithTx()`
- **Batch Operations** - Full support for `BatchDispatcher` with raw SQL CTEs
- **Familiar API** - Uses GORM idioms and conventions

## Basic Usage

### Create Repository

```go
import (
    "gorm.io/driver/postgres"
    "gorm.io/gorm"
    gormrepo "github.com/hacomono-lib/o4x/contrib/gorm"
)

// Open GORM connection
dsn := "host=localhost user=postgres password=postgres dbname=mydb port=5432 sslmode=disable"
db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
if err != nil {
    log.Fatal(err)
}

// Create repository
repo := gormrepo.NewOutboxRepository(db)
```

### Configuration Options

```go
// Outbox repository with custom options
repo := gormrepo.NewOutboxRepository(db,
    gormrepo.WithOutboxTableName("my_outbox"),
    gormrepo.WithStuckPublishingThreshold(10*time.Minute), // Default: 5 minutes
)

// Inbox repository with custom options
inboxRepo := gormrepo.NewInboxRepository(db,
    gormrepo.WithInboxTableName("my_inbox"),
    gormrepo.WithStuckInboxThreshold(30*time.Minute), // Default: 5 minutes
)
```

**Available Options:**
- `WithOutboxTableName(name)` - Custom outbox table name (default: `"outbox"`)
- `WithInboxTableName(name)` - Custom inbox table name (default: `"consumer_inbox"`)
- `WithStuckPublishingThreshold(duration)` - Threshold for detecting stuck messages in PUBLISHING state (default: `5*time.Minute`)
- `WithStuckInboxThreshold(duration)` - Threshold for detecting stuck messages in PROCESSING state (default: `5*time.Minute`)

## Transactional Outbox Pattern

The key to the outbox pattern is inserting messages within the same transaction as your business logic.

### Example: Order Creation

```go
import (
    "gorm.io/gorm"
    "github.com/hacomono-lib/o4x/core"
    gormrepo "github.com/hacomono-lib/o4x/contrib/gorm"
)

type Order struct {
    ID     string
    UserID string
    Amount int64
}

func CreateOrder(ctx context.Context, db *gorm.DB, order Order) error {
    // Start transaction
    tx := db.Begin()
    defer tx.Rollback()

    // 1. Create order (business logic)
    if err := tx.Create(&order).Error; err != nil {
        return err
    }

    // 2. Insert outbox message in same transaction
    repo := gormrepo.NewOutboxRepository(db)
    txRepo := repo.WithTx(tx) // Use transaction

    payload, _ := json.Marshal(order)
    _, err := txRepo.Insert(ctx, core.OutboxInsertParams{
        EventType:      "order.created",
        Payload:        payload,
        IdempotencyKey: fmt.Sprintf("order-%s", order.ID),
        MaxAttempts:     10,
    })
    if err != nil {
        return err
    }

    // 3. Commit both together (atomic!)
    return tx.Commit().Error
}
```

**Important:** Both the order and the outbox message are committed in a single transaction. If either fails, both are rolled back.

## Repository Methods

### Outbox Operations

```go
// Insert message
repo.Insert(ctx, core.OutboxInsertParams{
    EventType:      "user.created",
    Payload:        json.RawMessage(`{"user_id": "123"}`),
    IdempotencyKey: "user-123-created",
    MaxAttempts:     10,
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
msg, err := repo.GetByIdempotencyKey(ctx, eventType, key)

// Cleanup old messages
count, err := repo.DeleteOlderThan(ctx, []core.OutboxStatus{core.OutboxStatusPublished}, 7*24*time.Hour)
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
import gormrepo "github.com/hacomono-lib/o4x/contrib/gorm"

// Create consumer repository
consumerRepo := gormrepo.NewConsumerRepository(db)

// Use with consumer service
svc := consumer.NewService(sqsClient, handler, config)
```

### Using InboxRepository for Idempotency

```go
// InboxRepository provides database-level idempotency checking
inboxRepo := gormrepo.NewInboxRepository(db,
    gormrepo.WithInboxTableName("my_inbox"),
)

// Use in handler
type OrderHandler struct {
    db    *gorm.DB
    inbox core.InboxRepository
}

func (h *OrderHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    tx := h.db.Begin()
    defer tx.Rollback()

    // Check idempotency
    // CRITICAL: Use msg.EventID (Outbox ID), NOT msg.MessageID (changes on redelivery)
    inboxTx := h.inbox.WithTx(tx)
    processed, err := inboxTx.IsProcessed(ctx, "OrderHandler", msg.EventID)
    if err != nil {
        return err
    }
    if processed {
        return nil // Already completed, skip
    }

    // Process message...

    // Mark as completed
    if err := inboxTx.Complete(ctx, "OrderHandler", msg.EventID); err != nil {
        return err
    }

    return tx.Commit().Error
}
```

## GORM Configuration

### Connection Pool

```go
import "gorm.io/driver/postgres"

db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

// Get underlying sql.DB to configure pool
sqlDB, err := db.DB()

// Connection pool settings
sqlDB.SetMaxIdleConns(10)
sqlDB.SetMaxOpenConns(100)
sqlDB.SetConnMaxLifetime(time.Hour)
```

### Logging

```go
import (
    "gorm.io/gorm"
    "gorm.io/gorm/logger"
)

db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
    Logger: logger.Default.LogMode(logger.Info),
})
```

### Performance Mode

```go
db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
    PrepareStmt:            true, // Prepare statements for reuse
    SkipDefaultTransaction: true, // Disable auto-transactions
})
```

## Database Indexes

Create these indexes for optimal performance:

```sql
-- Outbox table
CREATE INDEX idx_outbox_status_created ON outbox(status, created_at)
    WHERE status = 'ENQUEUED';

CREATE INDEX idx_outbox_idempotency ON outbox(event_type, idempotency_key);

-- Consumer inbox table (optional, for idempotency checking)
-- Primary key (consumer_name, message_id) is automatically indexed
```

### Custom Logger

```go
import (
    "gorm.io/gorm/logger"
    "log"
)

newLogger := logger.New(
    log.New(os.Stdout, "\r\n", log.LstdFlags),
    logger.Config{
        SlowThreshold:             200 * time.Millisecond,
        LogLevel:                  logger.Warn,
        IgnoreRecordNotFoundError: true,
        Colorful:                  true,
    },
)

db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
    Logger: newLogger,
})
```

## Best Practices

1. **Always use transactions** - Use `WithTx()` to ensure atomicity with your business logic
2. **Set appropriate MaxAttempts** - Based on your use case (typically 5-10)
3. **Use meaningful idempotency keys** - Make them unique per event (e.g., `order-{id}-created`)
4. **Configure connection pool** - Based on your dispatcher worker count and load
5. **Create indexes** - Especially on `(status, created_at)` for fast polling
6. **Monitor stuck messages** - Call `ReviveStuckPublishing()` at startup
7. **Clean up old messages** - Periodically delete old PUBLISHED/DEAD messages
8. **Enable PrepareStmt** - For better performance with repeated queries

## Common Patterns

### Integration with GORM Models

```go
type User struct {
    gorm.Model
    Name  string
    Email string
}

func CreateUserWithEvent(ctx context.Context, db *gorm.DB, user *User) error {
    tx := db.Begin()
    defer tx.Rollback()

    // Create user
    if err := tx.Create(user).Error; err != nil {
        return err
    }

    // Publish event
    repo := gormrepo.NewOutboxRepository(db).WithTx(tx)
    payload, _ := json.Marshal(user)

    _, err := repo.Insert(ctx, core.OutboxInsertParams{
        EventType:      "user.created",
        Payload:        payload,
        IdempotencyKey: fmt.Sprintf("user-%d-created", user.ID),
        MaxAttempts:     10,
    })
    if err != nil {
        return err
    }

    return tx.Commit().Error
}
```

### Hooks for Automatic Event Publishing

```go
func (u *User) AfterCreate(tx *gorm.DB) error {
    ctx := context.Background()
    repo := gormrepo.NewOutboxRepository(tx).WithTx(tx)
    
    payload, _ := json.Marshal(u)
    _, err := repo.Insert(ctx, core.OutboxInsertParams{
        EventType:      "user.created",
        Payload:        payload,
        IdempotencyKey: fmt.Sprintf("user-%d-created", u.ID),
        MaxAttempts:     10,
    })
    return err
}
```

### Batch Insert with Events

```go
func CreateOrdersWithEvents(ctx context.Context, db *gorm.DB, orders []Order) error {
    tx := db.Begin()
    defer tx.Rollback()

    // Batch insert orders
    if err := tx.CreateInBatches(orders, 100).Error; err != nil {
        return err
    }

    // Insert events for each order
    repo := gormrepo.NewOutboxRepository(db).WithTx(tx)
    for _, order := range orders {
        payload, _ := json.Marshal(order)
        _, err := repo.Insert(ctx, core.OutboxInsertParams{
            EventType:      "order.created",
            Payload:        payload,
            IdempotencyKey: fmt.Sprintf("order-%s", order.ID),
            MaxAttempts:     10,
        })
        if err != nil {
            return err
        }
    }

    return tx.Commit().Error
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

```go
sqlDB, _ := db.DB()
sqlDB.SetMaxOpenConns(100) // Increase from default
```

### PreparedStatement cache issues

If you see "prepared statement already exists" errors:

```go
db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
    PrepareStmt: false, // Disable if causing issues
})
```

## GORM vs pgx

### When to use GORM

- You're already using GORM in your project
- You prefer ORM-style queries
- You want GORM hooks and callbacks
- You have complex model relationships

### When to use pgx

- You need maximum performance (pgx is 2-3x faster)
- You prefer raw SQL queries
- You're building a new project
- You want lower memory overhead

## Migration Example

If you want to switch from GORM to pgx:

```go
// Before (GORM)
import gormrepo "github.com/hacomono-lib/o4x/contrib/gorm"
repo := gormrepo.NewOutboxRepository(db)

// After (pgx)
import pgxrepo "github.com/hacomono-lib/o4x/contrib/pgx"
pool, _ := pgxpool.New(ctx, connString)
repo := pgxrepo.NewOutboxRepository(pool)
```

The API is identical, so only the initialization code changes.

