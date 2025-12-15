# Idempotency Guide

This document provides detailed guidance on implementing idempotency for o4x consumers.

## Table of Contents

- [Why Idempotency Matters](#why-idempotency-matters)
- [Decision Tree](#decision-tree)
- [Implementation Approaches](#implementation-approaches)
  - [InboxRepository (Recommended)](#inboxrepository-recommended)
  - [IdempotencyKey with Cache/Database](#idempotencykey-with-cachedatabase)
  - [Database Unique Constraint](#database-unique-constraint)
  - [Natural Idempotency](#natural-idempotency)
- [External API Requirements](#external-api-requirements)
- [consumer_name Definition](#consumer_name-definition)
- [Best Practices](#best-practices)

## Why Idempotency Matters

Both o4x and SQS guarantee **at-least-once delivery**. Your consumer may receive the same message multiple times in these scenarios:

- Message processing takes longer than visibility timeout
- Consumer crashes after processing but before ACK
- Network issues during message deletion
- SQS internal retries

## Decision Tree

### 1. Does your handler involve external API calls?

**YES** → Does the API support idempotency keys?
- **YES** → Use **InboxRepository** + pass `msg.MessageID` as idempotency key ✅
- **NO** → **Don't use async messaging** ⛔ (handle synchronously instead)

**NO** → Continue to step 2

### 2. Is your business logic fully idempotent?

Examples: `ON CONFLICT DO NOTHING`, `UPDATE ... SET status = 'completed' WHERE id = ?`

**YES** → Use **InboxRepository with auto-commit** OR **Application-Level** ✅

**NO** → Use **InboxRepository with transaction** ✅ (safest)

## Implementation Approaches

### Comparison Table

| Approach | Idempotency | Audit Trail | Complexity | Recommended For |
|----------|-------------|-------------|------------|-----------------|
| **InboxRepository (Transaction)** | ✅ Built-in | ✅ Processing status | Low | **DB operations** + **API calls with idempotency keys** |
| **InboxRepository (Auto-commit)** | ✅ Built-in | ✅ Processing status | Low | **Idempotent business logic** + **API with idempotency keys** |
| **Application-Level** | ✅ Custom logic | ⚠️ Custom | Medium | **Pure DB operations** (when InboxRepository overhead not justified) |

### InboxRepository (Recommended)

#### CRITICAL: TryStart is NOT Exclusive

**`TryStart()` does NOT provide exclusivity or mutual exclusion semantics.**

Multiple consumer workers MAY pass `TryStart()` concurrently for the same message.

This behavior is intentional and correct. Handlers MUST be safe to run multiple times.

**Why This Design:**
- Primary control: SQS visibility timeout prevents concurrent processing
- The inbox table represents **completed messages only**
- In-flight processing is controlled by the message broker (SQS visibility timeout)
- The only definitive point is `Complete()`

```text
TryStart   : optimistic check (may pass concurrently)
Complete   : final commit (single source of truth)
```

**Implementation:**
- `TryStart`: Optimistic check (no locking)
- `Complete`: `INSERT ... ON CONFLICT DO NOTHING` (idempotent)
- Returns `true` if NOT exists (proceed), `false` if exists (skip)
- Both `pgx` and `gorm` implementations use identical logic for consistency

**Note:** The inbox table intentionally records completed messages only.

#### Pattern 1: Transaction Support (Safest)

Use when you need atomicity between idempotency check and business logic:

```go
inboxRepo := pgx.NewInboxRepository(pool)

handler := consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
    tx, _ := db.Begin(ctx)
    defer tx.Rollback(ctx)

    // Check idempotency (within transaction)
    // CRITICAL: TryStart is an optimistic check, NOT an exclusive lock
    inboxTx := inboxRepo.WithTx(tx)
    ok, _ := inboxTx.TryStart(ctx, "order-service", msg.MessageID)
    if !ok {
        return nil // Already processed
    }

    // Process message (same transaction)
    tx.Exec(ctx, "INSERT INTO orders ...")

    // Mark completed
    inboxTx.Complete(ctx, "order-service", msg.MessageID)

    return tx.Commit(ctx)
})
```

**Benefits:**
- Atomic idempotency check with business logic
- Race-safe duplicate detection via `(consumer_name, message_id)` primary key
- Audit trail with processing status and timestamps
- If transaction rolls back, message will be retried

**Use Cases:**
- Database operations that must be atomic with idempotency check
- Complex business logic involving multiple tables
- When you need transactional guarantees

#### Pattern 2: Auto-Commit (Simple)

Use when business logic is already idempotent or external API calls are involved:

```go
inboxRepo := pgx.NewInboxRepository(pool)

handler := consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
    // Check idempotency (auto-commit)
    ok, _ := inboxRepo.TryStart(ctx, "payment-service", msg.MessageID)
    if !ok {
        return nil // Already processed
    }

    // Call external API with idempotency key
    params := &stripe.ChargeParams{
        Amount:   stripe.Int64(event.Amount),
        Currency: stripe.String("usd"),
    }
    params.SetIdempotencyKey(msg.MessageID) // CRITICAL: Use msg.MessageID

    charge, err := client.Charges.New(params)
    if err != nil {
        return err // Will retry
    }

    // Mark completed
    inboxRepo.Complete(ctx, "payment-service", msg.MessageID)
    return nil
})
```

**Benefits:**
- Simpler code (no transaction management)
- Works with external APIs
- Still provides audit trail

**Use Cases:**
- External API calls with idempotency support
- Business logic that is naturally idempotent
- When transaction overhead is not needed

### IdempotencyKey with Cache/Database

Use Redis or another cache for fast idempotency checks:

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

**Use Cases:**
- High-throughput scenarios where database writes would be a bottleneck
- When you need fast idempotency checks
- Temporary deduplication (TTL-based cleanup)

**Trade-offs:**
- ⚠️ Cache failures may cause duplicate processing
- ⚠️ No persistent audit trail
- ⚠️ TTL expiration may cause late duplicates to be processed

### Database Unique Constraint

Leverage database constraints for idempotency:

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

**Use Cases:**
- Simple idempotency requirements
- When you want database-level enforcement
- Integration with existing tables

**Trade-offs:**
- ⚠️ Requires custom cleanup logic
- ⚠️ Less flexible than InboxRepository

### Natural Idempotency

Design your business logic to be naturally idempotent:

```go
// Instead of: balance += amount (NOT idempotent)
// Use: UPDATE accounts SET balance = $1 WHERE id = $2 (idempotent with final state)

// Instead of: INSERT INTO ... (may fail on duplicate)
// Use: INSERT INTO ... ON CONFLICT DO NOTHING (idempotent)

// Update status transitions
tx.Exec(ctx,
    "UPDATE orders SET status = 'completed' WHERE id = $1 AND status = 'pending'",
    orderID,
)
```

**Use Cases:**
- Simple state updates
- When operations can be expressed as final states
- Minimizing overhead

**Trade-offs:**
- ⚠️ Not all business logic can be made naturally idempotent
- ⚠️ No explicit duplicate tracking

## External API Requirements

### Rule: No idempotency support = No async messaging

If your consumer handler calls an external API, that API **MUST** support idempotency keys.

This is **NOT** a recommendation. This is a **REQUIREMENT**.

At-least-once delivery guarantees mean:
- Handlers MAY crash after calling the API but before acknowledgment
- Messages WILL be delivered more than once
- The same API call WILL execute multiple times

Without idempotency keys, async processing **MUST NOT** be used.

### What will happen without idempotency keys:

- ❌ Duplicate payment charges (financial loss)
- ❌ Duplicate email sends (customer complaints)
- ❌ Duplicate state changes (data corruption)
- ❌ Duplicate resource creation (inconsistent state)

### Required API Capabilities:

- ✅ API accepts idempotency key header/parameter
- ✅ API deduplicates requests with same key
- ✅ API returns same response for duplicate requests

### Examples:

| API | Idempotency Support | Async Safe? |
|-----|---------------------|-------------|
| Stripe API | ✅ `Idempotency-Key` header | ✅ Safe |
| Twilio API | ✅ Idempotency headers | ✅ Safe |
| Shopify Admin API | ✅ Idempotency headers | ✅ Safe |
| SendGrid Mail Send API | ❌ No support | ❌ **MUST use sync** |
| Simple SMTP | ❌ No deduplication | ❌ **MUST use sync** |
| Legacy payment gateways | ❌ Often no support | ❌ **MUST use sync** |

### Pattern: Passing message_id as idempotency key

```go
handler := consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
    var event PaymentEvent
    json.Unmarshal(msg.Body, &event)

    // REQUIRED: Pass message_id as idempotency key to external API
    params := &stripe.ChargeParams{
        Amount:   stripe.Int64(event.Amount),
        Currency: stripe.String("usd"),
    }
    params.SetIdempotencyKey(msg.MessageID) // CRITICAL: Use msg.MessageID

    charge, err := client.Charges.New(params)
    if err != nil {
        return err // Will retry
    }

    return nil
})
```

### If the external API does NOT support idempotency:

```go
// WRONG: Do NOT do this with non-idempotent APIs
handler := consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
    // ❌ This will send duplicate emails on retry
    SendEmail(msg.Email, msg.Subject, msg.Body)
    return nil
})

// CORRECT: Handle synchronously in the API layer instead
func CreateOrder(ctx context.Context, order Order) error {
    tx, _ := db.Begin(ctx)
    defer tx.Rollback(ctx)

    // Insert order
    tx.Exec(ctx, "INSERT INTO orders ...")

    // Send email synchronously (before commit)
    // If email fails, entire transaction rolls back
    if err := SendEmail(order.Email, "Order Confirmed", ...); err != nil {
        return err
    }

    return tx.Commit(ctx)
}
```

## consumer_name Definition

The `consumer_name` parameter in `InboxRepository.TryStart()` is a **logical consumer identity** at the service or consumer-group level.

### What consumer_name IS:

- Logical service name (e.g., "order-service", "notification-service")
- Deployment unit identifier (e.g., "payment-processor-v2")
- Consumer group identity shared across all instances

### What consumer_name is NOT:

- ❌ Handler function name
- ❌ Event type name
- ❌ Struct name
- ❌ Per-handler identifier

### Key Rules:

- Use the same `consumer_name` for all instances of the same service
- Do NOT change `consumer_name` casually - it affects idempotency tracking
- Changing `consumer_name` causes messages to be treated as new (duplicate processing)

### Example:

```go
// CORRECT: All instances use same consumer_name
ok, _ := inboxRepo.TryStart(ctx, "order-service", msg.MessageID)

// WRONG: Different consumer_name per handler
ok, _ := inboxRepo.TryStart(ctx, "OrderCreatedHandler", msg.MessageID) // ❌
ok, _ := inboxRepo.TryStart(ctx, msg.EventType, msg.MessageID)         // ❌
```

## Best Practices

1. **Always check IdempotencyKey** before processing
2. **External APIs without idempotency support**: Do NOT use async messaging - handle synchronously instead ⛔
3. **External APIs with idempotency support**: Pass `msg.MessageID` or `msg.IdempotencyKey` to the API
4. **Use TTL for cleanup** - Processed keys don't need to live forever (7-30 days is typical)
5. **Return nil for duplicates** - To ACK the message and remove it from the queue
6. **Log duplicate detections** - For monitoring and debugging
7. **Test duplicate scenarios** - Simulate message redelivery in your tests
8. **Prefer transaction pattern**: Safer default unless performance is critical
9. **Set cleanup TTLs** via `InboxCleaner.DeleteOlderThan()` (completed: 7-30d, processing: 30-90d)
10. **Monitor stuck messages** in 'processing' status (indicates handler crashes)

## Cleanup

Use `InboxCleaner.DeleteOlderThan()` to clean up old inbox records:

```go
inboxRepo := pgx.NewInboxRepository(pool)

// Delete completed inbox messages older than 7 days
completedCount, err := inboxRepo.DeleteOlderThan(ctx, 7*24*time.Hour)
if err != nil {
    return fmt.Errorf("failed to delete inbox messages: %w", err)
}
log.Printf("Deleted %d inbox messages", completedCount)
```

**Recommended Retention:**
- Completed messages: 7-30 days (for audit trail and debugging)

Schedule cleanup with cron:

```go
c := cron.New()
c.AddFunc("0 2 * * *", func() {
    inboxRepo.DeleteOlderThan(ctx, 7*24*time.Hour)
})
c.Start()
```
