# Redis InboxRepository

Redis-based implementation of `core.InboxRepository` for idempotency checking in event-driven systems.

## Overview

This package provides a Redis implementation of the Transactional Inbox pattern for ensuring exactly-once message processing semantics. It uses Redis's atomic operations (Lua scripts) and TTL features for efficient idempotency checking.

## When to Use

### ✅ Recommended For:

1. **Handlers without DB transactions** - Pure API calls or stateless operations
2. **High-throughput scenarios** - Redis is faster than PostgreSQL for key-value lookups
3. **Distributed systems** - Shared Redis instance across multiple services
4. **Auto-cleanup required** - TTL-based automatic cleanup without manual jobs

### ❌ NOT Recommended For:

1. **Handlers with DB transactions** - Redis cannot participate in PostgreSQL transactions
2. **Strong consistency requirements** - PostgreSQL offers stronger ACID guarantees
3. **Audit trail needs** - PostgreSQL is better for long-term data retention

## Key Differences from PostgreSQL Version

| Feature | PostgreSQL (pgx/gorm) | Redis |
|---------|----------------------|-------|
| **Transaction Support** | ✅ Full ACID transactions | ❌ No rollback support |
| **WithTx()** | ✅ Supported | ❌ Not applicable |
| **Cleanup** | Manual (DeleteOlderThan) | Automatic (TTL) |
| **Performance** | ~1-10ms per operation | ~0.1-1ms per operation |
| **Data Durability** | ✅ Persisted to disk | ⚠️ Optional (RDB/AOF) |
| **Audit Trail** | ✅ Long-term storage | ⚠️ TTL-based expiration |

## Architecture

### Data Structure

```
Key: inbox:{consumer_name}:{message_id}
Value: JSON {
  "status": "PROCESSING" | "COMPLETED",
  "received_at": "2024-01-01T00:00:00Z",
  "processed_at": "2024-01-01T00:01:00Z"
}
TTL:
  - PROCESSING: 90 days (default)
  - COMPLETED: 7 days (default)
```

## Reference Implementation

### Complete Implementation Code

<details>
<summary>Click to expand: inbox_repo.go</summary>

```go
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hacomono-lib/o4x/core"
)

const (
	// Default TTLs for automatic cleanup
	defaultProcessingTTL = 90 * 24 * time.Hour // 90 days
	defaultCompletedTTL  = 7 * 24 * time.Hour  // 7 days
)

// InboxRepository implements core.InboxRepository and core.InboxCleaner for Redis.
//
// Key Design:
//   inbox:{consumer_name}:{message_id} → Hash {status, received_at, processed_at}
//
// Atomicity:
//   - TryStart uses Lua script for atomic check-and-set
//   - No transaction rollback support (Redis limitation)
//
// TTL Strategy:
//   - PROCESSING: 90 days (long retention for crash investigation)
//   - COMPLETED: 7 days (audit trail)
//   - Automatic cleanup via Redis TTL (no manual DeleteOlderThan needed)
//
// WARNING - Transaction Limitation:
//   Redis does NOT support transaction rollback like PostgreSQL.
//   If your handler uses DB transactions, you CANNOT use Redis InboxRepository
//   in the same transaction. Use PostgreSQL InboxRepository instead.
//
// Recommended Use Cases:
//   ✅ Handlers with NO DB transactions (e.g., pure API calls)
//   ✅ High-throughput scenarios where Redis speed is critical
//   ✅ Distributed systems with shared Redis instance
//   ❌ Handlers with DB transactions (use pgx/gorm InboxRepository)
type InboxRepository struct {
	client        redis.UniversalClient
	processingTTL time.Duration
	completedTTL  time.Duration
}

// inboxRecord represents the data stored in Redis
type inboxRecord struct {
	Status      string     `json:"status"`
	ReceivedAt  time.Time  `json:"received_at"`
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
}

// NewInboxRepository creates a new Redis inbox repository.
//
// Parameters:
//   - client: Redis client (standalone, cluster, or sentinel)
//   - processingTTL: TTL for PROCESSING records (0 = use default 90 days)
//   - completedTTL: TTL for COMPLETED records (0 = use default 7 days)
//
// Example:
//   client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//   repo := redis.NewInboxRepository(client, 0, 0) // Use defaults
func NewInboxRepository(client redis.UniversalClient, processingTTL, completedTTL time.Duration) *InboxRepository {
	if processingTTL == 0 {
		processingTTL = defaultProcessingTTL
	}
	if completedTTL == 0 {
		completedTTL = defaultCompletedTTL
	}

	return &InboxRepository{
		client:        client,
		processingTTL: processingTTL,
		completedTTL:  completedTTL,
	}
}

// buildKey constructs the Redis key for a consumer inbox record.
func (r *InboxRepository) buildKey(consumerName, messageID string) string {
	return fmt.Sprintf("inbox:%s:%s", consumerName, messageID)
}

// TryStart attempts to mark a message as "PROCESSING" in the inbox.
//
// Implementation:
//   - Uses Lua script for atomic check-and-set operation
//   - If key exists and status=COMPLETED -> returns (false, nil)
//   - If key exists and status=PROCESSING -> returns (true, nil) - retry scenario
//   - If key doesn't exist -> SET with PROCESSING -> returns (true, nil)
//
// Returns:
//   - (true, nil): Should proceed with processing (first time OR retry)
//   - (false, nil): Already COMPLETED, safe to skip
//   - (false, error): Redis error occurred
func (r *InboxRepository) TryStart(ctx context.Context, consumerName, messageID string) (bool, error) {
	key := r.buildKey(consumerName, messageID)

	// Lua script for atomic check-and-set
	// Returns:
	//   1 = created new record (first time)
	//   2 = existing record with PROCESSING status (retry)
	//   0 = existing record with COMPLETED status (duplicate)
	script := redis.NewScript(`
		local key = KEYS[1]
		local status_processing = ARGV[1]
		local status_completed = ARGV[2]
		local record_json = ARGV[3]
		local ttl = tonumber(ARGV[4])

		-- Check if key exists
		local existing = redis.call('GET', key)

		if existing then
			-- Parse existing record
			local data = cjson.decode(existing)

			if data.status == status_completed then
				return 0  -- Already completed
			else
				return 2  -- Still processing (retry scenario)
			end
		else
			-- Create new record
			redis.call('SET', key, record_json, 'EX', ttl)
			return 1  -- First time
		end
	`)

	now := time.Now()
	record := inboxRecord{
		Status:     string(core.InboxStatusProcessing),
		ReceivedAt: now,
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("failed to marshal inbox record: %w", err)
	}

	result, err := script.Run(ctx, r.client, []string{key},
		string(core.InboxStatusProcessing),
		string(core.InboxStatusCompleted),
		string(recordJSON),
		int64(r.processingTTL.Seconds()),
	).Int()

	if err != nil {
		return false, fmt.Errorf("failed to execute TryStart script: %w", err)
	}

	switch result {
	case 0:
		return false, nil // Already completed
	case 1, 2:
		return true, nil // First time or retry
	default:
		return false, fmt.Errorf("unexpected script result: %d", result)
	}
}

// Complete marks a message as "COMPLETED" in the inbox.
//
// Implementation:
//   - Updates status to "COMPLETED" and sets processed_at timestamp
//   - Sets TTL to completedTTL (shorter than processingTTL)
//   - If record doesn't exist, this is a no-op (returns nil)
//   - Idempotent: calling multiple times has no additional effect
//
// Returns:
//   - nil: Success (or record doesn't exist)
//   - error: Redis error occurred
func (r *InboxRepository) Complete(ctx context.Context, consumerName, messageID string) error {
	key := r.buildKey(consumerName, messageID)

	// Lua script for atomic update
	script := redis.NewScript(`
		local key = KEYS[1]
		local status_completed = ARGV[1]
		local processed_at = ARGV[2]
		local ttl = tonumber(ARGV[3])

		-- Check if key exists
		local existing = redis.call('GET', key)

		if not existing then
			return 0  -- Record doesn't exist (no-op)
		end

		-- Parse and update record
		local data = cjson.decode(existing)
		data.status = status_completed
		data.processed_at = processed_at

		local updated_json = cjson.encode(data)
		redis.call('SET', key, updated_json, 'EX', ttl)

		return 1  -- Updated
	`)

	now := time.Now()
	_, err := script.Run(ctx, r.client, []string{key},
		string(core.InboxStatusCompleted),
		now.Format(time.RFC3339),
		int64(r.completedTTL.Seconds()),
	).Result()

	if err != nil {
		return fmt.Errorf("failed to execute Complete script: %w", err)
	}

	return nil
}

// GetByMessageID retrieves an inbox record by consumer name and message ID.
//
// Returns:
//   - (*ConsumerInbox, nil): Record found
//   - (nil, ErrNotFound): Record doesn't exist
//   - (nil, error): Redis error occurred
func (r *InboxRepository) GetByMessageID(ctx context.Context, consumerName, messageID string) (*core.ConsumerInbox, error) {
	key := r.buildKey(consumerName, messageID)

	val, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, core.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get inbox record: %w", err)
	}

	var record inboxRecord
	if err := json.Unmarshal([]byte(val), &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal inbox record: %w", err)
	}

	inbox := &core.ConsumerInbox{
		ConsumerName: consumerName,
		MessageID:    messageID,
		Status:       core.InboxStatus(record.Status),
		ReceivedAt:   record.ReceivedAt,
		ProcessedAt:  record.ProcessedAt,
	}

	return inbox, nil
}

// DeleteOlderThan is a no-op for Redis implementation.
//
// Redis uses TTL for automatic cleanup, so manual deletion is not needed.
// Records are automatically deleted when their TTL expires:
//   - PROCESSING records: After processingTTL (default 90 days)
//   - COMPLETED records: After completedTTL (default 7 days)
//
// Returns 0 deleted records (always).
func (r *InboxRepository) DeleteOlderThan(ctx context.Context, status core.InboxStatus, olderThan time.Duration) (int64, error) {
	// No-op: Redis handles cleanup automatically via TTL
	return 0, nil
}
```

</details>

<details>
<summary>Click to expand: inbox_repo_test.go</summary>

```go
package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hacomono-lib/o4x/core"
)

func setupTestRedis(t *testing.T) (*InboxRepository, *miniredis.Miniredis) {
	t.Helper()

	// Start miniredis (in-memory Redis server for testing)
	mr := miniredis.RunT(t)

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Create repository with short TTLs for testing
	repo := NewInboxRepository(client, 10*time.Second, 5*time.Second)

	return repo, mr
}

func TestInboxRepository_TryStart_FirstTime(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// First time - should succeed
	ok, err := repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.True(t, ok, "first TryStart should return true")

	// Verify record was created
	inbox, err := repo.GetByMessageID(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.Equal(t, "OrderHandler", inbox.ConsumerName)
	assert.Equal(t, "msg-123", inbox.MessageID)
	assert.Equal(t, core.InboxStatusProcessing, inbox.Status)
	assert.Nil(t, inbox.ProcessedAt)
}

func TestInboxRepository_TryStart_Duplicate_Processing(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// First call
	ok, err := repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.True(t, ok)

	// Second call - still PROCESSING (retry scenario)
	ok, err = repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.True(t, ok, "retry with PROCESSING status should return true")
}

func TestInboxRepository_TryStart_Duplicate_Completed(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// First call
	ok, err := repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.True(t, ok)

	// Mark as completed
	err = repo.Complete(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)

	// Third call - COMPLETED (duplicate)
	ok, err = repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.False(t, ok, "duplicate with COMPLETED status should return false")
}

func TestInboxRepository_Complete(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// Start processing
	ok, err := repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.True(t, ok)

	// Complete
	err = repo.Complete(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)

	// Verify status was updated
	inbox, err := repo.GetByMessageID(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.Equal(t, core.InboxStatusCompleted, inbox.Status)
	assert.NotNil(t, inbox.ProcessedAt)
}

func TestInboxRepository_Complete_Idempotent(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// Start processing
	ok, err := repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.True(t, ok)

	// Complete multiple times
	err = repo.Complete(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)

	err = repo.Complete(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)

	// Should still be completed
	inbox, err := repo.GetByMessageID(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.Equal(t, core.InboxStatusCompleted, inbox.Status)
}

func TestInboxRepository_Complete_NonExistent(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// Complete non-existent record (should be no-op)
	err := repo.Complete(ctx, "OrderHandler", "msg-999")
	require.NoError(t, err)

	// Verify record doesn't exist
	_, err = repo.GetByMessageID(ctx, "OrderHandler", "msg-999")
	assert.ErrorIs(t, err, core.ErrNotFound)
}

func TestInboxRepository_GetByMessageID_NotFound(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// Get non-existent record
	_, err := repo.GetByMessageID(ctx, "OrderHandler", "msg-999")
	assert.ErrorIs(t, err, core.ErrNotFound)
}

func TestInboxRepository_TTL_AutoCleanup(t *testing.T) {
	repo, mr := setupTestRedis(t)
	ctx := context.Background()

	// Create processing record
	ok, err := repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.True(t, ok)

	// Fast-forward time beyond processing TTL
	mr.FastForward(11 * time.Second)

	// Record should be expired
	_, err = repo.GetByMessageID(ctx, "OrderHandler", "msg-123")
	assert.ErrorIs(t, err, core.ErrNotFound, "record should be auto-deleted after TTL")
}

func TestInboxRepository_TTL_CompletedShorterThanProcessing(t *testing.T) {
	repo, mr := setupTestRedis(t)
	ctx := context.Background()

	// Create and complete record
	ok, err := repo.TryStart(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)
	assert.True(t, ok)

	err = repo.Complete(ctx, "OrderHandler", "msg-123")
	require.NoError(t, err)

	// Fast-forward time beyond completed TTL but within processing TTL
	mr.FastForward(6 * time.Second)

	// Completed record should be expired (TTL = 5s)
	_, err = repo.GetByMessageID(ctx, "OrderHandler", "msg-123")
	assert.ErrorIs(t, err, core.ErrNotFound, "completed record should have shorter TTL")
}

func TestInboxRepository_ConcurrentTryStart(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// Simulate concurrent calls
	results := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			ok, err := repo.TryStart(ctx, "OrderHandler", "msg-concurrent")
			require.NoError(t, err)
			results <- ok
		}()
	}

	// Collect results
	var trueCount, falseCount int
	for i := 0; i < 10; i++ {
		if <-results {
			trueCount++
		} else {
			falseCount++
		}
	}

	// All calls should return true (PROCESSING status allows retries)
	assert.Equal(t, 10, trueCount, "all concurrent calls should succeed for PROCESSING status")
	assert.Equal(t, 0, falseCount)
}

func TestInboxRepository_DeleteOlderThan_NoOp(t *testing.T) {
	repo, _ := setupTestRedis(t)
	ctx := context.Background()

	// DeleteOlderThan is a no-op for Redis (TTL handles cleanup)
	deleted, err := repo.DeleteOlderThan(ctx, core.InboxStatusCompleted, 7*24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted, "DeleteOlderThan should always return 0 for Redis")
}
```

</details>

### Atomicity Strategy

**TryStart()** uses Lua script for atomic check-and-set:

```lua
-- Pseudocode
if key_exists then
  if status == "COMPLETED" then
    return 0  -- Duplicate
  else
    return 2  -- Retry (PROCESSING)
  end
else
  SET key value EX ttl
  return 1  -- First time
end
```

**Complete()** uses Lua script for atomic update with TTL adjustment:

```lua
-- Pseudocode
if key_exists then
  data.status = "COMPLETED"
  data.processed_at = now()
  SET key updated_value EX completed_ttl
  return 1
else
  return 0  -- No-op
end
```

## Usage

### Basic Usage (No Transaction)

```go
import (
	"github.com/redis/go-redis/v9"
	redisInbox "github.com/hacomono-lib/o4x/contrib/redis"
	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

// Setup
client := redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})
inboxRepo := redisInbox.NewInboxRepository(client, 0, 0) // Use defaults

// Handler implementation
type NotificationHandler struct {
	inboxRepo *redisInbox.InboxRepository
	emailAPI  EmailClient
}

func (h *NotificationHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	// 1. Check idempotency
	ok, err := h.inboxRepo.TryStart(ctx, "NotificationHandler", msg.MessageID)
	if err != nil {
		return err
	}
	if !ok {
		return nil // Already completed (duplicate)
	}

	// 2. Parse message
	var event NotificationEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return err
	}

	// 3. Call external API with idempotency key
	if err := h.emailAPI.Send(ctx, EmailRequest{
		To:             event.Email,
		Subject:        event.Subject,
		Body:           event.Body,
		IdempotencyKey: msg.MessageID, // API handles duplicates
	}); err != nil {
		return err // SQS will retry
	}

	// 4. Mark as completed
	return h.inboxRepo.Complete(ctx, "NotificationHandler", msg.MessageID)
}
```

### Custom TTL Configuration

```go
// Short TTLs for high-volume events
repo := redisInbox.NewInboxRepository(
	client,
	24*time.Hour,  // PROCESSING: 1 day
	1*time.Hour,   // COMPLETED: 1 hour
)

// Long TTLs for audit requirements
repo := redisInbox.NewInboxRepository(
	client,
	180*24*time.Hour,  // PROCESSING: 180 days
	30*24*time.Hour,   // COMPLETED: 30 days
)
```

### Health Monitoring

```go
// Monitor stuck messages in PROCESSING status
func monitorStuckMessages(ctx context.Context, client redis.UniversalClient) {
	// Use Redis SCAN to find keys with PROCESSING status
	iter := client.Scan(ctx, 0, "inbox:*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		val, _ := client.Get(ctx, key).Result()

		var record inboxRecord
		json.Unmarshal([]byte(val), &record)

		if record.Status == "PROCESSING" {
			if time.Since(record.ReceivedAt) > 1*time.Hour {
				// Alert: Message stuck in PROCESSING for > 1 hour
				log.Warn("stuck message detected", "key", key)
			}
		}
	}
}
```

## Limitations

### 1. No Transaction Support

**Problem**: Redis cannot participate in PostgreSQL transactions.

```go
// ❌ WRONG - This will NOT rollback inbox state if tx.Rollback() is called
tx := db.Begin()
defer tx.Rollback()

ok, _ := redisInboxRepo.TryStart(ctx, "OrderHandler", msg.MessageID)
if !ok {
	return nil
}

// Business logic
tx.Exec("INSERT INTO orders ...")

// If this fails and tx.Rollback() is called,
// the inbox record in Redis will NOT be rolled back!
return tx.Commit()
```

**Solution**: Use PostgreSQL InboxRepository for transactional handlers.

```go
// ✅ CORRECT - Use pgx/gorm InboxRepository with WithTx()
tx := pool.Begin(ctx)
defer tx.Rollback(ctx)

txInbox := pgxInboxRepo.WithTx(tx)
ok, _ := txInbox.TryStart(ctx, "OrderHandler", msg.MessageID)
if !ok {
	return nil
}

// Business logic
tx.Exec(ctx, "INSERT INTO orders ...")

// All or nothing - inbox state is part of the transaction
return tx.Commit(ctx)
```

### 2. Data Loss Risk

Redis persistence is optional (RDB/AOF). If Redis crashes before persistence:
- In-flight PROCESSING records may be lost
- Messages will be redelivered by SQS (safe, but duplicates possible)

**Mitigation**:
- Enable Redis persistence (AOF with appendfsync=everysec)
- Use Redis Sentinel/Cluster for high availability
- Accept eventual consistency (at-least-once delivery)

### 3. No Manual Cleanup Control

PostgreSQL version allows fine-grained cleanup:

```go
// PostgreSQL - manual control
deleted, _ := repo.DeleteOlderThan(ctx, core.InboxStatusCompleted, 7*24*time.Hour)
log.Info("cleaned up", "deleted", deleted)
```

Redis version uses automatic TTL:

```go
// Redis - automatic cleanup via TTL
// No manual control, no cleanup metrics
deleted, _ := repo.DeleteOlderThan(ctx, core.InboxStatusCompleted, 7*24*time.Hour)
// Always returns 0
```

## Testing

```bash
# Install dependencies
go get github.com/redis/go-redis/v9
go get github.com/alicebob/miniredis/v2

# Run tests
go test ./contrib/redis -v

# Run with race detector
go test ./contrib/redis -race -v
```

## Performance Tuning

### Redis Configuration

```conf
# redis.conf

# Persistence (balance between performance and durability)
appendonly yes
appendfsync everysec  # Good balance (sync every second)

# Memory management
maxmemory 2gb
maxmemory-policy allkeys-lru  # Evict old keys if memory is full

# Connection pooling
tcp-backlog 511
maxclients 10000
```

### Client Configuration

```go
client := redis.NewClient(&redis.Options{
	Addr:         "localhost:6379",
	PoolSize:     100,           // Max connections
	MinIdleConns: 10,            // Keep-alive connections
	MaxRetries:   3,             // Retry on network errors
	DialTimeout:  5 * time.Second,
	ReadTimeout:  3 * time.Second,
	WriteTimeout: 3 * time.Second,
})
```

## Migration from PostgreSQL

### Step 1: Add Redis InboxRepository

```go
// Keep PostgreSQL version for DB transactions
pgxInboxRepo := pgx.NewInboxRepository(pool)

// Add Redis version for non-transactional handlers
redisInboxRepo := redis.NewInboxRepository(redisClient, 0, 0)
```

### Step 2: Migrate Handlers Incrementally

```go
// Old handler (PostgreSQL)
type OrderHandler struct {
	db        *gorm.DB
	inboxRepo *pgx.InboxRepository // ✅ Keep for transactional handlers
}

// New handler (Redis)
type NotificationHandler struct {
	emailAPI  EmailClient
	inboxRepo *redis.InboxRepository // ✅ Migrate to Redis
}
```

### Step 3: Monitor Both Systems

```go
// Metrics
prometheusMetrics.Register(
	"inbox_repo_type", // Label: "postgres" or "redis"
	"inbox_operations_total",
	"inbox_errors_total",
)
```

## Dependencies

```go
require (
	github.com/redis/go-redis/v9 v9.0.0
	github.com/hacomono-lib/o4x v0.1.0
)

// For testing
require (
	github.com/alicebob/miniredis/v2 v2.30.0
	github.com/stretchr/testify v1.8.4
)
```

## Best Practices

1. **Choose the right repository**:
   - DB transactions → PostgreSQL InboxRepository
   - Pure API calls → Redis InboxRepository

2. **Set appropriate TTLs**:
   - High-volume events → Shorter TTLs (1-7 days)
   - Audit requirements → Longer TTLs (30-90 days)

3. **Monitor stuck messages**:
   - Alert on PROCESSING status > 1 hour
   - Investigate handler crashes

4. **Enable Redis persistence**:
   - AOF with appendfsync=everysec (recommended)
   - RDB snapshots as backup

5. **Use idempotency keys for external APIs**:
   - Pass msg.MessageID to APIs that support idempotency
   - Example: Stripe, Twilio, SendGrid

## Troubleshooting

### Messages stuck in PROCESSING

**Symptom**: Many records with PROCESSING status older than expected

**Causes**:
1. Handler crashes before calling Complete()
2. External API timeouts
3. SQS visibility timeout too short

**Solution**:
- Check handler error logs
- Increase SQS visibility timeout
- Add retry logic with exponential backoff

### High memory usage

**Symptom**: Redis memory grows unbounded

**Causes**:
1. TTL too long
2. High message volume
3. maxmemory-policy not set

**Solution**:
- Reduce completedTTL (default 7 days → 1 day)
- Set maxmemory and maxmemory-policy in redis.conf
- Monitor Redis memory metrics

### TryStart() returns false unexpectedly

**Symptom**: Messages skipped as duplicates when they shouldn't be

**Causes**:
1. Message ID collision (unlikely with UUIDs)
2. Completed record still exists (within TTL)

**Solution**:
- Check Redis key: `GET inbox:{consumer}:{message_id}`
- Verify TTL: `TTL inbox:{consumer}:{message_id}`
- Manually delete if needed: `DEL inbox:{consumer}:{message_id}`

## License

Same as o4x core library.
