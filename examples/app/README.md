# O4X Sample Web Application

A complete example web application demonstrating the Transactional Outbox pattern using o4x library.

## Overview

This sample application showcases a real-world implementation of the outbox pattern with:

- **REST API Server**: HTTP endpoints for order, user, and notification management
- **Outbox Dispatcher**: Publishes events from outbox to SQS
- **Event Consumer**: Processes events from SQS with various handler patterns
- **Benchmark Tool**: Load testing and performance measurement

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌──────────────┐
│   Client    │─────▶│   API Server │─────▶│  PostgreSQL │◀─────│  Dispatcher  │
└─────────────┘      └──────────────┘      │   + Outbox  │      └──────────────┘
                                            └─────────────┘             │
                                                                        ▼
                                                                  ┌──────────┐
                                                                  │   SQS    │
                                                                  └──────────┘
                                                                        │
                                                                        ▼
                                                                  ┌──────────┐
                                                                  │ Consumer │
                                                                  └──────────┘
```

### Database Tables

**o4x Core Tables** (required):
- `outbox` - Transactional outbox for publishing events
- `consumer_inbox` - Idempotency store for exactly-once message processing (optional)

**Application Tables** (business data):
- `users` - User accounts
- `orders` - Order records
- `inventory` - Product inventory with reservation
- `notifications` - Notification records

**Idempotency Tables** (demonstration):
- `order_confirmations` - Idempotent order confirmation processing
- `user_welcome_credits` - Idempotent welcome credit granting

### Components

1. **API Server** (`cmd/api`)
   - Handles HTTP requests
   - Inserts business data + outbox messages in a single transaction
   - Ensures atomic commit of both operations

2. **Dispatcher** (`cmd/dispatcher`)
   - Polls outbox table for ENQUEUED messages
   - Publishes to SQS (FIFO or Standard queues)
   - Supports multi-queue routing by topic prefix
   - Implements retry with exponential backoff

3. **Consumer** (`cmd/consumer`)
   - Receives messages from SQS
   - Routes to appropriate handlers by topic
   - Supports handler grouping for independent scaling
   - Tracks consumption state (optional)
   - Implements idempotent processing

## Handler Groups

The consumer supports **handler grouping** for independent scaling and resource optimization:

### Available Groups

- **`order`** - Order-related events (FIFO queue)
  - `order.created`
  - `order.confirmed`

- **`notification`** - Notification events (Standard queue)
  - `notification.email`
  - `notification.sms`
  - `notification.push`

- **`user`** - User-related events (Standard queue)
  - `user.registered`
  - `user.updated`

### Usage

**Via command-line flag:**
```bash
go run cmd/consumer/main.go --group=order
go run cmd/consumer/main.go --group=notification --workers=5
```

**Via environment variable:**
```bash
CONSUMER_GROUP=order go run cmd/consumer/main.go
```

**Docker deployment** (see docker-compose.yml):
- `consumer-order` - Handles order events from FIFO queue
- `consumer-notification` - Handles notifications from Standard queue (higher concurrency)
- `consumer-user` - Handles user events from Standard queue

### Benefits

1. **Independent Scaling**: Scale notification workers independently from order workers
2. **Resource Optimization**: Allocate more CPU/workers to slow handlers (e.g., external API calls)
3. **Failure Isolation**: Notification failures don't affect order processing
4. **Queue Affinity**: Each group connects to its appropriate queue (FIFO vs Standard)

## Features Demonstrated

### Event Types

**Order Events** (FIFO Queue - Strict Ordering):
- `order.created` - New order placed
- `order.confirmed` - Payment processed, inventory decreased

**User Events** (Standard Queue - High Throughput):
- `user.registered` - New user sign up
- `user.updated` - Profile changes

**Notification Events** (Standard Queue - High Throughput):
- `notification.email` - Email notifications
- `notification.sms` - SMS notifications
- `notification.push` - Push notifications

### Handler Patterns

1. **Success Case** (`OrderCreatedHandler`)
   - Normal processing flow
   - Calls downstream services
   - Transactional commit

2. **Idempotency** (`UserRegisteredHandler`)
   - Uses `message_id` for deduplication
   - `ON CONFLICT DO NOTHING` pattern
   - Safe to retry

3. **Retry Scenario** (`NotificationEmailHandler`)
   - Simulated failures for testing
   - Automatic retry via SQS visibility timeout
   - Exponential backoff

4. **External API Integration** (`UserUpdatedHandler`)
   - Syncing to external CRM (simulated)
   - Error handling
   - Timeout management

## Quick Start

### Prerequisites

- Go 1.25+
- Docker and Docker Compose
- Make (optional)

### Local Development

1. **Start infrastructure**:
```bash
cd examples/app
docker-compose up postgres localstack -d
```

2. **Run migrations**:
```bash
# Migrations run automatically on postgres startup via docker-entrypoint-initdb.d
# To run manually:
psql -h localhost -p 25432 -U postgres -d o4x < migrations/001_init.sql
```

3. **Start API server**:
```bash
go run cmd/api/main.go
# Listens on http://localhost:8000
```

4. **Start dispatcher**:
```bash
go run cmd/dispatcher/main.go --multi-queue --workers 2
# Health check: http://localhost:8080/health
```

5. **Start consumer** (with handler group):
```bash
# Order consumer (FIFO queue)
CONSUMER_GROUP=order SQS_QUEUE_URL=http://localhost:24566/000000000000/o4x-events.fifo \
go run cmd/consumer/main.go --workers 2

# Notification consumer (Standard queue)
CONSUMER_GROUP=notification SQS_QUEUE_URL=http://localhost:24566/000000000000/o4x-events-standard \
go run cmd/consumer/main.go --workers 5

# User consumer (Standard queue)
CONSUMER_GROUP=user SQS_QUEUE_URL=http://localhost:24566/000000000000/o4x-events-standard \
go run cmd/consumer/main.go --workers 2

# Health checks: http://localhost:8081/health, :8082/health, :8083/health
```

### Using Docker Compose

Run all services together (includes 3 specialized consumer services):
```bash
docker-compose up --build
```

Services:
- `api` - REST API server (:18000)
- `dispatcher` - Outbox publisher (:18080)
- `consumer-order` - Order event consumer (:18081)
- `consumer-notification` - Notification event consumer (:18082)
- `consumer-user` - User event consumer (:18083)

## Local Development (without Docker)

You can run services directly with `go run` for faster development iteration:

### 1. Start infrastructure only

```bash
# Start PostgreSQL and LocalStack
docker-compose up postgres localstack -d
```

### 2. Run services locally

```bash
# Terminal 1: API Server
cd examples/app
export DATABASE_URL="postgres://postgres:postgres@localhost:25432/o4x?sslmode=disable"
go run cmd/api/main.go

# Terminal 2: Dispatcher
cd examples/app
export DATABASE_URL="postgres://postgres:postgres@localhost:25432/o4x?sslmode=disable"
export SQS_ENDPOINT="http://localhost:24566"
export SQS_QUEUE_URL="http://localhost:24566/000000000000/o4x-events.fifo"
export STANDARD_QUEUE_URL="http://localhost:24566/000000000000/o4x-events-standard"
go run cmd/dispatcher/main.go --multi-queue --workers 2

# Terminal 3: Consumer (Order)
cd examples/app
export DATABASE_URL="postgres://postgres:postgres@localhost:25432/o4x?sslmode=disable"
export SQS_ENDPOINT="http://localhost:24566"
export SQS_QUEUE_URL="http://localhost:24566/000000000000/o4x-events.fifo"
export CONSUMER_GROUP="order"
go run cmd/consumer/main.go --workers 2

# Terminal 4: Consumer (Notification)
cd examples/app
export DATABASE_URL="postgres://postgres:postgres@localhost:25432/o4x?sslmode=disable"
export SQS_ENDPOINT="http://localhost:24566"
export SQS_QUEUE_URL="http://localhost:24566/000000000000/o4x-events-standard"
export CONSUMER_GROUP="notification"
go run cmd/consumer/main.go --workers 5 --message-concurrency 10
```

**Benefits of local development:**
- Faster startup (no Docker image build)
- Direct code debugging with debugger
- Immediate code changes without rebuild
- Lower resource usage

## API Examples

### Create Order

```bash
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "550e8400-e29b-41d4-a716-446655440000",
    "product_id": "product-001",
    "quantity": 2,
    "total_price": 5000
  }'
```

### Register User

```bash
curl -X POST http://localhost:8000/api/users \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "name": "John Doe"
  }'
```

### Send Notification

```bash
curl -X POST http://localhost:8000/api/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email",
    "recipient": "user@example.com",
    "subject": "Test Notification",
    "body": "This is a test notification"
  }'
```

### Confirm Order

```bash
curl -X POST "http://localhost:8000/api/orders/confirm?order_id=<order-id>"
```

## Benchmarking

### Run Benchmark

```bash
# Order creation benchmark
go run benchmark/main.go \
  --endpoint http://localhost:8000 \
  --type order \
  --requests 1000 \
  --concurrency 10

# User registration benchmark
go run benchmark/main.go \
  --endpoint http://localhost:8000 \
  --type user \
  --requests 1000 \
  --concurrency 20

# Duration-based benchmark (run for 30 seconds)
go run benchmark/main.go \
  --endpoint http://localhost:8000 \
  --type notification \
  --duration 30s \
  --concurrency 10
```

### Expected Performance

On a standard development machine:

```
=== Benchmark Results ===
Total Requests:     1000
Success Requests:   1000
Failed Requests:    0
Duration:           2.5s
Requests/sec:       400.00

=== Latency ===
Min:                5ms
Avg:                25ms
Max:                100ms
P50:                20ms
P95:                50ms
P99:                80ms
```

## Testing Scenarios

### Test Retry Mechanism

Start consumer with simulated failures:
```bash
go run cmd/consumer/main.go --simulate-failure --failure-rate 0.3
```

This will fail 30% of notification.email messages randomly, triggering retry.

### Test Idempotency

1. Start consumer with table truncation:
```bash
go run cmd/consumer/main.go --truncate-tables
```

2. Create an order and confirm it:
```bash
# Create order
ORDER_ID=$(curl -s -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "550e8400-e29b-41d4-a716-446655440000",
    "product_id": "product-001",
    "quantity": 2,
    "total_price": 5000
  }' | jq -r '.id')

# Confirm order
curl -X POST "http://localhost:8000/api/orders/confirm?order_id=$ORDER_ID"
```

3. Check idempotency tables:
```sql
-- Should have exactly one record per message_id, even if message is redelivered
SELECT * FROM order_confirmations WHERE order_id = '$ORDER_ID';
```

### Test Handler Processing Times

Configure realistic processing delays to simulate real-world scenarios:

```bash
# Fast handlers (DB operations only)
export USER_REGISTERED_SLEEP_MIN=5ms
export USER_REGISTERED_SLEEP_MAX=20ms

# Medium handlers (DB + internal service calls)
export ORDER_CREATED_SLEEP_MIN=20ms
export ORDER_CREATED_SLEEP_MAX=80ms

# Slow handlers (external API calls)
export NOTIFICATION_EMAIL_SLEEP_MIN=100ms
export NOTIFICATION_EMAIL_SLEEP_MAX=500ms

go run cmd/consumer/main.go
```

This helps test:
- Worker concurrency and throughput
- Visibility timeout appropriateness
- Bottleneck identification
- Realistic performance benchmarks

To disable sleep entirely, set both MIN and MAX to 0:
```bash
export NOTIFICATION_EMAIL_SLEEP_MIN=0
export NOTIFICATION_EMAIL_SLEEP_MAX=0
```

### Test Multi-Queue Routing

Start dispatcher in multi-queue mode:
```bash
go run cmd/dispatcher/main.go --multi-queue
```

Routing rules:
- `order.*`, `inventory.*` → FIFO queue (strict ordering)
- `notification.*`, `user.*` → Standard queue (high throughput)

### Test Parallel Message Processing

The consumer supports parallel processing of messages within each worker using the `--message-concurrency` flag:

```bash
# Sequential processing (default, safe for FIFO queues)
go run cmd/consumer/main.go --workers 2 --message-concurrency 1

# Parallel processing (Standard queues only)
go run cmd/consumer/main.go --workers 2 --message-concurrency 10
```

**When to use MessageConcurrency > 1:**
- **Standard queues only** (NOT compatible with FIFO queues)
- Fast handlers (<100ms processing time)
- High throughput requirements
- CPU-bound or I/O-bound handlers that benefit from parallelism

**Example scenario:**
```bash
# Configuration: 5 workers, 10 concurrent messages per worker
# Total parallelism: 5 * 10 = 50 messages processed simultaneously
go run cmd/consumer/main.go --workers 5 --message-concurrency 10

# With fast handlers (5-20ms), this can achieve 1000-2000 msg/sec throughput
export USER_REGISTERED_SLEEP_MIN=5ms
export USER_REGISTERED_SLEEP_MAX=20ms
```

**Performance comparison:**
```bash
# Sequential: 2 workers * 1 msg = 2 concurrent messages
go run cmd/consumer/main.go --workers 2 --message-concurrency 1

# Parallel: 2 workers * 10 msg = 20 concurrent messages (10x parallelism)
go run cmd/consumer/main.go --workers 2 --message-concurrency 10
```

**Important notes:**
- FIFO queues will error if `--message-concurrency` > 1 (breaks ordering guarantees)
- Handlers must be idempotent (at-least-once delivery semantics)
- Monitor database connection pool settings with high concurrency
- Start with low values (5-10) and increase based on metrics

### Monitor Message Flow

Watch outbox table:
```sql
SELECT topic, status, COUNT(*)
FROM outbox
GROUP BY topic, status
ORDER BY topic, status;
```

Watch consumer inbox (idempotency tracking):
```sql
SELECT consumer_name, status, COUNT(*)
FROM consumer_inbox
GROUP BY consumer_name, status
ORDER BY consumer_name, status;
```

Watch idempotency tables:
```sql
-- Check order confirmations (idempotency demonstration)
SELECT order_id, message_id, processed_at
FROM order_confirmations
ORDER BY processed_at DESC
LIMIT 10;

-- Check user welcome credits (idempotency demonstration)
SELECT user_id, credit_amount, message_id, granted_at
FROM user_welcome_credits
ORDER BY granted_at DESC
LIMIT 10;
```

## Configuration

### Environment Variables

**API Server**:
- `DATABASE_URL` - PostgreSQL connection string
- `PORT` - HTTP server port (default: 8000)

**Dispatcher**:
- `DATABASE_URL` - PostgreSQL connection string
- `SQS_ENDPOINT` - SQS endpoint URL (LocalStack or AWS)
- `SQS_QUEUE_URL` - FIFO queue URL
- `STANDARD_QUEUE_URL` - Standard queue URL
- `AWS_REGION` - AWS region (default: us-east-1)
- `HEALTH_PORT` - Health check port (default: 8080)

**Consumer**:
- `DATABASE_URL` - PostgreSQL connection string
- `SQS_ENDPOINT` - SQS endpoint URL
- `SQS_QUEUE_URL` - Queue URL to consume from (required)
- `CONSUMER_GROUP` - Handler group: order, user, notification (required)
- `AWS_REGION` - AWS region
- `HEALTH_PORT` - Health check port (default: 8081)

**Consumer Sleep Configuration** (simulates real-world processing times):
- `ORDER_CREATED_SLEEP_MIN` - Min sleep for order.created handler (default: 20ms)
- `ORDER_CREATED_SLEEP_MAX` - Max sleep for order.created handler (default: 80ms)
- `ORDER_CONFIRMED_SLEEP_MIN` - Min sleep for order.confirmed handler (default: 10ms)
- `ORDER_CONFIRMED_SLEEP_MAX` - Max sleep for order.confirmed handler (default: 40ms)
- `USER_REGISTERED_SLEEP_MIN` - Min sleep for user.registered handler (default: 5ms)
- `USER_REGISTERED_SLEEP_MAX` - Max sleep for user.registered handler (default: 20ms)
- `USER_UPDATED_SLEEP_MIN` - Min sleep for user.updated handler (default: 10ms)
- `USER_UPDATED_SLEEP_MAX` - Max sleep for user.updated handler (default: 30ms)
- `NOTIFICATION_EMAIL_SLEEP_MIN` - Min sleep for notification.email handler (default: 50ms)
- `NOTIFICATION_EMAIL_SLEEP_MAX` - Max sleep for notification.email handler (default: 200ms)
- `NOTIFICATION_SMS_SLEEP_MIN` - Min sleep for notification.sms handler (default: 30ms)
- `NOTIFICATION_SMS_SLEEP_MAX` - Max sleep for notification.sms handler (default: 150ms)
- `NOTIFICATION_PUSH_SLEEP_MIN` - Min sleep for notification.push handler (default: 20ms)
- `NOTIFICATION_PUSH_SLEEP_MAX` - Max sleep for notification.push handler (default: 100ms)

### Command-Line Flags

**Dispatcher**:
- `--multi-queue` - Enable multi-queue routing
- `--workers N` - Number of worker goroutines (default: 2)

**Consumer**:
- `--group` - Consumer group (required): order, user, notification
- `--simulate-failure` - Enable random failures for testing
- `--failure-rate` - Failure rate 0.0-1.0 (default: 0.3)
- `--workers N` - Number of worker goroutines (default: 2)
- `--message-concurrency N` - Number of messages to process concurrently within each worker (default: 1, >1 only for Standard queues)
- `--truncate-tables` - Truncate idempotency tables on startup (for development/testing)

**Benchmark**:
- `--endpoint` - API endpoint URL
- `--type` - Request type: order, user, notification
- `--requests N` - Total number of requests (default: 1000)
- `--concurrency N` - Number of concurrent workers (default: 10)
- `--duration D` - Test duration (0 = use request count)

## Health Checks

All services expose health check endpoints for container orchestration:

- **API**: `http://localhost:8000/health`
- **Dispatcher**:
  - Liveness: `http://localhost:8080/health`
  - Readiness: `http://localhost:8080/ready`
- **Consumers**:
  - Order: Liveness `http://localhost:18081/health`, Readiness `http://localhost:18081/ready`
  - Notification: Liveness `http://localhost:18082/health`, Readiness `http://localhost:18082/ready`
  - User: Liveness `http://localhost:18083/health`, Readiness `http://localhost:18083/ready`

## Key Learnings

1. **Atomic Commits**: Business data and events committed together in a single transaction
2. **At-Least-Once Delivery**: Handlers must be idempotent
3. **Queue Selection**: FIFO for ordering, Standard for throughput
4. **Retry Strategy**: Exponential backoff with max retries
5. **Dead Letter Handling**: Log and alert on DEAD messages
6. **Monitoring**: Track outbox and consumer status for observability
7. **Processing Time Simulation**: Configure realistic delays per handler type for accurate testing

## Troubleshooting

### Messages stuck in PUBLISHING

Run revive at dispatcher startup:
```go
repo.ReviveStuckPublishing(ctx)
```

### High failure rate

Check error_message in outbox:
```sql
SELECT error_message, COUNT(*)
FROM outbox
WHERE status = 'FAILED'
GROUP BY error_message;
```

### Performance issues

- Increase worker count
- Use multi-queue routing
- Add database indexes
- Check PostgreSQL connection pool settings

## Production Considerations

1. **Database Connection Pool**: Configure pgxpool appropriately
2. **Worker Scaling**: Adjust based on message volume
3. **Queue Type**: Choose FIFO vs Standard based on requirements
4. **Monitoring**: Set up alerts for FAILED/DEAD messages
5. **Cleanup**: Schedule periodic deletion of old PUBLISHED/CONSUMED messages
6. **Secrets Management**: Use AWS Secrets Manager, not environment variables
7. **Logging**: Structured logging with correlation IDs
8. **Metrics**: Export to Prometheus/CloudWatch

## License

Same as o4x library (see root LICENSE file)
