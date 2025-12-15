# O4X Example App Scripts

This directory contains scripts to easily start and test the O4X sample application.

## ⚠️ Important Notice

These scripts run benchmarks in a **LocalStack + docker-compose environment**.

**Benchmark results are environment-specific** and should not be extrapolated to production environments. Always validate performance in production-like infrastructure before making architectural decisions.

→ **For benchmark results and analysis**, see [docs/benchmarks/localstack-benchmark-results.md](../../../docs/benchmarks/localstack-benchmark-results.md)

## Scripts

### 1. start.sh - Application Startup Script

Starts all services (PostgreSQL, LocalStack, API, Dispatcher, Consumers).

```bash
cd /path/to/o4x/examples/app
./scripts/start.sh
```

**Services Started:**
- PostgreSQL (port 25432)
- LocalStack (port 24566)
- API (port 18000)
- Dispatcher (no fixed port, supports `--scale`)
- Consumer - Order (no fixed port, supports `--scale`)
- Consumer - Notification (no fixed port, supports `--scale`)
- Consumer - User (no fixed port, supports `--scale`)

**Health Check Endpoints:**
- API: http://localhost:18000/health
- Dispatcher: Use `docker-compose logs -f dispatcher` to monitor health
- Consumers: Use `docker-compose logs -f [consumer-name]` to monitor health

### 2. benchmark.sh - Benchmark Execution Script

Cleans up the database, runs benchmarks, and displays performance results.

**Basic Usage:**
```bash
./scripts/benchmark.sh
```

**With Options:**
```bash
./scripts/benchmark.sh \
  --requests 200 \
  --concurrency 20 \
  --type notification \
  --wait 10
```

**Available Options:**
- `-r, --requests NUM`: Number of requests (default: 200)
- `-c, --concurrency NUM`: Concurrency level (default: 20)
- `-t, --type TYPE`: Request type (notification|order|user, default: notification)
- `-w, --wait SECONDS`: Wait time for async processing (default: 10 seconds)
- `-h, --help`: Show help message

**Examples:**

```bash
# Run benchmark with default settings
./scripts/benchmark.sh

# Run 500 requests with concurrency 50
./scripts/benchmark.sh -r 500 -c 50

# Benchmark for order type
./scripts/benchmark.sh -t order

# Set wait time to 30 seconds
./scripts/benchmark.sh -w 30

# All options specified
./scripts/benchmark.sh -r 1000 -c 50 -t notification -w 15
```

**Output Metrics:**

The benchmark script displays the following information:

1. **API Performance**: Requests/sec, latency statistics
2. **Dispatcher Performance**: Processing time, throughput
3. **Consumer Performance**: Completion count, processing time, throughput
4. **Inbox Status Summary**: Overall completion rate

## Common Workflows

### 1. Initial Setup

```bash
# Start services
./scripts/start.sh

# Run benchmark
./scripts/benchmark.sh

# Scale consumers and test (3 instances)
docker-compose up -d --scale consumer-notification=3
./scripts/benchmark.sh --compare
```

### 2. Re-test After Code Changes

```bash
# Rebuild and restart services
docker-compose down
docker-compose build
./scripts/start.sh

# Run benchmark
./scripts/benchmark.sh
```

### 3. View Service Logs

```bash
# View dispatcher logs
docker-compose logs -f dispatcher

# View consumer logs
docker-compose logs -f consumer-notification

# View all service logs
docker-compose logs -f
```

### 4. Direct Database Access

```bash
# Connect to PostgreSQL
docker exec -it o4x-app-postgres psql -U postgres -d o4x

# View table contents
SELECT * FROM outbox ORDER BY created_at DESC LIMIT 10;
SELECT * FROM consumer_inbox ORDER BY completed_at DESC LIMIT 10;
```

### 5. Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove data volumes (complete database cleanup)
docker-compose down -v
```

## Troubleshooting

### Services Won't Start

```bash
# Check container status
docker-compose ps

# Check error logs
docker-compose logs

# Complete cleanup and restart
docker-compose down -v
./scripts/start.sh
```

### Benchmark Fails

```bash
# Check API health
curl http://localhost:18000/health

# Verify all services are running
docker-compose ps

# Clean database and retry
docker exec o4x-app-postgres psql -U postgres -d o4x -c "TRUNCATE outbox, consumer_inbox RESTART IDENTITY;"
./scripts/benchmark.sh
```

### Poor Performance

1. **Check Resource Limits**:
   ```bash
   # Check CPU/memory usage
   docker stats

   # Check container resource settings
   docker inspect o4x-app-dispatcher | grep -A 20 "Memory"
   ```

2. **Check GOMAXPROCS Setting**:
   ```bash
   # Check dispatcher environment variables
   docker exec o4x-app-dispatcher sh -c 'echo "GOMAXPROCS=$GOMAXPROCS"'
   ```

3. **Identify Bottlenecks from Logs**:
   ```bash
   # Check dispatcher batch size and throughput
   docker logs o4x-app-dispatcher | grep "batch"
   ```

## Consumer Scaling

All consumers support Docker Compose's `--scale` option for horizontal scaling.

**Important**: Scaling behavior observed in this LocalStack environment may differ from production AWS SQS.

→ **For configuration tuning guidelines**, see [docs/PERFORMANCE.md](../../../docs/PERFORMANCE.md#consumer-scaling)

→ **For environment-specific benchmark results**, see [docs/benchmarks/localstack-benchmark-results.md](../../../docs/benchmarks/localstack-benchmark-results.md)

### Usage

**Notification Consumer (Standard Queue - Recommended for Scaling):**

```bash
# Start with 3 instances
docker-compose up -d --scale consumer-notification=3

# Scale up to 5 instances
docker-compose up -d --scale consumer-notification=5

# Scale up to 10 instances
docker-compose up -d --scale consumer-notification=10

# Verify instances are running
docker-compose ps | grep notification

# Check logs for all notification consumers
docker-compose logs -f consumer-notification

# Run benchmark
./scripts/benchmark.sh

# Return to normal configuration (1 instance)
docker-compose down
./scripts/start.sh
```

**User Consumer (Standard Queue - Recommended for Scaling):**

```bash
# Scale user consumer
docker-compose up -d --scale consumer-user=3
```

**Order Consumer (FIFO Queue - NOT Recommended for Scaling):**

```bash
# ⚠️ WARNING: Order consumer uses FIFO queue
# Horizontal scaling is NOT recommended for FIFO queues
# Use vertical scaling (increase resources) instead

# If you must scale (not recommended):
# docker-compose up -d --scale consumer-order=2  # ⚠️ May break ordering guarantees
```

### Health Checks for Scaled Instances

Since scaled instances don't have fixed ports, use `docker-compose logs` to monitor health:

```bash
# Check logs for all notification consumers
docker-compose logs -f consumer-notification

# Check specific instance (e.g., instance 2)
docker logs examples-app-consumer-notification-2

# Check all consumers
docker-compose logs -f consumer-order consumer-notification consumer-user
```

### CRITICAL: FIFO Queue Scaling Caution

**⚠️ Do NOT scale order consumer (FIFO queue) with `--scale`**

- **FIFO Queue**: Guarantees message ordering per MessageGroupId
- **Horizontal Scaling**: Multiple instances may break ordering guarantees
- **Recommendation**: Use vertical scaling (increase CPU/memory) for FIFO consumers

**Why FIFO scaling breaks ordering:**
- SQS FIFO delivers messages with same MessageGroupId (event_type) in order
- Multiple consumers may process messages out of order
- Example: `OrderCreated` → `OrderConfirmed` may be processed as `OrderConfirmed` → `OrderCreated`

**Correct approach for FIFO consumers:**
- Keep instance count = 1
- Increase CPU/memory resources
- Optimize handler processing time

### Scaling Performance

**Important**: Performance characteristics vary by environment. Production AWS SQS may show different scaling patterns than LocalStack.

→ **For benchmark results from this environment**, see [docs/benchmarks/localstack-benchmark-results.md](../../../docs/benchmarks/localstack-benchmark-results.md)

**Idempotency Notes:**
- All instances use the same `consumer_name`, so idempotency tracking is shared
- The same message_id is processed only once (guaranteed by InboxRepository)

## Performance Tuning

→ **For configuration tuning guidelines**, see [docs/PERFORMANCE.md](../../../docs/PERFORMANCE.md)
