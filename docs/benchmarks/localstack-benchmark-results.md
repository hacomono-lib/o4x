# LocalStack Benchmark Results

## ⚠️ Environment-Specific Results

**This document records benchmark results obtained under specific conditions (LocalStack, docker-compose, resource-constrained containers, single outbox table).**

**These results MUST NOT be interpreted as general performance characteristics or architectural limitations of o4x in production environments.**

→ **For critical disclaimers and proper interpretation guidelines**, see [README.md](./README.md)

---

## Test Environment

### Infrastructure

- **SQS**: LocalStack (not production AWS SQS)
- **Database**: PostgreSQL 15 in docker-compose
- **Network**: localhost-only, no network latency
- **Outbox**: Single table (no partitioning)

### Resource Constraints

All services run with intentionally constrained resources to demonstrate realistic performance under limited conditions:

```yaml
# docker-compose.yml
dispatcher:
  cpu_quota: 50000      # 0.5 vCPU
  mem_limit: 1G
  environment:
    GOMAXPROCS: "1"

consumer-notification:
  cpu_quota: 50000      # 0.5 vCPU
  mem_limit: 1G
  environment:
    GOMAXPROCS: "1"
```

### Configuration

```go
// Dispatcher
config := core.DefaultBatchDispatcherConfig()
config.WorkerCount = 2
config.BatchSize = 10
config.PollInterval = 100 * time.Millisecond

// Consumer
config := consumer.DefaultServiceConfig(queueURL)
config.WorkerCount = 2
config.MessageConcurrency = 1
config.VisibilityTimeout = 60
```

---

## Performance Results

### Single Instance Performance

**Test**: 200 messages, notification consumer with simulated external API latency

| Component | Throughput | Notes |
|-----------|-----------|-------|
| API | ~2,200 req/sec | Outbox INSERT |
| Dispatcher | ~87-144 msg/sec | BatchSize=10, GOMAXPROCS=1 |
| Consumer (notification) | ~7 msg/sec | Simulated external API calls (100-300ms latency) |

**Important Context:**
- API throughput reflects INSERT-only operations (no external I/O)
- Dispatcher throughput limited by GOMAXPROCS=1 and LocalStack latency
- Consumer throughput dominated by simulated external API latency

### Consumer Horizontal Scaling Results

**Test Setup**: 200 messages, notification consumer with simulated external API latency

| Consumer Instances | Throughput | Improvement |
|--------------------|-----------|-------------|
| 1 (baseline) | 6-7 msg/sec | Baseline |
| 3 | 19 msg/sec | ~3x |
| 5 | 25 msg/sec | ~4x |
| 10 | 44 msg/sec | ~7x |

**Observations in This Environment:**
- Near-linear scaling up to 5 instances
- Diminishing returns beyond 10 instances (likely due to simulated external API bottleneck)
- All instances share the same `consumer_name` for idempotency tracking
- Same `message_id` processed only once across all instances (verified via InboxRepository)

**Why These Results Are Environment-Specific:**
1. **LocalStack SQS**: Does not reflect production AWS SQS characteristics (batching behavior, network latency, API limits)
2. **Simulated API Latency**: Does not reflect real external API behavior (retry logic, rate limits, connection pooling)
3. **Resource Constraints**: GOMAXPROCS=1 artificially limits dispatcher throughput
4. **No Network Latency**: localhost communication does not represent distributed system behavior

---

## Dispatcher Scaling Results

**Test Setup**: Continuous insertion workload, dispatcher processing capacity

In this benchmark environment (LocalStack + docker-compose), we observed the following:

- **Single dispatcher instance** (GOMAXPROCS=1, 2 workers): ~87-144 msg/sec
- **Multiple dispatcher instances** (same configuration): No significant throughput improvement

**Environment-Specific Factors:**
1. **SELECT FOR UPDATE SKIP LOCKED** behavior in this specific PostgreSQL configuration
2. **LocalStack SQS** API characteristics (not production AWS SQS)
3. **Single outbox table** without partitioning
4. **Resource constraints** (GOMAXPROCS=1)

**Why This Does NOT Represent General Dispatcher Behavior:**
- ❌ Production AWS SQS has different batching and throughput characteristics
- ❌ Partitioned outbox tables change lock contention patterns
- ❌ Higher GOMAXPROCS values change CPU utilization patterns
- ❌ Network-distributed systems have different bottlenecks

**Correct Interpretation:**
- ✅ In this specific environment, vertical scaling (GOMAXPROCS) was more effective than horizontal scaling
- ✅ Without work partitioning, lock contention limited horizontal scaling in this test
- ❌ "Dispatcher does not scale horizontally" (incorrect generalization)
- ✅ "In this LocalStack-based benchmark, without table partitioning, horizontal scaling showed limited benefit" (correct)

---

## Throughput Comparison Matrix

**Test**: 5000 messages, user event type

| Messages | Concurrency | Throughput (msg/sec) | Notes |
|----------|-------------|---------------------|-------|
| 5000 | 50 | ~120 msg/sec | Baseline |
| 5000 | 100 | ~180 msg/sec | Improved throughput |
| 5000 | 200 | ~210 msg/sec | Diminishing returns |

**Environment-Specific Observations:**
- Higher concurrency improved throughput in this LocalStack environment
- Returns diminished beyond concurrency=100 (likely LocalStack-specific bottleneck)
- Production AWS SQS may show different scaling characteristics

---

## Important Limitations

### What These Results Cannot Tell You

1. **Production AWS SQS Performance**: LocalStack behavior differs significantly from production SQS
2. **Network Latency Impact**: localhost communication eliminates network effects
3. **Real External API Behavior**: Simulated latency does not capture rate limits, retries, connection pooling
4. **Database Performance**: docker-compose PostgreSQL != managed RDS
5. **Horizontal Scaling Limits**: Results are specific to this configuration (GOMAXPROCS=1, single table, LocalStack)

### Production Validation Required

Before making architectural decisions:

1. ✅ Benchmark with production AWS SQS (not LocalStack)
2. ✅ Use production-like database infrastructure (managed RDS)
3. ✅ Test with realistic network latency
4. ✅ Use real external API integrations (not simulated)
5. ✅ Measure under production workload patterns
6. ✅ Consider table partitioning for high-throughput scenarios

---

## Running These Benchmarks

```bash
cd examples/app

# Start all services
./scripts/start.sh

# Run benchmark with default settings (200 requests, concurrency 20)
./scripts/benchmark.sh

# Scale consumers and re-test
docker-compose up -d --scale consumer-notification=5
./scripts/benchmark.sh

# Return to single instance
docker-compose down
./scripts/start.sh
```

→ **For complete operational procedures**, see [examples/app/scripts/README.md](../../examples/app/scripts/README.md)

---

## Conclusion

These benchmarks provide a baseline for:
- ✅ Regression testing
- ✅ Code optimization verification
- ✅ Relative performance comparison within this environment

These benchmarks do NOT provide:
- ❌ Production performance predictions
- ❌ Architectural scaling limits
- ❌ General horizontal scaling characteristics

**Always validate in production-like environments before making architectural decisions.**
