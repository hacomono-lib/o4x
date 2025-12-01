# Architecture Decision Records

This document records the key architectural decisions made during the development of o4x.

## Table of Contents

1. [ADR-001: Use PostgreSQL as the Primary Storage](#adr-001-use-postgresql-as-the-primary-storage)
2. [ADR-002: Adopt UUID v7 for Outbox IDs](#adr-002-adopt-uuid-v7-for-outbox-ids)
3. [ADR-003: Use SQS FIFO Queues](#adr-003-use-sqs-fifo-queues)
4. [ADR-004: Guarantee At-Least-Once Delivery](#adr-004-guarantee-at-least-once-delivery)
5. [ADR-005: Separate Outbox and Consumer State Machines](#adr-005-separate-outbox-and-consumer-state-machines)
6. [ADR-006: Pluggable Repository Pattern](#adr-006-pluggable-repository-pattern)
7. [ADR-007: Provide Both Dispatcher and BatchDispatcher](#adr-007-provide-both-dispatcher-and-batchdispatcher)
8. [ADR-008: Set RequeueInterval Default to 10 Seconds](#adr-008-set-requeueinterval-default-to-10-seconds)
9. [ADR-009: Use FOR UPDATE SKIP LOCKED for Concurrency](#adr-009-use-for-update-skip-locked-for-concurrency)
10. [ADR-010: Mark Oversized Messages as DEAD Immediately](#adr-010-mark-oversized-messages-as-dead-immediately)
11. [ADR-011: Make Consumer Component Optional](#adr-011-make-consumer-component-optional)
12. [ADR-012: Support Both pgx and GORM](#adr-012-support-both-pgx-and-gorm)

---

## ADR-001: Use PostgreSQL as the Primary Storage

**Status**: Accepted

**Context**:
- Outbox pattern requires transactional consistency between business data and outbox messages
- Need reliable ACID guarantees to prevent message loss
- Must support concurrent workers polling for messages efficiently

**Decision**:
We use PostgreSQL as the primary (and currently only) storage backend for o4x.

**Consequences**:
- ✅ Strong ACID guarantees ensure messages are never lost
- ✅ Advanced features like `FOR UPDATE SKIP LOCKED` enable efficient concurrent processing
- ✅ Mature ecosystem with excellent Go drivers (pgx, GORM)
- ✅ Wide adoption in production environments
- ⚠️ Users must run PostgreSQL (no support for MySQL, SQLite, etc. yet)
- 🔮 Future: Repository interface allows adding other databases if needed

---

## ADR-002: Adopt UUID v7 for Outbox IDs

**Status**: Accepted

**Context**:
- Need unique identifiers for outbox messages
- UUIDs (v4) are common but random, causing poor database index performance
- Auto-increment IDs don't work well in distributed systems
- Messages are often queried in time order (e.g., debugging, monitoring)

**Decision**:
We use UUID v7 (RFC 9562) for outbox IDs, which embeds timestamp information.

**Consequences**:
- ✅ **Time-ordered**: Messages naturally sort by creation time
- ✅ **Index-friendly**: Sequential UUIDs improve B-tree index performance (less page splits)
- ✅ **Globally unique**: Safe for distributed deployments
- ✅ **Debuggable**: ID itself indicates roughly when message was created
- ⚠️ UUID v7 is relatively new (RFC published 2024), but widely supported in Go libraries
- 📊 Performance: ~50% better index insertion performance vs UUID v4 in high-throughput scenarios

---

## ADR-003: Use SQS FIFO Queues

**Status**: Accepted

**Context**:
- Messages need ordering guarantees per topic (e.g., order.created events must be processed in order)
- Duplicate messages waste processing resources and can cause issues
- SQS offers both Standard (no ordering) and FIFO (ordered + deduplicated) queues

**Decision**:
o4x is designed for SQS FIFO queues with:
- `MessageGroupId` = topic (ordering per topic)
- `MessageDeduplicationId` = idempotency_key (deduplication)

**Consequences**:
- ✅ **Ordering guarantee**: Messages within same topic are processed in order
- ✅ **Built-in deduplication**: SQS prevents duplicates within 5-minute window
- ✅ **Topic isolation**: Different topics don't block each other
- ⚠️ **Throughput limit**: FIFO queues have lower throughput than Standard queues (300 TPS per message group)
- ⚠️ **Cost**: FIFO queues cost slightly more than Standard queues
- 🔮 Future: Consider supporting Standard queues for high-throughput, order-insensitive workloads

**Alternatives Considered**:
- **SQS Standard**: No ordering guarantee, but higher throughput. Rejected because ordering is critical for most event-driven systems.
- **Kafka**: Better throughput and ordering, but requires self-hosting and more operational overhead.

---

## ADR-004: Guarantee At-Least-Once Delivery

**Status**: Accepted

**Context**:
- Distributed systems face trade-offs between delivery guarantees:
  - **At-most-once**: Message may be lost (unacceptable for outbox pattern)
  - **At-least-once**: Message may be delivered multiple times (requires idempotent handlers)
  - **Exactly-once**: Extremely complex and expensive to implement
- Crash recovery scenarios can lead to duplicate publishes (e.g., PUBLISHING → crash → retry)

**Decision**:
o4x guarantees **at-least-once delivery**. Application handlers MUST be idempotent.

**Consequences**:
- ✅ **Simpler implementation**: No need for complex distributed coordination
- ✅ **Better performance**: No overhead of exactly-once semantics
- ✅ **Reliable**: Messages are never lost, even during crashes
- ⚠️ **Handler responsibility**: Applications must implement idempotency (via cache, database constraints, or idempotent operations)
- 📚 **Documentation burden**: Must clearly educate users about idempotency requirements

**Why Not Exactly-Once?**:
- Requires distributed transactions or 2PC (two-phase commit)
- Significant performance overhead
- Kafka's "exactly-once" is actually "effectively once" within Kafka ecosystem only
- Most real-world systems are designed for at-least-once anyway

---

## ADR-005: Separate Outbox and Consumer State Machines

**Status**: Accepted

**Context**:
- Outbox (publisher side) tracks message publishing state
- Consumer (consumer side) tracks message consumption state
- These are fundamentally different concerns with different lifecycles
- Early designs considered a unified state machine but became too complex

**Decision**:
Maintain two completely independent state machines:
- **Outbox**: ENQUEUED → PUBLISHING → PUBLISHED / FAILED / DEAD
- **Consumer**: CONSUMING → CONSUMED / FAILED / DEAD

**Consequences**:
- ✅ **Separation of concerns**: Publisher and consumer are decoupled
- ✅ **Simpler mental model**: Each side has clear, focused responsibilities
- ✅ **Independent scaling**: Publisher and consumer can scale independently
- ✅ **Optional consumer**: Consumer tracking is optional (not needed for Kafka, etc.)
- ⚠️ **No end-to-end visibility**: Cannot track a message from insertion to consumption in a single table
- 🔮 Future: Consider adding correlation IDs or distributed tracing for end-to-end tracking

---

## ADR-006: Pluggable Repository Pattern

**Status**: Accepted

**Context**:
- Different teams use different database libraries (pgx, GORM, sqlc, etc.)
- Want to avoid forcing users to adopt a specific library
- Core logic (dispatcher, worker) should be independent of database implementation

**Decision**:
Define `OutboxRepository` and `ConsumerRepository` interfaces in `core/` package. Provide implementations in `contrib/` packages.

**Consequences**:
- ✅ **Flexibility**: Users can choose their preferred database library or write custom adapters
- ✅ **Testability**: Easy to mock repositories for unit tests
- ✅ **Maintainability**: Core logic doesn't depend on database-specific code
- ⚠️ **Abstraction cost**: Interface methods must be carefully designed to support different backends
- ⚠️ **Multiple implementations to maintain**: Currently maintain pgx + GORM implementations

**Repository Interface Includes**:
- Standard operations: Insert, FetchAndLock, Update*
- Batch operations: FetchAndLockBatch, UpdateBatchToPublished
- Cleanup: DeleteOlderThan (implements OutboxCleaner)
- Transaction support: WithTx() for integration with business transactions

---

## ADR-007: Provide Both Dispatcher and BatchDispatcher

**Status**: Accepted

**Context**:
- Different workloads have different throughput requirements
- SQS supports batch operations (SendMessageBatch, up to 10 messages)
- Single-message processing is simpler but slower
- Batch processing is more complex but much faster

**Decision**:
Provide two implementations:
- **Dispatcher**: Process 1 message at a time (simple, lower throughput)
- **BatchDispatcher**: Process up to 10 messages at a time (complex, higher throughput)

**Consequences**:
- ✅ **User choice**: Simple projects use Dispatcher, high-throughput projects use BatchDispatcher
- ✅ **Performance**: BatchDispatcher can achieve 5-10x higher throughput
- ⚠️ **Maintenance burden**: Must maintain two similar implementations
- ⚠️ **Complexity**: BatchDispatcher requires more careful error handling (partial batch failures)

**Performance Comparison** (10 concurrent workers):
- Dispatcher: ~1,000 messages/sec
- BatchDispatcher: ~5,000-10,000 messages/sec (depending on batch size)

---

## ADR-008: Set RequeueInterval Default to 10 Seconds

**Status**: Accepted (Changed from 0 to 10s)

**Context**:
- BatchDispatcher has a `RequeueInterval` that controls how often FAILED messages are retried
- Original default was `0` (disabled), meaning FAILED messages never auto-retry
- Users had to manually set this, leading to messages stuck in FAILED state
- Zero value in Go often means "disabled", which is dangerous for retry mechanisms

**Decision**:
Change `RequeueInterval` default from `0` to `10 * time.Second`.

**Consequences**:
- ✅ **Better defaults**: FAILED messages automatically retry without user configuration
- ✅ **Fewer surprises**: Users don't accidentally disable retries
- ⚠️ **Breaking change**: Existing users relying on 0 (no auto-retry) will see behavior change
- 📚 **Documentation**: Clearly document how to disable (though not recommended)

**Recommended Values**:
- **High priority**: 1 second (faster retry)
- **Normal workloads**: 10 seconds (default, balanced)
- **Low priority / cost-sensitive**: 60 seconds (slower retry, fewer DB queries)

---

## ADR-009: Use FOR UPDATE SKIP LOCKED for Concurrency

**Status**: Accepted

**Context**:
- Multiple workers need to concurrently fetch messages without conflicts
- Traditional `SELECT ... FOR UPDATE` blocks other workers, reducing parallelism
- Need to avoid race conditions where two workers process the same message

**Decision**:
Use `FOR UPDATE SKIP LOCKED` in FetchAndLock queries:
```sql
SELECT * FROM outbox
WHERE status = 'ENQUEUED'
ORDER BY created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED
```

**Consequences**:
- ✅ **No contention**: Workers skip locked rows and grab the next available message
- ✅ **High parallelism**: Multiple workers efficiently process messages concurrently
- ✅ **No duplicate processing**: Locked rows prevent race conditions
- ⚠️ **PostgreSQL-specific**: Not all databases support SKIP LOCKED (requires PostgreSQL 9.5+)
- ⚠️ **Non-deterministic order**: Workers may skip messages temporarily, but they'll be processed eventually

**Alternatives Considered**:
- **FOR UPDATE NOWAIT**: Returns error if row is locked, requires retry logic
- **Advisory locks**: More complex, requires explicit lock management
- **Application-level locking**: Requires external coordination (Redis, etcd)

---

## ADR-010: Mark Oversized Messages as DEAD Immediately

**Status**: Accepted

**Context**:
- SQS has a hard limit of 256 KB per message
- Oversized messages will always fail to publish (not a transient error)
- Retrying oversized messages wastes resources and delays other messages

**Decision**:
Validate message size at Publisher layer. Mark messages >256 KB as DEAD immediately (PermanentError).

**Consequences**:
- ✅ **Fast failure**: No wasted retries for messages that will never succeed
- ✅ **Clear error**: Users get immediate feedback about oversized payloads
- ✅ **Better throughput**: Dispatcher doesn't waste time retrying impossible operations
- ⚠️ **Application responsibility**: Applications must design payloads to fit within limits
- 📚 **Documentation**: Must clearly document the 256 KB limit and best practices (use S3 references, etc.)

**Best Practices**:
- Store large data (files, documents) in S3 or database
- Send only references (URLs, IDs) in message payload
- Use SQS Extended Client pattern for >256 KB if needed

---

## ADR-011: Make Consumer Component Optional

**Status**: Accepted

**Context**:
- Outbox pattern core is universal (works with SQS, Kafka, RabbitMQ, etc.)
- SQS requires external state tracking (no built-in offset management like Kafka)
- Kafka manages consumer offsets internally (consumer groups)
- Not all users need consumption state tracking

**Decision**:
Place Consumer component in `contrib/sqs/consumer`, make it optional. Consumer repository can be `nil`.

**Consequences**:
- ✅ **Universal outbox**: Core outbox works with any message broker
- ✅ **Flexibility**: SQS users can choose to track consumption or not
- ✅ **Lighter weight**: Users who don't need tracking don't pay the cost
- ⚠️ **SQS-specific**: Consumer is tightly coupled to SQS API
- 🔮 Future: If we add Kafka support, no consumer component needed (Kafka has built-in offset management)

**When to Use Consumer Component**:
- Need to track message processing state
- Want idempotency checks in database
- Debugging message flow issues
- Compliance/audit requirements

**When to Skip Consumer Component**:
- Using Kafka (has built-in offset management)
- Implementing idempotency in application layer (cache, etc.)
- Processing messages without state tracking needs

---

## ADR-012: Support Both pgx and GORM

**Status**: Accepted

**Context**:
- Go ecosystem has two dominant PostgreSQL approaches:
  - **pgx**: High-performance, low-level driver
  - **GORM**: Popular ORM with higher-level abstractions
- Different teams have strong preferences (performance vs. productivity)
- Many projects already use one and don't want to switch

**Decision**:
Provide repository implementations for both pgx and GORM in `contrib/` packages.

**Consequences**:
- ✅ **User choice**: Teams can use their preferred database library
- ✅ **Lower adoption friction**: Works with existing codebases
- ✅ **Best of both worlds**: Performance-critical apps use pgx, others use GORM
- ⚠️ **Double maintenance**: Must maintain and test both implementations
- ⚠️ **Feature parity**: Need to ensure both support the same features
- ⚠️ **Documentation burden**: Must document both approaches

**Implementation Strategy**:
- Core interfaces define the contract
- Both implementations pass the same test suite
- Feature parity enforced through shared integration tests

**Performance Characteristics**:
- **pgx**: ~20-30% faster for high-throughput workloads, lower memory usage
- **GORM**: Easier to use, better for CRUD-heavy applications, good enough for most use cases

---

## Decision Process

For future ADRs, use this template:

```markdown
## ADR-XXX: Title

**Status**: [Proposed | Accepted | Deprecated | Superseded]

**Context**:
- What is the issue that we're seeing that is motivating this decision or change?
- What are the constraints?

**Decision**:
- What is the change that we're proposing and/or doing?

**Consequences**:
- ✅ Benefits
- ⚠️ Trade-offs or limitations
- 🔮 Future considerations

**Alternatives Considered** (optional):
- What other options were evaluated?
- Why were they rejected?
```

---

## References

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [UUID v7 RFC 9562](https://www.rfc-editor.org/rfc/rfc9562.html)
- [PostgreSQL FOR UPDATE SKIP LOCKED](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
- [SQS FIFO Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html)
