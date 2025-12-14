# Deployment and Schema Evolution Guide

This document provides detailed guidance on safely deploying changes to message payloads and handling schema evolution in o4x.

## Table of Contents

- [Overview](#overview)
- [Safe Changes](#safe-changes-backward-compatible)
- [Breaking Changes](#breaking-changes-not-backward-compatible)
- [Expand-Contract Pattern](#expand-contract-pattern-two-phase-deployment)
- [Dispatcher Stop Pattern](#dispatcher-stop-pattern-emergencylarge-changes)
- [Version Field Pattern](#version-field-pattern-advanced)
- [Deployment Order Guidelines](#deployment-order-guidelines)
- [Claude Code Integration](#claude-code-integration-optional)

## Overview

When modifying message payloads, understanding backward compatibility is critical to avoid breaking running consumers during deployment.

**Key Principle:** During rolling updates, old and new consumers coexist. Producer must send format that both versions understand until 100% of consumers updated.

## Safe Changes (Backward Compatible)

### ✅ Adding Optional Fields

Old consumers ignore new fields:

```go
// Before
type OrderCreated struct {
    OrderID string `json:"order_id"`
}

// After - Safe: old consumers still work
type OrderCreated struct {
    OrderID   string `json:"order_id"`
    UserEmail string `json:"user_email,omitempty"` // NEW field
}
```

**Deployment:** Producer first, then Consumer (safe either way)

### ✅ Adding New Event Types

Existing consumers are unaffected by new event types.

**Deployment:** Any order

## Breaking Changes (NOT Backward Compatible)

### ❌ Removing Fields

Old consumers expect the field, will fail/panic:

```go
// Before
type OrderCreated struct {
    OrderID string `json:"order_id"`
    UserID  string `json:"user_id"`
}

// After - BREAKING
type OrderCreated struct {
    OrderID string `json:"order_id"`
    // UserID removed ❌
}
```

### ❌ Renaming Fields

Old consumers can't find the field:

```go
// Before
type OrderCreated struct {
    UserID string `json:"user_id"`
}

// After - BREAKING
type OrderCreated struct {
    CustomerID string `json:"customer_id"` // Renamed from user_id ❌
}
```

### ❌ Changing Field Types

Unmarshal errors (string → int, etc.):

```go
// Before
type OrderCreated struct {
    Amount string `json:"amount"`
}

// After - BREAKING
type OrderCreated struct {
    Amount int64 `json:"amount"` // Type changed ❌
}
```

### ❌ Changing Field Semantics

Same field name, different meaning:

```go
// Before
type OrderCreated struct {
    Status string `json:"status"` // "pending", "completed"
}

// After - BREAKING
type OrderCreated struct {
    Status string `json:"status"` // Now: "draft", "active", "archived" ❌
}
```

## Expand-Contract Pattern (Two-Phase Deployment)

Use this pattern for planned breaking changes during normal deployments.

### Phase 1: Expand (Add Support for Both Versions)

**Step 1:** Deploy Consumer first with dual support:

```go
type OrderCreated struct {
    OrderID     string `json:"order_id"`
    CustomerID  string `json:"customer_id"`      // NEW field
    UserID      string `json:"user_id"`          // OLD field (deprecated)
}

func (h *Handler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    var event OrderCreated
    json.Unmarshal(msg.Body, &event)

    // Support both old and new fields
    customerID := event.CustomerID
    if customerID == "" {
        customerID = event.UserID // Fallback to old field
    }
    // ... use customerID
}
```

**Step 2:** Deploy Producer to send both fields:

```go
payload := OrderCreated{
    OrderID:    "123",
    CustomerID: "user-456", // NEW field
    UserID:     "user-456", // OLD field (for old consumers)
}
```

**Step 3:** Wait for 100% consumer rollout

Verify all consumers updated via metrics/logs.

### Phase 2: Contract (Remove Old Version)

After all consumers updated:

**Step 1:** Update Producer to send only new fields:

```go
payload := OrderCreated{
    OrderID:    "123",
    CustomerID: "user-456", // Only new field
}
```

**Step 2:** Update Consumer to remove old field support:

```go
type OrderCreated struct {
    OrderID    string `json:"order_id"`
    CustomerID string `json:"customer_id"` // Only new field
}
```

**Step 3:** Remove deprecated fields from struct

Clean up code after all deployments complete.

## Dispatcher Stop Pattern (Emergency/Large Changes)

Use this pattern for urgent breaking changes when Expand-Contract adds too much complexity.

### When to Use

- Emergency breaking changes requiring immediate deployment
- Large-scale schema changes affecting many fields
- Situations where Expand-Contract dual-support code is impractical

### Deployment Steps

**Step 1:** Stop Dispatcher

Prevents new messages from being sent to SQS. API continues to work, outbox writes continue.

```bash
# In your deployment system
kubectl scale deployment dispatcher --replicas=0
# or
systemctl stop o4x-dispatcher
```

**Step 2:** Monitor SQS queue until empty

Wait for Consumer to process all in-flight messages:

```bash
# Monitor SQS queue message count
aws sqs get-queue-attributes \
  --queue-url $QUEUE_URL \
  --attribute-names ApproximateNumberOfMessages

# Watch until it reaches 0
watch -n 5 'aws sqs get-queue-attributes --queue-url $QUEUE_URL --attribute-names ApproximateNumberOfMessages'
```

**Step 3:** Deploy new Consumer

Deploy Consumer version that expects new schema:

```bash
kubectl apply -f consumer-deployment-v2.yaml
```

**Step 4:** Deploy new Producer (if needed)

Update API/Producer code with new payload format.

**Step 5:** Restart Dispatcher

Accumulated outbox messages are sent to SQS with new schema format:

```bash
kubectl scale deployment dispatcher --replicas=3
# or
systemctl start o4x-dispatcher
```

### Benefits

- ✅ API continues to work (outbox writes continue during deployment)
- ✅ Messages safely stored in outbox table (no data loss)
- ✅ Guaranteed no old-format messages in SQS queue
- ✅ Simpler than Expand-Contract for urgent changes (no dual-version code)
- ⚠️ Message delivery delayed during deployment (typically minutes)

### Comparison with Expand-Contract

| Approach | Message Delay | Code Complexity | When to Use |
|----------|---------------|-----------------|-------------|
| **Expand-Contract** | None | High (dual-version support) | Planned changes, normal deployments |
| **Dispatcher Stop** | Minutes | None (config change only) | Emergency fixes, large-scale changes |

**Note:** This pattern is safe because the outbox table acts as a durable buffer. Messages accumulate during Dispatcher downtime and are reliably delivered once restarted.

## Version Field Pattern (Advanced)

For complex schema evolution, use explicit versioning:

### Implementation

```go
type EventEnvelope struct {
    Version string          `json:"version"` // "v1", "v2"
    Payload json.RawMessage `json:"payload"`
}

func (h *Handler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
    var envelope EventEnvelope
    json.Unmarshal(msg.Body, &envelope)

    switch envelope.Version {
    case "v1":
        var v1 OrderCreatedV1
        json.Unmarshal(envelope.Payload, &v1)
        return h.handleV1(ctx, v1)
    case "v2":
        var v2 OrderCreatedV2
        json.Unmarshal(envelope.Payload, &v2)
        return h.handleV2(ctx, v2)
    default:
        return fmt.Errorf("unsupported version: %s", envelope.Version)
    }
}
```

### When to Use Versioning

- Multiple breaking changes planned
- Long-lived messages in queue (>1 week retention)
- Complex payload with frequent evolution

### Trade-offs

- ✅ Explicit compatibility contracts
- ✅ Easier to maintain multiple versions
- ❌ Additional parsing overhead
- ❌ More complex consumer code

## Deployment Order Guidelines

### Summary Table

| Scenario | Deploy Order | Rationale |
|----------|-------------|-----------|
| **Adding fields** | Producer first, then Consumer | Safe - old consumers ignore new fields |
| **Breaking changes** | **Consumer first**, then Producer | Consumer must handle both old/new before Producer sends new format |
| **Rolling updates** | Consumer, wait for 100% rollout, then Producer | Ensures no old consumer receives new format |

### Critical Rule

During rolling updates, **old and new consumers coexist**. Producer must send format that both versions understand until 100% of consumers updated.

### Example: Field Rename Deployment

#### ❌ WRONG: Producer First

```
1. Deploy Producer (sends customer_id only)
2. Old Consumers still running, expect user_id
3. ❌ Old Consumers crash with missing field error
```

#### ✅ CORRECT: Consumer First

```
1. Deploy Consumer (reads both customer_id and user_id)
2. All Consumers now understand both fields
3. Deploy Producer (sends both customer_id and user_id)
4. Wait for 100% rollout
5. Deploy Producer (sends customer_id only)
6. Deploy Consumer (reads customer_id only)
```

## Claude Code Integration (Optional)

If you use **Claude Code** for code reviews, add this to your project's `CLAUDE.md` to automate compatibility checks:

```markdown
## o4x Message Schema Review Checklist

When reviewing PRs that modify message payloads or handlers, verify:

**Payload Compatibility:**
- [ ] Is this change backward compatible?
- [ ] Breaking changes use Expand-Contract pattern (dual field support)?
- [ ] Deployment order documented (Consumer first for breaking changes)?
- [ ] Safe: Adding optional fields ✅
- [ ] Unsafe: Removing/renaming fields, changing types ❌

**Idempotency Implementation:**
- [ ] Handler is idempotent (handles duplicate messages correctly)
- [ ] Pattern chosen based on use case:
  - DB-only operations → InboxRepository with transaction
  - External API with idempotency keys → InboxRepository (transaction or auto-commit)
  - Naturally idempotent logic (INSERT ... ON CONFLICT DO NOTHING) → InboxRepository auto-commit or application-level
- [ ] Handler returns `nil` for duplicates (not error)
- [ ] External API calls without idempotency support → Reject async messaging approach

**Deployment Safety:**
- [ ] Rolling update compatibility verified (old/new consumers coexist)
- [ ] No in-flight messages will break during deployment
```

This ensures Claude catches compatibility issues during PR reviews before deployment.

## Best Practices

1. **Default to Expand-Contract** for planned changes
2. **Use Dispatcher Stop** only for emergencies or large-scale changes
3. **Always deploy Consumer first** for breaking changes
4. **Document deployment order** in PR descriptions
5. **Test with old consumers** before rolling out new Producer
6. **Monitor metrics** during rollout for errors
7. **Version complex payloads** if you have frequent schema changes
8. **Keep rollback plan** ready (revert Consumer, then Producer)
