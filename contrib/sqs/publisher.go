package sqs

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/hacomono-lib/o4x/core"
)

// SQS message size limits
const (
	// MaxSQSMessageSize is the maximum payload size for SQS (256 KB)
	// Messages exceeding this limit will be rejected by SQS
	MaxSQSMessageSize = 256 * 1024 // 256 KB
)

// Publisher implements core.Publisher for AWS SQS FIFO queues
type Publisher struct {
	client   *sqs.Client
	queueURL string
}

// NewPublisher creates a new SQS publisher
func NewPublisher(client *sqs.Client, queueURL string) *Publisher {
	return &Publisher{
		client:   client,
		queueURL: queueURL,
	}
}

// Publish sends a message to the SQS FIFO queue
// Important for FIFO queues:
//   - MessageGroupId = topic (ensures ordering per topic)
//   - MessageDeduplicationId = idempotency_key (prevents duplicates)
func (p *Publisher) Publish(ctx context.Context, msg *core.Outbox) error {
	// Validate payload size to prevent permanent failures
	if len(msg.Payload) > MaxSQSMessageSize {
		return core.NewPermanentError(fmt.Errorf("%w: payload size %d bytes exceeds SQS limit of %d bytes",
			core.ErrPayloadTooLarge, len(msg.Payload), MaxSQSMessageSize))
	}

	_, err := p.client.SendMessage(ctx, buildSendMessageInput(p.queueURL, msg))
	if err != nil {
		return fmt.Errorf("sqs publish failed: %w", err)
	}
	return nil
}

// buildMessageAttributes creates SQS message attributes from outbox metadata
func buildMessageAttributes(msg *core.Outbox) map[string]sqstypes.MessageAttributeValue {
	return map[string]sqstypes.MessageAttributeValue{
		"topic": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msg.Topic),
		},
		"outbox_id": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msg.ID),
		},
		"idempotency_key": {
			DataType:    aws.String("String"),
			StringValue: aws.String(msg.IdempotencyKey),
		},
	}
}

// buildSendMessageInput creates a SendMessageInput from an outbox message
func buildSendMessageInput(queueURL string, msg *core.Outbox) *sqs.SendMessageInput {
	return &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String(string(msg.Payload)),
		MessageGroupId:         aws.String(msg.Topic),
		MessageDeduplicationId: aws.String(msg.IdempotencyKey),
		MessageAttributes:      buildMessageAttributes(msg),
	}
}

// buildBatchEntry creates a SendMessageBatchRequestEntry from an outbox message
func buildBatchEntry(msg *core.Outbox) sqstypes.SendMessageBatchRequestEntry {
	return sqstypes.SendMessageBatchRequestEntry{
		Id:                     aws.String(msg.ID),
		MessageBody:            aws.String(string(msg.Payload)),
		MessageGroupId:         aws.String(msg.Topic),
		MessageDeduplicationId: aws.String(msg.IdempotencyKey),
		MessageAttributes:      buildMessageAttributes(msg),
	}
}

// PublisherConfig holds configuration for the SQS publisher
type PublisherConfig struct {
	QueueURL string
	// For LocalStack development
	EndpointURL string
	Region      string
}

// TopicQueueRouter determines which queue URL to use for a given topic
type TopicQueueRouter interface {
	// QueueURL returns the queue URL for the given topic
	// If no specific mapping exists, returns the default queue URL
	QueueURL(topic string) string
}

// TopicQueueMap is a simple map-based implementation of TopicQueueRouter
// It supports exact matches and prefix matches with longest-prefix-first priority.
// Thread-safe for concurrent access.
type TopicQueueMap struct {
	mu           sync.RWMutex
	routes       map[string]string // exact matches
	prefixes     []prefixRoute     // prefix matches, sorted by length descending
	defaultQueue string
}

// prefixRoute holds a prefix pattern and its queue URL
type prefixRoute struct {
	prefix   string
	queueURL string
}

// NewTopicQueueMap creates a new TopicQueueMap with a default queue
func NewTopicQueueMap(defaultQueue string) *TopicQueueMap {
	return &TopicQueueMap{
		routes:       make(map[string]string),
		prefixes:     nil,
		defaultQueue: defaultQueue,
	}
}

// Register maps a topic to a specific queue URL
func (m *TopicQueueMap) Register(topic, queueURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.routes[topic] = queueURL
}

// RegisterPrefix maps all topics with a given prefix to a specific queue URL.
// Longer prefixes take priority over shorter ones.
func (m *TopicQueueMap) RegisterPrefix(prefix, queueURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove existing prefix if present
	for i, p := range m.prefixes {
		if p.prefix == prefix {
			m.prefixes = append(m.prefixes[:i], m.prefixes[i+1:]...)
			break
		}
	}

	// Insert sorted by length descending (longest prefix first)
	route := prefixRoute{prefix: prefix, queueURL: queueURL}
	inserted := false
	for i, p := range m.prefixes {
		if len(prefix) > len(p.prefix) {
			m.prefixes = append(m.prefixes[:i], append([]prefixRoute{route}, m.prefixes[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		m.prefixes = append(m.prefixes, route)
	}
}

// QueueURL returns the queue URL for the given topic.
// Priority: exact match > longest prefix match > default queue
func (m *TopicQueueMap) QueueURL(topic string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Exact match first (O(1))
	if url, ok := m.routes[topic]; ok {
		return url
	}

	// Check prefix matches (longest prefix first due to sorted order)
	for _, p := range m.prefixes {
		if len(topic) >= len(p.prefix) && topic[:len(p.prefix)] == p.prefix {
			return p.queueURL
		}
	}

	return m.defaultQueue
}

// MultiQueuePublisher implements core.Publisher with topic-based queue routing
type MultiQueuePublisher struct {
	client *sqs.Client
	router TopicQueueRouter
}

// NewMultiQueuePublisher creates a new multi-queue SQS publisher
func NewMultiQueuePublisher(client *sqs.Client, router TopicQueueRouter) *MultiQueuePublisher {
	return &MultiQueuePublisher{
		client: client,
		router: router,
	}
}

// Publish sends a message to the appropriate SQS queue based on topic
func (p *MultiQueuePublisher) Publish(ctx context.Context, msg *core.Outbox) error {
	// Validate payload size
	if len(msg.Payload) > MaxSQSMessageSize {
		return core.NewPermanentError(fmt.Errorf("%w: payload size %d bytes exceeds SQS limit of %d bytes",
			core.ErrPayloadTooLarge, len(msg.Payload), MaxSQSMessageSize))
	}

	queueURL := p.router.QueueURL(msg.Topic)
	_, err := p.client.SendMessage(ctx, buildSendMessageInput(queueURL, msg))
	if err != nil {
		return fmt.Errorf("sqs publish to %s failed: %w", queueURL, err)
	}
	return nil
}

// SQS SendMessageBatch limit
const sqsMaxBatchSize = 10

// BatchPublisher implements core.BatchPublisher for AWS SQS FIFO queues
// It uses SendMessageBatch for improved throughput
type BatchPublisher struct {
	client   *sqs.Client
	queueURL string
}

// NewBatchPublisher creates a new SQS batch publisher
func NewBatchPublisher(client *sqs.Client, queueURL string) *BatchPublisher {
	return &BatchPublisher{
		client:   client,
		queueURL: queueURL,
	}
}

// Publish sends a single message (implements core.Publisher)
func (p *BatchPublisher) Publish(ctx context.Context, msg *core.Outbox) error {
	// Validate payload size
	if len(msg.Payload) > MaxSQSMessageSize {
		return core.NewPermanentError(fmt.Errorf("%w: payload size %d bytes exceeds SQS limit of %d bytes",
			core.ErrPayloadTooLarge, len(msg.Payload), MaxSQSMessageSize))
	}

	_, err := p.client.SendMessage(ctx, buildSendMessageInput(p.queueURL, msg))
	if err != nil {
		return fmt.Errorf("sqs publish failed: %w", err)
	}
	return nil
}

// PublishBatch sends multiple messages in a single batch operation
// SQS allows up to 10 messages per batch
func (p *BatchPublisher) PublishBatch(ctx context.Context, msgs []*core.Outbox) []core.PublishResult {
	if len(msgs) == 0 {
		return nil
	}

	results := make([]core.PublishResult, len(msgs))

	// Build batch entries and validate payload sizes
	entries := make([]sqstypes.SendMessageBatchRequestEntry, 0, len(msgs))
	for i, msg := range msgs {
		results[i].OutboxID = msg.ID

		// Validate payload size - mark as permanent error if too large
		if len(msg.Payload) > MaxSQSMessageSize {
			results[i].Success = false
			results[i].Error = core.NewPermanentError(fmt.Errorf("%w: payload size %d bytes exceeds SQS limit of %d bytes",
				core.ErrPayloadTooLarge, len(msg.Payload), MaxSQSMessageSize))
			continue
		}

		entries = append(entries, buildBatchEntry(msg))
	}

	// If all messages failed validation, return early
	if len(entries) == 0 {
		return results
	}

	// Send batch
	output, err := p.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(p.queueURL),
		Entries:  entries,
	})

	// If the entire batch failed, mark all non-validated messages as failed
	if err != nil {
		for i := range msgs {
			// Skip messages that already failed validation
			if results[i].Error != nil {
				continue
			}
			results[i].Success = false
			results[i].Error = fmt.Errorf("sqs batch publish failed: %w", err)
		}
		return results
	}

	// Create a map for quick lookup of results by ID
	successMap := make(map[string]string) // id -> messageId
	failureMap := make(map[string]error)  // id -> error

	for _, success := range output.Successful {
		successMap[aws.ToString(success.Id)] = aws.ToString(success.MessageId)
	}

	for _, failure := range output.Failed {
		failureMap[aws.ToString(failure.Id)] = fmt.Errorf("sqs batch entry failed: code=%s, message=%s",
			aws.ToString(failure.Code), aws.ToString(failure.Message))
	}

	// Map results back to original order
	for i, msg := range msgs {
		// Skip messages that already failed validation
		if results[i].Error != nil {
			continue
		}

		if messageID, ok := successMap[msg.ID]; ok {
			results[i].Success = true
			results[i].MessageID = messageID
		} else if err, ok := failureMap[msg.ID]; ok {
			results[i].Success = false
			results[i].Error = err
		} else {
			// Should not happen, but handle gracefully
			results[i].Success = false
			results[i].Error = fmt.Errorf("sqs batch: no result for message %s", msg.ID)
		}
	}

	return results
}

// MaxBatchSize returns the maximum batch size for SQS (10)
func (p *BatchPublisher) MaxBatchSize() int {
	return sqsMaxBatchSize
}

// MultiBatchPublisher implements core.BatchPublisher with topic-based queue routing
type MultiBatchPublisher struct {
	client *sqs.Client
	router TopicQueueRouter
}

// NewMultiBatchPublisher creates a new multi-queue SQS batch publisher
func NewMultiBatchPublisher(client *sqs.Client, router TopicQueueRouter) *MultiBatchPublisher {
	return &MultiBatchPublisher{
		client: client,
		router: router,
	}
}

// Publish sends a single message to the appropriate queue
func (p *MultiBatchPublisher) Publish(ctx context.Context, msg *core.Outbox) error {
	// Validate payload size
	if len(msg.Payload) > MaxSQSMessageSize {
		return core.NewPermanentError(fmt.Errorf("%w: payload size %d bytes exceeds SQS limit of %d bytes",
			core.ErrPayloadTooLarge, len(msg.Payload), MaxSQSMessageSize))
	}

	queueURL := p.router.QueueURL(msg.Topic)
	_, err := p.client.SendMessage(ctx, buildSendMessageInput(queueURL, msg))
	if err != nil {
		return fmt.Errorf("sqs publish to %s failed: %w", queueURL, err)
	}
	return nil
}

// PublishBatch sends messages to appropriate queues based on topic.
// Messages are grouped by queue URL and sent in parallel batches for better performance.
func (p *MultiBatchPublisher) PublishBatch(ctx context.Context, msgs []*core.Outbox) []core.PublishResult {
	if len(msgs) == 0 {
		return nil
	}

	results := make([]core.PublishResult, len(msgs))
	for i, msg := range msgs {
		results[i].OutboxID = msg.ID
	}

	// Group messages by queue URL
	queueGroups := make(map[string][]*indexedMessage)
	for i, msg := range msgs {
		queueURL := p.router.QueueURL(msg.Topic)
		queueGroups[queueURL] = append(queueGroups[queueURL], &indexedMessage{
			index: i,
			msg:   msg,
		})
	}

	// Send batches to all queues in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex // protects results slice

	for queueURL, group := range queueGroups {
		wg.Add(1)
		go func(queueURL string, group []*indexedMessage) {
			defer wg.Done()
			p.publishBatchToQueueWithLock(ctx, queueURL, group, results, &mu)
		}(queueURL, group)
	}

	wg.Wait()
	return results
}

// indexedMessage holds a message with its original index
type indexedMessage struct {
	index int
	msg   *core.Outbox
}

// publishBatchToQueueWithLock sends a batch to a specific queue with mutex protection for results.
// Used for parallel publishing to multiple queues.
func (p *MultiBatchPublisher) publishBatchToQueueWithLock(ctx context.Context, queueURL string, group []*indexedMessage, results []core.PublishResult, mu *sync.Mutex) {
	// Validate payload sizes and build entries
	entries := make([]sqstypes.SendMessageBatchRequestEntry, 0, len(group))
	mu.Lock()
	for _, im := range group {
		// Validate payload size
		if len(im.msg.Payload) > MaxSQSMessageSize {
			results[im.index].Success = false
			results[im.index].Error = core.NewPermanentError(fmt.Errorf("%w: payload size %d bytes exceeds SQS limit of %d bytes",
				core.ErrPayloadTooLarge, len(im.msg.Payload), MaxSQSMessageSize))
			continue
		}
		entries = append(entries, buildBatchEntry(im.msg))
	}
	mu.Unlock()

	// If all messages failed validation, return early
	if len(entries) == 0 {
		return
	}

	output, err := p.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	})

	// Lock for writing to results slice
	mu.Lock()
	defer mu.Unlock()

	// If the entire batch failed, mark all non-validated messages as failed
	if err != nil {
		for _, im := range group {
			// Skip messages that already failed validation
			if results[im.index].Error != nil {
				continue
			}
			results[im.index].Success = false
			results[im.index].Error = fmt.Errorf("sqs batch publish to %s failed: %w", queueURL, err)
		}
		return
	}

	// Build lookup maps
	successMap := make(map[string]string)
	failureMap := make(map[string]error)

	for _, success := range output.Successful {
		successMap[aws.ToString(success.Id)] = aws.ToString(success.MessageId)
	}

	for _, failure := range output.Failed {
		failureMap[aws.ToString(failure.Id)] = fmt.Errorf("sqs batch entry failed: code=%s, message=%s",
			aws.ToString(failure.Code), aws.ToString(failure.Message))
	}

	// Map results
	for _, im := range group {
		// Skip messages that already failed validation
		if results[im.index].Error != nil {
			continue
		}

		if messageID, ok := successMap[im.msg.ID]; ok {
			results[im.index].Success = true
			results[im.index].MessageID = messageID
		} else if err, ok := failureMap[im.msg.ID]; ok {
			results[im.index].Success = false
			results[im.index].Error = err
		} else {
			results[im.index].Success = false
			results[im.index].Error = fmt.Errorf("sqs batch: no result for message %s", im.msg.ID)
		}
	}
}

// MaxBatchSize returns the maximum batch size for SQS (10)
func (p *MultiBatchPublisher) MaxBatchSize() int {
	return sqsMaxBatchSize
}
