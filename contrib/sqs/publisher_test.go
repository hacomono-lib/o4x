package sqs

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/hacomono-lib/o4x/core"
)

// EventTypeQueueMapSuite tests EventTypeQueueMap implementation
type EventTypeQueueMapSuite struct {
	suite.Suite
}

func TestEventTypeQueueMapSuite(t *testing.T) {
	suite.Run(t, new(EventTypeQueueMapSuite))
}

func (s *EventTypeQueueMapSuite) TestExactMatch_ReturnsRegisteredQueue() {
	// Arrange
	m := NewEventTypeQueueMap("default-queue")
	m.Register("order.created", "order-queue")

	// Act
	result := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "order-queue", result)
}

func (s *EventTypeQueueMapSuite) TestNoMatch_ReturnsDefaultQueue() {
	// Arrange
	m := NewEventTypeQueueMap("default-queue")
	m.Register("order.created", "order-queue")

	// Act
	result := m.QueueURL("unknown.topic")

	// Assert
	assert.Equal(s.T(), "default-queue", result)
}

func (s *EventTypeQueueMapSuite) TestPrefixMatch_ReturnsRegisteredQueue() {
	// Arrange
	m := NewEventTypeQueueMap("default-queue")
	m.RegisterPrefix("order.", "order-queue")

	// Act
	result := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "order-queue", result)
}

func (s *EventTypeQueueMapSuite) TestLongestPrefixMatch_TakesPriority() {
	// Arrange
	m := NewEventTypeQueueMap("default-queue")
	m.RegisterPrefix("order.", "order-queue")
	m.RegisterPrefix("order.payment.", "payment-queue")

	// Act
	resultPayment := m.QueueURL("order.payment.completed")
	resultOrder := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "payment-queue", resultPayment, "longer prefix should match")
	assert.Equal(s.T(), "order-queue", resultOrder, "shorter prefix should match when no longer prefix exists")
}

func (s *EventTypeQueueMapSuite) TestExactMatchTakesPriorityOverPrefix() {
	// Arrange
	m := NewEventTypeQueueMap("default-queue")
	m.RegisterPrefix("order.", "order-queue")
	m.Register("order.special", "special-queue")

	// Act
	resultSpecial := m.QueueURL("order.special")
	resultOther := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "special-queue", resultSpecial, "exact match should take priority")
	assert.Equal(s.T(), "order-queue", resultOther, "prefix should still work for other topics")
}

func (s *EventTypeQueueMapSuite) TestMultiplePrefixes_SortedByLengthDescending() {
	// Arrange
	m := NewEventTypeQueueMap("default-queue")
	// Register in non-sorted order
	m.RegisterPrefix("a", "a-queue")
	m.RegisterPrefix("abc", "abc-queue")
	m.RegisterPrefix("ab", "ab-queue")
	m.RegisterPrefix("abcd", "abcd-queue")

	// Act
	resultAbcd := m.QueueURL("abcde")
	resultAbc := m.QueueURL("abcx")
	resultAb := m.QueueURL("abx")
	resultA := m.QueueURL("ax")

	// Assert
	assert.Equal(s.T(), "abcd-queue", resultAbcd)
	assert.Equal(s.T(), "abc-queue", resultAbc)
	assert.Equal(s.T(), "ab-queue", resultAb)
	assert.Equal(s.T(), "a-queue", resultA)
}

func (s *EventTypeQueueMapSuite) TestReRegisterPrefix_UpdatesQueue() {
	// Arrange
	m := NewEventTypeQueueMap("default-queue")
	m.RegisterPrefix("order.", "old-queue")
	m.RegisterPrefix("order.", "new-queue")

	// Act
	result := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "new-queue", result, "re-registering should update the queue")
}

func (s *EventTypeQueueMapSuite) TestEmptyTopic_ReturnsDefaultQueue() {
	// Arrange
	m := NewEventTypeQueueMap("default-queue")
	m.RegisterPrefix("order.", "order-queue")

	// Act
	result := m.QueueURL("")

	// Assert
	assert.Equal(s.T(), "default-queue", result)
}

// IsFifoQueueSuite tests isFifoQueue function edge cases
type IsFifoQueueSuite struct {
	suite.Suite
}

func TestIsFifoQueueSuite(t *testing.T) {
	suite.Run(t, new(IsFifoQueueSuite))
}

func (s *IsFifoQueueSuite) TestStandardQueue_ReturnsFalse() {
	testCases := []string{
		"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
		"http://localhost:4566/000000000000/standard-queue",
		"https://sqs.ap-northeast-1.amazonaws.com/123/queue-fifo-test", // .fifo in middle
		"https://sqs.us-west-2.amazonaws.com/456/my.fifo.queue",        // .fifo not at end
	}

	for _, url := range testCases {
		s.Run(url, func() {
			result := isFifoQueue(url)
			assert.False(s.T(), result, "URL should not be detected as FIFO: %s", url)
		})
	}
}

func (s *IsFifoQueueSuite) TestFifoQueue_ReturnsTrue() {
	testCases := []string{
		"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo",
		"http://localhost:4566/000000000000/orders.fifo",
		"https://sqs.ap-northeast-1.amazonaws.com/123/test-queue.fifo",
	}

	for _, url := range testCases {
		s.Run(url, func() {
			result := isFifoQueue(url)
			assert.True(s.T(), result, "URL should be detected as FIFO: %s", url)
		})
	}
}

func (s *IsFifoQueueSuite) TestEdgeCases() {
	testCases := []struct {
		name     string
		url      string
		expected bool
	}{
		{"Empty string", "", false},
		{"Short URL (4 chars)", "test", false},
		{"Exactly .fifo", ".fifo", true},
		{"Just .fif (4 chars)", ".fif", false},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			result := isFifoQueue(tc.url)
			assert.Equal(s.T(), tc.expected, result)
		})
	}
}

// MockSQSClient for testing
type MockSQSClient struct {
	mock.Mock
}

func (m *MockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sqs.SendMessageOutput), args.Error(1)
}

func (m *MockSQSClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sqs.SendMessageBatchOutput), args.Error(1)
}

func (m *MockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (m *MockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sqs.DeleteMessageOutput), args.Error(1)
}

// PublisherSuite tests Publisher implementation
type PublisherSuite struct {
	suite.Suite
	mockClient *MockSQSClient
	ctx        context.Context
}

func TestPublisherSuite(t *testing.T) {
	suite.Run(t, new(PublisherSuite))
}

func (s *PublisherSuite) SetupTest() {
	s.mockClient = new(MockSQSClient)
	s.ctx = context.Background()
}

func (s *PublisherSuite) TestPublish_OversizedPayload_ReturnsPermanentError() {
	// Arrange
	publisher := NewPublisher(s.mockClient, "https://sqs.../queue.fifo")
	oversizedPayload := make([]byte, MaxSQSMessageSize+1)

	msg := &core.Outbox{
		ID:             "test-id",
		EventType:      "test.event",
		Payload:        oversizedPayload,
		IdempotencyKey: "test-key",
	}

	// Act
	err := publisher.Publish(s.ctx, msg)

	// Assert
	assert.Error(s.T(), err)
	assert.True(s.T(), !core.IsRetryable(err), "oversized error should not be retryable")
	assert.ErrorIs(s.T(), err, core.ErrPayloadTooLarge)
}

func (s *PublisherSuite) TestPublish_FifoQueue_SetsGroupAndDeduplicationId() {
	// Arrange
	publisher := NewPublisher(s.mockClient, "https://sqs.../queue.fifo")
	msg := &core.Outbox{
		ID:             "test-id",
		EventType:      "order.created",
		Payload:        []byte(`{"order_id":"123"}`),
		IdempotencyKey: "order-123",
	}

	s.mockClient.On("SendMessage", s.ctx, mock.MatchedBy(func(input *sqs.SendMessageInput) bool {
		// Verify FIFO-specific fields are set
		return input.MessageGroupId != nil && *input.MessageGroupId == "order.created" &&
			input.MessageDeduplicationId != nil && *input.MessageDeduplicationId == "order-123"
	})).Return(&sqs.SendMessageOutput{MessageId: aws.String("sqs-msg-id")}, nil)

	// Act
	err := publisher.Publish(s.ctx, msg)

	// Assert
	assert.NoError(s.T(), err)
	s.mockClient.AssertExpectations(s.T())
}

func (s *PublisherSuite) TestPublish_StandardQueue_DoesNotSetFifoFields() {
	// Arrange
	publisher := NewPublisher(s.mockClient, "https://sqs.../standard-queue")
	msg := &core.Outbox{
		ID:             "test-id",
		EventType:      "order.created",
		Payload:        []byte(`{"order_id":"123"}`),
		IdempotencyKey: "order-123",
	}

	s.mockClient.On("SendMessage", s.ctx, mock.MatchedBy(func(input *sqs.SendMessageInput) bool {
		// Verify FIFO-specific fields are NOT set for standard queue
		return input.MessageGroupId == nil && input.MessageDeduplicationId == nil
	})).Return(&sqs.SendMessageOutput{MessageId: aws.String("sqs-msg-id")}, nil)

	// Act
	err := publisher.Publish(s.ctx, msg)

	// Assert
	assert.NoError(s.T(), err)
	s.mockClient.AssertExpectations(s.T())
}

// BatchPublisherSuite tests BatchPublisher implementation
type BatchPublisherSuite struct {
	suite.Suite
	mockClient *MockSQSClient
	ctx        context.Context
}

func TestBatchPublisherSuite(t *testing.T) {
	suite.Run(t, new(BatchPublisherSuite))
}

func (s *BatchPublisherSuite) SetupTest() {
	s.mockClient = new(MockSQSClient)
	s.ctx = context.Background()
}

func (s *BatchPublisherSuite) TestPublishBatch_PartialFailure_ReturnsCorrectResults() {
	// Arrange
	publisher := NewBatchPublisher(s.mockClient, "https://sqs.../queue.fifo")
	msgs := []*core.Outbox{
		{ID: "msg-1", EventType: "test.event", Payload: []byte(`{"id":1}`), IdempotencyKey: "key-1"},
		{ID: "msg-2", EventType: "test.event", Payload: []byte(`{"id":2}`), IdempotencyKey: "key-2"},
		{ID: "msg-3", EventType: "test.event", Payload: []byte(`{"id":3}`), IdempotencyKey: "key-3"},
	}

	// Mock: msg-1 succeeds, msg-2 fails, msg-3 succeeds
	s.mockClient.On("SendMessageBatch", s.ctx, mock.Anything).Return(&sqs.SendMessageBatchOutput{
		Successful: []sqstypes.SendMessageBatchResultEntry{
			{Id: aws.String("msg-1"), MessageId: aws.String("sqs-1")},
			{Id: aws.String("msg-3"), MessageId: aws.String("sqs-3")},
		},
		Failed: []sqstypes.BatchResultErrorEntry{
			{Id: aws.String("msg-2"), Code: aws.String("InternalError"), Message: aws.String("SQS error")},
		},
	}, nil)

	// Act
	results := publisher.PublishBatch(s.ctx, msgs)

	// Assert
	assert.Len(s.T(), results, 3)
	assert.True(s.T(), results[0].Success)
	assert.Equal(s.T(), "sqs-1", results[0].MessageID)
	assert.False(s.T(), results[1].Success)
	assert.Error(s.T(), results[1].Error)
	assert.True(s.T(), results[2].Success)
	assert.Equal(s.T(), "sqs-3", results[2].MessageID)
}

func (s *BatchPublisherSuite) TestPublishBatch_OversizedMessages_MarkedAsPermanentError() {
	// Arrange
	publisher := NewBatchPublisher(s.mockClient, "https://sqs.../queue.fifo")
	oversizedPayload := make([]byte, MaxSQSMessageSize+1)
	msgs := []*core.Outbox{
		{ID: "msg-1", EventType: "test.event", Payload: []byte(`{"id":1}`), IdempotencyKey: "key-1"},
		{ID: "msg-2", EventType: "test.event", Payload: oversizedPayload, IdempotencyKey: "key-2"}, // Oversized
		{ID: "msg-3", EventType: "test.event", Payload: []byte(`{"id":3}`), IdempotencyKey: "key-3"},
	}

	// Mock: Only msg-1 and msg-3 should be sent
	s.mockClient.On("SendMessageBatch", s.ctx, mock.MatchedBy(func(input *sqs.SendMessageBatchInput) bool {
		// Should only contain 2 entries (msg-2 filtered out)
		return len(input.Entries) == 2
	})).Return(&sqs.SendMessageBatchOutput{
		Successful: []sqstypes.SendMessageBatchResultEntry{
			{Id: aws.String("msg-1"), MessageId: aws.String("sqs-1")},
			{Id: aws.String("msg-3"), MessageId: aws.String("sqs-3")},
		},
	}, nil)

	// Act
	results := publisher.PublishBatch(s.ctx, msgs)

	// Assert
	assert.Len(s.T(), results, 3)
	assert.True(s.T(), results[0].Success)
	assert.False(s.T(), results[1].Success)
	assert.ErrorIs(s.T(), results[1].Error, core.ErrPayloadTooLarge)
	assert.False(s.T(), core.IsRetryable(results[1].Error), "oversized error should not be retryable")
	assert.True(s.T(), results[2].Success)
}

func (s *BatchPublisherSuite) TestPublishBatch_EntireBatchFails_AllMarkedAsFailed() {
	// Arrange
	publisher := NewBatchPublisher(s.mockClient, "https://sqs.../queue.fifo")
	msgs := []*core.Outbox{
		{ID: "msg-1", EventType: "test.event", Payload: []byte(`{"id":1}`), IdempotencyKey: "key-1"},
		{ID: "msg-2", EventType: "test.event", Payload: []byte(`{"id":2}`), IdempotencyKey: "key-2"},
	}

	// Mock: Entire batch fails
	s.mockClient.On("SendMessageBatch", s.ctx, mock.Anything).Return(
		nil, fmt.Errorf("network error"),
	)

	// Act
	results := publisher.PublishBatch(s.ctx, msgs)

	// Assert
	assert.Len(s.T(), results, 2)
	assert.False(s.T(), results[0].Success)
	assert.Error(s.T(), results[0].Error)
	assert.False(s.T(), results[1].Success)
	assert.Error(s.T(), results[1].Error)
}

func (s *BatchPublisherSuite) TestMaxBatchSize_Returns10() {
	publisher := NewBatchPublisher(s.mockClient, "https://sqs.../queue")
	assert.Equal(s.T(), 10, publisher.MaxBatchSize())
}

// MultiBatchPublisherSuite tests MultiBatchPublisher with concurrent queues
type MultiBatchPublisherSuite struct {
	suite.Suite
	mockClient *MockSQSClient
	ctx        context.Context
}

func TestMultiBatchPublisherSuite(t *testing.T) {
	suite.Run(t, new(MultiBatchPublisherSuite))
}

func (s *MultiBatchPublisherSuite) SetupTest() {
	s.mockClient = new(MockSQSClient)
	s.ctx = context.Background()
}

func (s *MultiBatchPublisherSuite) TestPublishBatch_MultipleQueues_PublishesInParallel() {
	// Arrange
	router := NewEventTypeQueueMap("https://sqs.../default-queue")
	router.Register("order.created", "https://sqs.../orders-queue.fifo")
	router.Register("payment.completed", "https://sqs.../payments-queue.fifo")

	publisher := NewMultiBatchPublisher(s.mockClient, router)

	msgs := []*core.Outbox{
		{ID: "msg-1", EventType: "order.created", Payload: []byte(`{"id":1}`), IdempotencyKey: "key-1"},
		{ID: "msg-2", EventType: "payment.completed", Payload: []byte(`{"id":2}`), IdempotencyKey: "key-2"},
		{ID: "msg-3", EventType: "notification.sent", Payload: []byte(`{"id":3}`), IdempotencyKey: "key-3"}, // Default queue
	}

	// Track concurrent calls
	var mu sync.Mutex
	callCount := 0

	// Mock: Expect 3 separate batch calls (one per queue)
	s.mockClient.On("SendMessageBatch", s.ctx, mock.Anything).Run(func(args mock.Arguments) {
		mu.Lock()
		defer mu.Unlock()
		callCount++
	}).Return(&sqs.SendMessageBatchOutput{
		Successful: []sqstypes.SendMessageBatchResultEntry{
			{Id: aws.String("msg-1"), MessageId: aws.String("sqs-1")},
			{Id: aws.String("msg-2"), MessageId: aws.String("sqs-2")},
			{Id: aws.String("msg-3"), MessageId: aws.String("sqs-3")},
		},
	}, nil).Times(3)

	// Act
	results := publisher.PublishBatch(s.ctx, msgs)

	// Assert
	assert.Len(s.T(), results, 3)
	for i := range results {
		assert.True(s.T(), results[i].Success, "message %d should succeed", i)
	}
	assert.Equal(s.T(), 3, callCount, "should make 3 separate batch calls")
	s.mockClient.AssertExpectations(s.T())
}

func (s *MultiBatchPublisherSuite) TestPublishBatch_SameQueue_BatchesTogether() {
	// Arrange
	router := NewEventTypeQueueMap("https://sqs.../default-queue")
	router.RegisterPrefix("order.", "https://sqs.../orders-queue.fifo")

	publisher := NewMultiBatchPublisher(s.mockClient, router)

	msgs := []*core.Outbox{
		{ID: "msg-1", EventType: "order.created", Payload: []byte(`{"id":1}`), IdempotencyKey: "key-1"},
		{ID: "msg-2", EventType: "order.updated", Payload: []byte(`{"id":2}`), IdempotencyKey: "key-2"},
		{ID: "msg-3", EventType: "order.cancelled", Payload: []byte(`{"id":3}`), IdempotencyKey: "key-3"},
	}

	// Mock: Expect 1 batch call with 3 messages
	s.mockClient.On("SendMessageBatch", s.ctx, mock.MatchedBy(func(input *sqs.SendMessageBatchInput) bool {
		return aws.ToString(input.QueueUrl) == "https://sqs.../orders-queue.fifo" &&
			len(input.Entries) == 3
	})).Return(&sqs.SendMessageBatchOutput{
		Successful: []sqstypes.SendMessageBatchResultEntry{
			{Id: aws.String("msg-1"), MessageId: aws.String("sqs-1")},
			{Id: aws.String("msg-2"), MessageId: aws.String("sqs-2")},
			{Id: aws.String("msg-3"), MessageId: aws.String("sqs-3")},
		},
	}, nil).Once()

	// Act
	results := publisher.PublishBatch(s.ctx, msgs)

	// Assert
	assert.Len(s.T(), results, 3)
	assert.True(s.T(), results[0].Success)
	assert.True(s.T(), results[1].Success)
	assert.True(s.T(), results[2].Success)
	s.mockClient.AssertExpectations(s.T())
}

func (s *MultiBatchPublisherSuite) TestPublishBatch_PartialFailureInOneQueue_OtherQueuesSucceed() {
	// Arrange
	router := NewEventTypeQueueMap("https://sqs.../default-queue")
	router.Register("order.created", "https://sqs.../orders-queue")
	router.Register("payment.completed", "https://sqs.../payments-queue")

	publisher := NewMultiBatchPublisher(s.mockClient, router)

	msgs := []*core.Outbox{
		{ID: "msg-1", EventType: "order.created", Payload: []byte(`{"id":1}`), IdempotencyKey: "key-1"},
		{ID: "msg-2", EventType: "payment.completed", Payload: []byte(`{"id":2}`), IdempotencyKey: "key-2"},
	}

	// Mock: orders-queue succeeds, payments-queue fails
	s.mockClient.On("SendMessageBatch", s.ctx, mock.MatchedBy(func(input *sqs.SendMessageBatchInput) bool {
		return aws.ToString(input.QueueUrl) == "https://sqs.../orders-queue"
	})).Return(&sqs.SendMessageBatchOutput{
		Successful: []sqstypes.SendMessageBatchResultEntry{
			{Id: aws.String("msg-1"), MessageId: aws.String("sqs-1")},
		},
	}, nil).Once()

	s.mockClient.On("SendMessageBatch", s.ctx, mock.MatchedBy(func(input *sqs.SendMessageBatchInput) bool {
		return aws.ToString(input.QueueUrl) == "https://sqs.../payments-queue"
	})).Return(nil, fmt.Errorf("payments queue unavailable")).Once()

	// Act
	results := publisher.PublishBatch(s.ctx, msgs)

	// Assert
	assert.Len(s.T(), results, 2)
	assert.True(s.T(), results[0].Success, "order queue should succeed")
	assert.False(s.T(), results[1].Success, "payment queue should fail")
	assert.Error(s.T(), results[1].Error)
	s.mockClient.AssertExpectations(s.T())
}
