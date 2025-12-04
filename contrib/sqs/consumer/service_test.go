package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// Ensure MockSQSClient implements SQSClient interface
var _ SQSClient = (*MockSQSClient)(nil)

// MockSQSClient is a test double for SQS client
type MockSQSClient struct {
	mu sync.Mutex

	ReceiveMessageFunc func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageFunc  func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)

	ReceiveMessageCalls []sqs.ReceiveMessageInput
	DeleteMessageCalls  []sqs.DeleteMessageInput
}

func (m *MockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	m.mu.Lock()
	m.ReceiveMessageCalls = append(m.ReceiveMessageCalls, *params)
	m.mu.Unlock()

	if m.ReceiveMessageFunc != nil {
		return m.ReceiveMessageFunc(ctx, params, optFns...)
	}
	return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{}}, nil
}

func (m *MockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.mu.Lock()
	m.DeleteMessageCalls = append(m.DeleteMessageCalls, *params)
	m.mu.Unlock()

	if m.DeleteMessageFunc != nil {
		return m.DeleteMessageFunc(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

// MockConsumerRepository is a test double for consumer Repository
type MockConsumerRepository struct {
	mu sync.Mutex

	InsertOrUpdateFunc   func(ctx context.Context, params ConsumerMessageInsertParams) (*ConsumerMessage, error)
	UpdateToConsumedFunc func(ctx context.Context, id string) error
	UpdateToFailedFunc   func(ctx context.Context, id string, errMsg string) error
	UpdateToDeadFunc     func(ctx context.Context, id string, errMsg string) error
	GetByMessageIDFunc   func(ctx context.Context, messageID string) (*ConsumerMessage, error)

	InsertOrUpdateCalls   []ConsumerMessageInsertParams
	UpdateToConsumedCalls []string
	UpdateToFailedCalls   []updateFailedCall
	UpdateToDeadCalls     []updateDeadCall
}

type updateFailedCall struct {
	ID     string
	ErrMsg string
}

type updateDeadCall struct {
	ID     string
	ErrMsg string
}

func (m *MockConsumerRepository) InsertOrUpdate(ctx context.Context, params ConsumerMessageInsertParams) (*ConsumerMessage, error) {
	m.mu.Lock()
	m.InsertOrUpdateCalls = append(m.InsertOrUpdateCalls, params)
	m.mu.Unlock()

	if m.InsertOrUpdateFunc != nil {
		return m.InsertOrUpdateFunc(ctx, params)
	}
	return &ConsumerMessage{
		ID:           GenerateID(),
		MessageID:    params.MessageID,
		ReceiveCount: params.ReceiveCount,
		MaxRetries:   params.MaxRetries,
		Status:       ConsumerStatusConsuming,
	}, nil
}

func (m *MockConsumerRepository) UpdateToConsumed(ctx context.Context, id string) error {
	m.mu.Lock()
	m.UpdateToConsumedCalls = append(m.UpdateToConsumedCalls, id)
	m.mu.Unlock()

	if m.UpdateToConsumedFunc != nil {
		return m.UpdateToConsumedFunc(ctx, id)
	}
	return nil
}

func (m *MockConsumerRepository) UpdateToFailed(ctx context.Context, id, errMsg string) error {
	m.mu.Lock()
	m.UpdateToFailedCalls = append(m.UpdateToFailedCalls, updateFailedCall{ID: id, ErrMsg: errMsg})
	m.mu.Unlock()

	if m.UpdateToFailedFunc != nil {
		return m.UpdateToFailedFunc(ctx, id, errMsg)
	}
	return nil
}

func (m *MockConsumerRepository) UpdateToDead(ctx context.Context, id, errMsg string) error {
	m.mu.Lock()
	m.UpdateToDeadCalls = append(m.UpdateToDeadCalls, updateDeadCall{ID: id, ErrMsg: errMsg})
	m.mu.Unlock()

	if m.UpdateToDeadFunc != nil {
		return m.UpdateToDeadFunc(ctx, id, errMsg)
	}
	return nil
}

func (m *MockConsumerRepository) GetByMessageID(ctx context.Context, messageID string) (*ConsumerMessage, error) {
	if m.GetByMessageIDFunc != nil {
		return m.GetByMessageIDFunc(ctx, messageID)
	}
	return nil, ErrNotFound
}

func (m *MockConsumerRepository) DeleteOlderThan(ctx context.Context, status ConsumerStatus, olderThan time.Duration) (int64, error) {
	return 0, nil
}

// ServiceSuite tests Service functionality
type ServiceSuite struct {
	suite.Suite
	sqsClient *MockSQSClient
	repo      *MockConsumerRepository
	logger    *slog.Logger
}

func TestServiceSuite(t *testing.T) {
	suite.Run(t, new(ServiceSuite))
}

func (s *ServiceSuite) SetupTest() {
	s.sqsClient = &MockSQSClient{}
	s.repo = &MockConsumerRepository{}
	s.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (s *ServiceSuite) TestNewService_WithDefaultConfig() {
	// Arrange
	config := DefaultServiceConfig("http://localhost:4566/queue/test")

	// Act
	service := NewService(s.sqsClient, s.repo, nil, config)

	// Assert
	assert.NotNil(s.T(), service)
	assert.Equal(s.T(), int32(10), service.config.MaxNumberOfMessages)
	assert.Equal(s.T(), int32(20), service.config.WaitTimeSeconds)
	assert.Equal(s.T(), int32(30), service.config.VisibilityTimeout)
	assert.Equal(s.T(), 5, service.config.MaxRetries)
	assert.Equal(s.T(), 1, service.config.WorkerCount)
}

func (s *ServiceSuite) TestNewService_WithCustomConfig() {
	// Arrange
	config := ServiceConfig{
		QueueURL:            "http://localhost:4566/queue/custom",
		MaxNumberOfMessages: 5,
		WaitTimeSeconds:     10,
		VisibilityTimeout:   60,
		MaxRetries:          3,
		WorkerCount:         2,
		ShutdownTimeout:     60 * time.Second,
		Logger:              s.logger,
	}

	// Act
	service := NewService(s.sqsClient, s.repo, nil, config)

	// Assert
	assert.Equal(s.T(), "http://localhost:4566/queue/custom", service.config.QueueURL)
	assert.Equal(s.T(), int32(5), service.config.MaxNumberOfMessages)
	assert.Equal(s.T(), int32(10), service.config.WaitTimeSeconds)
	assert.Equal(s.T(), int32(60), service.config.VisibilityTimeout)
	assert.Equal(s.T(), 3, service.config.MaxRetries)
	assert.Equal(s.T(), 2, service.config.WorkerCount)
}

func (s *ServiceSuite) TestService_StartAndStop() {
	// Arrange
	config := ServiceConfig{
		QueueURL:        "http://localhost:4566/queue/test",
		WaitTimeSeconds: 1,
		Logger:          s.logger,
	}
	service := NewService(s.sqsClient, nil, nil, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)
	assert.True(s.T(), service.IsRunning())

	service.Stop()

	// Assert
	assert.False(s.T(), service.IsRunning())
}

func (s *ServiceSuite) TestService_ProcessesMessageWithRepo() {
	// Arrange
	var handledMessage *SQSMessage
	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		handledMessage = msg
		return nil
	})

	msgReceived := make(chan struct{})
	var receiveCount int32

	s.sqsClient.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		count := atomic.AddInt32(&receiveCount, 1)
		if count == 1 {
			defer close(msgReceived)
			return &sqs.ReceiveMessageOutput{
				Messages: []sqstypes.Message{
					{
						MessageId:     aws.String("msg-123"),
						ReceiptHandle: aws.String("receipt-123"),
						Body:          aws.String(`{"data":"test"}`),
						Attributes: map[string]string{
							string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "1",
						},
						MessageAttributes: map[string]sqstypes.MessageAttributeValue{
							"topic": {
								DataType:    aws.String("String"),
								StringValue: aws.String("test.topic"),
							},
							"outbox_id": {
								DataType:    aws.String("String"),
								StringValue: aws.String("outbox-uuid-123"),
							},
						},
					},
				},
			}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}

	config := ServiceConfig{
		QueueURL:        "http://localhost:4566/queue/test",
		WaitTimeSeconds: 1,
		MaxRetries:      5,
		Logger:          s.logger,
	}
	service := NewService(s.sqsClient, s.repo, handler, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)

	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		s.T().Fatal("timeout waiting for message")
	}

	time.Sleep(100 * time.Millisecond)
	service.Stop()

	// Assert
	assert.NotNil(s.T(), handledMessage)
	assert.Equal(s.T(), "msg-123", handledMessage.MessageID)
	assert.Equal(s.T(), "test.topic", handledMessage.Topic)
	assert.NotNil(s.T(), handledMessage.OutboxID)
	assert.Equal(s.T(), "outbox-uuid-123", *handledMessage.OutboxID)

	// Verify repo calls
	assert.Len(s.T(), s.repo.InsertOrUpdateCalls, 1)
	assert.Equal(s.T(), "msg-123", s.repo.InsertOrUpdateCalls[0].MessageID)
	assert.Len(s.T(), s.repo.UpdateToConsumedCalls, 1)

	// Verify SQS delete was called
	s.sqsClient.mu.Lock()
	assert.Greater(s.T(), len(s.sqsClient.DeleteMessageCalls), 0)
	s.sqsClient.mu.Unlock()
}

func (s *ServiceSuite) TestService_ProcessesMessageWithoutRepo() {
	// Arrange
	var handledMessage *SQSMessage
	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		handledMessage = msg
		return nil
	})

	msgReceived := make(chan struct{})
	var receiveCount int32

	s.sqsClient.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		count := atomic.AddInt32(&receiveCount, 1)
		if count == 1 {
			defer close(msgReceived)
			return &sqs.ReceiveMessageOutput{
				Messages: []sqstypes.Message{
					{
						MessageId:     aws.String("msg-456"),
						ReceiptHandle: aws.String("receipt-456"),
						Body:          aws.String(`{"data":"test"}`),
						Attributes: map[string]string{
							string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "1",
						},
					},
				},
			}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}

	config := ServiceConfig{
		QueueURL:        "http://localhost:4566/queue/test",
		WaitTimeSeconds: 1,
		Logger:          s.logger,
	}
	// No repo - passing nil
	service := NewService(s.sqsClient, nil, handler, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)

	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		s.T().Fatal("timeout waiting for message")
	}

	time.Sleep(100 * time.Millisecond)
	service.Stop()

	// Assert
	assert.NotNil(s.T(), handledMessage)
	assert.Equal(s.T(), "msg-456", handledMessage.MessageID)

	// Verify SQS delete was called
	s.sqsClient.mu.Lock()
	assert.Greater(s.T(), len(s.sqsClient.DeleteMessageCalls), 0)
	s.sqsClient.mu.Unlock()
}

func (s *ServiceSuite) TestService_HandlesFailureWithRepo() {
	// Arrange
	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		return errors.New("handler error")
	})

	msgReceived := make(chan struct{})
	var receiveCount int32

	s.sqsClient.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		count := atomic.AddInt32(&receiveCount, 1)
		if count == 1 {
			defer close(msgReceived)
			return &sqs.ReceiveMessageOutput{
				Messages: []sqstypes.Message{
					{
						MessageId:     aws.String("msg-fail"),
						ReceiptHandle: aws.String("receipt-fail"),
						Body:          aws.String(`{"data":"test"}`),
						Attributes: map[string]string{
							string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "1",
						},
					},
				},
			}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}

	config := ServiceConfig{
		QueueURL:        "http://localhost:4566/queue/test",
		WaitTimeSeconds: 1,
		MaxRetries:      5,
		Logger:          s.logger,
	}
	service := NewService(s.sqsClient, s.repo, handler, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)

	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		s.T().Fatal("timeout waiting for message")
	}

	time.Sleep(100 * time.Millisecond)
	service.Stop()

	// Assert
	assert.Len(s.T(), s.repo.UpdateToFailedCalls, 1)
	assert.Contains(s.T(), s.repo.UpdateToFailedCalls[0].ErrMsg, "handler error")

	// Verify SQS delete was NOT called (message will be redelivered)
	s.sqsClient.mu.Lock()
	deleteCalls := len(s.sqsClient.DeleteMessageCalls)
	s.sqsClient.mu.Unlock()
	assert.Equal(s.T(), 0, deleteCalls)
}

func (s *ServiceSuite) TestService_MarksDeadAfterMaxRetries() {
	// Arrange
	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		return errors.New("persistent error")
	})

	// Configure repo to return high receive count
	s.repo.InsertOrUpdateFunc = func(ctx context.Context, params ConsumerMessageInsertParams) (*ConsumerMessage, error) {
		return &ConsumerMessage{
			ID:           GenerateID(),
			MessageID:    params.MessageID,
			ReceiveCount: 5, // Already at max
			MaxRetries:   5,
			Status:       ConsumerStatusConsuming,
		}, nil
	}

	msgReceived := make(chan struct{})
	var receiveCount int32

	s.sqsClient.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		count := atomic.AddInt32(&receiveCount, 1)
		if count == 1 {
			defer close(msgReceived)
			return &sqs.ReceiveMessageOutput{
				Messages: []sqstypes.Message{
					{
						MessageId:     aws.String("msg-dead"),
						ReceiptHandle: aws.String("receipt-dead"),
						Body:          aws.String(`{"data":"test"}`),
						Attributes: map[string]string{
							string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "5",
						},
					},
				},
			}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}

	config := ServiceConfig{
		QueueURL:        "http://localhost:4566/queue/test",
		WaitTimeSeconds: 1,
		MaxRetries:      5,
		Logger:          s.logger,
	}
	service := NewService(s.sqsClient, s.repo, handler, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)

	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		s.T().Fatal("timeout waiting for message")
	}

	time.Sleep(100 * time.Millisecond)
	service.Stop()

	// Assert
	assert.Len(s.T(), s.repo.UpdateToDeadCalls, 1)

	// Verify SQS delete WAS called (to prevent further redelivery)
	s.sqsClient.mu.Lock()
	assert.Greater(s.T(), len(s.sqsClient.DeleteMessageCalls), 0)
	s.sqsClient.mu.Unlock()
}

func (s *ServiceSuite) TestService_IsRunningReturnsFalseWhenNotStarted() {
	// Arrange
	config := ServiceConfig{
		QueueURL: "http://localhost:4566/queue/test",
		Logger:   s.logger,
	}
	service := NewService(s.sqsClient, nil, nil, config)

	// Act & Assert
	assert.False(s.T(), service.IsRunning())
}

func (s *ServiceSuite) TestService_ContextCancellationStopsPolling() {
	// Arrange
	config := ServiceConfig{
		QueueURL:        "http://localhost:4566/queue/test",
		WaitTimeSeconds: 1,
		Logger:          s.logger,
	}
	service := NewService(s.sqsClient, nil, nil, config)

	ctx, cancel := context.WithCancel(context.Background())

	// Act
	err := service.Start(ctx)
	assert.NoError(s.T(), err)

	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
	service.Stop()

	// Assert
	assert.False(s.T(), service.IsRunning())
}

// ServiceConfigSuite tests ServiceConfig defaults
type ServiceConfigSuite struct {
	suite.Suite
}

func TestServiceConfigSuite(t *testing.T) {
	suite.Run(t, new(ServiceConfigSuite))
}

func (s *ServiceConfigSuite) TestDefaultServiceConfig_ReturnsExpectedValues() {
	// Arrange & Act
	config := DefaultServiceConfig("http://localhost:4566/queue/test")

	// Assert
	assert.Equal(s.T(), "http://localhost:4566/queue/test", config.QueueURL)
	assert.Equal(s.T(), int32(10), config.MaxNumberOfMessages)
	assert.Equal(s.T(), int32(20), config.WaitTimeSeconds)
	assert.Equal(s.T(), int32(30), config.VisibilityTimeout)
	assert.Equal(s.T(), 5, config.MaxRetries)
	assert.Equal(s.T(), 1, config.WorkerCount)
	assert.Equal(s.T(), 30*time.Second, config.ShutdownTimeout)
	assert.Equal(s.T(), 60*time.Second, config.ForceTimeout)
	assert.NotNil(s.T(), config.OnForceShutdown)
	assert.NotNil(s.T(), config.Logger)
}

// ConsumerMessageSuite tests ConsumerMessage model
type ConsumerMessageSuite struct {
	suite.Suite
}

func TestConsumerMessageSuite(t *testing.T) {
	suite.Run(t, new(ConsumerMessageSuite))
}

func (s *ConsumerMessageSuite) TestShouldMarkDead_ReturnsTrueWhenMaxRetriesExceeded() {
	// Arrange
	msg := &ConsumerMessage{
		ReceiveCount: 5,
		MaxRetries:   5,
	}

	// Act & Assert
	assert.True(s.T(), msg.ShouldMarkDead())
}

func (s *ConsumerMessageSuite) TestShouldMarkDead_ReturnsFalseWhenUnderMaxRetries() {
	// Arrange
	msg := &ConsumerMessage{
		ReceiveCount: 3,
		MaxRetries:   5,
	}

	// Act & Assert
	assert.False(s.T(), msg.ShouldMarkDead())
}

// MessageConcurrencySuite tests MessageConcurrency feature
type MessageConcurrencySuite struct {
	suite.Suite
	sqsClient *MockSQSClient
	logger    *slog.Logger
}

func TestMessageConcurrencySuite(t *testing.T) {
	suite.Run(t, new(MessageConcurrencySuite))
}

func (s *MessageConcurrencySuite) SetupTest() {
	s.sqsClient = &MockSQSClient{}
	s.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (s *MessageConcurrencySuite) TestIsFIFO_ReturnsTrueForFIFOQueue() {
	// Test various FIFO queue URLs
	testCases := []struct {
		queueURL string
		expected bool
	}{
		{"http://localhost:4566/queue/test.fifo", true},
		{"https://sqs.us-east-1.amazonaws.com/123456789/my-queue.fifo", true},
		{"http://localhost:14566/000000000000/o4x-events.fifo", true},
		{"http://localhost:4566/queue/test", false},
		{"https://sqs.us-east-1.amazonaws.com/123456789/my-queue", false},
		{"http://localhost:14566/000000000000/o4x-events-standard", false},
		{".fifo", true},     // Edge case: exactly 5 chars
		{"test", false},     // Edge case: less than 5 chars
		{"", false},         // Edge case: empty string
		{"fifi", false},     // Edge case: 4 chars, similar to .fifo
		{"test.fif", false}, // Edge case: ends with .fif not .fifo
	}

	for _, tc := range testCases {
		result := isFIFO(tc.queueURL)
		assert.Equal(s.T(), tc.expected, result, "queueURL: %s", tc.queueURL)
	}
}

func (s *MessageConcurrencySuite) TestStart_ErrorsWithMessageConcurrencyOnFIFOQueue() {
	// Arrange
	config := ServiceConfig{
		QueueURL:           "http://localhost:4566/queue/test.fifo",
		MessageConcurrency: 5,
		Logger:             s.logger,
	}
	service := NewService(s.sqsClient, nil, nil, config)

	// Act
	err := service.Start(context.Background())

	// Assert
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "MessageConcurrency>1 is not compatible with FIFO queues")
	assert.Contains(s.T(), err.Error(), "test.fifo")
}

func (s *MessageConcurrencySuite) TestStart_SucceedsWithMessageConcurrencyOnStandardQueue() {
	// Arrange
	config := ServiceConfig{
		QueueURL:           "http://localhost:4566/queue/test-standard",
		MessageConcurrency: 5,
		WaitTimeSeconds:    1,
		Logger:             s.logger,
	}
	service := NewService(s.sqsClient, nil, nil, config)

	// Act
	err := service.Start(context.Background())

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), service.IsRunning())

	service.Stop()
}

func (s *MessageConcurrencySuite) TestStart_SucceedsWithMessageConcurrency1OnFIFOQueue() {
	// Arrange
	config := ServiceConfig{
		QueueURL:           "http://localhost:4566/queue/test.fifo",
		MessageConcurrency: 1,
		WaitTimeSeconds:    1,
		Logger:             s.logger,
	}
	service := NewService(s.sqsClient, nil, nil, config)

	// Act
	err := service.Start(context.Background())

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), service.IsRunning())

	service.Stop()
}

func (s *MessageConcurrencySuite) TestParallelProcessing_ProcessesMessagesInParallel() {
	// Arrange
	var processedMessages []string
	var mu sync.Mutex
	processingStarted := make(chan string, 5)
	processComplete := make(chan struct{})

	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		mu.Lock()
		processedMessages = append(processedMessages, msg.MessageID)
		mu.Unlock()

		// Signal that processing started for this message
		processingStarted <- msg.MessageID

		// Block until we're told to complete
		<-processComplete
		return nil
	})

	msgReceived := make(chan struct{})
	var receiveCount int32

	s.sqsClient.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		count := atomic.AddInt32(&receiveCount, 1)
		if count == 1 {
			defer close(msgReceived)
			// Return 5 messages at once
			messages := make([]sqstypes.Message, 5)
			for i := 0; i < 5; i++ {
				msgID := aws.String(fmt.Sprintf("msg-%d", i))
				messages[i] = sqstypes.Message{
					MessageId:     msgID,
					ReceiptHandle: aws.String(fmt.Sprintf("receipt-%d", i)),
					Body:          aws.String(`{"data":"test"}`),
					Attributes: map[string]string{
						string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "1",
					},
				}
			}
			return &sqs.ReceiveMessageOutput{Messages: messages}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}

	config := ServiceConfig{
		QueueURL:           "http://localhost:4566/queue/test-standard",
		WaitTimeSeconds:    1,
		MessageConcurrency: 5, // Allow 5 messages to be processed in parallel
		Logger:             s.logger,
	}
	service := NewService(s.sqsClient, nil, handler, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)

	// Wait for messages to be received
	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		s.T().Fatal("timeout waiting for messages")
	}

	// Wait for all 5 messages to start processing (in parallel)
	startedCount := 0
	timeout := time.After(2 * time.Second)
	for startedCount < 5 {
		select {
		case <-processingStarted:
			startedCount++
		case <-timeout:
			s.T().Fatalf("timeout: only %d messages started processing, expected 5", startedCount)
		}
	}

	// At this point, all 5 messages are being processed in parallel
	// Let them complete
	for i := 0; i < 5; i++ {
		processComplete <- struct{}{}
	}

	time.Sleep(100 * time.Millisecond)
	service.Stop()

	// Assert
	assert.Len(s.T(), processedMessages, 5, "all 5 messages should be processed")
}

func (s *MessageConcurrencySuite) TestParallelProcessing_RespectsMessageConcurrencyLimit() {
	// Arrange
	var activeCount int32
	var maxActiveCount int32
	processingStarted := make(chan struct{}, 10)
	processComplete := make(chan struct{})

	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		active := atomic.AddInt32(&activeCount, 1)

		// Track max active count
		for {
			current := atomic.LoadInt32(&maxActiveCount)
			if active <= current || atomic.CompareAndSwapInt32(&maxActiveCount, current, active) {
				break
			}
		}

		processingStarted <- struct{}{}
		<-processComplete

		atomic.AddInt32(&activeCount, -1)
		return nil
	})

	msgReceived := make(chan struct{})
	var receiveCount int32

	s.sqsClient.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		count := atomic.AddInt32(&receiveCount, 1)
		if count == 1 {
			defer close(msgReceived)
			// Return 10 messages
			messages := make([]sqstypes.Message, 10)
			for i := 0; i < 10; i++ {
				messages[i] = sqstypes.Message{
					MessageId:     aws.String(fmt.Sprintf("msg-%d", i)),
					ReceiptHandle: aws.String(fmt.Sprintf("receipt-%d", i)),
					Body:          aws.String(`{"data":"test"}`),
					Attributes: map[string]string{
						string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "1",
					},
				}
			}
			return &sqs.ReceiveMessageOutput{Messages: messages}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}

	config := ServiceConfig{
		QueueURL:           "http://localhost:4566/queue/test-standard",
		WaitTimeSeconds:    1,
		MessageConcurrency: 3, // Limit to 3 concurrent messages
		Logger:             s.logger,
	}
	service := NewService(s.sqsClient, nil, handler, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)

	// Wait for messages to be received
	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		s.T().Fatal("timeout waiting for messages")
	}

	// Wait for 3 messages to start processing
	for i := 0; i < 3; i++ {
		select {
		case <-processingStarted:
		case <-time.After(2 * time.Second):
			s.T().Fatal("timeout waiting for messages to start processing")
		}
	}

	// Give a bit of time to ensure no more than 3 start
	time.Sleep(100 * time.Millisecond)

	// Check that at most 3 messages are active
	maxActive := atomic.LoadInt32(&maxActiveCount)
	assert.LessOrEqual(s.T(), maxActive, int32(3), "max active messages should not exceed MessageConcurrency")

	// Let them complete
	for i := 0; i < 10; i++ {
		processComplete <- struct{}{}
	}

	time.Sleep(100 * time.Millisecond)
	service.Stop()

	// Assert final max active count
	maxActive = atomic.LoadInt32(&maxActiveCount)
	assert.LessOrEqual(s.T(), maxActive, int32(3), "max active messages should respect MessageConcurrency limit")
}

func (s *MessageConcurrencySuite) TestSequentialProcessing_WithMessageConcurrency1() {
	// Arrange
	var processOrder []string
	var mu sync.Mutex
	processingStarted := make(chan struct{})
	processComplete := make(chan struct{})

	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		processingStarted <- struct{}{}
		mu.Lock()
		processOrder = append(processOrder, msg.MessageID)
		mu.Unlock()
		<-processComplete
		return nil
	})

	msgReceived := make(chan struct{})
	var receiveCount int32

	s.sqsClient.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		count := atomic.AddInt32(&receiveCount, 1)
		if count == 1 {
			defer close(msgReceived)
			// Return 3 messages
			messages := make([]sqstypes.Message, 3)
			for i := 0; i < 3; i++ {
				messages[i] = sqstypes.Message{
					MessageId:     aws.String(fmt.Sprintf("msg-%d", i)),
					ReceiptHandle: aws.String(fmt.Sprintf("receipt-%d", i)),
					Body:          aws.String(`{"data":"test"}`),
					Attributes: map[string]string{
						string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "1",
					},
				}
			}
			return &sqs.ReceiveMessageOutput{Messages: messages}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}

	config := ServiceConfig{
		QueueURL:           "http://localhost:4566/queue/test.fifo",
		WaitTimeSeconds:    1,
		MessageConcurrency: 1, // Sequential processing
		Logger:             s.logger,
	}
	service := NewService(s.sqsClient, nil, handler, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)

	// Wait for messages to be received
	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		s.T().Fatal("timeout waiting for messages")
	}

	// Process messages one by one
	for i := 0; i < 3; i++ {
		select {
		case <-processingStarted:
			// Allow this message to complete before next starts
			processComplete <- struct{}{}
			time.Sleep(50 * time.Millisecond)
		case <-time.After(2 * time.Second):
			s.T().Fatalf("timeout waiting for message %d to start", i)
		}
	}

	service.Stop()

	// Assert
	assert.Len(s.T(), processOrder, 3)
	// With sequential processing, messages should be processed in order
	assert.Equal(s.T(), "msg-0", processOrder[0])
	assert.Equal(s.T(), "msg-1", processOrder[1])
	assert.Equal(s.T(), "msg-2", processOrder[2])
}

func (s *MessageConcurrencySuite) TestGracefulShutdown_WithParallelProcessing() {
	// Arrange
	var processedCount int32
	processComplete := make(chan struct{})

	handler := HandlerFunc(func(ctx context.Context, msg *SQSMessage) error {
		<-processComplete
		atomic.AddInt32(&processedCount, 1)
		return nil
	})

	msgReceived := make(chan struct{})
	var receiveCount int32

	s.sqsClient.ReceiveMessageFunc = func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		count := atomic.AddInt32(&receiveCount, 1)
		if count == 1 {
			defer close(msgReceived)
			// Return 5 messages
			messages := make([]sqstypes.Message, 5)
			for i := 0; i < 5; i++ {
				messages[i] = sqstypes.Message{
					MessageId:     aws.String(fmt.Sprintf("msg-%d", i)),
					ReceiptHandle: aws.String(fmt.Sprintf("receipt-%d", i)),
					Body:          aws.String(`{"data":"test"}`),
					Attributes: map[string]string{
						string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "1",
					},
				}
			}
			return &sqs.ReceiveMessageOutput{Messages: messages}, nil
		}
		return &sqs.ReceiveMessageOutput{}, nil
	}

	config := ServiceConfig{
		QueueURL:           "http://localhost:4566/queue/test-standard",
		WaitTimeSeconds:    1,
		MessageConcurrency: 5,
		Logger:             s.logger,
	}
	service := NewService(s.sqsClient, nil, handler, config)

	// Act
	err := service.Start(context.Background())
	assert.NoError(s.T(), err)

	// Wait for messages to be received
	select {
	case <-msgReceived:
	case <-time.After(2 * time.Second):
		s.T().Fatal("timeout waiting for messages")
	}

	time.Sleep(100 * time.Millisecond)

	// Allow all messages to complete
	for i := 0; i < 5; i++ {
		processComplete <- struct{}{}
	}

	// Stop should wait for all goroutines to complete
	service.Stop()

	// Assert
	processed := atomic.LoadInt32(&processedCount)
	assert.Equal(s.T(), int32(5), processed, "all messages should be processed before shutdown completes")
}
