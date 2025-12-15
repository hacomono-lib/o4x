package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

// mockSQSClient is a mock implementation of SQSClient for testing
type mockSQSClient struct {
	messages         []sqstypes.Message
	receiveCallCount atomic.Int32
	deleteCallCount  atomic.Int32
	receiveDelay     time.Duration
	receiveError     error
	deleteError      error
	mu               sync.Mutex
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	m.receiveCallCount.Add(1)

	if m.receiveDelay > 0 {
		select {
		case <-time.After(m.receiveDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if m.receiveError != nil {
		return nil, m.receiveError
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.messages) == 0 {
		return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{}}, nil
	}

	// Return up to MaxNumberOfMessages
	batchSize := int(params.MaxNumberOfMessages)
	if batchSize > len(m.messages) {
		batchSize = len(m.messages)
	}
	if batchSize == 0 {
		batchSize = 1 // Default to 1 if not specified
	}

	messages := make([]sqstypes.Message, batchSize)
	copy(messages, m.messages[:batchSize])
	m.messages = m.messages[batchSize:]

	return &sqs.ReceiveMessageOutput{
		Messages: messages,
	}, nil
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.deleteCallCount.Add(1)
	if m.deleteError != nil {
		return nil, m.deleteError
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSClient) addMessage(messageID, body string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, sqstypes.Message{
		MessageId:     aws.String(messageID),
		Body:          aws.String(body),
		ReceiptHandle: aws.String(fmt.Sprintf("receipt-%s", messageID)),
	})
}

func (m *mockSQSClient) addMessageWithReceiveCount(messageID, body string, receiveCount int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	attributes := map[string]string{
		"ApproximateReceiveCount": fmt.Sprintf("%d", receiveCount),
	}
	m.messages = append(m.messages, sqstypes.Message{
		MessageId:       aws.String(messageID),
		Body:            aws.String(body),
		ReceiptHandle:   aws.String(fmt.Sprintf("receipt-%s", messageID)),
		Attributes:      attributes,
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"event_type": {
				DataType:    aws.String("String"),
				StringValue: aws.String("test.event"),
			},
		},
	})
}

// mockHandler is a mock implementation of Handler for testing
type mockHandler struct {
	handleFunc    func(context.Context, *consumer.SQSMessage) error
	callCount     atomic.Int32
	processedMsgs sync.Map // messageID -> count
}

func (h *mockHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	h.callCount.Add(1)
	if h.handleFunc != nil {
		return h.handleFunc(ctx, msg)
	}
	// Track processed messages
	count, _ := h.processedMsgs.LoadOrStore(msg.MessageID, &atomic.Int32{})
	count.(*atomic.Int32).Add(1)
	return nil
}

func (h *mockHandler) getCallCount() int32 {
	return h.callCount.Load()
}

// TestNewService_FIFOValidation tests that FIFO queues reject MessageConcurrency > 1
func TestNewService_FIFOValidation(t *testing.T) {
	tests := []struct {
		name               string
		queueURL           string
		messageConcurrency int
		expectError        bool
	}{
		{
			name:               "FIFO queue with MessageConcurrency=1 (valid)",
			queueURL:           "https://sqs.us-east-1.amazonaws.com/123456789012/test.fifo",
			messageConcurrency: 1,
			expectError:        false,
		},
		{
			name:               "FIFO queue with MessageConcurrency>1 (invalid)",
			queueURL:           "https://sqs.us-east-1.amazonaws.com/123456789012/test.fifo",
			messageConcurrency: 10,
			expectError:        true,
		},
		{
			name:               "Standard queue with MessageConcurrency>1 (valid)",
			queueURL:           "https://sqs.us-east-1.amazonaws.com/123456789012/test",
			messageConcurrency: 10,
			expectError:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockSQSClient{}
			mockHandler := &mockHandler{}

			cfg := consumer.DefaultServiceConfig(tt.queueURL)
			cfg.MessageConcurrency = tt.messageConcurrency

			service := consumer.NewService(mockClient, mockHandler, cfg)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- service.Start(ctx)
			}()

			// Give it time to validate
			time.Sleep(50 * time.Millisecond)

			select {
			case err := <-errCh:
				if tt.expectError && err == nil {
					t.Errorf("expected error for FIFO + MessageConcurrency>1, got nil")
				}
				if !tt.expectError && err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			default:
				// Service started successfully
				if tt.expectError {
					t.Error("expected error but service started")
				}
				service.Stop()
			}
		})
	}
}

// TestService_MessageProcessing tests basic message processing flow
func TestService_MessageProcessing(t *testing.T) {
	mockClient := &mockSQSClient{}
	mockHandler := &mockHandler{}

	// Add test messages
	mockClient.addMessage("msg-1", `{"event_type":"test.event","payload":{"data":"value"}}`)
	mockClient.addMessage("msg-2", `{"event_type":"test.event","payload":{"data":"value2"}}`)

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.WorkerCount = 1
	cfg.MessageConcurrency = 1

	service := consumer.NewService(mockClient, mockHandler, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for messages to be processed
	time.Sleep(500 * time.Millisecond)

	service.Stop()

	// Verify both messages were processed
	if count := mockHandler.getCallCount(); count != 2 {
		t.Errorf("expected 2 messages processed, got %d", count)
	}

	// Verify both messages were deleted from SQS
	if count := mockClient.deleteCallCount.Load(); count != 2 {
		t.Errorf("expected 2 delete calls, got %d", count)
	}
}

// TestService_ErrorHandling tests that handler errors don't stop the service
func TestService_ErrorHandling(t *testing.T) {
	mockClient := &mockSQSClient{}

	var callCount atomic.Int32
	mockHandler := &mockHandler{
		handleFunc: func(ctx context.Context, msg *consumer.SQSMessage) error {
			count := callCount.Add(1)
			if count == 1 {
				return errors.New("simulated error")
			}
			return nil
		},
	}

	mockClient.addMessage("msg-1", `{"event_type":"test.event","payload":{}}`)
	mockClient.addMessage("msg-2", `{"event_type":"test.event","payload":{}}`)

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.WorkerCount = 1

	service := consumer.NewService(mockClient, mockHandler, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
	service.Stop()

	// First message fails (not deleted), second succeeds (deleted)
	if count := mockClient.deleteCallCount.Load(); count != 1 {
		t.Errorf("expected 1 delete call (only successful message), got %d", count)
	}

	if count := callCount.Load(); count != 2 {
		t.Errorf("expected 2 handler calls, got %d", count)
	}
}

// TestService_MessageConcurrency tests parallel message processing
func TestService_MessageConcurrency(t *testing.T) {
	mockClient := &mockSQSClient{}

	var processingCount atomic.Int32
	var maxConcurrent atomic.Int32

	mockHandler := &mockHandler{
		handleFunc: func(ctx context.Context, msg *consumer.SQSMessage) error {
			current := processingCount.Add(1)
			defer processingCount.Add(-1)

			// Track max concurrent
			for {
				max := maxConcurrent.Load()
				if current <= max || maxConcurrent.CompareAndSwap(max, current) {
					break
				}
			}

			// Simulate processing time
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	// Add multiple messages
	for i := 0; i < 10; i++ {
		mockClient.addMessage(fmt.Sprintf("msg-%d", i), `{"event_type":"test.event","payload":{}}`)
	}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.WorkerCount = 1
	cfg.MessageConcurrency = 5 // Process 5 messages concurrently

	service := consumer.NewService(mockClient, mockHandler, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	time.Sleep(2 * time.Second)
	service.Stop()

	// Verify that we achieved some level of concurrency
	maxConcurrentValue := maxConcurrent.Load()
	if maxConcurrentValue < 2 {
		t.Errorf("expected concurrent processing (maxConcurrentValue >= 2), got maxConcurrentValue=%d", maxConcurrentValue)
	}

	t.Logf("Max concurrent processing: %d", maxConcurrentValue)
}

// TestService_GracefulShutdown tests that shutdown waits for in-flight messages
func TestService_GracefulShutdown(t *testing.T) {
	mockClient := &mockSQSClient{}

	var processingStarted atomic.Bool
	var processingCompleted atomic.Bool

	mockHandler := &mockHandler{
		handleFunc: func(ctx context.Context, msg *consumer.SQSMessage) error {
			processingStarted.Store(true)
			time.Sleep(500 * time.Millisecond)
			processingCompleted.Store(true)
			return nil
		},
	}

	mockClient.addMessage("msg-1", `{"event_type":"test.event","payload":{}}`)

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.ShutdownTimeout = 2 * time.Second

	service := consumer.NewService(mockClient, mockHandler, cfg)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for processing to start
	for !processingStarted.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	// Trigger shutdown while message is processing
	cancel()
	service.Stop()

	// Verify processing completed
	if !processingCompleted.Load() {
		t.Error("expected message processing to complete during graceful shutdown")
	}

	// Verify message was deleted
	if count := mockClient.deleteCallCount.Load(); count != 1 {
		t.Errorf("expected message to be deleted after completion, got %d deletes", count)
	}
}

// TestService_HealthStatus tests health check functionality
func TestService_HealthStatus(t *testing.T) {
	mockClient := &mockSQSClient{}
	mockHandler := &mockHandler{}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	service := consumer.NewService(mockClient, mockHandler, cfg)

	// Before start
	status := service.HealthStatus()
	if status.IsHealthy() {
		t.Error("service should not be healthy before start")
	}

	// After start
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	status = service.HealthStatus()
	if !status.IsHealthy() {
		t.Error("service should be healthy after start")
	}

	// After stop
	service.Stop()
	time.Sleep(100 * time.Millisecond)

	status = service.HealthStatus()
	if status.IsHealthy() {
		t.Error("service should not be healthy after stop")
	}
}

// TestService_IsStale tests staleness detection
func TestService_IsStale(t *testing.T) {
	mockClient := &mockSQSClient{}
	mockHandler := &mockHandler{}

	mockClient.addMessage("msg-1", `{"event_type":"test.event","payload":{}}`)

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	service := consumer.NewService(mockClient, mockHandler, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for message to be processed
	time.Sleep(500 * time.Millisecond)

	status := service.HealthStatus()

	// Should not be stale immediately after processing
	if status.IsStale(1 * time.Second) {
		t.Error("service should not be stale immediately after processing")
	}

	// Should be stale after enough time
	time.Sleep(2 * time.Second)
	status = service.HealthStatus()
	if !status.IsStale(1 * time.Second) {
		t.Error("service should be stale after threshold")
	}

	service.Stop()
}

// TestService_ReceiveError tests handling of SQS receive errors
func TestService_ReceiveError(t *testing.T) {
	mockClient := &mockSQSClient{
		receiveError: errors.New("simulated SQS error"),
	}
	mockHandler := &mockHandler{}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")

	service := consumer.NewService(mockClient, mockHandler, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
	service.Stop()

	// Verify no handler calls (receive failed)
	if count := mockHandler.getCallCount(); count != 0 {
		t.Errorf("expected no handler calls on receive error, got %d", count)
	}

	// Verify receive was attempted
	if count := mockClient.receiveCallCount.Load(); count < 1 {
		t.Error("expected at least one receive attempt")
	}
}

// TestService_IsRunning tests the IsRunning() method
func TestService_IsRunning(t *testing.T) {
	mockClient := &mockSQSClient{}
	mockHandler := &mockHandler{}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")

	service := consumer.NewService(mockClient, mockHandler, cfg)

	// Test before start
	if service.IsRunning() {
		t.Error("service should not be running before Start()")
	}

	// Start service
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for service to start
	time.Sleep(100 * time.Millisecond)

	// Test while running
	if !service.IsRunning() {
		t.Error("service should be running after Start()")
	}

	// Stop service
	cancel()
	service.Stop()

	// Wait for stop
	time.Sleep(100 * time.Millisecond)

	// Test after stop
	if service.IsRunning() {
		t.Error("service should not be running after Stop()")
	}
}

// TestService_Hooks_OnMessageDead tests the OnMessageDead hook
func TestService_Hooks_OnMessageDead(t *testing.T) {
	mockClient := &mockSQSClient{}
	mockHandler := &mockHandler{
		handleFunc: func(ctx context.Context, msg *consumer.SQSMessage) error {
			return errors.New("handler error")
		},
	}

	var deadCalled atomic.Bool
	var deadMsg *consumer.SQSMessage

	hooks := &consumer.Hooks{
		OnMessageDead: func(ctx context.Context, msg *consumer.SQSMessage, err error) {
			deadCalled.Store(true)
			deadMsg = msg
		},
	}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.MaxRetries = 2
	cfg.Hooks = hooks

	service := consumer.NewService(mockClient, mockHandler, cfg)

	// Add a message that has exceeded max retries
	messageID := "msg-dead"
	mockClient.addMessageWithReceiveCount(messageID, `{"event_type":"test.event"}`, 3) // ReceiveCount > MaxRetries

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for message processing
	time.Sleep(500 * time.Millisecond)
	service.Stop()

	// Verify OnMessageDead was called
	if !deadCalled.Load() {
		t.Error("expected OnMessageDead to be called")
	}
	if deadMsg == nil || deadMsg.MessageID != messageID {
		t.Errorf("expected dead message with ID %s, got %v", messageID, deadMsg)
	}
}

// TestService_Hooks_OnDeleteFailure tests the OnDeleteFailure hook
func TestService_Hooks_OnDeleteFailure(t *testing.T) {
	mockClient := &mockSQSClient{
		deleteError: errors.New("delete failed"),
	}
	mockHandler := &mockHandler{
		handleFunc: func(ctx context.Context, msg *consumer.SQSMessage) error {
			return nil // Success
		},
	}

	var deleteFailureCalled atomic.Bool
	var deleteFailureMsg *consumer.SQSMessage

	hooks := &consumer.Hooks{
		OnDeleteFailure: func(ctx context.Context, msg *consumer.SQSMessage, err error) {
			deleteFailureCalled.Store(true)
			deleteFailureMsg = msg
		},
	}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.Hooks = hooks

	service := consumer.NewService(mockClient, mockHandler, cfg)

	// Add a message
	messageID := "msg-delete-fail"
	mockClient.addMessage(messageID, `{"event_type":"test.event"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for message processing
	time.Sleep(500 * time.Millisecond)
	service.Stop()

	// Verify OnDeleteFailure was called
	if !deleteFailureCalled.Load() {
		t.Error("expected OnDeleteFailure to be called")
	}
	if deleteFailureMsg == nil || deleteFailureMsg.MessageID != messageID {
		t.Errorf("expected delete failure message with ID %s, got %v", messageID, deleteFailureMsg)
	}
}

// TestDefaultServiceConfig tests all branches of DefaultServiceConfig
func TestDefaultServiceConfig(t *testing.T) {
	// Test with FIFO queue
	fifoConfig := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test.fifo")
	if fifoConfig.MessageConcurrency != 1 {
		t.Errorf("FIFO queue should have MessageConcurrency=1, got %d", fifoConfig.MessageConcurrency)
	}

	// Test with Standard queue
	standardConfig := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	if standardConfig.MessageConcurrency != 1 {
		t.Errorf("Standard queue should have MessageConcurrency=1, got %d", standardConfig.MessageConcurrency)
	}

	// Verify other defaults
	if standardConfig.WorkerCount != 1 {
		t.Errorf("expected WorkerCount=1, got %d", standardConfig.WorkerCount)
	}
	if standardConfig.WaitTimeSeconds != 20 {
		t.Errorf("expected WaitTimeSeconds=20, got %d", standardConfig.WaitTimeSeconds)
	}
	if standardConfig.MaxRetries != 5 {
		t.Errorf("expected MaxRetries=5, got %d", standardConfig.MaxRetries)
	}
	if standardConfig.Logger == nil {
		t.Error("expected Logger to be non-nil")
	}
	if standardConfig.MaxNumberOfMessages != 10 {
		t.Errorf("expected MaxNumberOfMessages=10, got %d", standardConfig.MaxNumberOfMessages)
	}
}

// TestService_NewService_AllValidations tests all validation paths in NewService
func TestService_NewService_AllValidations(t *testing.T) {
	mockClient := &mockSQSClient{}
	mockHandler := &mockHandler{}

	// Test FIFO with MessageConcurrency > 1
	cfg := consumer.ServiceConfig{
		QueueURL:           "https://sqs.us-east-1.amazonaws.com/123456789012/test.fifo",
		WorkerCount:        1,
		MessageConcurrency: 10,
		WaitTimeSeconds:    20,
		MaxRetries:         5,
	}

	service := consumer.NewService(mockClient, mockHandler, cfg)
	ctx := context.Background()
	err := service.Start(ctx)
	if err == nil {
		t.Error("expected error for FIFO queue with MessageConcurrency > 1")
	}
	if err != nil && !strings.Contains(err.Error(), "MessageConcurrency") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestService_Hooks_Panic tests panic recovery in hooks
func TestService_Hooks_Panic(t *testing.T) {
	mockClient := &mockSQSClient{}
	mockHandler := &mockHandler{}

	hooks := &consumer.Hooks{
		OnConsumeStart: func(ctx context.Context, msg *consumer.SQSMessage) {
			panic("OnConsumeStart panic")
		},
		OnConsumeSuccess: func(ctx context.Context, msg *consumer.SQSMessage, duration time.Duration) {
			panic("OnConsumeSuccess panic")
		},
		OnConsumeFailure: func(ctx context.Context, msg *consumer.SQSMessage, err error, duration time.Duration, retryable bool) {
			panic("OnConsumeFailure panic")
		},
	}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.Hooks = hooks

	service := consumer.NewService(mockClient, mockHandler, cfg)

	// Add a message
	mockClient.addMessage("msg-panic", `{"event_type":"test.event"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for message processing
	time.Sleep(500 * time.Millisecond)
	service.Stop()

	// Verify message was processed despite panics
	if count := mockHandler.getCallCount(); count < 1 {
		t.Error("expected handler to be called despite hook panics")
	}
}

// TestService_Hooks_OnMessageDead_Panic tests panic in OnMessageDead hook
func TestService_Hooks_OnMessageDead_Panic(t *testing.T) {
	mockClient := &mockSQSClient{}
	mockHandler := &mockHandler{
		handleFunc: func(ctx context.Context, msg *consumer.SQSMessage) error {
			return errors.New("handler error")
		},
	}

	hooks := &consumer.Hooks{
		OnMessageDead: func(ctx context.Context, msg *consumer.SQSMessage, err error) {
			panic("OnMessageDead panic")
		},
	}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.MaxRetries = 2
	cfg.Hooks = hooks

	service := consumer.NewService(mockClient, mockHandler, cfg)

	// Add a message that has exceeded max retries
	mockClient.addMessageWithReceiveCount("msg-dead-panic", `{"event_type":"test.event"}`, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for message processing
	time.Sleep(500 * time.Millisecond)
	service.Stop()

	// Verify service continues despite panic
	if mockClient.deleteCallCount.Load() == 0 {
		t.Error("expected delete to be called despite hook panic")
	}
}

// TestService_Hooks_OnDeleteFailure_Panic tests panic in OnDeleteFailure hook
func TestService_Hooks_OnDeleteFailure_Panic(t *testing.T) {
	mockClient := &mockSQSClient{
		deleteError: errors.New("delete failed"),
	}
	mockHandler := &mockHandler{}

	hooks := &consumer.Hooks{
		OnDeleteFailure: func(ctx context.Context, msg *consumer.SQSMessage, err error) {
			panic("OnDeleteFailure panic")
		},
	}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.Hooks = hooks

	service := consumer.NewService(mockClient, mockHandler, cfg)

	// Add a message
	mockClient.addMessage("msg-delete-panic", `{"event_type":"test.event"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for message processing
	time.Sleep(500 * time.Millisecond)
	service.Stop()

	// Verify service continues despite panic
	if count := mockHandler.getCallCount(); count < 1 {
		t.Error("expected handler to be called despite hook panic")
	}
}

// TestService_Stop_MultipleWorkers tests stopping with multiple workers
func TestService_Stop_MultipleWorkers(t *testing.T) {
	mockClient := &mockSQSClient{
		receiveDelay: 100 * time.Millisecond,
	}
	mockHandler := &mockHandler{}

	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")
	cfg.WorkerCount = 5

	service := consumer.NewService(mockClient, mockHandler, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = service.Start(ctx)
	}()

	// Wait for all workers to start
	time.Sleep(200 * time.Millisecond)

	// Stop service
	service.Stop()

	// Verify service stopped
	if service.IsRunning() {
		t.Error("service should be stopped")
	}
}

// TestDefaultServiceConfig_VisibilityTimeout tests default visibility timeout calculation
func TestDefaultServiceConfig_VisibilityTimeout(t *testing.T) {
	cfg := consumer.DefaultServiceConfig("https://sqs.us-east-1.amazonaws.com/123456789012/test")

	// Verify visibility timeout is set (should be > 0)
	if cfg.VisibilityTimeout <= 0 {
		t.Errorf("expected positive VisibilityTimeout, got %d", cfg.VisibilityTimeout)
	}
}
