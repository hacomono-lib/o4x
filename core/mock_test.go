package core

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// MockOutboxRepository is a test double for OutboxRepository
type MockOutboxRepository struct {
	mu       sync.Mutex
	messages map[string]*Outbox

	// Behavior controls
	InsertFunc                     func(ctx context.Context, params OutboxInsertParams) (*Outbox, error)
	FetchAndLockToPublishingFunc   func(ctx context.Context) (*Outbox, error)
	UpdateToPublishedFunc          func(ctx context.Context, id string) error
	UpdateToFailedFunc             func(ctx context.Context, id string, errMsg string) error
	UpdateToDeadFunc               func(ctx context.Context, id string, errMsg string) error
	RequeueFailedFunc              func(ctx context.Context) (int64, error)
	GetByIDFunc                    func(ctx context.Context, id string) (*Outbox, error)
	GetByIdempotencyKeyFunc        func(ctx context.Context, eventType, idempotencyKey string) (*Outbox, error)
	ReviveStuckPublishingFunc      func(ctx context.Context) (int64, error)
	FetchLockAndMarkPublishingFunc func(ctx context.Context, limit int) ([]*Outbox, error)
	UpdateBatchToPublishedFunc     func(ctx context.Context, ids []string) (int64, error)

	// Call tracking
	InsertCalls                     []OutboxInsertParams
	FetchAndLockToPublishingCalls   int
	UpdateToPublishedCalls          []string
	UpdateToFailedCalls             []updateToFailedCall
	UpdateToDeadCalls               []updateToDeadCall
	RequeueFailedCalls              int
	ReviveStuckPublishingCalls      int
	FetchLockAndMarkPublishingCalls []int
	UpdateBatchToPublishedCalls     [][]string
}

type updateToFailedCall struct {
	ID     string
	ErrMsg string
}

type updateToDeadCall struct {
	ID     string
	ErrMsg string
}

func NewMockOutboxRepository() *MockOutboxRepository {
	return &MockOutboxRepository{
		messages: make(map[string]*Outbox),
	}
}

func (m *MockOutboxRepository) Insert(ctx context.Context, params OutboxInsertParams) (*Outbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InsertCalls = append(m.InsertCalls, params)
	if m.InsertFunc != nil {
		return m.InsertFunc(ctx, params)
	}
	msg := &Outbox{
		ID:             GenerateID(),
		EventType:      params.EventType,
		Payload:        params.Payload,
		IdempotencyKey: params.IdempotencyKey,
		Status:         OutboxStatusEnqueued,
		MaxAttempts:     params.MaxAttempts,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	m.messages[msg.ID] = msg
	return msg, nil
}

func (m *MockOutboxRepository) FetchAndLockToPublishing(ctx context.Context) (*Outbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FetchAndLockToPublishingCalls++
	if m.FetchAndLockToPublishingFunc != nil {
		return m.FetchAndLockToPublishingFunc(ctx)
	}
	for _, msg := range m.messages {
		if msg.Status == OutboxStatusEnqueued {
			msg.Status = OutboxStatusPublishing
			msg.UpdatedAt = time.Now()
			return msg, nil
		}
	}
	return nil, ErrNoMessage
}

func (m *MockOutboxRepository) UpdateToPublished(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UpdateToPublishedCalls = append(m.UpdateToPublishedCalls, id)
	if m.UpdateToPublishedFunc != nil {
		return m.UpdateToPublishedFunc(ctx, id)
	}
	if msg, ok := m.messages[id]; ok {
		msg.Status = OutboxStatusPublished
		msg.UpdatedAt = time.Now()
	}
	return nil
}

func (m *MockOutboxRepository) UpdateToFailed(ctx context.Context, id, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UpdateToFailedCalls = append(m.UpdateToFailedCalls, updateToFailedCall{ID: id, ErrMsg: errMsg})
	if m.UpdateToFailedFunc != nil {
		return m.UpdateToFailedFunc(ctx, id, errMsg)
	}
	if msg, ok := m.messages[id]; ok {
		msg.Status = OutboxStatusFailed
		msg.ErrorMessage = &errMsg
		msg.AttemptCount++
		msg.UpdatedAt = time.Now()
	}
	return nil
}

func (m *MockOutboxRepository) UpdateToDead(ctx context.Context, id, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UpdateToDeadCalls = append(m.UpdateToDeadCalls, updateToDeadCall{ID: id, ErrMsg: errMsg})
	if m.UpdateToDeadFunc != nil {
		return m.UpdateToDeadFunc(ctx, id, errMsg)
	}
	if msg, ok := m.messages[id]; ok {
		msg.Status = OutboxStatusDead
		msg.ErrorMessage = &errMsg
		msg.UpdatedAt = time.Now()
	}
	return nil
}

func (m *MockOutboxRepository) RequeueFailed(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RequeueFailedCalls++
	if m.RequeueFailedFunc != nil {
		return m.RequeueFailedFunc(ctx)
	}
	var count int64
	for _, msg := range m.messages {
		if msg.Status == OutboxStatusFailed && msg.AttemptCount < msg.MaxAttempts {
			msg.Status = OutboxStatusEnqueued
			msg.UpdatedAt = time.Now()
			count++
		}
	}
	return count, nil
}

func (m *MockOutboxRepository) GetByID(ctx context.Context, id string) (*Outbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.GetByIDFunc != nil {
		return m.GetByIDFunc(ctx, id)
	}
	if msg, ok := m.messages[id]; ok {
		return msg, nil
	}
	return nil, ErrNotFound
}

func (m *MockOutboxRepository) GetByIdempotencyKey(ctx context.Context, eventType, idempotencyKey string) (*Outbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.GetByIdempotencyKeyFunc != nil {
		return m.GetByIdempotencyKeyFunc(ctx, eventType, idempotencyKey)
	}
	for _, msg := range m.messages {
		if msg.EventType == eventType && msg.IdempotencyKey == idempotencyKey {
			return msg, nil
		}
	}
	return nil, ErrNotFound
}

func (m *MockOutboxRepository) ReviveStuckPublishing(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReviveStuckPublishingCalls++
	if m.ReviveStuckPublishingFunc != nil {
		return m.ReviveStuckPublishingFunc(ctx)
	}
	var count int64
	for _, msg := range m.messages {
		if msg.Status != OutboxStatusPublishing {
			continue
		}
		msg.Status = OutboxStatusFailed
		errMsg := "revived from PUBLISHING (crash recovery)"
		msg.ErrorMessage = &errMsg
		msg.AttemptCount++ // Increment to enforce max_attempts limit
		msg.UpdatedAt = time.Now()
		count++
	}
	return count, nil
}

// BatchOutboxRepository methods
func (m *MockOutboxRepository) FetchLockAndMarkPublishing(ctx context.Context, limit int) ([]*Outbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FetchLockAndMarkPublishingCalls = append(m.FetchLockAndMarkPublishingCalls, limit)
	if m.FetchLockAndMarkPublishingFunc != nil {
		return m.FetchLockAndMarkPublishingFunc(ctx, limit)
	}
	var result []*Outbox
	for _, msg := range m.messages {
		if msg.Status == OutboxStatusEnqueued && len(result) < limit {
			msg.Status = OutboxStatusPublishing
			msg.UpdatedAt = time.Now()
			result = append(result, msg)
		}
	}
	return result, nil
}

func (m *MockOutboxRepository) UpdateBatchToPublished(ctx context.Context, ids []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UpdateBatchToPublishedCalls = append(m.UpdateBatchToPublishedCalls, ids)
	if m.UpdateBatchToPublishedFunc != nil {
		return m.UpdateBatchToPublishedFunc(ctx, ids)
	}
	var count int64
	for _, id := range ids {
		if msg, ok := m.messages[id]; ok && msg.Status == OutboxStatusPublishing {
			msg.Status = OutboxStatusPublished
			msg.UpdatedAt = time.Now()
			count++
		}
	}
	return count, nil
}

// AddMessage adds a message directly to the mock repository
func (m *MockOutboxRepository) AddMessage(msg *Outbox) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages[msg.ID] = msg
}

// GetMessage retrieves a message from the mock repository
func (m *MockOutboxRepository) GetMessage(id string) *Outbox {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messages[id]
}

// MockPublisher is a test double for Publisher
type MockPublisher struct {
	mu sync.Mutex

	// Behavior controls
	PublishFunc      func(ctx context.Context, msg *Outbox) error
	PublishBatchFunc func(ctx context.Context, msgs []*Outbox) []PublishResult
	MaxBatchSizeVal  int

	// Call tracking
	PublishCalls      []*Outbox
	PublishBatchCalls [][]*Outbox
}

func NewMockPublisher() *MockPublisher {
	return &MockPublisher{
		MaxBatchSizeVal: 10,
	}
}

func (m *MockPublisher) Publish(ctx context.Context, msg *Outbox) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PublishCalls = append(m.PublishCalls, msg)
	if m.PublishFunc != nil {
		return m.PublishFunc(ctx, msg)
	}
	return nil
}

func (m *MockPublisher) PublishBatch(ctx context.Context, msgs []*Outbox) []PublishResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PublishBatchCalls = append(m.PublishBatchCalls, msgs)
	if m.PublishBatchFunc != nil {
		return m.PublishBatchFunc(ctx, msgs)
	}
	results := make([]PublishResult, len(msgs))
	for i, msg := range msgs {
		results[i] = PublishResult{
			OutboxID:  msg.ID,
			Success:   true,
			MessageID: "mock-message-id-" + msg.ID,
		}
	}
	return results
}

func (m *MockPublisher) MaxBatchSize() int {
	return m.MaxBatchSizeVal
}

// Helper functions for tests
func createTestOutbox(eventType string, payload interface{}) *Outbox {
	data, _ := json.Marshal(payload)
	return &Outbox{
		ID:             GenerateID(),
		EventType:      eventType,
		Payload:        data,
		IdempotencyKey: GenerateID(),
		Status:         OutboxStatusEnqueued,
		MaxAttempts:     3,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

func createTestOutboxWithRetry(eventType string, payload interface{}, attemptCount, maxAttempts int) *Outbox {
	msg := createTestOutbox(eventType, payload)
	msg.AttemptCount = attemptCount
	msg.MaxAttempts = maxAttempts
	return msg
}
