package gorm

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/hacomono-lib/o4x/core"
)

// testDatabaseURL returns the test database URL from environment or default
func testDatabaseURL() string {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		// Use o4x_test database for tests (created by init-test-db.sql in docker-compose)
		url = "postgres://postgres:postgres@localhost:15432/o4x_test?sslmode=disable"
	}
	return url
}

// OutboxRepositorySuite tests OutboxRepository with real PostgreSQL database
type OutboxRepositorySuite struct {
	suite.Suite
	db        *gorm.DB
	repo      *OutboxRepository
	tableName string
}

func TestOutboxRepositorySuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(OutboxRepositorySuite))
}

func (s *OutboxRepositorySuite) SetupSuite() {
	db, err := gorm.Open(postgres.Open(testDatabaseURL()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		s.T().Skipf("failed to connect to test database: %v (ensure docker-compose is running)", err)
	}
	s.db = db
	s.tableName = "outbox"
	s.repo = NewOutboxRepository(db)

	// Clean up table before starting suite to prevent interference from previous test runs.
	// Note: This suite shares the 'outbox' table with contrib/pgx tests.
	// To avoid flaky tests, use `make test` (with -p 1) to run packages sequentially.
	_ = s.db.Exec("DELETE FROM " + s.tableName)
}

func (s *OutboxRepositorySuite) TearDownSuite() {
	if s.db != nil {
		sqlDB, err := s.db.DB()
		if err == nil {
			sqlDB.Close()
		}
	}
}

func (s *OutboxRepositorySuite) SetupTest() {
	// Clean up outbox table before each test
	result := s.db.Exec("DELETE FROM " + s.tableName)
	s.Require().NoError(result.Error)
}

// Helper to create test payload
func (s *OutboxRepositorySuite) createPayload(data map[string]interface{}) json.RawMessage {
	b, _ := json.Marshal(data)
	return b
}

func (s *OutboxRepositorySuite) TestInsert_CreatesNewOutboxMessage() {
	// Arrange
	ctx := context.Background()
	params := core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-1",
		MaxAttempts:    5,
	}

	// Act
	msg, err := s.repo.Insert(ctx, params)

	// Assert
	assert.NoError(s.T(), err)
	assert.NotEmpty(s.T(), msg.ID)
	assert.Equal(s.T(), "test.event", msg.EventType)
	assert.Equal(s.T(), "test-idem-key-1", msg.IdempotencyKey)
	assert.Equal(s.T(), core.OutboxStatusEnqueued, msg.Status)
	assert.Equal(s.T(), 1, msg.AttemptCount)
	assert.Equal(s.T(), 5, msg.MaxAttempts)
}

func (s *OutboxRepositorySuite) TestFetchAndLockToPublishing_ReturnsEnqueuedMessageAndMarksPublishing() {
	// Arrange
	ctx := context.Background()
	inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-2",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Act
	msg, err := s.repo.FetchAndLockToPublishing(ctx)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), inserted.ID, msg.ID)
	assert.Equal(s.T(), core.OutboxStatusPublishing, msg.Status) // Already marked as PUBLISHING

	// Verify in database
	dbMsg, _ := s.repo.GetByID(ctx, inserted.ID)
	assert.Equal(s.T(), core.OutboxStatusPublishing, dbMsg.Status)
}

func (s *OutboxRepositorySuite) TestFetchAndLockToPublishing_ReturnsErrNoMessageWhenEmpty() {
	// Arrange
	ctx := context.Background()
	// Table is empty after SetupTest

	// Act
	msg, err := s.repo.FetchAndLockToPublishing(ctx)

	// Assert
	assert.ErrorIs(s.T(), err, core.ErrNoMessage)
	assert.Nil(s.T(), msg)
}

func (s *OutboxRepositorySuite) TestUpdateToPublished_ChangesStatus() {
	// Arrange
	ctx := context.Background()
	_, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-4",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Fetch and lock to PUBLISHING first
	locked, err := s.repo.FetchAndLockToPublishing(ctx)
	s.Require().NoError(err)

	// Act
	err = s.repo.UpdateToPublished(ctx, locked.ID)

	// Assert
	assert.NoError(s.T(), err)
	msg, _ := s.repo.GetByID(ctx, locked.ID)
	assert.Equal(s.T(), core.OutboxStatusPublished, msg.Status)
}

func (s *OutboxRepositorySuite) TestUpdateToFailed_ChangesStatusAndIncrementsRetryCount() {
	// Arrange
	ctx := context.Background()
	_, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-5",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Fetch and lock to PUBLISHING first
	locked, err := s.repo.FetchAndLockToPublishing(ctx)
	s.Require().NoError(err)

	// Act
	err = s.repo.UpdateToFailed(ctx, locked.ID, "test error message")

	// Assert
	assert.NoError(s.T(), err)
	msg, _ := s.repo.GetByID(ctx, locked.ID)
	assert.Equal(s.T(), core.OutboxStatusFailed, msg.Status)
	assert.Equal(s.T(), 2, msg.AttemptCount)
	assert.NotNil(s.T(), msg.ErrorMessage)
	assert.Equal(s.T(), "test error message", *msg.ErrorMessage)
	assert.NotNil(s.T(), msg.NextRetryAt)
}

func (s *OutboxRepositorySuite) TestUpdateToDead_ChangesStatus() {
	// Arrange
	ctx := context.Background()
	_, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-6",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Fetch and lock to PUBLISHING first
	locked, err := s.repo.FetchAndLockToPublishing(ctx)
	s.Require().NoError(err)

	// Act
	err = s.repo.UpdateToDead(ctx, locked.ID, "permanent error")

	// Assert
	assert.NoError(s.T(), err)
	msg, _ := s.repo.GetByID(ctx, locked.ID)
	assert.Equal(s.T(), core.OutboxStatusDead, msg.Status)
	assert.NotNil(s.T(), msg.ErrorMessage)
	assert.Equal(s.T(), "permanent error", *msg.ErrorMessage)
}

func (s *OutboxRepositorySuite) TestRequeueFailed_MovesFailedToEnqueuedWithBackoff() {
	// Arrange
	ctx := context.Background()

	// Insert and mark as failed
	inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-7",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)
	// Set to PUBLISHING before calling UpdateToFailed
	result := s.db.Exec("UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = ?", inserted.ID)
	s.Require().NoError(result.Error)
	err = s.repo.UpdateToFailed(ctx, inserted.ID, "temporary error")
	s.Require().NoError(err)

	// Set updated_at and next_retry_at to past to make it eligible for requeue
	result = s.db.Exec("UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '10 seconds', next_retry_at = NOW() - INTERVAL '1 second' WHERE id = ?", inserted.ID)
	s.Require().NoError(result.Error)

	// Act - use short base interval so message is eligible
	count, err := s.repo.RequeueFailed(ctx)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), count)
	msg, _ := s.repo.GetByID(ctx, inserted.ID)
	assert.Equal(s.T(), core.OutboxStatusEnqueued, msg.Status)
}

func (s *OutboxRepositorySuite) TestRequeueFailed_RespectsExponentialBackoff() {
	// Arrange
	ctx := context.Background()

	// Insert and fail multiple times
	inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-8",
		MaxAttempts:    10,
	})
	s.Require().NoError(err)

	// Fail 3 times (retry_count = 3, backoff = 1s * 2^2 = 4s)
	var result *gorm.DB
	for i := 0; i < 3; i++ {
		// Set to PUBLISHING before calling UpdateToFailed
		result = s.db.Exec("UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = ?", inserted.ID)
		s.Require().NoError(result.Error)
		err = s.repo.UpdateToFailed(ctx, inserted.ID, "temporary error")
		s.Require().NoError(err)
		result = s.db.Exec("UPDATE "+s.tableName+" SET status = 'ENQUEUED' WHERE id = ?", inserted.ID)
		s.Require().NoError(result.Error)
	}
	// Set to PUBLISHING before final UpdateToFailed
	result = s.db.Exec("UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = ?", inserted.ID)
	s.Require().NoError(result.Error)
	err = s.repo.UpdateToFailed(ctx, inserted.ID, "temporary error")
	s.Require().NoError(err)

	// Set updated_at to only 2 seconds ago (less than 4s backoff)
	result = s.db.Exec("UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '2 seconds' WHERE id = ?", inserted.ID)
	s.Require().NoError(result.Error)

	// Act
	count, err := s.repo.RequeueFailed(ctx)

	// Assert - should not be requeued due to backoff
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(0), count)
	msg, _ := s.repo.GetByID(ctx, inserted.ID)
	assert.Equal(s.T(), core.OutboxStatusFailed, msg.Status)
}

func (s *OutboxRepositorySuite) TestRequeueFailed_DoesNotRequeueMaxRetriesExceeded() {
	// Arrange
	ctx := context.Background()

	// Insert with maxRetries=1 and fail once
	inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-9",
		MaxAttempts:    1,
	})
	s.Require().NoError(err)
	// Set to PUBLISHING before calling UpdateToFailed
	result := s.db.Exec("UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = ?", inserted.ID)
	s.Require().NoError(result.Error)
	err = s.repo.UpdateToFailed(ctx, inserted.ID, "error")
	s.Require().NoError(err)

	// Set updated_at to past
	result = s.db.Exec("UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '10 seconds' WHERE id = ?", inserted.ID)
	s.Require().NoError(result.Error)

	// Act
	count, err := s.repo.RequeueFailed(ctx)

	// Assert - should not be requeued because retry_count >= max_retries
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(0), count)
	msg, _ := s.repo.GetByID(ctx, inserted.ID)
	assert.Equal(s.T(), core.OutboxStatusFailed, msg.Status)
}

func (s *OutboxRepositorySuite) TestGetByID_ReturnsMessage() {
	// Arrange
	ctx := context.Background()
	inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-10",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Act
	msg, err := s.repo.GetByID(ctx, inserted.ID)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), inserted.ID, msg.ID)
	assert.Equal(s.T(), "test.event", msg.EventType)
}

func (s *OutboxRepositorySuite) TestGetByID_ReturnsErrNotFoundForNonExistent() {
	// Arrange
	ctx := context.Background()

	// Act
	msg, err := s.repo.GetByID(ctx, "00000000-0000-0000-0000-000000000000")

	// Assert
	assert.ErrorIs(s.T(), err, core.ErrNotFound)
	assert.Nil(s.T(), msg)
}

func (s *OutboxRepositorySuite) TestGetByIdempotencyKey_ReturnsMessage() {
	// Arrange
	ctx := context.Background()
	inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-11",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Act
	msg, err := s.repo.GetByIdempotencyKey(ctx, "test.event", "test-idem-key-11")

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), inserted.ID, msg.ID)
}

func (s *OutboxRepositorySuite) TestGetByIdempotencyKey_ReturnsErrNotFoundForNonExistent() {
	// Arrange
	ctx := context.Background()

	// Act
	msg, err := s.repo.GetByIdempotencyKey(ctx, "test.event", "non-existent-key")

	// Assert
	assert.ErrorIs(s.T(), err, core.ErrNotFound)
	assert.Nil(s.T(), msg)
}

func (s *OutboxRepositorySuite) TestReviveStuckPublishing_MovesPublishingToFailed() {
	// Arrange
	ctx := context.Background()

	// Insert and set to PUBLISHING using FetchAndLockToPublishing
	inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-12",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Fetch and lock to make it PUBLISHING
	msg, err := s.repo.FetchAndLockToPublishing(ctx)
	s.Require().NoError(err)
	s.Require().Equal(inserted.ID, msg.ID)

	// Simulate stuck message by setting updated_at to 10 minutes ago
	err = s.db.Exec("UPDATE outbox SET updated_at = NOW() - INTERVAL '10 minutes' WHERE id = ?", msg.ID).Error
	s.Require().NoError(err)

	// Act
	count, err := s.repo.ReviveStuckPublishing(ctx)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), count)
	revivedMsg, _ := s.repo.GetByID(ctx, inserted.ID)
	assert.Equal(s.T(), core.OutboxStatusFailed, revivedMsg.Status)
	// Note: attempt_count is incremented during ReviveStuckPublishing
	// to enforce max_attempts limit and prevent infinite retries
	assert.Equal(s.T(), 2, revivedMsg.AttemptCount) // 1 → 2 (incremented)
	assert.NotNil(s.T(), revivedMsg.ErrorMessage)
	assert.Equal(s.T(), "revived from PUBLISHING (crash recovery)", *revivedMsg.ErrorMessage)
	assert.NotNil(s.T(), revivedMsg.NextRetryAt) // next_retry_at is set for exponential backoff
}

func (s *OutboxRepositorySuite) TestFetchLockAndMarkPublishing_AtomicallyLocksAndUpdates() {
	// Arrange
	ctx := context.Background()

	// Insert multiple messages
	for i := 0; i < 5; i++ {
		_, err := s.repo.Insert(ctx, core.OutboxInsertParams{
			EventType:      "test.event",
			Payload:        s.createPayload(map[string]interface{}{"index": i}),
			IdempotencyKey: "test-idem-key-batch-" + string(rune('a'+i)),
			MaxAttempts:    3,
		})
		s.Require().NoError(err)
	}

	// Act
	msgs, err := s.repo.FetchLockAndMarkPublishing(ctx, 3)

	// Assert
	assert.NoError(s.T(), err)
	assert.Len(s.T(), msgs, 3)
	for _, msg := range msgs {
		assert.Equal(s.T(), core.OutboxStatusPublishing, msg.Status)
	}
}

func (s *OutboxRepositorySuite) TestUpdateBatchToPublished_UpdatesMultipleMessages() {
	// Arrange
	ctx := context.Background()

	// Insert multiple messages
	for i := 0; i < 3; i++ {
		_, err := s.repo.Insert(ctx, core.OutboxInsertParams{
			EventType:      "test.event",
			Payload:        s.createPayload(map[string]interface{}{"index": i}),
			IdempotencyKey: "test-idem-key-batch2-" + string(rune('a'+i)),
			MaxAttempts:    3,
		})
		s.Require().NoError(err)
	}

	// Fetch and lock messages to make them PUBLISHING
	msgs, err := s.repo.FetchLockAndMarkPublishing(ctx, 3)
	s.Require().NoError(err)
	s.Require().Len(msgs, 3)

	var ids []string
	for _, msg := range msgs {
		ids = append(ids, msg.ID)
	}

	// Act
	count, err := s.repo.UpdateBatchToPublished(ctx, ids)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(len(ids)), count)
	for _, id := range ids {
		msg, _ := s.repo.GetByID(ctx, id)
		assert.Equal(s.T(), core.OutboxStatusPublished, msg.Status)
	}
}

func (s *OutboxRepositorySuite) TestDeleteOlderThan_DeletesOldMessages() {
	// Arrange
	ctx := context.Background()

	// Insert and publish a message
	inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-delete",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)
	// Set to PUBLISHING before calling UpdateToPublished
	result := s.db.Exec("UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = ?", inserted.ID)
	s.Require().NoError(result.Error)
	err = s.repo.UpdateToPublished(ctx, inserted.ID)
	s.Require().NoError(err)

	// Set updated_at to 2 days ago
	result = s.db.Exec("UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '2 days' WHERE id = ?", inserted.ID)
	s.Require().NoError(result.Error)

	// Act
	count, err := s.repo.DeleteOlderThan(ctx, []core.OutboxStatus{core.OutboxStatusPublished}, 24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), count)

	// Verify message is deleted
	_, err = s.repo.GetByID(ctx, inserted.ID)
	assert.ErrorIs(s.T(), err, core.ErrNotFound)
}

func (s *OutboxRepositorySuite) TestDeleteOlderThan_MultipleStatuses() {
	// Test that multiple statuses can be deleted in a single call
	ctx := context.Background()

	// Insert and publish a message
	published, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.published",
		Payload:        s.createPayload(map[string]interface{}{"key": "published"}),
		IdempotencyKey: "test-published-multi",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)
	result := s.db.Exec("UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = ?", published.ID)
	s.Require().NoError(result.Error)
	err = s.repo.UpdateToPublished(ctx, published.ID)
	s.Require().NoError(err)

	// Insert and mark as DEAD
	dead, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.dead",
		Payload:        s.createPayload(map[string]interface{}{"key": "dead"}),
		IdempotencyKey: "test-dead-multi",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)
	result = s.db.Exec("UPDATE "+s.tableName+" SET status = 'DEAD' WHERE id = ?", dead.ID)
	s.Require().NoError(result.Error)

	// Set both to 2 days ago
	result = s.db.Exec("UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '2 days' WHERE id IN (?, ?)", published.ID, dead.ID)
	s.Require().NoError(result.Error)

	// Act: Delete both PUBLISHED and DEAD messages older than 24 hours in a single call
	count, err := s.repo.DeleteOlderThan(ctx, []core.OutboxStatus{core.OutboxStatusPublished, core.OutboxStatusDead}, 24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(2), count, "Should delete both PUBLISHED and DEAD messages")

	// Verify both messages are deleted
	_, err = s.repo.GetByID(ctx, published.ID)
	assert.ErrorIs(s.T(), err, core.ErrNotFound, "Published message should be deleted")

	_, err = s.repo.GetByID(ctx, dead.ID)
	assert.ErrorIs(s.T(), err, core.ErrNotFound, "Dead message should be deleted")
}

func (s *OutboxRepositorySuite) TestWithTx_UsesTransactionForInsert() {
	// Arrange
	ctx := context.Background()
	tx := s.db.Begin()
	s.Require().NoError(tx.Error)
	defer tx.Rollback() //nolint:errcheck // rollback error is irrelevant in deferred cleanup

	txRepo := s.repo.WithTx(tx)

	// Act
	inserted, err := txRepo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-tx",
		MaxAttempts:    3,
	})
	assert.NoError(s.T(), err)

	// Before commit, should be visible within tx but not outside
	msgInTx, err := txRepo.GetByID(ctx, inserted.ID)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), msgInTx)

	// Rollback
	err = tx.Rollback().Error
	assert.NoError(s.T(), err)

	// Assert - message should not exist after rollback
	_, err = s.repo.GetByID(ctx, inserted.ID)
	assert.ErrorIs(s.T(), err, core.ErrNotFound)
}

func (s *OutboxRepositorySuite) TestInsertOutboxJSON_MarshalPayloadAndInserts() {
	// Arrange
	ctx := context.Background()
	type testPayload struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}
	payload := testPayload{Name: "test", Value: 42}

	// Act
	msg, err := s.repo.InsertOutboxJSON(ctx, "test.event", payload, "test-idem-key-json", 3)

	// Assert
	assert.NoError(s.T(), err)
	assert.NotEmpty(s.T(), msg.ID)
	assert.Equal(s.T(), "test.event", msg.EventType)

	// Verify payload was correctly marshaled
	var unmarshaled testPayload
	err = json.Unmarshal(msg.Payload, &unmarshaled)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "test", unmarshaled.Name)
	assert.Equal(s.T(), 42, unmarshaled.Value)
}

// WithCustomTableNameSuite tests repository with custom table name
type WithCustomTableNameSuite struct {
	suite.Suite
	db        *gorm.DB
	tableName string
}

func TestWithCustomTableNameSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(WithCustomTableNameSuite))
}

func (s *WithCustomTableNameSuite) SetupSuite() {
	db, err := gorm.Open(postgres.Open(testDatabaseURL()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		s.T().Skipf("failed to connect to test database: %v (ensure docker-compose is running)", err)
	}
	s.db = db
	s.tableName = "custom_outbox_gorm"

	// Create custom table for testing
	result := db.Exec(`
		CREATE TABLE IF NOT EXISTS custom_outbox_gorm (
			id UUID PRIMARY KEY,
			event_type TEXT NOT NULL,
			payload JSONB NOT NULL,
			metadata JSONB,
			idempotency_key TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'ENQUEUED',
			error_message TEXT,
			attempt_count INT NOT NULL DEFAULT 1,
			max_attempts INT NOT NULL DEFAULT 10,
			next_retry_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	s.Require().NoError(result.Error)
}

func (s *WithCustomTableNameSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Exec("DROP TABLE IF EXISTS " + s.tableName)
		sqlDB, err := s.db.DB()
		if err == nil {
			sqlDB.Close()
		}
	}
}

func (s *WithCustomTableNameSuite) SetupTest() {
	result := s.db.Exec("DELETE FROM " + s.tableName)
	s.Require().NoError(result.Error)
}

func (s *WithCustomTableNameSuite) TestWithOutboxTableName_UsesCustomTable() {
	// Arrange
	ctx := context.Background()
	repo := NewOutboxRepository(s.db, WithOutboxTableName(s.tableName))

	// Act
	inserted, err := repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        json.RawMessage(`{"key":"value"}`),
		IdempotencyKey: "custom-table-key",
		MaxAttempts:    3,
	})

	// Assert
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), inserted)

	// Verify it's in the custom table
	var count int64
	result := s.db.Raw("SELECT COUNT(*) FROM " + s.tableName).Scan(&count)
	assert.NoError(s.T(), result.Error)
	assert.Equal(s.T(), int64(1), count)
}

func (s *OutboxRepositorySuite) TestFetchAndLockToPublishing_MultipleWorkersNoCollision() {
	// Test that multiple workers can fetch messages concurrently without collision
	// This verifies SELECT ... FOR UPDATE SKIP LOCKED works correctly
	ctx := context.Background()

	// Insert 10 messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		_, err := s.repo.Insert(ctx, core.OutboxInsertParams{
			EventType:      "test.event",
			Payload:        s.createPayload(map[string]interface{}{"index": i}),
			IdempotencyKey: "concurrent-test-" + string(rune('0'+i)),
			MaxAttempts:    3,
		})
		s.Require().NoError(err)
	}

	// Spawn 10 concurrent workers trying to fetch messages
	numWorkers := 10
	type result struct {
		msgID string
		err   error
	}
	results := make(chan result, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			msg, err := s.repo.FetchAndLockToPublishing(ctx)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{msgID: msg.ID}
		}()
	}

	// Collect results
	fetchedIDs := make(map[string]bool)
	var noMessageErrors int
	for i := 0; i < numWorkers; i++ {
		res := <-results
		if res.err != nil {
			if res.err == core.ErrNoMessage {
				noMessageErrors++
			} else {
				s.T().Errorf("unexpected error: %v", res.err)
			}
		} else {
			// Verify no duplicate IDs (critical: each message should be locked by only one worker)
			if fetchedIDs[res.msgID] {
				s.T().Errorf("duplicate message ID fetched: %s", res.msgID)
			}
			fetchedIDs[res.msgID] = true
		}
	}

	// Assert
	// All 10 messages should be fetched by different workers
	s.Assert().Equal(numMessages, len(fetchedIDs), "all messages should be fetched exactly once")
	s.Assert().Equal(0, noMessageErrors, "no worker should get ErrNoMessage since we have enough messages")

	// Verify all messages are in PUBLISHING state
	for msgID := range fetchedIDs {
		msg, err := s.repo.GetByID(ctx, msgID)
		s.Require().NoError(err)
		s.Assert().Equal(core.OutboxStatusPublishing, msg.Status)
	}
}

func (s *OutboxRepositorySuite) TestFetchLockAndMarkPublishing_MultipleWorkersNoCollision() {
	// Test batch fetch with multiple workers competing for the same pool of messages
	ctx := context.Background()

	// Insert 20 messages
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		_, err := s.repo.Insert(ctx, core.OutboxInsertParams{
			EventType:      "test.event",
			Payload:        s.createPayload(map[string]interface{}{"index": i}),
			IdempotencyKey: "batch-concurrent-test-" + string(rune('a'+(i%26))),
			MaxAttempts:    3,
		})
		s.Require().NoError(err)
	}

	// Spawn 4 concurrent workers, each trying to fetch 5 messages
	numWorkers := 4
	batchSize := 5
	type result struct {
		msgIDs []string
		err    error
	}
	results := make(chan result, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			msgs, err := s.repo.FetchLockAndMarkPublishing(ctx, batchSize)
			if err != nil {
				results <- result{err: err}
				return
			}
			ids := make([]string, len(msgs))
			for i, msg := range msgs {
				ids[i] = msg.ID
			}
			results <- result{msgIDs: ids}
		}()
	}

	// Collect results
	allFetchedIDs := make(map[string]bool)
	for i := 0; i < numWorkers; i++ {
		res := <-results
		if res.err != nil {
			s.T().Errorf("unexpected error: %v", res.err)
			continue
		}

		// Each worker should fetch exactly batchSize messages (or fewer if not enough remain)
		s.Assert().LessOrEqual(len(res.msgIDs), batchSize)

		// Verify no duplicates across workers
		for _, msgID := range res.msgIDs {
			if allFetchedIDs[msgID] {
				s.T().Errorf("duplicate message ID fetched across workers: %s", msgID)
			}
			allFetchedIDs[msgID] = true
		}
	}

	// Assert
	// All 20 messages should be fetched exactly once across all workers
	s.Assert().Equal(numMessages, len(allFetchedIDs), "all messages should be fetched exactly once across all workers")

	// Verify all messages are in PUBLISHING state
	for msgID := range allFetchedIDs {
		msg, err := s.repo.GetByID(ctx, msgID)
		s.Require().NoError(err)
		s.Assert().Equal(core.OutboxStatusPublishing, msg.Status)
	}
}

func (s *OutboxRepositorySuite) TestInsertOutboxJSON_MarshalStructAsPayload() {
	// Arrange
	ctx := context.Background()
	type TestPayload struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}
	payload := TestPayload{OrderID: "order-123", Amount: 1000}

	// Act
	msg, err := s.repo.InsertOutboxJSON(ctx, "order.created", payload, "test-json-key", 5)

	// Assert
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), msg)
	assert.Equal(s.T(), "order.created", msg.EventType)
	assert.Equal(s.T(), "test-json-key", msg.IdempotencyKey)
	assert.Equal(s.T(), 5, msg.MaxAttempts)
	assert.JSONEq(s.T(), `{"order_id":"order-123","amount":1000}`, string(msg.Payload))
}

func (s *OutboxRepositorySuite) TestInsertOutboxJSONWithMetadata_MarshalStructWithMetadata() {
	// Arrange
	ctx := context.Background()
	type TestPayload struct {
		UserID string `json:"user_id"`
	}
	type TestMetadata struct {
		Source string `json:"source"`
		Trace  string `json:"trace_id"`
	}
	payload := TestPayload{UserID: "user-456"}
	metadata := TestMetadata{Source: "api", Trace: "trace-123"}

	// Act
	msg, err := s.repo.InsertOutboxJSONWithMetadata(ctx, "user.registered", payload, metadata, "test-metadata-key", 3)

	// Assert
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), msg)
	assert.Equal(s.T(), "user.registered", msg.EventType)
	assert.JSONEq(s.T(), `{"user_id":"user-456"}`, string(msg.Payload))
	assert.NotNil(s.T(), msg.Metadata)
	assert.JSONEq(s.T(), `{"source":"api","trace_id":"trace-123"}`, string(msg.Metadata))
}

func (s *OutboxRepositorySuite) TestInsertOutboxJSONWithMetadata_NilMetadata() {
	// Arrange
	ctx := context.Background()
	type TestPayload struct {
		Data string `json:"data"`
	}
	payload := TestPayload{Data: "test"}

	// Act
	msg, err := s.repo.InsertOutboxJSONWithMetadata(ctx, "test.event", payload, nil, "test-nil-metadata-key", 3)

	// Assert
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), msg)
	assert.Nil(s.T(), msg.Metadata)
}

// ConfigOptionsSuite tests config options
type ConfigOptionsSuite struct {
	suite.Suite
	db *gorm.DB
}

func TestConfigOptionsSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(ConfigOptionsSuite))
}

func (s *ConfigOptionsSuite) SetupSuite() {
	db, err := gorm.Open(postgres.Open(testDatabaseURL()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		s.T().Skipf("failed to connect to test database: %v (ensure docker-compose is running)", err)
	}
	s.db = db
}

func (s *ConfigOptionsSuite) TearDownSuite() {
	if s.db != nil {
		sqlDB, err := s.db.DB()
		if err == nil {
			sqlDB.Close()
		}
	}
}

func (s *ConfigOptionsSuite) TestWithInboxTableName_SetsCustomInboxTable() {
	// Arrange
	ctx := context.Background()
	customInboxTableName := "custom_inbox_gorm"

	// Create custom inbox table
	err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS ` + customInboxTableName + ` (
			consumer_name TEXT NOT NULL,
			event_id UUID NOT NULL,
			completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (consumer_name, event_id)
		)
	`).Error
	s.Require().NoError(err)
	defer s.db.Exec("DROP TABLE IF EXISTS " + customInboxTableName) //nolint:errcheck // test cleanup, error not critical

	// Create InboxRepository with custom table name
	inboxRepo := NewInboxRepository(s.db, WithInboxTableName(customInboxTableName))

	// Act - IsProcessed checks if event exists (should not exist initially)
	eventID := uuid.New()
	processed, err := inboxRepo.IsProcessed(ctx, "test-consumer", eventID)
	assert.NoError(s.T(), err)
	assert.False(s.T(), processed, "first call should return false (not yet completed)")

	// Complete the event to insert a record
	err = inboxRepo.Complete(ctx, "test-consumer", eventID)
	assert.NoError(s.T(), err)

	// Verify it's in the custom table
	var count int64
	err = s.db.Table(customInboxTableName).Count(&count).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), count)

	// Cleanup
	_ = s.db.Exec("DELETE FROM " + customInboxTableName)
}

func (s *ConfigOptionsSuite) TestWithStuckPublishingThreshold_CustomThreshold() {
	// Arrange
	ctx := context.Background()
	customThreshold := 30 * time.Second
	repo := NewOutboxRepository(s.db, WithStuckPublishingThreshold(customThreshold))

	// Insert and lock message to PUBLISHING
	inserted, err := repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        json.RawMessage(`{"key":"value"}`),
		IdempotencyKey: "threshold-test-key-gorm",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Lock to PUBLISHING
	_, err = repo.FetchAndLockToPublishing(ctx)
	s.Require().NoError(err)

	// Set updated_at to 45 seconds ago (exceeds 30s threshold)
	err = s.db.Exec("UPDATE outbox SET updated_at = NOW() - INTERVAL '45 seconds' WHERE id = ?", inserted.ID).Error
	s.Require().NoError(err)

	// Act - Revive stuck messages
	count, err := repo.ReviveStuckPublishing(ctx)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), count, "should revive 1 stuck message")

	// Verify status changed to FAILED
	msg, err := repo.GetByID(ctx, inserted.ID)
	s.Require().NoError(err)
	assert.Equal(s.T(), core.OutboxStatusFailed, msg.Status)

	// Cleanup
	_ = s.db.Exec("DELETE FROM outbox WHERE id = ?", inserted.ID)
}
