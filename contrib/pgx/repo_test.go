package pgx

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

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
	pool      *pgxpool.Pool
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
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDatabaseURL())
	if err != nil {
		s.T().Skipf("failed to connect to test database: %v (ensure docker-compose is running)", err)
	}
	s.pool = pool
	s.tableName = "outbox"
	s.repo = NewOutboxRepository(pool)

	// Clean up table before starting suite to prevent interference from previous test runs.
	// Note: This suite shares the 'outbox' table with contrib/gorm tests.
	// To avoid flaky tests, use `make test` (with -p 1) to run packages sequentially.
	_, _ = s.pool.Exec(ctx, "DELETE FROM "+s.tableName)
}

func (s *OutboxRepositorySuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *OutboxRepositorySuite) SetupTest() {
	// Clean up outbox table before each test
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, "DELETE FROM "+s.tableName)
	s.Require().NoError(err)
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
	_, err := s.repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        s.createPayload(map[string]interface{}{"key": "value"}),
		IdempotencyKey: "test-idem-key-7",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Fetch and lock to PUBLISHING first
	locked, err := s.repo.FetchAndLockToPublishing(ctx)
	s.Require().NoError(err)

	err = s.repo.UpdateToFailed(ctx, locked.ID, "temporary error")
	s.Require().NoError(err)

	// Set next_retry_at to past to make it eligible for requeue
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET next_retry_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", locked.ID)
	s.Require().NoError(err)

	// Act - use short base interval so message is eligible
	count, err := s.repo.RequeueFailed(ctx)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), count)
	msg, _ := s.repo.GetByID(ctx, locked.ID)
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
	for i := 0; i < 3; i++ {
		// Set to PUBLISHING before calling UpdateToFailed
		_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = $1", inserted.ID)
		s.Require().NoError(err)
		err = s.repo.UpdateToFailed(ctx, inserted.ID, "temporary error")
		s.Require().NoError(err)
		_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET status = 'ENQUEUED' WHERE id = $1", inserted.ID)
		s.Require().NoError(err)
	}
	// Set to PUBLISHING before final UpdateToFailed
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = $1", inserted.ID)
	s.Require().NoError(err)
	err = s.repo.UpdateToFailed(ctx, inserted.ID, "temporary error")
	s.Require().NoError(err)

	// Set updated_at to only 2 seconds ago (less than 4s backoff)
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '2 seconds' WHERE id = $1", inserted.ID)
	s.Require().NoError(err)

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
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = $1", inserted.ID)
	s.Require().NoError(err)
	err = s.repo.UpdateToFailed(ctx, inserted.ID, "error")
	s.Require().NoError(err)

	// Set updated_at to past
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '10 seconds' WHERE id = $1", inserted.ID)
	s.Require().NoError(err)

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
	_, err = s.pool.Exec(ctx, "UPDATE outbox SET updated_at = NOW() - INTERVAL '10 minutes' WHERE id = $1", msg.ID)
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
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = $1", inserted.ID)
	s.Require().NoError(err)
	err = s.repo.UpdateToPublished(ctx, inserted.ID)
	s.Require().NoError(err)

	// Set updated_at to 2 days ago
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '2 days' WHERE id = $1", inserted.ID)
	s.Require().NoError(err)

	// Act
	count, err := s.repo.DeleteOlderThan(ctx, core.OutboxStatusPublished, 24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), count)

	// Verify message is deleted
	_, err = s.repo.GetByID(ctx, inserted.ID)
	assert.ErrorIs(s.T(), err, core.ErrNotFound)
}

func (s *OutboxRepositorySuite) TestDeleteOlderThan_E2E_ComprehensiveScenarios() {
	// E2E test for DeleteOlderThan with multiple statuses and time ranges
	ctx := context.Background()

	// Create messages with different statuses and ages
	testCases := []struct {
		eventType       string
		status          core.OutboxStatus
		ageHours        int
		shouldDelete    bool
		deleteThreshold time.Duration
	}{
		// PUBLISHED messages
		{"test.old.published", core.OutboxStatusPublished, 48, true, 24 * time.Hour},     // 48h old, delete >24h
		{"test.recent.published", core.OutboxStatusPublished, 12, false, 24 * time.Hour}, // 12h old, keep

		// DEAD messages
		{"test.old.dead", core.OutboxStatusDead, 72, true, 48 * time.Hour},     // 72h old, delete >48h
		{"test.recent.dead", core.OutboxStatusDead, 36, false, 48 * time.Hour}, // 36h old, keep

		// ENQUEUED messages (should not be deleted even if old)
		{"test.old.enqueued", core.OutboxStatusEnqueued, 100, false, 24 * time.Hour}, // 100h old, but wrong status

		// Edge case: very recent (just created)
		{"test.very.recent", core.OutboxStatusPublished, 0, false, 24 * time.Hour}, // just created, should keep
	}

	messageIDs := make(map[string]string)

	// Insert all test messages
	for _, tc := range testCases {
		inserted, err := s.repo.Insert(ctx, core.OutboxInsertParams{
			EventType:      tc.eventType,
			Payload:        s.createPayload(map[string]interface{}{"test": "data"}),
			IdempotencyKey: "e2e-" + tc.eventType,
			MaxAttempts:    3,
		})
		s.Require().NoError(err)
		messageIDs[tc.eventType] = inserted.ID

		// Update to desired status
		switch tc.status {
		case core.OutboxStatusPublished:
			_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET status = 'PUBLISHING' WHERE id = $1", inserted.ID)
			s.Require().NoError(err)
			err = s.repo.UpdateToPublished(ctx, inserted.ID)
			s.Require().NoError(err)
		case core.OutboxStatusDead:
			_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET status = $1 WHERE id = $2", string(tc.status), inserted.ID)
			s.Require().NoError(err)
		}
		// ENQUEUED is already the default status

		// Set age by updating updated_at
		interval := fmt.Sprintf("%d hours", tc.ageHours)
		_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET updated_at = NOW() - INTERVAL '"+interval+"' WHERE id = $1", inserted.ID)
		s.Require().NoError(err)
	}

	// Act: Delete PUBLISHED messages older than 24 hours
	countPublished, err := s.repo.DeleteOlderThan(ctx, core.OutboxStatusPublished, 24*time.Hour)
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), countPublished, "Should delete exactly 1 PUBLISHED message (48h old)")

	// Act: Delete DEAD messages older than 48 hours
	countDead, err := s.repo.DeleteOlderThan(ctx, core.OutboxStatusDead, 48*time.Hour)
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), countDead, "Should delete exactly 1 DEAD message (72h old)")

	// Verify each message's existence
	for _, tc := range testCases {
		msgID := messageIDs[tc.eventType]
		_, err := s.repo.GetByID(ctx, msgID)

		if tc.shouldDelete {
			s.Assert().ErrorIs(err, core.ErrNotFound, "Message %s should be deleted", tc.eventType)
		} else {
			s.Assert().NoError(err, "Message %s should still exist", tc.eventType)
		}
	}

	// Cleanup: Delete remaining messages
	for eventType, msgID := range messageIDs {
		if _, err := s.repo.GetByID(ctx, msgID); err == nil {
			_, _ = s.pool.Exec(ctx, "DELETE FROM "+s.tableName+" WHERE id = $1", msgID)
		}
		_ = eventType // avoid unused warning
	}
}

func (s *OutboxRepositorySuite) TestWithTx_UsesTransactionForInsert() {
	// Arrange
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx.Rollback(ctx) //nolint:errcheck // rollback error is irrelevant in deferred cleanup

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
	err = tx.Rollback(ctx)
	assert.NoError(s.T(), err)

	// Assert - message should not exist after rollback
	_, err = s.repo.GetByID(ctx, inserted.ID)
	assert.ErrorIs(s.T(), err, core.ErrNotFound)
}

// WithCustomTableNameSuite tests repository with custom table name
type WithCustomTableNameSuite struct {
	suite.Suite
	pool      *pgxpool.Pool
	tableName string
}

func TestWithCustomTableNameSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(WithCustomTableNameSuite))
}

func (s *WithCustomTableNameSuite) SetupSuite() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDatabaseURL())
	if err != nil {
		s.T().Skipf("failed to connect to test database: %v (ensure docker-compose is running)", err)
	}
	s.pool = pool
	s.tableName = "custom_outbox"

	// Create custom table for testing
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS custom_outbox (
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
	s.Require().NoError(err)
}

func (s *WithCustomTableNameSuite) TearDownSuite() {
	if s.pool != nil {
		ctx := context.Background()
		_, _ = s.pool.Exec(ctx, "DROP TABLE IF EXISTS "+s.tableName)
		s.pool.Close()
	}
}

func (s *WithCustomTableNameSuite) SetupTest() {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, "DELETE FROM "+s.tableName)
	s.Require().NoError(err)
}

func (s *WithCustomTableNameSuite) TestWithOutboxTableName_UsesCustomTable() {
	// Arrange
	ctx := context.Background()
	repo := NewOutboxRepository(s.pool, WithOutboxTableName(s.tableName))

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
	var count int
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+s.tableName).Scan(&count)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, count)
}

// ConfigOptionsSuite tests config options
type ConfigOptionsSuite struct {
	suite.Suite
	pool *pgxpool.Pool
}

func TestConfigOptionsSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(ConfigOptionsSuite))
}

func (s *ConfigOptionsSuite) SetupSuite() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDatabaseURL())
	if err != nil {
		s.T().Skipf("failed to connect to test database: %v (ensure docker-compose is running)", err)
	}
	s.pool = pool
}

func (s *ConfigOptionsSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *ConfigOptionsSuite) TestWithInboxTableName_SetsCustomInboxTable() {
	// Arrange
	ctx := context.Background()
	customInboxTableName := "custom_inbox"

	// Create custom inbox table
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+customInboxTableName+` (
			consumer_name TEXT NOT NULL,
			event_id UUID NOT NULL,
			completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (consumer_name, event_id)
		)
	`)
	s.Require().NoError(err)
	defer s.pool.Exec(ctx, "DROP TABLE IF EXISTS "+customInboxTableName) //nolint:errcheck // test cleanup, error not critical

	// Create InboxRepository with custom table name
	inboxRepo := NewInboxRepository(s.pool, WithInboxTableName(customInboxTableName))

	// Act - IsProcessed checks if event exists (should not exist initially)
	eventID := uuid.New()
	processed, err := inboxRepo.IsProcessed(ctx, "test-consumer", eventID)
	assert.NoError(s.T(), err)
	assert.False(s.T(), processed, "first call should return false (not yet completed)")

	// Complete the event to insert a record
	err = inboxRepo.Complete(ctx, "test-consumer", eventID)
	assert.NoError(s.T(), err)

	// Verify it's in the custom table
	var count int
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+customInboxTableName).Scan(&count)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, count)

	// Cleanup
	_, _ = s.pool.Exec(ctx, "DELETE FROM "+customInboxTableName)
}

func (s *ConfigOptionsSuite) TestWithStuckPublishingThreshold_CustomThreshold() {
	// Arrange
	ctx := context.Background()
	customThreshold := 30 * time.Second
	repo := NewOutboxRepository(s.pool, WithStuckPublishingThreshold(customThreshold))

	// Insert and lock message to PUBLISHING
	inserted, err := repo.Insert(ctx, core.OutboxInsertParams{
		EventType:      "test.event",
		Payload:        json.RawMessage(`{"key":"value"}`),
		IdempotencyKey: "threshold-test-key",
		MaxAttempts:    3,
	})
	s.Require().NoError(err)

	// Lock to PUBLISHING
	_, err = repo.FetchAndLockToPublishing(ctx)
	s.Require().NoError(err)

	// Set updated_at to 45 seconds ago (exceeds 30s threshold)
	_, err = s.pool.Exec(ctx, "UPDATE outbox SET updated_at = NOW() - INTERVAL '45 seconds' WHERE id = $1", inserted.ID)
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
	_, _ = s.pool.Exec(ctx, "DELETE FROM outbox WHERE id = $1", inserted.ID)
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
			IdempotencyKey: fmt.Sprintf("concurrent-test-%d", i),
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
			IdempotencyKey: fmt.Sprintf("batch-concurrent-test-%d", i),
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
