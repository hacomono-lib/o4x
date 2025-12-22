package pgx

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/hacomono-lib/o4x/core"
)

// InboxRepositorySuite tests InboxRepository with real PostgreSQL database
type InboxRepositorySuite struct {
	suite.Suite
	pool      *pgxpool.Pool
	repo      *InboxRepository
	tableName string
}

func TestInboxRepositorySuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	suite.Run(t, new(InboxRepositorySuite))
}

func (s *InboxRepositorySuite) SetupSuite() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDatabaseURL())
	if err != nil {
		s.T().Skipf("failed to connect to test database: %v (ensure docker-compose is running)", err)
	}
	s.pool = pool
	s.tableName = "consumer_inbox"
	s.repo = NewInboxRepository(pool)

	// Clean up table before starting suite
	_, _ = s.pool.Exec(ctx, "DELETE FROM "+s.tableName)
}

func (s *InboxRepositorySuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *InboxRepositorySuite) SetupTest() {
	// Clean up inbox table before each test
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, "DELETE FROM "+s.tableName)
	s.Require().NoError(err)
}

func (s *InboxRepositorySuite) TestIsProcessed_FirstTime_ReturnsFalse() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act: IsProcessed checks existence (returns false if NOT exists)
	processed, err := s.repo.IsProcessed(ctx, consumerName, eventID)

	// Assert
	assert.NoError(s.T(), err)
	assert.False(s.T(), processed, "First IsProcessed should return false (not yet completed)")

	// Complete to insert the record
	err = s.repo.Complete(ctx, consumerName, eventID)
	assert.NoError(s.T(), err)

	// Verify record was created in database
	inbox, err := s.repo.GetByEventID(ctx, consumerName, eventID)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), consumerName, inbox.ConsumerName)
	assert.Equal(s.T(), eventID, inbox.EventID)
	assert.NotZero(s.T(), inbox.CompletedAt)
}

func (s *InboxRepositorySuite) TestIsProcessed_Duplicate_ReturnsTrue() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// First call - IsProcessed returns false (not yet completed)
	processed1, err1 := s.repo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err1)
	s.Require().False(processed1)

	// Complete to insert the record
	err := s.repo.Complete(ctx, consumerName, eventID)
	s.Require().NoError(err)

	// Act: Second call with same event_id (duplicate)
	processed2, err2 := s.repo.IsProcessed(ctx, consumerName, eventID)

	// Assert: Should return true (already completed)
	assert.NoError(s.T(), err2)
	assert.True(s.T(), processed2, "Duplicate IsProcessed should return true (already completed)")
}

func (s *InboxRepositorySuite) TestIsProcessed_DifferentConsumerName_ReturnsFalse() {
	// Arrange
	ctx := context.Background()
	eventID := uuid.New()

	// First call with OrderHandler
	processed1, err1 := s.repo.IsProcessed(ctx, "OrderHandler", eventID)
	s.Require().NoError(err1)
	s.Require().False(processed1)

	// Complete for OrderHandler
	s.repo.Complete(ctx, "OrderHandler", eventID)

	// Act: Second call with different consumer name
	processed2, err2 := s.repo.IsProcessed(ctx, "EmailHandler", eventID)

	// Assert: Different consumer_name means different primary key
	assert.NoError(s.T(), err2)
	assert.False(s.T(), processed2, "Different consumer_name should return false for IsProcessed")
}

func (s *InboxRepositorySuite) TestIsProcessed_Concurrent_MultipleCallsConsistent() {
	// This test verifies that Complete's ON CONFLICT prevents race conditions.
	// Multiple workers can call IsProcessed() concurrently, but Complete() ensures only one record is created.
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act: Simulate concurrent IsProcessed + Complete calls
	results := make(chan bool, 10)
	errors := make(chan error, 20) // 10 for IsProcessed + 10 for Complete
	for i := 0; i < 10; i++ {
		go func() {
			processed, err := s.repo.IsProcessed(ctx, consumerName, eventID)
			errors <- err
			results <- processed
			if !processed {
				// Complete if IsProcessed returned false
				completeErr := s.repo.Complete(ctx, consumerName, eventID)
				errors <- completeErr
			}
		}()
	}

	// Collect results
	completeCount := 0
	for i := 0; i < 10; i++ {
		err := <-errors
		assert.NoError(s.T(), err, "IsProcessed should not return error")
		if !<-results {
			completeCount++
		}
	}

	// Collect Complete errors
	for i := 0; i < completeCount; i++ {
		err := <-errors
		assert.NoError(s.T(), err, "Complete should not return error")
	}

	// Assert: Multiple IsProcessed may return false, but Complete ensures only one record
	// (Due to race conditions, multiple workers might read "not exists" simultaneously)
	assert.GreaterOrEqual(s.T(), completeCount, 1, "At least one Complete should succeed")

	// Verify only one record was created (Complete's ON CONFLICT guarantees this)
	var count int64
	err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+s.tableName+" WHERE consumer_name = $1 AND event_id = $2", consumerName, eventID).Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Only one record should exist in database")
}

func (s *InboxRepositorySuite) TestComplete_NonExistentRecord_NoError() {
	// Complete should successfully insert even for non-existent prior IsProcessed
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act: Complete for non-existent record (should insert)
	err := s.repo.Complete(ctx, consumerName, eventID)

	// Assert: Should be idempotent (no error)
	assert.NoError(s.T(), err)

	// Verify record was created
	inbox, err := s.repo.GetByEventID(ctx, consumerName, eventID)
	assert.NoError(s.T(), err)
	assert.NotZero(s.T(), inbox.CompletedAt)
}

func (s *InboxRepositorySuite) TestComplete_Idempotent_MultipleCallsSucceed() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act: Call Complete multiple times (first inserts, subsequent are no-op)
	err1 := s.repo.Complete(ctx, consumerName, eventID)
	err2 := s.repo.Complete(ctx, consumerName, eventID)
	err3 := s.repo.Complete(ctx, consumerName, eventID)

	// Assert: All calls should succeed (ON CONFLICT DO NOTHING)
	assert.NoError(s.T(), err1)
	assert.NoError(s.T(), err2)
	assert.NoError(s.T(), err3)

	// Verify only one record exists
	var count int64
	err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+s.tableName+" WHERE consumer_name = $1 AND event_id = $2", consumerName, eventID).Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count)
}

func (s *InboxRepositorySuite) TestGetByEventID_Found() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	processed, err := s.repo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err)
	s.Require().False(processed)

	// Complete to insert the record
	s.repo.Complete(ctx, consumerName, eventID)

	// Act
	inbox, err := s.repo.GetByEventID(ctx, consumerName, eventID)

	// Assert
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), inbox)
	assert.Equal(s.T(), consumerName, inbox.ConsumerName)
	assert.Equal(s.T(), eventID, inbox.EventID)
	assert.NotZero(s.T(), inbox.CompletedAt)
}

func (s *InboxRepositorySuite) TestGetByEventID_NotFound() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act
	inbox, err := s.repo.GetByEventID(ctx, consumerName, eventID)

	// Assert
	assert.ErrorIs(s.T(), err, core.ErrNotFound)
	assert.Nil(s.T(), inbox)
}

func (s *InboxRepositorySuite) TestDeleteOlderThan() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"

	// Insert 3 events
	for i := 1; i <= 3; i++ {
		eventID := uuid.New()
		processed, err := s.repo.IsProcessed(ctx, consumerName, eventID)
		s.Require().NoError(err)
		s.Require().False(processed)
		s.repo.Complete(ctx, consumerName, eventID)
	}

	// Manually update completed_at to simulate old messages
	_, err := s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET completed_at = NOW() - INTERVAL '10 days' WHERE consumer_name = $1", consumerName)
	s.Require().NoError(err)

	// Act: Delete messages older than 7 days
	deleted, err := s.repo.DeleteOlderThan(ctx, 7*24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(3), deleted, "Should delete 3 old messages")
}

func (s *InboxRepositorySuite) TestDeleteOlderThan_KeepsRecentMessages() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"

	// Insert old event
	oldEventID := uuid.New()
	processed1, err1 := s.repo.IsProcessed(ctx, consumerName, oldEventID)
	s.Require().NoError(err1)
	s.Require().False(processed1)
	s.repo.Complete(ctx, consumerName, oldEventID)

	// Insert recent event
	recentEventID := uuid.New()
	processed2, err2 := s.repo.IsProcessed(ctx, consumerName, recentEventID)
	s.Require().NoError(err2)
	s.Require().False(processed2)
	s.repo.Complete(ctx, consumerName, recentEventID)

	// Make one event old
	_, err := s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET completed_at = NOW() - INTERVAL '10 days' WHERE event_id = $1", oldEventID)
	s.Require().NoError(err)

	// Act: Delete messages older than 7 days
	deleted, err := s.repo.DeleteOlderThan(ctx, 7*24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), deleted, "Should delete only old message")

	// Verify recent message still exists
	var count int64
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+s.tableName+" WHERE event_id = $1", recentEventID).Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Recent message should remain")
}

func (s *InboxRepositorySuite) TestDeleteOlderThan_SubSecondDuration() {
	// Test for bug fix: sub-second durations should not be truncated to 0
	// Previously, 50ms would become 0 seconds and delete all records
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"

	// Insert message
	eventID := uuid.New()
	processed, err := s.repo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err)
	s.Require().False(processed)
	s.repo.Complete(ctx, consumerName, eventID)

	// Set completed_at to 1 second ago
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET completed_at = clock_timestamp() - INTERVAL '1 second' WHERE event_id = $1", eventID)
	s.Require().NoError(err)

	// Act: Delete messages older than 500ms
	deleted, err := s.repo.DeleteOlderThan(ctx, 500*time.Millisecond)

	// Assert: Message should be deleted (1s > 500ms)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), deleted, "Should delete message older than 500ms")

	// Insert another message
	eventID2 := uuid.New()
	processed2, err2 := s.repo.IsProcessed(ctx, consumerName, eventID2)
	s.Require().NoError(err2)
	s.Require().False(processed2)
	s.repo.Complete(ctx, consumerName, eventID2)

	// Set completed_at to 100ms ago
	_, err = s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET completed_at = clock_timestamp() - INTERVAL '100 milliseconds' WHERE event_id = $1", eventID2)
	s.Require().NoError(err)

	// Act: Delete messages older than 500ms
	deleted2, err2 := s.repo.DeleteOlderThan(ctx, 500*time.Millisecond)

	// Assert: Message should NOT be deleted (100ms < 500ms)
	assert.NoError(s.T(), err2)
	assert.Equal(s.T(), int64(0), deleted2, "Should NOT delete message younger than 500ms")

	// Verify message still exists
	var count int64
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+s.tableName+" WHERE event_id = $1", eventID2).Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Recent message should remain")
}

func (s *InboxRepositorySuite) TestWithTx_RollbackPreventsInsertion() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act: Start transaction and rollback
	tx, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx.Rollback(ctx)

	txRepo := s.repo.WithTx(tx)

	processed, err := txRepo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err)
	s.Require().False(processed)

	// Complete within transaction
	txRepo.Complete(ctx, consumerName, eventID)

	tx.Rollback(ctx)

	// Assert: Record should not exist after rollback
	_, err = s.repo.GetByEventID(ctx, consumerName, eventID)
	assert.ErrorIs(s.T(), err, core.ErrNotFound, "Rollback should prevent record insertion")
}

func (s *InboxRepositorySuite) TestWithTx_CommitPersistsInsertion() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act: Start transaction and commit
	tx, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx.Rollback(ctx)

	txRepo := s.repo.WithTx(tx)

	processed, err := txRepo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err)
	s.Require().False(processed)

	// Complete within transaction
	txRepo.Complete(ctx, consumerName, eventID)

	tx.Commit(ctx)

	// Assert: Record should exist after commit
	inbox, err := s.repo.GetByEventID(ctx, consumerName, eventID)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), eventID, inbox.EventID)
}

func (s *InboxRepositorySuite) TestWithTx_BusinessTransactionWithInbox() {
	// This test demonstrates the typical usage pattern:
	// Business logic + idempotency check in same transaction

	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Simulate business table
	_, err := s.pool.Exec(ctx, "CREATE TEMP TABLE IF NOT EXISTS test_orders (id TEXT PRIMARY KEY, amount INT)")
	s.Require().NoError(err)
	defer s.pool.Exec(ctx, "DROP TABLE IF EXISTS test_orders")

	// Act: Transaction with business logic + inbox
	tx, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx.Rollback(ctx)

	// 1. Check idempotency
	txRepo := s.repo.WithTx(tx)
	processed, err := txRepo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err)
	if processed {
		tx.Rollback(ctx)
		s.T().Fatal("Expected first IsProcessed to return false")
	}

	// 2. Execute business logic
	_, err = tx.Exec(ctx, "INSERT INTO test_orders (id, amount) VALUES ($1, $2)", "order-123", 1000)
	s.Require().NoError(err)

	// 3. Mark as completed within transaction
	err = txRepo.Complete(ctx, consumerName, eventID)
	s.Require().NoError(err)

	tx.Commit(ctx)

	// Assert: Both business data and inbox record should exist
	var count int64
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_orders WHERE id = $1", "order-123").Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Business record should exist")

	inbox, err := s.repo.GetByEventID(ctx, consumerName, eventID)
	assert.NoError(s.T(), err)
	assert.NotZero(s.T(), inbox.CompletedAt)
}

func (s *InboxRepositorySuite) TestWithTx_DuplicatePreventsBusinessLogic() {
	// This test demonstrates duplicate detection preventing duplicate business logic

	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Simulate business table
	_, err := s.pool.Exec(ctx, "CREATE TEMP TABLE IF NOT EXISTS test_orders (id TEXT PRIMARY KEY, amount INT)")
	s.Require().NoError(err)
	defer s.pool.Exec(ctx, "DROP TABLE IF EXISTS test_orders")

	// First event processing
	tx1, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	txRepo1 := s.repo.WithTx(tx1)
	processed1, _ := txRepo1.IsProcessed(ctx, consumerName, eventID)
	s.Require().False(processed1)
	tx1.Exec(ctx, "INSERT INTO test_orders (id, amount) VALUES ($1, $2)", "order-456", 2000)
	txRepo1.Complete(ctx, consumerName, eventID) // Mark as completed
	tx1.Commit(ctx)

	// Act: Duplicate event processing
	tx2, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx2.Rollback(ctx)
	txRepo2 := s.repo.WithTx(tx2)
	processed2, err2 := txRepo2.IsProcessed(ctx, consumerName, eventID)

	// Assert: Should detect duplicate
	assert.NoError(s.T(), err2)
	assert.True(s.T(), processed2, "Duplicate should be detected (IsProcessed returns true)")

	tx2.Rollback(ctx)

	// Verify only one business record exists
	var count int64
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_orders WHERE id = $1", "order-456").Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Only one business record should exist")
}
