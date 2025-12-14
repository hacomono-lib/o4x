package pgx

import (
	"context"
	"testing"
	"time"

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

func (s *InboxRepositorySuite) TestTryStart_FirstTime_ReturnsTrue() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	// Act: TryStart checks existence (returns true if NOT exists)
	ok, err := s.repo.TryStart(ctx, consumerName, messageID)

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), ok, "First TryStart should return true")

	// Complete to insert the record
	err = s.repo.Complete(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)

	// Verify record was created in database
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), consumerName, inbox.ConsumerName)
	assert.Equal(s.T(), messageID, inbox.MessageID)
	assert.NotZero(s.T(), inbox.CompletedAt)
}

func (s *InboxRepositorySuite) TestTryStart_Duplicate_ReturnsFalse() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	// First call - TryStart returns true
	ok1, err1 := s.repo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err1)
	s.Require().True(ok1)

	// Complete to insert the record
	err := s.repo.Complete(ctx, consumerName, messageID)
	s.Require().NoError(err)

	// Act: Second call with same message_id (duplicate)
	ok2, err2 := s.repo.TryStart(ctx, consumerName, messageID)

	// Assert: Should return false (already completed)
	assert.NoError(s.T(), err2)
	assert.False(s.T(), ok2, "Duplicate TryStart should return false")
}

func (s *InboxRepositorySuite) TestTryStart_DifferentConsumerName_ReturnsTrue() {
	// Arrange
	ctx := context.Background()
	messageID := "msg-123"

	// First call with OrderHandler
	ok1, err1 := s.repo.TryStart(ctx, "OrderHandler", messageID)
	s.Require().NoError(err1)
	s.Require().True(ok1)

	// Complete for OrderHandler
	s.repo.Complete(ctx, "OrderHandler", messageID)

	// Act: Second call with different consumer name
	ok2, err2 := s.repo.TryStart(ctx, "EmailHandler", messageID)

	// Assert: Different consumer_name means different primary key
	assert.NoError(s.T(), err2)
	assert.True(s.T(), ok2, "Different consumer_name should allow TryStart")
}

func (s *InboxRepositorySuite) TestTryStart_Concurrent_OnlyOneSucceeds() {
	// This test verifies that Complete's ON CONFLICT prevents race conditions.
	// Multiple workers can pass TryStart() concurrently, but Complete() ensures only one record is created.
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-concurrent-123"

	// Act: Simulate concurrent TryStart + Complete calls
	results := make(chan bool, 10)
	errors := make(chan error, 20) // 10 for TryStart + 10 for Complete
	for i := 0; i < 10; i++ {
		go func() {
			ok, err := s.repo.TryStart(ctx, consumerName, messageID)
			errors <- err
			results <- ok
			if ok {
				// Complete if TryStart returned true
				completeErr := s.repo.Complete(ctx, consumerName, messageID)
				errors <- completeErr
			}
		}()
	}

	// Collect results
	tryStartSuccessCount := 0
	for i := 0; i < 10; i++ {
		err := <-errors
		assert.NoError(s.T(), err, "TryStart should not return error")
		if <-results {
			tryStartSuccessCount++
		}
	}

	// Collect Complete errors
	for i := 0; i < tryStartSuccessCount; i++ {
		err := <-errors
		assert.NoError(s.T(), err, "Complete should not return error")
	}

	// Assert: Multiple TryStart may succeed, but Complete ensures only one record
	// (Due to race conditions, multiple workers might read "not exists" simultaneously)
	assert.GreaterOrEqual(s.T(), tryStartSuccessCount, 1, "At least one TryStart should succeed")

	// Verify only one record was created (Complete's ON CONFLICT guarantees this)
	var count int64
	err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+s.tableName+" WHERE consumer_name = $1 AND message_id = $2", consumerName, messageID).Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Only one record should exist in database")
}

func (s *InboxRepositorySuite) TestComplete_NonExistentRecord_NoError() {
	// Complete should successfully insert even for non-existent prior TryStart
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "non-existent-msg"

	// Act: Complete for non-existent record (should insert)
	err := s.repo.Complete(ctx, consumerName, messageID)

	// Assert: Should be idempotent (no error)
	assert.NoError(s.T(), err)

	// Verify record was created
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)
	assert.NotZero(s.T(), inbox.CompletedAt)
}

func (s *InboxRepositorySuite) TestComplete_Idempotent_MultipleCallsSucceed() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	// Act: Call Complete multiple times (first inserts, subsequent are no-op)
	err1 := s.repo.Complete(ctx, consumerName, messageID)
	err2 := s.repo.Complete(ctx, consumerName, messageID)
	err3 := s.repo.Complete(ctx, consumerName, messageID)

	// Assert: All calls should succeed (ON CONFLICT DO NOTHING)
	assert.NoError(s.T(), err1)
	assert.NoError(s.T(), err2)
	assert.NoError(s.T(), err3)

	// Verify only one record exists
	var count int64
	err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+s.tableName+" WHERE consumer_name = $1 AND message_id = $2", consumerName, messageID).Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count)
}

func (s *InboxRepositorySuite) TestGetByMessageID_Found() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	ok, err := s.repo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	s.Require().True(ok)

	// Complete to insert the record
	s.repo.Complete(ctx, consumerName, messageID)

	// Act
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)

	// Assert
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), inbox)
	assert.Equal(s.T(), consumerName, inbox.ConsumerName)
	assert.Equal(s.T(), messageID, inbox.MessageID)
	assert.NotZero(s.T(), inbox.CompletedAt)
}

func (s *InboxRepositorySuite) TestGetByMessageID_NotFound() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "non-existent-msg"

	// Act
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)

	// Assert
	assert.ErrorIs(s.T(), err, core.ErrNotFound)
	assert.Nil(s.T(), inbox)
}

func (s *InboxRepositorySuite) TestDeleteOlderThan() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"

	// Insert 3 messages
	for i := 1; i <= 3; i++ {
		messageID := "msg-old-" + string(rune('0'+i))
		ok, err := s.repo.TryStart(ctx, consumerName, messageID)
		s.Require().NoError(err)
		s.Require().True(ok)
		s.repo.Complete(ctx, consumerName, messageID)
	}

	// Manually update completed_at to simulate old messages
	_, err := s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET completed_at = NOW() - INTERVAL '10 days' WHERE message_id LIKE 'msg-old-%'")
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

	// Insert old message
	ok1, err1 := s.repo.TryStart(ctx, consumerName, "msg-old")
	s.Require().NoError(err1)
	s.Require().True(ok1)
	s.repo.Complete(ctx, consumerName, "msg-old")

	// Insert recent message
	ok2, err2 := s.repo.TryStart(ctx, consumerName, "msg-recent")
	s.Require().NoError(err2)
	s.Require().True(ok2)
	s.repo.Complete(ctx, consumerName, "msg-recent")

	// Make one message old
	_, err := s.pool.Exec(ctx, "UPDATE "+s.tableName+" SET completed_at = NOW() - INTERVAL '10 days' WHERE message_id = 'msg-old'")
	s.Require().NoError(err)

	// Act: Delete messages older than 7 days
	deleted, err := s.repo.DeleteOlderThan(ctx, 7*24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), deleted, "Should delete only old message")

	// Verify recent message still exists
	var count int64
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+s.tableName+" WHERE message_id = 'msg-recent'").Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Recent message should remain")
}

func (s *InboxRepositorySuite) TestWithTx_RollbackPreventsInsertion() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-tx-rollback"

	// Act: Start transaction and rollback
	tx, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx.Rollback(ctx)

	txRepo := s.repo.WithTx(tx)

	ok, err := txRepo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	s.Require().True(ok)

	// Complete within transaction
	txRepo.Complete(ctx, consumerName, messageID)

	tx.Rollback(ctx)

	// Assert: Record should not exist after rollback
	_, err = s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.ErrorIs(s.T(), err, core.ErrNotFound, "Rollback should prevent record insertion")
}

func (s *InboxRepositorySuite) TestWithTx_CommitPersistsInsertion() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-tx-commit"

	// Act: Start transaction and commit
	tx, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx.Rollback(ctx)

	txRepo := s.repo.WithTx(tx)

	ok, err := txRepo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	s.Require().True(ok)

	// Complete within transaction
	txRepo.Complete(ctx, consumerName, messageID)

	tx.Commit(ctx)

	// Assert: Record should exist after commit
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), messageID, inbox.MessageID)
}

func (s *InboxRepositorySuite) TestWithTx_BusinessTransactionWithInbox() {
	// This test demonstrates the typical usage pattern:
	// Business logic + idempotency check in same transaction

	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-business-tx"

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
	ok, err := txRepo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	if !ok {
		tx.Rollback(ctx)
		s.T().Fatal("Expected first TryStart to succeed")
	}

	// 2. Execute business logic
	_, err = tx.Exec(ctx, "INSERT INTO test_orders (id, amount) VALUES ($1, $2)", "order-123", 1000)
	s.Require().NoError(err)

	// 3. Mark as completed within transaction
	err = txRepo.Complete(ctx, consumerName, messageID)
	s.Require().NoError(err)

	tx.Commit(ctx)

	// Assert: Both business data and inbox record should exist
	var count int64
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_orders WHERE id = $1", "order-123").Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Business record should exist")

	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)
	assert.NotZero(s.T(), inbox.CompletedAt)
}

func (s *InboxRepositorySuite) TestWithTx_DuplicatePreventsBusinessLogic() {
	// This test demonstrates duplicate detection preventing duplicate business logic

	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-duplicate-prevention"

	// Simulate business table
	_, err := s.pool.Exec(ctx, "CREATE TEMP TABLE IF NOT EXISTS test_orders (id TEXT PRIMARY KEY, amount INT)")
	s.Require().NoError(err)
	defer s.pool.Exec(ctx, "DROP TABLE IF EXISTS test_orders")

	// First message processing
	tx1, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	txRepo1 := s.repo.WithTx(tx1)
	ok1, _ := txRepo1.TryStart(ctx, consumerName, messageID)
	s.Require().True(ok1)
	tx1.Exec(ctx, "INSERT INTO test_orders (id, amount) VALUES ($1, $2)", "order-456", 2000)
	txRepo1.Complete(ctx, consumerName, messageID) // Mark as completed
	tx1.Commit(ctx)

	// Act: Duplicate message processing
	tx2, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx2.Rollback(ctx)
	txRepo2 := s.repo.WithTx(tx2)
	ok2, err2 := txRepo2.TryStart(ctx, consumerName, messageID)

	// Assert: Should detect duplicate
	assert.NoError(s.T(), err2)
	assert.False(s.T(), ok2, "Duplicate should be detected")

	tx2.Rollback(ctx)

	// Verify only one business record exists
	var count int64
	err = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_orders WHERE id = $1", "order-456").Scan(&count)
	s.Require().NoError(err)
	assert.Equal(s.T(), int64(1), count, "Only one business record should exist")
}
