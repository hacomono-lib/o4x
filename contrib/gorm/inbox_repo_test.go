package gorm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/hacomono-lib/o4x/core"
)

// InboxRepositorySuite tests InboxRepository with real PostgreSQL database
type InboxRepositorySuite struct {
	suite.Suite
	db        *gorm.DB
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
	db, err := gorm.Open(postgres.Open(testDatabaseURL()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		s.T().Skipf("failed to connect to test database: %v (ensure docker-compose is running)", err)
	}
	s.db = db
	s.tableName = "consumer_inbox"
	s.repo = NewInboxRepository(db)

	// Clean up table before starting suite
	_ = s.db.Exec("DELETE FROM " + s.tableName)
}

func (s *InboxRepositorySuite) TearDownSuite() {
	if s.db != nil {
		sqlDB, err := s.db.DB()
		if err == nil {
			sqlDB.Close()
		}
	}
}

func (s *InboxRepositorySuite) SetupTest() {
	// Clean up inbox table before each test
	result := s.db.Exec("DELETE FROM " + s.tableName)
	s.Require().NoError(result.Error)
}

func (s *InboxRepositorySuite) TestTryStart_FirstTime_ReturnsTrue() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	// Act
	ok, err := s.repo.TryStart(ctx, consumerName, messageID)

	// Assert
	assert.NoError(s.T(), err)
	assert.True(s.T(), ok, "First TryStart should return true")

	// Verify record was created in database
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), consumerName, inbox.ConsumerName)
	assert.Equal(s.T(), messageID, inbox.MessageID)
	assert.Equal(s.T(), core.InboxStatusProcessing, inbox.Status)
	assert.NotZero(s.T(), inbox.ReceivedAt)
	assert.Nil(s.T(), inbox.ProcessedAt)
}

func (s *InboxRepositorySuite) TestTryStart_ProcessingState_ReturnsTrue() {
	// This tests the retry scenario: handler failed, message still processing
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-retry-123"

	// First call - creates processing record
	ok1, err1 := s.repo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err1)
	s.Require().True(ok1)

	// Simulate handler failure (Complete() NOT called)
	// Record remains in "processing" state

	// Act: Second call with same message_id (retry scenario)
	ok2, err2 := s.repo.TryStart(ctx, consumerName, messageID)

	// Assert: Should return true to allow retry
	assert.NoError(s.T(), err2)
	assert.True(s.T(), ok2, "TryStart with processing record should return true (retry)")

	// Verify record is still processing
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), core.InboxStatusProcessing, inbox.Status)
}

func (s *InboxRepositorySuite) TestTryStart_CompletedState_ReturnsFalse() {
	// This tests duplicate detection: message already completed
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	// First call - creates processing record
	ok1, err1 := s.repo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err1)
	s.Require().True(ok1)

	// Complete the message
	err := s.repo.Complete(ctx, consumerName, messageID)
	s.Require().NoError(err)

	// Act: Second call with same message_id (after completion)
	ok2, err2 := s.repo.TryStart(ctx, consumerName, messageID)

	// Assert: Should return false (already completed)
	assert.NoError(s.T(), err2)
	assert.False(s.T(), ok2, "TryStart with completed record should return false")
}

func (s *InboxRepositorySuite) TestTryStart_DifferentConsumerName_ReturnsTrue() {
	// Arrange
	ctx := context.Background()
	messageID := "msg-123"

	// First call with OrderHandler
	ok1, err1 := s.repo.TryStart(ctx, "OrderHandler", messageID)
	s.Require().NoError(err1)
	s.Require().True(ok1)

	// Act: Second call with different consumer name
	ok2, err2 := s.repo.TryStart(ctx, "EmailHandler", messageID)

	// Assert: Different consumer_name means different primary key
	assert.NoError(s.T(), err2)
	assert.True(s.T(), ok2, "Different consumer_name should allow TryStart")
}

func (s *InboxRepositorySuite) TestTryStart_Concurrent_AllReturnTrueForProcessing() {
	// This test verifies that ON CONFLICT prevents race conditions
	// and that all concurrent calls return true for PROCESSING status (retry scenario)
	//
	// Design: TryStart returns true when:
	//   1. Record doesn't exist -> INSERT succeeds -> true
	//   2. Record exists with PROCESSING status -> true (retry scenario)
	// Returns false only when status is COMPLETED
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-concurrent-123"

	// Act: Simulate concurrent TryStart calls
	results := make(chan bool, 10)
	errors := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			ok, err := s.repo.TryStart(ctx, consumerName, messageID)
			errors <- err
			results <- ok
		}()
	}

	// Collect results
	successCount := 0
	for i := 0; i < 10; i++ {
		err := <-errors
		assert.NoError(s.T(), err, "TryStart should not return error")
		if <-results {
			successCount++
		}
	}

	// Assert: All goroutines should return true (PROCESSING allows retry)
	assert.Equal(s.T(), 10, successCount, "All concurrent TryStart should return true for PROCESSING status")

	// Verify only one record was created
	var count int64
	s.db.Table(s.tableName).Where("consumer_name = ? AND message_id = ?", consumerName, messageID).Count(&count)
	assert.Equal(s.T(), int64(1), count, "Only one record should exist in database")
}

func (s *InboxRepositorySuite) TestComplete_UpdatesStatusAndProcessedAt() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	ok, err := s.repo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	s.Require().True(ok)

	// Act
	err = s.repo.Complete(ctx, consumerName, messageID)

	// Assert
	assert.NoError(s.T(), err)

	// Verify status was updated
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), core.InboxStatusCompleted, inbox.Status)
	assert.NotNil(s.T(), inbox.ProcessedAt)
}

func (s *InboxRepositorySuite) TestComplete_NonExistentRecord_NoError() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "non-existent-msg"

	// Act: Complete for non-existent record
	err := s.repo.Complete(ctx, consumerName, messageID)

	// Assert: Should be idempotent (no error)
	assert.NoError(s.T(), err)
}

func (s *InboxRepositorySuite) TestComplete_Idempotent_MultipleCallsSucceed() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	ok, err := s.repo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	s.Require().True(ok)

	// Act: Call Complete multiple times
	err1 := s.repo.Complete(ctx, consumerName, messageID)
	err2 := s.repo.Complete(ctx, consumerName, messageID)
	err3 := s.repo.Complete(ctx, consumerName, messageID)

	// Assert: All calls should succeed
	assert.NoError(s.T(), err1)
	assert.NoError(s.T(), err2)
	assert.NoError(s.T(), err3)
}

func (s *InboxRepositorySuite) TestGetByMessageID_Found() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-123"

	ok, err := s.repo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	s.Require().True(ok)

	// Act
	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)

	// Assert
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), inbox)
	assert.Equal(s.T(), consumerName, inbox.ConsumerName)
	assert.Equal(s.T(), messageID, inbox.MessageID)
	assert.Equal(s.T(), core.InboxStatusProcessing, inbox.Status)
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

func (s *InboxRepositorySuite) TestDeleteOlderThan_CompletedMessages() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"

	// Insert 3 completed messages with different timestamps
	for i := 1; i <= 3; i++ {
		messageID := "msg-old-" + string(rune('0'+i))
		ok, err := s.repo.TryStart(ctx, consumerName, messageID)
		s.Require().NoError(err)
		s.Require().True(ok)

		err = s.repo.Complete(ctx, consumerName, messageID)
		s.Require().NoError(err)
	}

	// Manually update received_at to simulate old messages
	// Note: This is a test-only hack to bypass DEFAULT now()
	result := s.db.Exec("UPDATE " + s.tableName + " SET received_at = NOW() - INTERVAL '10 days' WHERE message_id LIKE 'msg-old-%'")
	s.Require().NoError(result.Error)

	// Act: Delete messages older than 7 days
	deleted, err := s.repo.DeleteOlderThan(ctx, core.InboxStatusCompleted, 7*24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(3), deleted, "Should delete 3 old completed messages")
}

func (s *InboxRepositorySuite) TestDeleteOlderThan_OnlyDeletesSpecifiedStatus() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"

	// Insert 2 processing messages
	ok1, err1 := s.repo.TryStart(ctx, consumerName, "msg-processing-1")
	s.Require().NoError(err1)
	s.Require().True(ok1)

	ok2, err2 := s.repo.TryStart(ctx, consumerName, "msg-processing-2")
	s.Require().NoError(err2)
	s.Require().True(ok2)

	// Insert 2 completed messages
	ok3, err3 := s.repo.TryStart(ctx, consumerName, "msg-completed-1")
	s.Require().NoError(err3)
	s.Require().True(ok3)
	s.Require().NoError(s.repo.Complete(ctx, consumerName, "msg-completed-1"))

	ok4, err4 := s.repo.TryStart(ctx, consumerName, "msg-completed-2")
	s.Require().NoError(err4)
	s.Require().True(ok4)
	s.Require().NoError(s.repo.Complete(ctx, consumerName, "msg-completed-2"))

	// Make all messages old
	result := s.db.Exec("UPDATE " + s.tableName + " SET received_at = NOW() - INTERVAL '10 days'")
	s.Require().NoError(result.Error)

	// Act: Delete only completed messages
	deleted, err := s.repo.DeleteOlderThan(ctx, core.InboxStatusCompleted, 7*24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(2), deleted, "Should delete only completed messages")

	// Verify processing messages still exist
	var count int64
	s.db.Table(s.tableName).Where("status = ?", string(core.InboxStatusProcessing)).Count(&count)
	assert.Equal(s.T(), int64(2), count, "Processing messages should remain")
}

func (s *InboxRepositorySuite) TestWithTx_RollbackPreventsInsertion() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	messageID := "msg-tx-rollback"

	// Act: Start transaction and rollback
	tx := s.db.Begin()
	txRepo := s.repo.WithTx(tx)

	ok, err := txRepo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	s.Require().True(ok)

	tx.Rollback()

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
	tx := s.db.Begin()
	txRepo := s.repo.WithTx(tx)

	ok, err := txRepo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	s.Require().True(ok)

	tx.Commit()

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

	// Simulate business table (use GORM's db.Exec for simplicity)
	s.db.Exec("CREATE TEMP TABLE IF NOT EXISTS test_orders (id TEXT PRIMARY KEY, amount INT)")
	defer s.db.Exec("DROP TABLE test_orders")

	// Act: Transaction with business logic + inbox
	tx := s.db.Begin()

	// 1. Check idempotency
	txRepo := s.repo.WithTx(tx)
	ok, err := txRepo.TryStart(ctx, consumerName, messageID)
	s.Require().NoError(err)
	if !ok {
		tx.Rollback()
		s.T().Fatal("Expected first TryStart to succeed")
	}

	// 2. Execute business logic
	result := tx.Exec("INSERT INTO test_orders (id, amount) VALUES (?, ?)", "order-123", 1000)
	s.Require().NoError(result.Error)

	// 3. Mark as completed
	err = txRepo.Complete(ctx, consumerName, messageID)
	s.Require().NoError(err)

	// 4. Commit transaction
	tx.Commit()

	// Assert: Both business data and inbox record should exist
	var count int64
	s.db.Raw("SELECT COUNT(*) FROM test_orders WHERE id = ?", "order-123").Scan(&count)
	assert.Equal(s.T(), int64(1), count, "Business record should exist")

	inbox, err := s.repo.GetByMessageID(ctx, consumerName, messageID)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), core.InboxStatusCompleted, inbox.Status)
}
