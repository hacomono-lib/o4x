package gorm

import (
	"context"
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

func (s *InboxRepositorySuite) TestIsProcessed_FirstTime_ReturnsFalse() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act: IsProcessed checks existence (returns false if NOT yet processed)
	processed, err := s.repo.IsProcessed(ctx, consumerName, eventID)

	// Assert
	assert.NoError(s.T(), err)
	assert.False(s.T(), processed, "First IsProcessed should return false")

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

	// First call - IsProcessed returns false
	processed1, err1 := s.repo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err1)
	s.Require().False(processed1)

	// Complete to insert the record
	err := s.repo.Complete(ctx, consumerName, eventID)
	s.Require().NoError(err)

	// Act: Second call with same eventID (duplicate)
	processed2, err2 := s.repo.IsProcessed(ctx, consumerName, eventID)

	// Assert: Should return true (already completed)
	assert.NoError(s.T(), err2)
	assert.True(s.T(), processed2, "Duplicate IsProcessed should return true")
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

func (s *InboxRepositorySuite) TestIsProcessed_Concurrent_OnlyOneSucceeds() {
	// This test verifies that Complete's ON CONFLICT prevents race conditions.
	// Multiple workers can pass IsProcessed() concurrently, but Complete() ensures only one record is created.
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
			results <- !processed // Invert: true if NOT yet processed
			if !processed {
				// Complete if IsProcessed returned false
				completeErr := s.repo.Complete(ctx, consumerName, eventID)
				errors <- completeErr
			}
		}()
	}

	// Collect results
	isProcessedFalseCount := 0
	for i := 0; i < 10; i++ {
		err := <-errors
		assert.NoError(s.T(), err, "IsProcessed should not return error")
		if <-results {
			isProcessedFalseCount++
		}
	}

	// Collect Complete errors
	for i := 0; i < isProcessedFalseCount; i++ {
		err := <-errors
		assert.NoError(s.T(), err, "Complete should not return error")
	}

	// Assert: Multiple IsProcessed may return false, but Complete ensures only one record
	// (Due to race conditions, multiple workers might read "not exists" simultaneously)
	assert.GreaterOrEqual(s.T(), isProcessedFalseCount, 1, "At least one IsProcessed should return false")

	// Verify only one record was created (Complete's ON CONFLICT guarantees this)
	var count int64
	s.db.Table(s.tableName).Where("consumer_name = ? AND event_id = ?", consumerName, eventID).Count(&count)
	assert.Equal(s.T(), int64(1), count, "Only one record should exist in database")
}

func (s *InboxRepositorySuite) TestComplete_NonExistentRecord_NoError() {
	// Complete should successfully insert even for non-existent prior IsProcessed check
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
	s.db.Table(s.tableName).Where("consumer_name = ? AND event_id = ?", consumerName, eventID).Count(&count)
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

	// Insert 3 messages
	for i := 1; i <= 3; i++ {
		eventID := uuid.New()
		processed, err := s.repo.IsProcessed(ctx, consumerName, eventID)
		s.Require().NoError(err)
		s.Require().False(processed)
		s.repo.Complete(ctx, consumerName, eventID)
	}

	// Manually update completed_at to simulate old messages
	result := s.db.Exec("UPDATE " + s.tableName + " SET completed_at = NOW() - INTERVAL '10 days' WHERE consumer_name = ?", consumerName)
	s.Require().NoError(result.Error)

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
	oldEventID := uuid.New()
	processed1, err1 := s.repo.IsProcessed(ctx, consumerName, oldEventID)
	s.Require().NoError(err1)
	s.Require().False(processed1)
	s.repo.Complete(ctx, consumerName, oldEventID)

	// Insert recent message
	recentEventID := uuid.New()
	processed2, err2 := s.repo.IsProcessed(ctx, consumerName, recentEventID)
	s.Require().NoError(err2)
	s.Require().False(processed2)
	s.repo.Complete(ctx, consumerName, recentEventID)

	// Make one message old
	result := s.db.Exec("UPDATE "+s.tableName+" SET completed_at = NOW() - INTERVAL '10 days' WHERE event_id = ?", oldEventID)
	s.Require().NoError(result.Error)

	// Act: Delete messages older than 7 days
	deleted, err := s.repo.DeleteOlderThan(ctx, 7*24*time.Hour)

	// Assert
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), int64(1), deleted, "Should delete only old message")

	// Verify recent message still exists
	var count int64
	s.db.Table(s.tableName).Where("event_id = ?", recentEventID).Count(&count)
	assert.Equal(s.T(), int64(1), count, "Recent message should remain")
}

func (s *InboxRepositorySuite) TestWithTx_RollbackPreventsInsertion() {
	// Arrange
	ctx := context.Background()
	consumerName := "OrderHandler"
	eventID := uuid.New()

	// Act: Start transaction and rollback
	tx := s.db.Begin()
	txRepo := s.repo.WithTx(tx)

	processed, err := txRepo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err)
	s.Require().False(processed)

	// Complete within transaction
	txRepo.Complete(ctx, consumerName, eventID)

	tx.Rollback()

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
	tx := s.db.Begin()
	txRepo := s.repo.WithTx(tx)

	processed, err := txRepo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err)
	s.Require().False(processed)

	// Complete within transaction
	txRepo.Complete(ctx, consumerName, eventID)

	tx.Commit()

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
	s.db.Exec("CREATE TEMP TABLE IF NOT EXISTS test_orders (id TEXT PRIMARY KEY, amount INT)")
	defer s.db.Exec("DROP TABLE test_orders")

	// Act: Transaction with business logic + inbox
	tx := s.db.Begin()

	// 1. Check idempotency
	txRepo := s.repo.WithTx(tx)
	processed, err := txRepo.IsProcessed(ctx, consumerName, eventID)
	s.Require().NoError(err)
	if processed {
		tx.Rollback()
		s.T().Fatal("Expected first IsProcessed to return false")
	}

	// 2. Execute business logic
	result := tx.Exec("INSERT INTO test_orders (id, amount) VALUES (?, ?)", "order-123", 1000)
	s.Require().NoError(result.Error)

	// 3. Mark as completed within transaction
	err = txRepo.Complete(ctx, consumerName, eventID)
	s.Require().NoError(err)

	tx.Commit()

	// Assert: Both business data and inbox record should exist
	var count int64
	s.db.Raw("SELECT COUNT(*) FROM test_orders WHERE id = ?", "order-123").Scan(&count)
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
	s.db.Exec("CREATE TEMP TABLE IF NOT EXISTS test_orders (id TEXT PRIMARY KEY, amount INT)")
	defer s.db.Exec("DROP TABLE test_orders")

	// First message processing
	tx1 := s.db.Begin()
	txRepo1 := s.repo.WithTx(tx1)
	processed1, _ := txRepo1.IsProcessed(ctx, consumerName, eventID)
	s.Require().False(processed1)
	tx1.Exec("INSERT INTO test_orders (id, amount) VALUES (?, ?)", "order-456", 2000)
	txRepo1.Complete(ctx, consumerName, eventID) // Mark as completed
	tx1.Commit()

	// Act: Duplicate message processing
	tx2 := s.db.Begin()
	txRepo2 := s.repo.WithTx(tx2)
	processed2, err2 := txRepo2.IsProcessed(ctx, consumerName, eventID)

	// Assert: Should detect duplicate
	assert.NoError(s.T(), err2)
	assert.True(s.T(), processed2, "Duplicate should be detected")

	tx2.Rollback()

	// Verify only one business record exists
	var count int64
	s.db.Raw("SELECT COUNT(*) FROM test_orders WHERE id = ?", "order-456").Scan(&count)
	assert.Equal(s.T(), int64(1), count, "Only one business record should exist")
}
