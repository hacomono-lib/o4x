package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SchemaSuite struct {
	suite.Suite
}

func TestSchemaSuite(t *testing.T) {
	suite.Run(t, new(SchemaSuite))
}

func (s *SchemaSuite) TestOutboxDDL_GeneratesCorrectSchema() {
	// Arrange
	tableName := "outbox"

	// Act
	ddl := OutboxDDL(tableName)

	// Assert
	// Check ENUM type
	assert.Contains(s.T(), ddl, "CREATE TYPE outbox_status AS ENUM")
	assert.Contains(s.T(), ddl, "'ENQUEUED'")
	assert.Contains(s.T(), ddl, "'PUBLISHING'")
	assert.Contains(s.T(), ddl, "'PUBLISHED'")
	assert.Contains(s.T(), ddl, "'FAILED'")
	assert.Contains(s.T(), ddl, "'DEAD'")

	// Check table creation
	assert.Contains(s.T(), ddl, "CREATE TABLE outbox")
	assert.Contains(s.T(), ddl, "id               UUID PRIMARY KEY")
	assert.Contains(s.T(), ddl, "event_type       TEXT NOT NULL")
	assert.Contains(s.T(), ddl, "payload          JSONB NOT NULL")
	assert.Contains(s.T(), ddl, "metadata         JSONB")
	assert.Contains(s.T(), ddl, "idempotency_key  TEXT NOT NULL")
	assert.Contains(s.T(), ddl, "status           outbox_status NOT NULL DEFAULT 'ENQUEUED'")
	assert.Contains(s.T(), ddl, "error_message    TEXT")
	assert.Contains(s.T(), ddl, "attempt_count    INT NOT NULL DEFAULT 1")
	assert.Contains(s.T(), ddl, "max_attempts     INT NOT NULL DEFAULT 10")
	assert.Contains(s.T(), ddl, "created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()")
	assert.Contains(s.T(), ddl, "updated_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()")

	// Check indexes
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_outbox_status_created_at")
	assert.Contains(s.T(), ddl, "ON outbox (status, created_at)")
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_outbox_enqueued_created_at")
	assert.Contains(s.T(), ddl, "ON outbox (created_at)")
	assert.Contains(s.T(), ddl, "WHERE status = 'ENQUEUED'")
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_outbox_status_next_retry_at")
	assert.Contains(s.T(), ddl, "ON outbox (status, next_retry_at)")
	assert.Contains(s.T(), ddl, "WHERE status = 'FAILED' AND next_retry_at IS NOT NULL")

	// Check unique constraint
	assert.Contains(s.T(), ddl, "ADD CONSTRAINT uq_outbox_event_type_idempotency")
	assert.Contains(s.T(), ddl, "UNIQUE (event_type, idempotency_key)")
}

func (s *SchemaSuite) TestOutboxDDL_WithCustomTableName() {
	// Arrange
	tableName := "custom_outbox"

	// Act
	ddl := OutboxDDL(tableName)

	// Assert
	assert.Contains(s.T(), ddl, "CREATE TYPE custom_outbox_status AS ENUM")
	assert.Contains(s.T(), ddl, "CREATE TABLE custom_outbox")
	assert.Contains(s.T(), ddl, "status           custom_outbox_status NOT NULL")
	assert.Contains(s.T(), ddl, "idx_custom_outbox_status_created_at")
	assert.Contains(s.T(), ddl, "idx_custom_outbox_enqueued_created_at")
	assert.Contains(s.T(), ddl, "idx_custom_outbox_status_next_retry_at")
	assert.Contains(s.T(), ddl, "uq_custom_outbox_event_type_idempotency")
}

func (s *SchemaSuite) TestDropOutboxDDL_GeneratesCorrectStatements() {
	// Arrange
	tableName := "outbox"

	// Act
	ddl := DropOutboxDDL(tableName)

	// Assert
	assert.Contains(s.T(), ddl, "DROP TABLE IF EXISTS outbox;")
	assert.Contains(s.T(), ddl, "DROP TYPE IF EXISTS outbox_status;")
}

func (s *SchemaSuite) TestConsumerInboxDDL_GeneratesCorrectSchema() {
	// Arrange
	tableName := "consumer_inbox"

	// Act
	ddl := ConsumerInboxDDL(tableName)

	// Assert
	// Check table creation
	assert.Contains(s.T(), ddl, "CREATE TABLE consumer_inbox")
	assert.Contains(s.T(), ddl, "consumer_name    TEXT NOT NULL")
	assert.Contains(s.T(), ddl, "event_id         UUID NOT NULL")
	assert.Contains(s.T(), ddl, "completed_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()")
	assert.Contains(s.T(), ddl, "PRIMARY KEY (consumer_name, event_id)")

	// Check index for cleanup
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_consumer_inbox_completed_at")
	assert.Contains(s.T(), ddl, "ON consumer_inbox (completed_at)")

	// Check index for event_id lookups
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_consumer_inbox_event_id")
	assert.Contains(s.T(), ddl, "ON consumer_inbox (event_id)")

	// Check comments
	assert.Contains(s.T(), ddl, "Consumer Inbox Schema")
	assert.Contains(s.T(), ddl, "Track COMPLETED events only")
}

func (s *SchemaSuite) TestConsumerInboxDDL_WithCustomTableName() {
	// Arrange
	tableName := "custom_inbox"

	// Act
	ddl := ConsumerInboxDDL(tableName)

	// Assert
	assert.Contains(s.T(), ddl, "CREATE TABLE custom_inbox")
	assert.Contains(s.T(), ddl, "idx_custom_inbox_completed_at")
	assert.Contains(s.T(), ddl, "ON custom_inbox (completed_at)")
	assert.Contains(s.T(), ddl, "idx_custom_inbox_event_id")
	assert.Contains(s.T(), ddl, "ON custom_inbox (event_id)")
}

func (s *SchemaSuite) TestDropConsumerInboxDDL_GeneratesCorrectStatements() {
	// Arrange
	tableName := "consumer_inbox"

	// Act
	ddl := DropConsumerInboxDDL(tableName)

	// Assert
	assert.Contains(s.T(), ddl, "DROP TABLE IF EXISTS consumer_inbox;")
}

func (s *SchemaSuite) TestDropConsumerInboxDDL_WithCustomTableName() {
	// Arrange
	tableName := "custom_inbox"

	// Act
	ddl := DropConsumerInboxDDL(tableName)

	// Assert
	assert.Contains(s.T(), ddl, "DROP TABLE IF EXISTS custom_inbox;")
}
