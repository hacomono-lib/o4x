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
	assert.Contains(s.T(), ddl, "topic            TEXT NOT NULL")
	assert.Contains(s.T(), ddl, "payload          JSONB NOT NULL")
	assert.Contains(s.T(), ddl, "idempotency_key  TEXT NOT NULL")
	assert.Contains(s.T(), ddl, "status           outbox_status NOT NULL DEFAULT 'ENQUEUED'")
	assert.Contains(s.T(), ddl, "error_message    TEXT")
	assert.Contains(s.T(), ddl, "retry_count      INT NOT NULL DEFAULT 0")
	assert.Contains(s.T(), ddl, "max_retries      INT NOT NULL DEFAULT 10")
	assert.Contains(s.T(), ddl, "created_at       TIMESTAMPTZ NOT NULL DEFAULT now()")
	assert.Contains(s.T(), ddl, "updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()")

	// Check index
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_outbox_status_created_at")
	assert.Contains(s.T(), ddl, "ON outbox (status, created_at)")

	// Check unique constraint
	assert.Contains(s.T(), ddl, "ADD CONSTRAINT uq_outbox_topic_idempotency")
	assert.Contains(s.T(), ddl, "UNIQUE (topic, idempotency_key)")
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
	assert.Contains(s.T(), ddl, "uq_custom_outbox_topic_idempotency")
}

func (s *SchemaSuite) TestConsumerMessagesDDL_GeneratesCorrectSchema() {
	// Arrange
	tableName := "consumer_messages"

	// Act
	ddl := ConsumerMessagesDDL(tableName)

	// Assert
	// Check ENUM type
	assert.Contains(s.T(), ddl, "CREATE TYPE consumer_messages_status AS ENUM")
	assert.Contains(s.T(), ddl, "'CONSUMING'")
	assert.Contains(s.T(), ddl, "'CONSUMED'")
	assert.Contains(s.T(), ddl, "'FAILED'")
	assert.Contains(s.T(), ddl, "'DEAD'")

	// Check table creation
	assert.Contains(s.T(), ddl, "CREATE TABLE consumer_messages")
	assert.Contains(s.T(), ddl, "id               UUID PRIMARY KEY")
	assert.Contains(s.T(), ddl, "outbox_id        UUID")
	assert.Contains(s.T(), ddl, "message_id       TEXT NOT NULL")
	assert.Contains(s.T(), ddl, "receipt_handle   TEXT NOT NULL")
	assert.Contains(s.T(), ddl, "receive_count    INT NOT NULL")
	assert.Contains(s.T(), ddl, "queue_url        TEXT NOT NULL")
	assert.Contains(s.T(), ddl, "status           consumer_messages_status NOT NULL DEFAULT 'CONSUMING'")
	assert.Contains(s.T(), ddl, "error_message    TEXT")
	assert.Contains(s.T(), ddl, "last_error_at    TIMESTAMPTZ")
	assert.Contains(s.T(), ddl, "max_retries      INT NOT NULL DEFAULT 5")
	assert.Contains(s.T(), ddl, "processed_at     TIMESTAMPTZ")
	assert.Contains(s.T(), ddl, "created_at       TIMESTAMPTZ NOT NULL DEFAULT now()")
	assert.Contains(s.T(), ddl, "updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()")

	// Check unique constraint
	assert.Contains(s.T(), ddl, "ADD CONSTRAINT uq_consumer_messages_message_id UNIQUE (message_id)")

	// Check status index
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_consumer_messages_status ON consumer_messages (status)")

	// Check outbox_id index (no FK constraint, just index for correlation queries)
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_consumer_messages_outbox_id ON consumer_messages (outbox_id)")
	assert.Contains(s.T(), ddl, "WHERE outbox_id IS NOT NULL")

	// No FK constraint (independent lifecycles)
	assert.NotContains(s.T(), ddl, "FOREIGN KEY")
	assert.NotContains(s.T(), ddl, "REFERENCES")
}

func (s *SchemaSuite) TestConsumerMessagesDDL_WithCustomTableName() {
	// Arrange
	tableName := "my_consumer"

	// Act
	ddl := ConsumerMessagesDDL(tableName)

	// Assert
	assert.Contains(s.T(), ddl, "CREATE TYPE my_consumer_status AS ENUM")
	assert.Contains(s.T(), ddl, "CREATE TABLE my_consumer")
	assert.Contains(s.T(), ddl, "uq_my_consumer_message_id")
	assert.Contains(s.T(), ddl, "idx_my_consumer_status")
	assert.Contains(s.T(), ddl, "idx_my_consumer_outbox_id")

	// No FK constraint
	assert.NotContains(s.T(), ddl, "FOREIGN KEY")
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

func (s *SchemaSuite) TestDropConsumerMessagesDDL_GeneratesCorrectStatements() {
	// Arrange
	tableName := "consumer_messages"

	// Act
	ddl := DropConsumerMessagesDDL(tableName)

	// Assert
	assert.Contains(s.T(), ddl, "DROP TABLE IF EXISTS consumer_messages;")
	assert.Contains(s.T(), ddl, "DROP TYPE IF EXISTS consumer_messages_status;")
}

func (s *SchemaSuite) TestMigrationSQL_GeneratesCompleteMigration() {
	// Arrange
	outboxTableName := "outbox"
	consumerTableName := "consumer_messages"

	// Act
	sql := MigrationSQL(outboxTableName, consumerTableName)

	// Assert
	// Check header
	assert.Contains(s.T(), sql, "-- o4x Migration: Outbox + Consumer Tables")
	assert.Contains(s.T(), sql, "-- Generated by github.com/hacomono-lib/o4x/schema")

	// Check transaction boundaries
	assert.Contains(s.T(), sql, "BEGIN;")
	assert.Contains(s.T(), sql, "COMMIT;")

	// Check outbox schema included
	assert.Contains(s.T(), sql, "CREATE TYPE outbox_status AS ENUM")
	assert.Contains(s.T(), sql, "CREATE TABLE outbox")

	// Check consumer schema included
	assert.Contains(s.T(), sql, "CREATE TYPE consumer_messages_status AS ENUM")
	assert.Contains(s.T(), sql, "CREATE TABLE consumer_messages")

	// Check outbox_id index is created
	assert.Contains(s.T(), sql, "idx_consumer_messages_outbox_id")

	// No FK constraint (independent lifecycles)
	assert.NotContains(s.T(), sql, "FOREIGN KEY")
	assert.NotContains(s.T(), sql, "REFERENCES outbox")
}

func (s *SchemaSuite) TestRollbackSQL_GeneratesCorrectRollback() {
	// Arrange
	outboxTableName := "outbox"
	consumerTableName := "consumer_messages"

	// Act
	sql := RollbackSQL(outboxTableName, consumerTableName)

	// Assert
	// Check header
	assert.Contains(s.T(), sql, "-- o4x Rollback: Drop Outbox + Consumer Tables")
	assert.Contains(s.T(), sql, "-- Generated by github.com/hacomono-lib/o4x/schema")

	// Check transaction boundaries
	assert.Contains(s.T(), sql, "BEGIN;")
	assert.Contains(s.T(), sql, "COMMIT;")

	// Check both tables and types are dropped (order doesn't matter without FK)
	assert.Contains(s.T(), sql, "DROP TABLE IF EXISTS consumer_messages;")
	assert.Contains(s.T(), sql, "DROP TYPE IF EXISTS consumer_messages_status;")
	assert.Contains(s.T(), sql, "DROP TABLE IF EXISTS outbox;")
	assert.Contains(s.T(), sql, "DROP TYPE IF EXISTS outbox_status;")
}

func (s *SchemaSuite) TestMigrationSQL_WithCustomTableNames() {
	// Arrange
	outboxTableName := "my_outbox"
	consumerTableName := "my_consumer"

	// Act
	sql := MigrationSQL(outboxTableName, consumerTableName)

	// Assert
	assert.Contains(s.T(), sql, "CREATE TABLE my_outbox")
	assert.Contains(s.T(), sql, "CREATE TABLE my_consumer")
	assert.Contains(s.T(), sql, "CREATE TYPE my_outbox_status AS ENUM")
	assert.Contains(s.T(), sql, "CREATE TYPE my_consumer_status AS ENUM")
	assert.Contains(s.T(), sql, "idx_my_consumer_outbox_id")

	// No FK constraint
	assert.NotContains(s.T(), sql, "REFERENCES my_outbox")
}
