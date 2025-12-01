package schema

import (
	"strings"
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
	ddl := ConsumerMessagesDDL(tableName, "")

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

	// Check index
	assert.Contains(s.T(), ddl, "CREATE INDEX idx_consumer_messages_status ON consumer_messages (status)")

	// No FK constraint without outbox table
	assert.NotContains(s.T(), ddl, "FOREIGN KEY")
}

func (s *SchemaSuite) TestConsumerMessagesDDL_WithForeignKey() {
	// Arrange
	tableName := "consumer_messages"
	outboxTableName := "outbox"

	// Act
	ddl := ConsumerMessagesDDL(tableName, outboxTableName)

	// Assert
	assert.Contains(s.T(), ddl, "CONSTRAINT fk_consumer_messages_outbox FOREIGN KEY (outbox_id)")
	assert.Contains(s.T(), ddl, "REFERENCES outbox (id) ON DELETE SET NULL")
}

func (s *SchemaSuite) TestConsumerMessagesDDL_WithCustomTableNames() {
	// Arrange
	tableName := "my_consumer"
	outboxTableName := "my_outbox"

	// Act
	ddl := ConsumerMessagesDDL(tableName, outboxTableName)

	// Assert
	assert.Contains(s.T(), ddl, "CREATE TYPE my_consumer_status AS ENUM")
	assert.Contains(s.T(), ddl, "CREATE TABLE my_consumer")
	assert.Contains(s.T(), ddl, "fk_my_consumer_outbox")
	assert.Contains(s.T(), ddl, "REFERENCES my_outbox")
	assert.Contains(s.T(), ddl, "uq_my_consumer_message_id")
	assert.Contains(s.T(), ddl, "idx_my_consumer_status")
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

	// Check FK is created (since outbox table is provided)
	assert.Contains(s.T(), sql, "fk_consumer_messages_outbox")
	assert.Contains(s.T(), sql, "REFERENCES outbox")
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

	// Check consumer is dropped before outbox (FK dependency)
	consumerDropIdx := strings.Index(sql, "DROP TABLE IF EXISTS consumer_messages")
	outboxDropIdx := strings.Index(sql, "DROP TABLE IF EXISTS outbox")
	assert.Greater(s.T(), outboxDropIdx, consumerDropIdx, "consumer should be dropped before outbox due to FK")

	// Check both tables and types are dropped
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
	assert.Contains(s.T(), sql, "REFERENCES my_outbox")
}
