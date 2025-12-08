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
	assert.Contains(s.T(), ddl, "metadata         JSONB")
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

func (s *SchemaSuite) TestDropOutboxDDL_GeneratesCorrectStatements() {
	// Arrange
	tableName := "outbox"

	// Act
	ddl := DropOutboxDDL(tableName)

	// Assert
	assert.Contains(s.T(), ddl, "DROP TABLE IF EXISTS outbox;")
	assert.Contains(s.T(), ddl, "DROP TYPE IF EXISTS outbox_status;")
}
