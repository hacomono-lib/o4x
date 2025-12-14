package core

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// ErrorsSuite tests error types and IsRetryable function
type ErrorsSuite struct {
	suite.Suite
}

func TestErrorsSuite(t *testing.T) {
	suite.Run(t, new(ErrorsSuite))
}

func (s *ErrorsSuite) TestIsRetryable_NilError() {
	// Arrange
	var err error = nil

	// Act
	result := IsRetryable(err)

	// Assert
	assert.False(s.T(), result, "nil error should not be retryable")
}

func (s *ErrorsSuite) TestIsRetryable_RegularError() {
	// Arrange
	err := errors.New("some error")

	// Act
	result := IsRetryable(err)

	// Assert
	assert.True(s.T(), result, "regular error should be retryable by default")
}

func (s *ErrorsSuite) TestIsRetryable_PermanentError() {
	// Arrange
	err := NewPermanentError(errors.New("permanent"))

	// Act
	result := IsRetryable(err)

	// Assert
	assert.False(s.T(), result, "PermanentError should not be retryable")
}

func (s *ErrorsSuite) TestIsRetryable_TransientError() {
	// Arrange
	err := NewTransientError(errors.New("transient"))

	// Act
	result := IsRetryable(err)

	// Assert
	assert.True(s.T(), result, "TransientError should be retryable")
}

func (s *ErrorsSuite) TestIsRetryable_WrappedPermanentError() {
	// Arrange
	err := errors.Join(errors.New("wrapper"), NewPermanentError(errors.New("permanent")))

	// Act
	result := IsRetryable(err)

	// Assert
	assert.False(s.T(), result, "wrapped PermanentError should not be retryable")
}

func (s *ErrorsSuite) TestPermanentError_ErrorContainsCause() {
	// Arrange
	cause := errors.New("original error")
	err := NewPermanentError(cause)

	// Act & Assert
	assert.Equal(s.T(), "permanent error: original error", err.Error())
	assert.True(s.T(), errors.Is(err, cause), "Unwrap should return original cause")
}

func (s *ErrorsSuite) TestTransientError_ErrorContainsCause() {
	// Arrange
	cause := errors.New("original error")
	err := NewTransientError(cause)

	// Act & Assert
	assert.Equal(s.T(), "transient error: original error", err.Error())
	assert.True(s.T(), errors.Is(err, cause), "Unwrap should return original cause")
}

// PublishErrorSuite tests PublishError type
type PublishErrorSuite struct {
	suite.Suite
}

func TestPublishErrorSuite(t *testing.T) {
	suite.Run(t, new(PublishErrorSuite))
}

func (s *PublishErrorSuite) TestWithRetryableCause_IsRetryable() {
	// Arrange
	cause := errors.New("network timeout")
	err := &PublishError{
		OutboxID:  "test-id",
		EventType: "test-topic",
		Cause:     cause,
	}

	// Act
	result := err.IsRetryable()

	// Assert
	assert.True(s.T(), result, "PublishError with regular cause should be retryable")
}

func (s *PublishErrorSuite) TestWithRetryableCause_UnwrapReturnsCause() {
	// Arrange
	cause := errors.New("network timeout")
	err := &PublishError{
		OutboxID:  "test-id",
		EventType: "test-topic",
		Cause:     cause,
	}

	// Act & Assert
	assert.True(s.T(), errors.Is(err, cause), "Unwrap should return original cause")
}

func (s *PublishErrorSuite) TestWithPermanentCause_IsNotRetryable() {
	// Arrange
	cause := NewPermanentError(errors.New("validation failed"))
	err := &PublishError{
		OutboxID:  "test-id",
		EventType: "test-topic",
		Cause:     cause,
	}

	// Act
	result := err.IsRetryable()

	// Assert
	assert.False(s.T(), result, "PublishError with PermanentError cause should not be retryable")
}

// CustomRetryableErrorSuite tests custom RetryableError implementations
type CustomRetryableErrorSuite struct {
	suite.Suite
}

func TestCustomRetryableErrorSuite(t *testing.T) {
	suite.Run(t, new(CustomRetryableErrorSuite))
}

// customRetryableError implements RetryableError for testing
type customRetryableError struct {
	retryable bool
}

func (e *customRetryableError) Error() string {
	return "custom error"
}

func (e *customRetryableError) IsRetryable() bool {
	return e.retryable
}

func (s *CustomRetryableErrorSuite) TestCustomRetryableError_ReturnsTrue() {
	// Arrange
	err := &customRetryableError{retryable: true}

	// Act
	result := IsRetryable(err)

	// Assert
	assert.True(s.T(), result, "custom retryable error should return true")
}

func (s *CustomRetryableErrorSuite) TestCustomNonRetryableError_ReturnsFalse() {
	// Arrange
	err := &customRetryableError{retryable: false}

	// Act
	result := IsRetryable(err)

	// Assert
	assert.False(s.T(), result, "custom non-retryable error should return false")
}

func (s *ErrorsSuite) TestTruncateErrorMessage_ShortMessage() {
	// Arrange & Act & Assert
	msg := "short error message"
	assert.Equal(s.T(), msg, TruncateErrorMessage(msg))
	assert.Equal(s.T(), "", TruncateErrorMessage(""))
}

func (s *ErrorsSuite) TestTruncateErrorMessage_LongMessage() {
	// Arrange
	msg := strings.Repeat("x", MaxErrorMessageLength+100)

	// Act
	result := TruncateErrorMessage(msg)

	// Assert
	assert.Equal(s.T(), MaxErrorMessageLength, len(result))
	assert.Contains(s.T(), result, "... (truncated)")
}

// ValidateTableNameSuite tests ValidateTableName function
type ValidateTableNameSuite struct {
	suite.Suite
}

func TestValidateTableNameSuite(t *testing.T) {
	suite.Run(t, new(ValidateTableNameSuite))
}

func (s *ValidateTableNameSuite) TestValidTableNames() {
	validNames := []string{
		"outbox",
		"my_outbox",
		"Outbox",
		"outbox123",
		"_private",
		"public.outbox",
		"my_schema.my_table",
		"Schema1.Table2",
	}

	for _, name := range validNames {
		s.Run(name, func() {
			err := ValidateTableName(name)
			assert.NoError(s.T(), err, "table name %q should be valid", name)
		})
	}
}

func (s *ValidateTableNameSuite) TestInvalidTableNames() {
	invalidNames := []string{
		"",                  // empty
		"123table",          // starts with number
		"table-name",        // contains hyphen
		"table name",        // contains space
		"table;drop",        // contains semicolon
		"table--comment",    // contains double hyphen
		"table'injection",   // contains quote
		"table\"double",     // contains double quote
		"table.schema.name", // multiple dots
		".table",            // starts with dot
		"table.",            // ends with dot
	}

	for _, name := range invalidNames {
		s.Run(name, func() {
			err := ValidateTableName(name)
			assert.ErrorIs(s.T(), err, ErrInvalidTableName, "table name %q should be invalid", name)
		})
	}
}
