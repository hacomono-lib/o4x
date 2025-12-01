package sqs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// TopicQueueMapSuite tests TopicQueueMap implementation
type TopicQueueMapSuite struct {
	suite.Suite
}

func TestTopicQueueMapSuite(t *testing.T) {
	suite.Run(t, new(TopicQueueMapSuite))
}

func (s *TopicQueueMapSuite) TestExactMatch_ReturnsRegisteredQueue() {
	// Arrange
	m := NewTopicQueueMap("default-queue")
	m.Register("order.created", "order-queue")

	// Act
	result := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "order-queue", result)
}

func (s *TopicQueueMapSuite) TestNoMatch_ReturnsDefaultQueue() {
	// Arrange
	m := NewTopicQueueMap("default-queue")
	m.Register("order.created", "order-queue")

	// Act
	result := m.QueueURL("unknown.topic")

	// Assert
	assert.Equal(s.T(), "default-queue", result)
}

func (s *TopicQueueMapSuite) TestPrefixMatch_ReturnsRegisteredQueue() {
	// Arrange
	m := NewTopicQueueMap("default-queue")
	m.RegisterPrefix("order.", "order-queue")

	// Act
	result := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "order-queue", result)
}

func (s *TopicQueueMapSuite) TestLongestPrefixMatch_TakesPriority() {
	// Arrange
	m := NewTopicQueueMap("default-queue")
	m.RegisterPrefix("order.", "order-queue")
	m.RegisterPrefix("order.payment.", "payment-queue")

	// Act
	resultPayment := m.QueueURL("order.payment.completed")
	resultOrder := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "payment-queue", resultPayment, "longer prefix should match")
	assert.Equal(s.T(), "order-queue", resultOrder, "shorter prefix should match when no longer prefix exists")
}

func (s *TopicQueueMapSuite) TestExactMatchTakesPriorityOverPrefix() {
	// Arrange
	m := NewTopicQueueMap("default-queue")
	m.RegisterPrefix("order.", "order-queue")
	m.Register("order.special", "special-queue")

	// Act
	resultSpecial := m.QueueURL("order.special")
	resultOther := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "special-queue", resultSpecial, "exact match should take priority")
	assert.Equal(s.T(), "order-queue", resultOther, "prefix should still work for other topics")
}

func (s *TopicQueueMapSuite) TestMultiplePrefixes_SortedByLengthDescending() {
	// Arrange
	m := NewTopicQueueMap("default-queue")
	// Register in non-sorted order
	m.RegisterPrefix("a", "a-queue")
	m.RegisterPrefix("abc", "abc-queue")
	m.RegisterPrefix("ab", "ab-queue")
	m.RegisterPrefix("abcd", "abcd-queue")

	// Act
	resultAbcd := m.QueueURL("abcde")
	resultAbc := m.QueueURL("abcx")
	resultAb := m.QueueURL("abx")
	resultA := m.QueueURL("ax")

	// Assert
	assert.Equal(s.T(), "abcd-queue", resultAbcd)
	assert.Equal(s.T(), "abc-queue", resultAbc)
	assert.Equal(s.T(), "ab-queue", resultAb)
	assert.Equal(s.T(), "a-queue", resultA)
}

func (s *TopicQueueMapSuite) TestReRegisterPrefix_UpdatesQueue() {
	// Arrange
	m := NewTopicQueueMap("default-queue")
	m.RegisterPrefix("order.", "old-queue")
	m.RegisterPrefix("order.", "new-queue")

	// Act
	result := m.QueueURL("order.created")

	// Assert
	assert.Equal(s.T(), "new-queue", result, "re-registering should update the queue")
}

func (s *TopicQueueMapSuite) TestEmptyTopic_ReturnsDefaultQueue() {
	// Arrange
	m := NewTopicQueueMap("default-queue")
	m.RegisterPrefix("order.", "order-queue")

	// Act
	result := m.QueueURL("")

	// Assert
	assert.Equal(s.T(), "default-queue", result)
}
