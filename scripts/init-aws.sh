#!/bin/bash

# Wait for LocalStack to be ready
echo "Waiting for LocalStack to be ready..."
sleep 5

# Create SQS FIFO queue
awslocal sqs create-queue \
  --queue-name o4x-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

# Create SQS Standard queue
awslocal sqs create-queue \
  --queue-name o4x-events-standard

echo "SQS queues created successfully"
