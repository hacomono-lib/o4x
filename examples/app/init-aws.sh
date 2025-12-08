#!/bin/bash

# Initialize SQS queues for the application

echo "Creating SQS queues..."

# Create FIFO queue for order events (strict ordering)
awslocal sqs create-queue \
  --queue-name o4x-events-order.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

# Create Standard queue for notification events (higher throughput)
awslocal sqs create-queue \
  --queue-name o4x-events-notification

# Create Standard queue for user events (higher throughput)
awslocal sqs create-queue \
  --queue-name o4x-events-user

echo "SQS queues created successfully"

# List all queues
awslocal sqs list-queues
