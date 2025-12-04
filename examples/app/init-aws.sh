#!/bin/bash

# Initialize SQS queues for the application

echo "Creating SQS queues..."

# Create FIFO queue for order/inventory events (strict ordering)
awslocal sqs create-queue \
  --queue-name o4x-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

# Create Standard queue for notifications and user events (higher throughput)
awslocal sqs create-queue \
  --queue-name o4x-events-standard

echo "SQS queues created successfully"

# List all queues
awslocal sqs list-queues
