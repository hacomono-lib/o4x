#!/bin/bash

# Create FIFO SQS queue for o4x events
awslocal sqs create-queue \
  --queue-name o4x-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

echo "SQS FIFO queue created: o4x-events.fifo"

# List queues to verify
awslocal sqs list-queues
