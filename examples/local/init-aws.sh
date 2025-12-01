#!/bin/bash

# Create FIFO SQS queue for o4x events
awslocal sqs create-queue \
  --queue-name o4x-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false,VisibilityTimeout=30,ReceiveMessageWaitTimeSeconds=20

echo "SQS FIFO queue created: o4x-events.fifo"

# Create Standard SQS queue for o4x events
awslocal sqs create-queue \
  --queue-name o4x-events-standard \
  --attributes VisibilityTimeout=30,ReceiveMessageWaitTimeSeconds=20

echo "SQS Standard queue created: o4x-events-standard"

# List queues to verify
awslocal sqs list-queues
