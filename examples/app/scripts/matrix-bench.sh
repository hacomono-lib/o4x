#!/bin/bash
set -e

# O4X Dispatcher Matrix Benchmark Script
# Tests dispatcher performance across different instance × worker combinations

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== O4X Dispatcher Matrix Benchmark ===${NC}\n"

# Configuration
REQUESTS=${1:-1000}        # Number of messages to send (measurement phase)
EVENT_TYPE=${2:-user}       # Event type (user/notification/order)
WARMUP_REQUESTS=${3:-1000}  # Warmup messages (default: 1000)
REPORT_DIR="reports/matrix"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Matrix configuration
DISPATCHER_INSTANCES=(1 3 5)
WORKER_COUNTS=(1 5 10)

echo -e "${YELLOW}Configuration:${NC}"
echo "  Warmup Messages: $WARMUP_REQUESTS"
echo "  Measurement Messages: $REQUESTS"
echo "  Event Type: $EVENT_TYPE"
echo "  Report Dir: $REPORT_DIR"
echo "  Dispatcher Instances: ${DISPATCHER_INSTANCES[@]}"
echo "  Worker Counts: ${WORKER_COUNTS[@]}"
echo ""

# Create report directory
mkdir -p "$REPORT_DIR"

# Navigate to app directory
cd "$(dirname "$0")/.."

echo -e "${YELLOW}Step 1: Ensure services are running...${NC}"
docker-compose up -d postgres localstack api consumer-user
sleep 5

# Wait for API to be ready
echo -n "Waiting for API..."
for i in {1..30}; do
    if curl -s http://localhost:18000/health > /dev/null 2>&1; then
        echo -e " ${GREEN}✓${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

echo -e "\n${YELLOW}Step 2: Running matrix benchmarks...${NC}\n"

TOTAL_TESTS=$((${#DISPATCHER_INSTANCES[@]} * ${#WORKER_COUNTS[@]}))
CURRENT_TEST=0

# Results summary array
declare -a RESULTS

for instances in "${DISPATCHER_INSTANCES[@]}"; do
    for workers in "${WORKER_COUNTS[@]}"; do
        CURRENT_TEST=$((CURRENT_TEST + 1))

        echo -e "${BLUE}=== Test $CURRENT_TEST/$TOTAL_TESTS: dispatcher=$instances workers=$workers ===${NC}"

        REPORT_FILE="$REPORT_DIR/${TIMESTAMP}_d${instances}_w${workers}.txt"

        # Clean up any existing dispatcher-bench containers
        echo "Stopping existing dispatcher-bench instances..."
        docker-compose stop dispatcher-bench > /dev/null 2>&1 || true
        docker-compose rm -f dispatcher-bench > /dev/null 2>&1 || true

        # Clean database
        echo "Cleaning database..."
        docker exec o4x-app-postgres psql -U postgres -d o4x -c \
            "TRUNCATE outbox, consumer_inbox RESTART IDENTITY;" > /dev/null 2>&1

        # Scale dispatcher-bench
        echo "Starting $instances dispatcher-bench instance(s) with $workers workers each..."
        WORKERS=$workers docker-compose up -d --scale dispatcher-bench=$instances dispatcher-bench

        sleep 3

        # === WARMUP PHASE ===
        if [ "$WARMUP_REQUESTS" -gt 0 ]; then
            echo "Running warmup phase with $WARMUP_REQUESTS messages..."
            go run benchmark/main.go \
                --endpoint http://localhost:18000 \
                --type $EVENT_TYPE \
                --requests $WARMUP_REQUESTS \
                --concurrency 50 \
                --format text > /dev/null 2>&1

            # Wait for warmup to complete
            WARMUP_WAIT_TIME=60
            for i in $(seq 1 $WARMUP_WAIT_TIME); do
                WARMUP_DONE=$(docker exec o4x-app-postgres psql -U postgres -d o4x -t -c \
                    "SELECT COUNT(*) FROM outbox WHERE status='PUBLISHED';" | tr -d ' ')
                if [ "$WARMUP_DONE" -ge "$WARMUP_REQUESTS" ]; then
                    echo "Warmup complete ($WARMUP_DONE/$WARMUP_REQUESTS published)"
                    break
                fi
                sleep 1
            done

            # Clean database after warmup
            echo "Cleaning database after warmup..."
            docker exec o4x-app-postgres psql -U postgres -d o4x -c \
                "TRUNCATE outbox, consumer_inbox RESTART IDENTITY;" > /dev/null 2>&1

            sleep 2
        fi

        # === MEASUREMENT PHASE ===
        # Send messages
        echo "Sending $REQUESTS messages (measurement phase)..."
        SEND_START=$(date +%s)
        go run benchmark/main.go \
            --endpoint http://localhost:18000 \
            --type $EVENT_TYPE \
            --requests $REQUESTS \
            --concurrency 50 \
            --format text > /dev/null 2>&1
        SEND_END=$(date +%s)
        SEND_DURATION=$((SEND_END - SEND_START))

        echo "Messages sent in ${SEND_DURATION}s. Waiting for processing..."

        # Wait for processing to complete (check outbox PUBLISHED status)
        # Scale wait time based on message count (60s for 1000 msgs = 0.06s per msg)
        WAIT_TIME=$((REQUESTS * 60 / 1000))
        # Minimum 60s, maximum 600s (10 minutes)
        if [ $WAIT_TIME -lt 60 ]; then WAIT_TIME=60; fi
        if [ $WAIT_TIME -gt 600 ]; then WAIT_TIME=600; fi
        PROCESSED=0
        for i in $(seq 1 $WAIT_TIME); do
            PUBLISHED=$(docker exec o4x-app-postgres psql -U postgres -d o4x -t -c \
                "SELECT COUNT(*) FROM outbox WHERE status='PUBLISHED';" | tr -d ' ')

            PROCESSED=$PUBLISHED

            if [ "$PROCESSED" -ge "$REQUESTS" ]; then
                echo -e "${GREEN}✓ All messages published after ${i}s${NC}"
                break
            fi

            if [ $((i % 10)) -eq 0 ]; then
                echo "  Progress: $PROCESSED / $REQUESTS ($i/${WAIT_TIME}s)"
            fi

            sleep 1
        done

        # Collect metrics
        echo "Collecting metrics..."

        # Dispatcher throughput
        DISPATCHER_COUNT=$(docker exec o4x-app-postgres psql -U postgres -d o4x -t -c \
            "SELECT COUNT(*) FROM outbox WHERE status='PUBLISHED';" | tr -d ' ')

        # Processing duration
        PROC_DURATION=$(docker exec o4x-app-postgres psql -U postgres -d o4x -t -c \
            "SELECT EXTRACT(EPOCH FROM (MAX(updated_at) - MIN(created_at))) \
             FROM outbox WHERE status='PUBLISHED';" | tr -d ' ')

        # Calculate throughput
        if [ "$PROC_DURATION" != "" ] && [ "$(echo "$PROC_DURATION > 0" | bc)" -eq 1 ]; then
            THROUGHPUT=$(echo "scale=2; $DISPATCHER_COUNT / $PROC_DURATION" | bc)
        else
            THROUGHPUT="N/A"
        fi

        # Save report
        cat > "$REPORT_FILE" <<EOF
========================================
O4X Matrix Benchmark Report
========================================
Timestamp: $(date '+%Y-%m-%d %H:%M:%S')
Configuration:
  Dispatcher Instances: $instances
  Workers per Instance:  $workers
  Total Workers:        $((instances * workers))
  Messages:             $REQUESTS
  Event Type:           $EVENT_TYPE

========================================
RESULTS
========================================

Send Phase:
  Duration:             ${SEND_DURATION}s

Dispatch Phase:
  Published:            $DISPATCHER_COUNT
  Duration:             ${PROC_DURATION}s
  Throughput:           ${THROUGHPUT} msg/sec
  Success Rate:         $(echo "scale=2; $DISPATCHER_COUNT * 100 / $REQUESTS" | bc)%

========================================
EOF

        # Display summary
        echo -e "${GREEN}Test Complete:${NC}"
        echo "  Published: $DISPATCHER_COUNT / $REQUESTS"
        echo "  Throughput: ${THROUGHPUT} msg/sec"
        echo "  Report: $REPORT_FILE"
        echo ""

        # Store result for final summary
        RESULTS+=("d=$instances w=$workers | Throughput: ${THROUGHPUT} msg/sec | Published: $DISPATCHER_COUNT/$REQUESTS")

        # Brief pause between tests
        sleep 2
    done
done

# Final summary
echo -e "\n${BLUE}=== Matrix Benchmark Complete ===${NC}\n"
echo -e "${YELLOW}Summary of All Tests:${NC}"
echo "----------------------------------------"
printf "%-20s | %-25s | %s\n" "Configuration" "Throughput" "Completion"
echo "----------------------------------------"
for result in "${RESULTS[@]}"; do
    echo "$result" | awk -F'|' '{printf "%-20s | %-25s | %s\n", $1, $2, $3}'
done
echo "----------------------------------------"

echo -e "\n${GREEN}All reports saved to: $REPORT_DIR${NC}"
echo -e "${YELLOW}To view detailed report:${NC}"
echo "  cat $REPORT_DIR/${TIMESTAMP}_d*_w*.txt"

# Cleanup
echo -e "\n${YELLOW}Cleaning up...${NC}"
docker-compose stop dispatcher-bench > /dev/null 2>&1
docker-compose rm -f dispatcher-bench > /dev/null 2>&1

echo -e "${GREEN}Done!${NC}"
