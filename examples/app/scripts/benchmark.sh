#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Default values
REQUESTS=200
CONCURRENCY=20
TYPE="notification"
WAIT_TIME=10
REBUILD=false
SAVE_REPORT=true
COMPARE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--requests)
            REQUESTS="$2"
            shift 2
            ;;
        -c|--concurrency)
            CONCURRENCY="$2"
            shift 2
            ;;
        -t|--type)
            TYPE="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_TIME="$2"
            shift 2
            ;;
        --rebuild)
            REBUILD=true
            shift
            ;;
        --no-save)
            SAVE_REPORT=false
            shift
            ;;
        --compare)
            COMPARE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -r, --requests NUM      Number of requests (default: 200)"
            echo "  -c, --concurrency NUM   Concurrency level (default: 20)"
            echo "  -t, --type TYPE         Request type: notification|order|user (default: notification)"
            echo "  -w, --wait SECONDS      Wait time for async processing (default: 10)"
            echo "  --rebuild               Rebuild containers before benchmarking"
            echo "  --no-save               Don't save benchmark report to file"
            echo "  --compare               Compare with previous benchmark result"
            echo "  -h, --help              Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Basic benchmark"
            echo "  $0 --rebuild                          # Rebuild and benchmark"
            echo "  $0 -r 500 -c 50                       # Custom requests/concurrency"
            echo "  $0 --compare                          # Compare with previous run"
            echo "  $0 --rebuild --compare                # Rebuild, benchmark, and compare"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Navigate to app directory
cd "$(dirname "$0")/.."

# Create reports directory
REPORTS_DIR="reports"
mkdir -p "$REPORTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="$REPORTS_DIR/benchmark_${TIMESTAMP}.txt"
LATEST_LINK="$REPORTS_DIR/latest.txt"
PREVIOUS_REPORT="$REPORTS_DIR/previous.txt"

echo -e "${GREEN}=== O4X Benchmark Script ===${NC}\n"
echo "Configuration:"
echo "  Requests:     $REQUESTS"
echo "  Concurrency:  $CONCURRENCY"
echo "  Type:         $TYPE"
echo "  Wait time:    ${WAIT_TIME}s"
echo "  Rebuild:      $REBUILD"
echo "  Save report:  $SAVE_REPORT"
echo "  Compare:      $COMPARE"
if [ "$SAVE_REPORT" = true ]; then
    echo "  Report file:  $REPORT_FILE"
fi
echo ""

# Rebuild containers if requested
if [ "$REBUILD" = true ]; then
    echo -e "${YELLOW}Step 0: Rebuilding containers...${NC}"
    echo "Stopping services..."
    docker-compose stop api dispatcher consumer-order consumer-notification consumer-user > /dev/null 2>&1
    echo "Building containers..."
    docker-compose build api dispatcher consumer-order consumer-notification consumer-user
    echo "Starting services..."
    docker-compose up -d api dispatcher consumer-order consumer-notification consumer-user
    echo "Waiting for services to be healthy (10s)..."
    sleep 10
    echo -e "${GREEN}✓ Containers rebuilt${NC}\n"
fi

# Check if API is running
if ! curl -s http://localhost:18000/health > /dev/null 2>&1; then
    echo -e "${RED}Error: API is not running. Please start services first.${NC}"
    echo "Run: ./scripts/start.sh"
    exit 1
fi

# Step 1: Clean database
echo -e "${YELLOW}Step 1: Cleaning database...${NC}"
docker exec o4x-app-postgres psql -U postgres -d o4x -c "TRUNCATE outbox, consumer_inbox RESTART IDENTITY;" > /dev/null
echo -e "${GREEN}✓ Database cleaned${NC}\n"

# Step 2: Run benchmark
echo -e "${YELLOW}Step 2: Running benchmark...${NC}"
go run benchmark/main.go \
    --endpoint http://localhost:18000 \
    --type "$TYPE" \
    --requests "$REQUESTS" \
    --concurrency "$CONCURRENCY" \
    --format text

# Step 3: Wait for async processing
echo -e "\n${YELLOW}Step 3: Waiting ${WAIT_TIME}s for async processing...${NC}"
sleep "$WAIT_TIME"

# Step 4: Show results
echo -e "\n${BLUE}=== Performance Results ===${NC}\n"

# Function to output both to terminal and file
output() {
    echo -e "$1"
    if [ "$SAVE_REPORT" = true ]; then
        echo -e "$1" | sed 's/\x1b\[[0-9;]*m//g' >> "$REPORT_FILE"
    fi
}

# Generate report header
if [ "$SAVE_REPORT" = true ]; then
    cat > "$REPORT_FILE" << EOF
========================================
O4X Benchmark Report
========================================
Timestamp: $(date '+%Y-%m-%d %H:%M:%S')
Configuration:
  Requests:     $REQUESTS
  Concurrency:  $CONCURRENCY
  Type:         $TYPE
  Wait time:    ${WAIT_TIME}s
  Rebuilt:      $REBUILD

========================================
PERFORMANCE RESULTS
========================================

EOF
fi

# Dispatcher Performance
output "${BLUE}Dispatcher Performance:${NC}"
DISPATCHER_DATA=$(docker exec o4x-app-postgres psql -U postgres -d o4x -t -c "
SELECT
  status,
  COUNT(*),
  ROUND(EXTRACT(EPOCH FROM (MAX(updated_at) - MIN(created_at)))::numeric, 2),
  ROUND((COUNT(*) / EXTRACT(EPOCH FROM (MAX(updated_at) - MIN(created_at))))::numeric)
FROM outbox
GROUP BY status
ORDER BY status;")

echo "$DISPATCHER_DATA" | while read -r status count duration throughput; do
    output "  Status: $status | Count: $count | Duration: ${duration}s | Throughput: $throughput msg/sec"
done

# Consumer Performance
output "\n${BLUE}Consumer Performance:${NC}"
TOTAL_MESSAGES=$(docker exec o4x-app-postgres psql -U postgres -d o4x -t -c "SELECT COUNT(*) FROM outbox;")
CONSUMER_DATA=$(docker exec o4x-app-postgres psql -U postgres -d o4x -t -c "
SELECT
  consumer_name,
  COUNT(*),
  ROUND(EXTRACT(EPOCH FROM (MAX(completed_at) - MIN(completed_at)))::numeric, 2),
  ROUND((COUNT(*) / EXTRACT(EPOCH FROM (MAX(completed_at) - MIN(completed_at))))::numeric)
FROM consumer_inbox
GROUP BY consumer_name
ORDER BY consumer_name;")

echo "$CONSUMER_DATA" | while read -r consumer count duration throughput; do
    output "  Consumer: $consumer | Completed: $count/$TOTAL_MESSAGES | Duration: ${duration}s | Throughput: $throughput msg/sec"
done

# Inbox Status Summary
output "\n${BLUE}Inbox Status Summary:${NC}"
INBOX_SUMMARY=$(docker exec o4x-app-postgres psql -U postgres -d o4x -t -c "
SELECT
  (SELECT COUNT(*) FROM outbox),
  COUNT(*),
  ((SELECT COUNT(*) FROM outbox) - COUNT(*)),
  ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM outbox))::numeric, 1)
FROM consumer_inbox;")

read -r total completed pending completion <<< "$INBOX_SUMMARY"
output "  Total messages: $total | Completed: $completed | Pending: $pending | Completion: $completion%"

echo ""

# Save links for comparison
if [ "$SAVE_REPORT" = true ]; then
    # Backup current latest as previous
    if [ -f "$LATEST_LINK" ]; then
        cp "$LATEST_LINK" "$PREVIOUS_REPORT"
    fi
    # Create new latest link
    cp "$REPORT_FILE" "$LATEST_LINK"
    echo -e "${GREEN}✓ Report saved: $REPORT_FILE${NC}"
fi

# Compare with previous report if requested
if [ "$COMPARE" = true ] && [ -f "$PREVIOUS_REPORT" ]; then
    echo -e "\n${MAGENTA}=== Comparison with Previous Run ===${NC}\n"

    # Extract key metrics
    PREV_DISPATCHER_THROUGHPUT=$(grep "Dispatcher Performance" -A 1 "$PREVIOUS_REPORT" | grep "Throughput" | sed 's/.*Throughput: \([0-9]*\) msg.*/\1/')
    CURR_DISPATCHER_THROUGHPUT=$(echo "$DISPATCHER_DATA" | awk '{print $4}')

    PREV_CONSUMER_THROUGHPUT=$(grep "Consumer Performance" -A 1 "$PREVIOUS_REPORT" | grep "Throughput" | sed 's/.*Throughput: \([0-9]*\) msg.*/\1/')
    CURR_CONSUMER_THROUGHPUT=$(echo "$CONSUMER_DATA" | awk '{print $4}')

    if [ -n "$PREV_DISPATCHER_THROUGHPUT" ] && [ -n "$CURR_DISPATCHER_THROUGHPUT" ]; then
        DISPATCHER_DIFF=$((CURR_DISPATCHER_THROUGHPUT - PREV_DISPATCHER_THROUGHPUT))
        if [ "$DISPATCHER_DIFF" -gt 0 ]; then
            echo -e "${GREEN}  Dispatcher: $CURR_DISPATCHER_THROUGHPUT msg/sec (+$DISPATCHER_DIFF)${NC}"
        elif [ "$DISPATCHER_DIFF" -lt 0 ]; then
            echo -e "${RED}  Dispatcher: $CURR_DISPATCHER_THROUGHPUT msg/sec ($DISPATCHER_DIFF)${NC}"
        else
            echo -e "  Dispatcher: $CURR_DISPATCHER_THROUGHPUT msg/sec (no change)"
        fi
    fi

    if [ -n "$PREV_CONSUMER_THROUGHPUT" ] && [ -n "$CURR_CONSUMER_THROUGHPUT" ]; then
        CONSUMER_DIFF=$((CURR_CONSUMER_THROUGHPUT - PREV_CONSUMER_THROUGHPUT))
        if [ "$CONSUMER_DIFF" -gt 0 ]; then
            echo -e "${GREEN}  Consumer: $CURR_CONSUMER_THROUGHPUT msg/sec (+$CONSUMER_DIFF)${NC}"
        elif [ "$CONSUMER_DIFF" -lt 0 ]; then
            echo -e "${RED}  Consumer: $CURR_CONSUMER_THROUGHPUT msg/sec ($CONSUMER_DIFF)${NC}"
        else
            echo -e "  Consumer: $CURR_CONSUMER_THROUGHPUT msg/sec (no change)"
        fi
    fi

    echo ""
    echo -e "  Previous report: $PREVIOUS_REPORT"
    echo -e "  Current report:  $REPORT_FILE"
elif [ "$COMPARE" = true ]; then
    echo -e "\n${YELLOW}No previous report found for comparison${NC}"
fi

echo ""
echo -e "${GREEN}✓ Benchmark completed${NC}"
