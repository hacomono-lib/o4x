#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== O4X Example App Startup Script ===${NC}\n"

# Check if docker-compose is running
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: docker-compose is not installed${NC}"
    exit 1
fi

# Navigate to app directory
cd "$(dirname "$0")/.."

echo -e "${YELLOW}Step 1: Stopping existing containers...${NC}"
docker-compose down

echo -e "\n${YELLOW}Step 2: Starting infrastructure (PostgreSQL + LocalStack)...${NC}"
docker-compose up -d postgres localstack

echo -e "\n${YELLOW}Step 3: Waiting for services to be healthy...${NC}"
sleep 5

# Wait for postgres
echo -n "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if docker exec o4x-app-postgres pg_isready -U postgres > /dev/null 2>&1; then
        echo -e " ${GREEN}✓${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for localstack
echo -n "Waiting for LocalStack to be ready..."
for i in {1..30}; do
    if docker exec o4x-app-localstack awslocal sqs list-queues > /dev/null 2>&1; then
        echo -e " ${GREEN}✓${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

echo -e "\n${YELLOW}Step 4: Starting application services (API, Dispatcher, Consumers)...${NC}"
docker-compose up -d api dispatcher consumer-order consumer-notification consumer-user

echo -e "\n${YELLOW}Step 5: Waiting for application services to start...${NC}"
sleep 3

# Check health endpoints
echo -n "Checking API health..."
for i in {1..10}; do
    if curl -s http://localhost:18000/health > /dev/null 2>&1; then
        echo -e " ${GREEN}✓${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

echo -e "\n${GREEN}=== All services started successfully! ===${NC}\n"

echo "Service endpoints:"
echo "  API:        http://localhost:18000"
echo "  Dispatcher: No fixed port (supports --scale)"
echo "  Consumers:  No fixed ports (all support --scale)"
echo ""
echo "To check service health:"
echo "  docker-compose logs -f dispatcher"
echo "  docker-compose logs -f consumer-order"
echo "  docker-compose logs -f consumer-notification"
echo "  docker-compose logs -f consumer-user"
echo ""
echo "Database:"
echo "  PostgreSQL: localhost:25432 (user: postgres, pass: postgres, db: o4x)"
echo ""
echo "Queue:"
echo "  LocalStack: http://localhost:24566"
echo ""
echo -e "${YELLOW}To view logs:${NC}"
echo "  docker-compose logs -f [service-name]"
echo ""
echo -e "${YELLOW}To stop all services:${NC}"
echo "  docker-compose down"
