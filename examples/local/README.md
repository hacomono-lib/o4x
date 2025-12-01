# Local Development Environment

This directory contains the local development environment for o4x using Docker Compose.

## Components

- **PostgreSQL** - Database with o4x and o4x_test databases
- **LocalStack** - AWS SQS emulator

## Quick Start

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop services
docker compose down

# Clean up (remove volumes)
docker compose down -v
```

## Database Schema

The database schema is **dynamically generated** from `schema/schema.go` during Docker image build.

### Schema Files (Generated at Build Time)

1. **01_schema.sql** - Main database (o4x) schema
2. **03_init_test_db.sql** - Test database (o4x_test) schema

Both are generated from the same source: `go run cmd/o4x-schema/main.go --with-consumer`

### Seed Data (Static)

- **02_seed.sql** - Sample outbox messages for testing

### Rebuilding After Schema Changes

When you modify `schema/schema.go`, rebuild the Docker image:

```bash
docker compose down -v
docker compose build --no-cache postgres
docker compose up -d
```

The `--no-cache` flag ensures the schema is regenerated from the latest code.

## Dockerfile.postgres

The Dockerfile uses a multi-stage build:

1. **Builder stage**: Runs Go code to generate SQL from `schema/schema.go`
2. **Final stage**: PostgreSQL image with generated SQL files

This ensures `schema/schema.go` is the single source of truth for DDL.

## Connection Details

### Main Database (o4x)
- Host: localhost
- Port: 15432
- Database: o4x
- User: postgres
- Password: postgres

Connection string:
```
postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable
```

### Test Database (o4x_test)
- Host: localhost
- Port: 15432
- Database: o4x_test
- User: postgres
- Password: postgres

Connection string:
```
postgres://postgres:postgres@localhost:15432/o4x_test?sslmode=disable
```

### LocalStack SQS
- Endpoint: http://localhost:14566
- Region: us-east-1
- Queue URL: http://localhost:14566/000000000000/o4x-test-queue.fifo

## Running Examples

### 1. Enqueue Messages

```bash
cd examples/local/cmd/enqueue
go run main.go
```

### 2. Start Dispatcher

```bash
cd examples/local/cmd/dispatcher
go run main.go
```

### 3. Start Consumer

```bash
cd examples/local/cmd/consumer
go run main.go
```

## Troubleshooting

### Schema not updating

If your schema changes aren't reflected:

```bash
# Rebuild with no cache
docker compose build --no-cache postgres
docker compose up -d
```

### Port already in use

If ports 15432 or 14566 are already in use, modify `docker-compose.yml`:

```yaml
ports:
  - "25432:5432"  # Change 15432 to 25432
```

### Check database schema

```bash
# Check o4x database
docker exec o4x-postgres psql -U postgres -d o4x -c "\d outbox"

# Check o4x_test database
docker exec o4x-postgres psql -U postgres -d o4x_test -c "\d consumer_messages"
```

## Notes

- The `init-test-db.sql` file is **generated dynamically** - there is no static file
- Volumes are named `local_postgres-data` and `local_localstack-data`
- Use `docker compose down -v` to completely reset the environment

