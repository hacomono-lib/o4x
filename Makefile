.PHONY: test test-short test-coverage lint build up down clean schema schema-inbox schema-gen

# Test commands
# Note: -p 1 is required to prevent flaky tests caused by concurrent package execution.
# Both contrib/pgx and contrib/gorm share the same 'outbox' table, which causes race conditions
# when tests run in parallel. This does NOT affect goroutine-level parallelism within tests.
test: up ## Run all tests (requires DB - run 'make up' first)
	go test ./... --count=1 -race -p 1

test-v: up ## Run all tests with verbose output
	go test ./... -v --count=1 -race -p 1

test-short: ## Run unit tests only (no DB required)
	go test ./... -short --count=1 -race

test-coverage: ## Run tests with coverage report
	go test ./... -coverprofile=coverage.out --count=1 -race -p 1
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Docker commands (Root infrastructure for CI/tests)
up: ## Start local infrastructure (PostgreSQL + LocalStack)
	docker-compose up -d

down: ## Stop local infrastructure
	docker-compose down

logs: ## Show docker-compose logs
	docker-compose logs -f

ps: ## Show docker-compose status
	docker-compose ps

# Example app commands
app-up: ## Start example application
	cd examples/app && docker-compose up -d

app-down: ## Stop example application
	cd examples/app && docker-compose down

app-logs: ## Show example application logs
	cd examples/app && docker-compose logs -f

# Build commands
build: ## Build all packages
	go build ./...

lint: ## Run linter (requires golangci-lint)
	golangci-lint run

# Schema commands
schema: ## Generate schema SQL (stdout)
	go run cmd/o4x-schema/main.go

schema-inbox: ## Generate schema SQL with consumer_inbox table (stdout)
	go run cmd/o4x-schema/main.go --with-inbox

schema-gen: ## Generate scripts/schema.sql for test DB initialization
	go run cmd/o4x-schema/main.go --with-inbox > scripts/schema.sql
	@echo "Generated scripts/schema.sql"

# Utility commands
clean: ## Clean up generated files
	rm -f coverage.out coverage.html

tidy: ## Run go mod tidy
	go mod tidy

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
