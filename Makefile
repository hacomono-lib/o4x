.PHONY: test test-short test-coverage lint build up down clean schema

# Test commands
# Note: -p 1 is required to prevent flaky tests caused by concurrent package execution.
# Both contrib/pgx and contrib/gorm share the same 'outbox' table, which causes race conditions
# when tests run in parallel. This does NOT affect goroutine-level parallelism within tests.
test: ## Run all tests (requires DB - run 'make up' first)
	go test ./... --count=1 -race -p 1

test-v: ## Run all tests with verbose output
	go test ./... -v --count=1 -race -p 1

test-short: ## Run unit tests only (no DB required)
	go test ./... -short --count=1 -race

test-coverage: ## Run tests with coverage report
	go test ./... -coverprofile=coverage.out --count=1 -race -p 1
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Docker commands
up: ## Start local infrastructure (PostgreSQL + LocalStack)
	cd examples/local && docker-compose up -d

down: ## Stop local infrastructure
	cd examples/local && docker-compose down

logs: ## Show docker-compose logs
	cd examples/local && docker-compose logs -f

ps: ## Show docker-compose status
	cd examples/local && docker-compose ps

# Build commands
build: ## Build all packages
	go build ./...

lint: ## Run linter (requires golangci-lint)
	golangci-lint run

# Schema commands
schema: ## Generate schema SQL
	go run cmd/o4x-schema/main.go

schema-consumer: ## Generate schema SQL with consumer tables
	go run cmd/o4x-schema/main.go --with-consumer

# Utility commands
clean: ## Clean up generated files
	rm -f coverage.out coverage.html

tidy: ## Run go mod tidy
	go mod tidy

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
