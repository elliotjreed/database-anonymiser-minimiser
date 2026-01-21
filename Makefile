.PHONY: build build-nocgo test test-coverage test-race lint fmt tidy clean help

BINARY := dbmask
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

build: ## Build the CLI with CGO (SQLite support)
	CGO_ENABLED=1 go build -o $(BINARY) ./cmd/dbmask

build-nocgo: ## Build without CGO (no SQLite support, cross-platform)
	CGO_ENABLED=0 go build -o $(BINARY) ./cmd/dbmask

build-release: ## Build with version info and optimizations
	CGO_ENABLED=1 go build -ldflags="-s -w -X main.version=$(VERSION)" -o $(BINARY) ./cmd/dbmask

test: ## Run tests
	go test ./...

test-coverage: ## Run tests with coverage report
	go test -v -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

test-race: ## Run tests with race detection
	go test -v -race ./...

test-all: ## Run tests with race detection and coverage
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

lint: ## Run linter (requires golangci-lint)
	golangci-lint run

fmt: ## Format code
	go fmt ./...
	gofmt -s -w .

tidy: ## Tidy dependencies
	go mod tidy

clean: ## Remove build artifacts
	rm -f $(BINARY)
	rm -f coverage.out

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
