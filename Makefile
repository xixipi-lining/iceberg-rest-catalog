.PHONY: build run test clean deps fmt lint docker-build

# Application name
APP_NAME = iceberg-rest-catalog

# Build directory
BUILD_DIR = build

# Go related variables
GO_VERSION = 1.24.4
GOOS = linux
GOARCH = amd64

# Default target
all: deps fmt lint test build

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go mod download

# Build application
build:
	@echo "Building application..."
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -ldflags="-w -s" -o $(BUILD_DIR)/$(APP_NAME) main.go

# Run locally
run:
	@echo "Running application..."
	go run main.go

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Run tests with coverage report
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Code linting
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# Clean build artifacts
clean:
	@echo "Cleaning up..."
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Docker build
docker-build:
	@echo "Building Docker image..."
	docker build -t $(APP_NAME):latest .

# Docker run
docker-run:
	@echo "Running Docker container..."
	docker run -p 8080:8080 $(APP_NAME):latest


# cmd/server/Install development tools
install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Check code quality
quality: fmt lint test

# Prepare for release
release: clean quality build

# Help information
help:
	@echo "Available targets:"
	@echo "  deps          - Install dependencies"
	@echo "  build         - Build the application"
	@echo "  run           - Run the application locally"
	@echo "  test          - Run tests"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  fmt           - Format code"
	@echo "  lint          - Run linter"
	@echo "  clean         - Clean build artifacts"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-run    - Run Docker container"
	@echo "  install-tools - Install development tools"
	@echo "  quality       - Run code quality checks"
	@echo "  release       - Prepare for release"
	@echo "  help          - Show this help message" 