.PHONY: build run test clean deps fmt lint docker-build

# 应用名称
APP_NAME = iceberg-rest-catalog

# 构建目录
BUILD_DIR = build

# Go 相关变量
GO_VERSION = 1.24.4
GOOS = linux
GOARCH = amd64

# 默认目标
all: deps fmt lint test build

# 安装依赖
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go mod download

# 构建应用
build:
	@echo "Building application..."
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -ldflags="-w -s" -o $(BUILD_DIR)/$(APP_NAME) cmd/server/main.go

# 本地运行
run:
	@echo "Running application..."
	go run cmd/server/main.go

# 运行测试
test:
	@echo "Running tests..."
	go test -v ./...

# 运行测试并生成覆盖率报告
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# 格式化代码
fmt:
	@echo "Formatting code..."
	go fmt ./...

# 代码检查
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# 清理构建产物
clean:
	@echo "Cleaning up..."
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Docker 构建
docker-build:
	@echo "Building Docker image..."
	docker build -t $(APP_NAME):latest .

# Docker 运行
docker-run:
	@echo "Running Docker container..."
	docker run -p 8080:8080 $(APP_NAME):latest

# 开发环境启动
dev:
	@echo "Starting development server..."
	air

# 生成API文档
docs:
	@echo "Generating API documentation..."
	swag init -g cmd/server/main.go -o docs/swagger

# 安装开发工具
install-tools:
	@echo "Installing development tools..."
	go install github.com/cosmtrek/air@latest
	go install github.com/swaggo/swag/cmd/swag@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# 检查代码质量
quality: fmt lint test

# 发布准备
release: clean quality build

# 帮助信息
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
	@echo "  dev           - Start development server with hot reload"
	@echo "  docs          - Generate API documentation"
	@echo "  install-tools - Install development tools"
	@echo "  quality       - Run code quality checks"
	@echo "  release       - Prepare for release"
	@echo "  help          - Show this help message" 