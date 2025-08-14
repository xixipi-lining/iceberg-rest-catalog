# Build stage
FROM golang:1.24.4-alpine AS builder

# Set working directory
WORKDIR /app

# Install necessary packages (including gcc and musl-dev for sqlite support)
RUN apk add --no-cache git ca-certificates tzdata gcc musl-dev sqlite-dev

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build application (enable CGO for sqlite support)
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-w -s" -o iceberg-rest-catalog main.go

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata sqlite wget

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Set working directory
WORKDIR /app

# Copy binary from build stage
COPY --from=builder /app/iceberg-rest-catalog .

# Create config and data directories
RUN mkdir -p /app/data /app/configs && \
    chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Expose port (default port 8080 from main.go)
EXPOSE 8080

# Set environment variables
ENV GIN_MODE=release

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1


# Start application
CMD ["./iceberg-rest-catalog"]
