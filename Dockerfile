# Multi-stage build for Fayan
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev sqlite-dev

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the crawler and API binaries
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o /build/fayan-crawler ./cmd/crawler/main.go
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o /build/fayan-api ./cmd/api/main.go

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates sqlite-libs

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/fayan-crawler .
COPY --from=builder /build/fayan-api .

# Copy config file
COPY config.yaml .

# Create data directory
RUN mkdir -p /app/data

# Default command (will be overridden by docker-compose)
CMD ["./fayan-crawler"]
