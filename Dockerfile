# Multi-stage build for Fayan

# Stage 1: Build frontend
FROM node:20-alpine AS frontend-builder

WORKDIR /build/web

# Copy package files
COPY web/package.json web/package-lock.json ./

# Install dependencies
RUN npm ci

# Copy frontend source
COPY web/ ./

# Build frontend
RUN npm run build

# Stage 2: Build Go binaries
FROM golang:1.24-alpine AS go-builder

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev sqlite-dev

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Copy built frontend to embed in API
COPY --from=frontend-builder /build/web/dist ./cmd/api/static/

# Build the crawler and API binaries
RUN CGO_ENABLED=1 CGO_CFLAGS="-DSQLITE_ENABLE_FTS5" GOOS=linux go build -a -installsuffix cgo -o /build/fayan-crawler ./cmd/crawler/main.go
RUN CGO_ENABLED=1 CGO_CFLAGS="-DSQLITE_ENABLE_FTS5" GOOS=linux go build -a -installsuffix cgo -o /build/fayan-api ./cmd/api/main.go

# Stage 3: Final image
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates sqlite-libs

WORKDIR /app

# Copy binaries from builder
COPY --from=go-builder /build/fayan-crawler .
COPY --from=go-builder /build/fayan-api .

# Copy config file
COPY config.yaml .

# Create data directory
RUN mkdir -p /app/data

# Default command (will be overridden by docker-compose)
CMD ["./fayan-crawler"]
