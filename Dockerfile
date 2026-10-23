# Stage 1: Build the Go binary
FROM golang:1.23-bookworm AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o stream-server main.go

# Stage 2: Lightweight runtime with FFmpeg
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/stream-server .

EXPOSE 8080
CMD ["./stream-server"]
