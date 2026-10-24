# cache-bust-124
# Stage 1: Build the Go binary
FROM golang:1.23-bookworm AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o stream-server main.go

# Stage 2: Lightweight runtime with FFmpeg & Intel QSV Drivers
FROM debian:bookworm-slim

# Explicitly tell VA-API to use the modern Intel iHD driver
ENV LIBVA_DRIVER_NAME=iHD

# Enable non-free repos and install FFmpeg + Intel Media drivers in one layer
RUN sed -i 's/Components: main/Components: main contrib non-free non-free-firmware/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        ca-certificates \
        intel-media-va-driver-non-free \
        libmfx1 \
        libmfx-gen1.2 \
        libvpl2 \
        libva-drm2 \
        vainfo \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/stream-server .

EXPOSE 8080
CMD ["./stream-server"]
