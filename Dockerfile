# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.25.6
ARG GO_IMAGE=golang:${GO_VERSION}-alpine@sha256:98e6cffc31ccc44c7c15d83df1d69891efee8115a5bb7ede2bf30a38af3e3c92
ARG ALPINE_VERSION=3.21.6
ARG ALPINE_IMAGE=alpine:${ALPINE_VERSION}@sha256:c3f8e73fdb79deaebaa2037150150191b9dcbfba68b4a46d70103204c53f4709

# Build stage
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
	go mod download
COPY . .

ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=0 \
	GOOS=$TARGETOS \
	GOARCH=$TARGETARCH \
	GOFLAGS="-buildvcs=false"

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg/mod \
	go build -trimpath -ldflags="-s -w -buildid=" -o /out/arc ./cmd/arc && \
	go build -trimpath -ldflags="-s -w -buildid=" -o /out/arc-relay ./cmd/arc-relay && \
	go build -trimpath -ldflags="-s -w -buildid=" -o /out/arc-blob ./cmd/arc-blob

# Runtime stage
FROM ${ALPINE_IMAGE} AS runtime

RUN apk add --no-cache ca-certificates \
	&& addgroup -S arc \
	&& adduser -S -G arc -h /home/arc arc \
	&& mkdir -p /var/lib/arc /etc/arc \
	&& chown -R arc:arc /var/lib/arc /etc/arc /home/arc

ENV ARC_DATA_DIR=/var/lib/arc \
	HOME=/home/arc
WORKDIR /home/arc
USER arc:arc

COPY --from=builder /out/arc /usr/local/bin/arc
COPY --from=builder /out/arc-relay /usr/local/bin/arc-relay
COPY --from=builder /out/arc-blob /usr/local/bin/arc-blob

EXPOSE 50051 7946/tcp 7946/udp 9090

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
	CMD nc -z 127.0.0.1 50051 || exit 1
