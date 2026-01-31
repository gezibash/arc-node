# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /usr/local/bin/arc ./cmd/arc

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /usr/local/bin/arc /usr/local/bin/arc

# Default data directory
RUN mkdir -p /root/.arc
VOLUME ["/root/.arc"]

# Config file can be mounted at this path
VOLUME ["/etc/arc"]

EXPOSE 50051

ENTRYPOINT ["arc", "node", "start"]
# Pass --config /etc/arc/config.hcl (or .yaml/.json) to use a mounted config file
# e.g. docker run -v ./my-config.hcl:/etc/arc/config.hcl arc --config /etc/arc/config.hcl
