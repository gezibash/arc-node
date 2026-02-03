# CLAUDE.md — arc-node

## Architecture

Arc uses a **relay + capability** architecture. The relay is a dumb message
router. Everything else (storage, indexing, naming) connects as a capability.

```
┌─────────────┐
│    Relay    │  ← routes envelopes, nothing else
└──────┬──────┘
       │
┌──────┼──────────────┐
▼      ▼              ▼
Clients          Capabilities
(send/listen)    (arc-store, arc-index, arc-naming)
```

**Key principle:** The relay is stateless. It holds nothing but "who's connected
right now". Persistence, indexing, history - all capabilities.

## Documentation

For detailed implementation guidance, read `.docs/`:

```
.docs/
├── INDEX.md      # Issue links, dependency graph, milestones
├── RELAY.md      # Implementation guide - mental model, patterns, invariants
└── DECISIONS.md  # Architecture decisions - "why did we do it this way?"
```

The architecture spec is GitHub issue #76.

## Current State

The monolithic node has been removed. We now have a clean relay architecture:

| Component | Location | Status |
|-----------|----------|--------|
| Relay server | `internal/relay/` | Done |
| Relay proto | `api/arc/relay/v1/` | Done |
| Relay client | `pkg/client/` | Done |
| CLI (send/listen) | `cmd/arc/send/`, `cmd/arc/listen/` | Done |
| Key management | `internal/keyring/` | Stable |
| Config | `internal/config/` | Stable |

Capabilities (arc-store, arc-index, arc-naming) will be separate binaries.

## Core Primitives (from `github.com/gezibash/arc`)

```go
identity.Keypair                    // Ed25519 keypair
identity.PublicKey  [32]byte        // Ed25519 public key
identity.Signature  [64]byte        // Ed25519 signature
message.Message                     // From, To, Content, ContentType, Timestamp, Signature
reference.Reference [32]byte        // SHA-256 content hash
reference.Compute(data) Reference   // SHA-256
message.Sign(msg, kp)               // Sign message with keypair
message.Verify(msg) (bool, error)   // Verify Ed25519 signature
```

## Relay Mental Model

**The relay is a postal service, not a warehouse.**

- Routes envelopes by labels (addressed, capability, label-match)
- Non-blocking send to subscriber buffers
- Full buffer = drop (protects the commons)
- No persistence, no replay, no guaranteed delivery
- Receipt means "queued in buffer", not "processed"

See `.docs/RELAY.md` for full details.

## CLI

The CLI is **client-first**. Primary commands are `send` and `listen`:

```bash
arc send "hello"                      # send envelope
arc send --labels topic=news "msg"    # with labels
arc send --to @alice "hi"             # addressed

arc listen                            # receive all
arc listen --labels topic=news        # filter by labels
arc listen --name myname              # register name

arc relay start                       # run relay server (admin)
arc keys generate                     # key management
```

## Build

```bash
task build          # go build -o bin/arc ./cmd/arc
task lint           # run all linters (buf lint + golangci-lint)
buf generate        # regenerate proto
go test ./...       # run tests
```

## Patterns & Conventions

### Constructors
Named `New()` or `NewXxx()`. Return `(*T, error)`.

### Context Keys
Unexported: `type ctxKey struct{}`.

### Errors
```go
var ErrNotFound = errors.New("not found")           // sentinel
return fmt.Errorf("route envelope: %w", err)        // wrapped
return status.Errorf(codes.NotFound, "...")         // gRPC
```

### Atomic Close
```go
closed atomic.Bool
func (r *Relay) Close() error {
    if r.closed.Swap(true) { return nil }
    // cleanup
}
```

### Non-blocking Send
```go
select {
case s.buffer <- env:
    return true
default:
    return false  // drop
}
```

### Tests
- `t.Helper()` on test helpers
- `t.Cleanup()` for teardown
- `status.FromError(err)` for gRPC assertions

## Roadmap

Foundation → Relay Core → {Gossip, Direct Connection, SDK} → {Naming, Capabilities}

See `.docs/INDEX.md` for the full dependency graph and issue links.
