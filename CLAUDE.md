# CLAUDE.md — arc-node

## Where We're Going

Arc is becoming a **relay + capability** architecture. The relay is a dumb
message router. Everything else (storage, indexing, naming) becomes a
**capability** that connects to the relay like any other client.

```
Before (monolithic node):        After (relay + capabilities):
┌─────────────────────┐          ┌─────────────┐
│       Node          │          │    Relay    │  ← routes envelopes, nothing else
│  ┌───────────────┐  │          └──────┬──────┘
│  │   blobstore   │  │                 │
│  ├───────────────┤  │     ┌───────────┼───────────┐
│  │   indexstore  │  │     ▼           ▼           ▼
│  ├───────────────┤  │  arc-store  arc-index  arc-naming
│  │    server     │  │  (storage)  (indexing) (resolution)
│  └───────────────┘  │
└─────────────────────┘
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

## Current State (Being Refactored)

The existing code is a monolithic node. We're extracting pieces:

| Current Location | Becomes | Status |
|------------------|---------|--------|
| `internal/server/` | `internal/relay/` | Planned (#78-81) |
| `internal/blobstore/` | `arc-store` capability | Planned (#91) |
| `internal/indexstore/` | `arc-index` capability | Planned (#92) |
| `internal/envelope/` | Keep (signing/verification) | Stable |
| `internal/keyring/` | Keep (key management) | Stable |
| `pkg/client/` | Refactor for relay | Planned (#93-95) |

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

## Build

```bash
task build          # go build -o bin/arc ./cmd/arc
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
