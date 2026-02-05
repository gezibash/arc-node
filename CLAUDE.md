# CLAUDE.md — arc-node

## Architecture

Arc uses a **minimal core + plugin** architecture:

```
arc (minimal core)           arc-relay (plugin)           arc-blob (plugin)
├── keys/                    ├── start (server)           ├── start (server)
├── names/                   ├── send                     ├── put
└── version                  ├── listen                   ├── get
                             └── discover                 └── want
```

**Discovery:** `arc relay send "hello"` → discovers `arc-relay` in PATH → `exec("arc-relay", ["send", "hello"])`

The relay is a message router and gossip node. Everything else (storage, indexing, naming) connects as a capability.

**Key principle:** The relay holds no persistent state. All state is in-memory
and rebuilt on restart — local subscriptions from connected clients, network
state from gossip convergence. Persistence, indexing, history — all capabilities.

### Network Topology

Capabilities are leaf nodes. Relays are the mesh. Gossip is the backbone.

```
arc-store ──┐                              ┌── arc-store
arc-index ──┤── gRPC ──→ Relay A ←─gossip─→ Relay B ──── gRPC ──┤── arc-index
arc-naming ─┘              ↑                  ↑           └── arc-runner
                           │gossip            │gossip
                           ↓                  ↓
                         Relay C ←──gossip──→ Relay D
                           ↑                  ↑
                      gRPC │                  │ gRPC
                           │                  │
                        arc-store          arc-naming
```

Each relay exposes two ports:

```
Relay
├── gRPC :9090    ← clients, capabilities, envelope routing
└── UDP  :7946    ← memberlist gossip (SWIM probes, state sync)
    (+ TCP :7946)   (memberlist uses TCP for push/pull)
```

**Data plane (gRPC):** Capabilities connect to their local relay. They subscribe
with labels, handle requests. They don't know about the network.

**Control plane (UDP/TCP):** Relays gossip with each other via HashiCorp memberlist
using native SWIM protocol over UDP. Every relay has a converged view of the entire
network — what exists, where it is, who's healthy. UDP gives precise failure
detection; TCP handles bulk state sync. A client on Relay D can discover a storage
provider on Relay A without either the client or the provider knowing about gossip.

**State flows in one direction:**

```
Capability state  ──→  Local relay  ──→  Gossip mesh  ──→  Every relay
  (subscribe)          (observes)        (propagates)       (knows)
```

Capabilities push state up. Gossip spreads it sideways. Consumers query locally.
Nobody reaches across the network directly — the network comes to you.

**Gossip state sections** (extensible registry, add new sections without protocol changes):

| Section | What | Change frequency |
|---------|------|------------------|
| `relays` | Relay membership, addresses, pubkeys | Rare (joins/leaves) |
| `capabilities` | What capabilities exist where | Moderate (subscriptions change) |
| `names` | Who's registered where (@bob → relay-a.arc) | Moderate |
| `health` | Per-relay load, connections, uptime, version | Frequent (metrics) |
| `topology` | Inter-relay latency, connectivity | Frequent |
| `dns` | External name resolution cache (.eth, .com) | Moderate (TTL-based) |

### Type Safety Layering

```
pkg/relay          free-form map[string]string — dumb pipe
                   doesn't know what "blob" or "index" means
                   just routes by label match
                        │
pkg/capability     transition layer — still takes map[string]string
                   (RequestConfig, DiscoverConfig) but provides the
                   structure that typed requests compile down to
                        │
pkg/blob           fully typed — PutReq, GetReq, DiscoverFilter
pkg/index          no map[string]string visible to callers
pkg/<cap>          each capability owns its label vocabulary
```

The relay MUST stay free-form — it routes envelopes for capabilities that
don't exist yet. pkg/capability is the boundary where types emerge.
Everything above constructs typed structs and calls internal `.config()` or
`.labels()` to produce the map. The map exists at the wire boundary only.

### Import Graph

```
cmd/arc-relay   → pkg/relay               (it IS the relay)
cmd/arc-blob    → pkg/blob, pkg/capability (NO pkg/relay)
pkg/blob        → pkg/capability           (NO pkg/relay)
pkg/capability  → pkg/relay                (ONLY bridge to relay)
future apps     → pkg/blob, pkg/index      (NO pkg/capability, NO pkg/relay)
```

### Composition Principle

**Every higher-level function MUST compose from lower-level primitives.**

```
pkg/blob        → composes pkg/capability primitives
pkg/capability  → composes pkg/relay primitives
pkg/relay       → composes transport/gRPC primitives
```

This means:
- `blob.Client.Discover()` MUST call `capability.Client.Discover()` — never bypass
- `blob.Client.Put()` MUST call `capability.Client.Request()` — never call relay directly
- CLI commands (`arc-blob`) MUST use `pkg/blob` — never call capability or relay directly

**Why:** Stronger guarantees. If capability discovery works, blob discovery works.
If we find a bug, we fix it once at the right layer. No duplicate code paths
that can diverge. Tests at lower layers give confidence to higher layers.

**Discovery Composition Chain (concrete example):**

```
arc blob discover (CLI)
  │
  ├─ cmd/arc-blob/discover.go:69
  │    c.Discover(ctx, blob.DiscoverFilter{...})
  │    where c is *blob.Client
  │
  ▼
blob.Client.Discover()
  │
  ├─ pkg/blob/client.go:36
  │    c.cap.Discover(ctx, cfg)
  │    where c.cap is *capability.Client
  │
  ▼
capability.Client.Discover()
  │
  ├─ pkg/capability/client.go:264
  │    c.tr.Discover(ctx, filter, cfg.Limit)
  │    where c.tr is transport.Transport
  │
  ▼
transport.Transport.Discover() [interface]
  │
  ├─ pkg/relay/discover.go:17
  │    relay.Client.Discover() [implements interface]
  │
  ▼
gRPC DiscoverFrame to relay server
```

**Violations to watch for:**
- CLI importing `pkg/relay` directly (should only use `pkg/blob`)
- `pkg/blob` importing `pkg/relay` (should only use `pkg/capability`)
- Duplicate discovery logic that doesn't flow through this chain
- Direct gRPC calls from capability code (should use transport interface)

### Discover-First Pattern

Every operation requires a target. Either discover one or specify explicitly:

```go
// Auto-discover (client finds best target)
result, _ := blobClient.Put(ctx, data)

// Explicit target (--to flag)
result, _ := blobClient.PutTo(ctx, target, data)

// Filtered discovery
targets, _ := blobClient.Discover(ctx, blob.DiscoverFilter{
    Backend: blob.BackendAWS,
    Tier:    blob.TierHot,
})
target := targets.SortByLatency().First()
result, _ := blobClient.PutTo(ctx, target, data)
```

Targets are not "servers" — they can be apps, people, services, devices.
Each target has identity (pubkey), labels, and infrastructure metadata
(latency, direct address, connected duration, petname).

## Documentation

For detailed implementation guidance, read `.docs/`:

```
.docs/
├── INDEX.md      # Issue links, dependency graph, milestones
├── RELAY.md      # Implementation guide - mental model, patterns, invariants
└── DECISIONS.md  # Architecture decisions - "why did we do it this way?"
```

## Core Primitives (`pkg/identity`)

```go
identity.PublicKey{Algo, Bytes}     // Algorithm-tagged public key
identity.Signature{Algo, Bytes}     // Algorithm-tagged signature
identity.Signer                     // Interface: PublicKey(), Sign(), Algorithm()
identity.EncodePublicKey(pk)        // → "ed25519:hex..."
identity.DecodePublicKey(s)         // Parse "algo:hex" → PublicKey
identity.Verify(pk, payload, sig)   // Verify signature

// Implementations
ed25519.Generate() → (*Keypair, error)
ed25519.FromSeed(seed) → (*Keypair, error)
secp256k1.Generate() → (*Keypair, error)
secp256k1.FromSeed(seed) → (*Keypair, error)
```

## Capability Framework

### Type-Safe Capability Pattern

Each capability defines three things in its `pkg/<cap>/` package:

**1. Label vocabulary** (`labels.go`) — enums, constants, typed filters:
```go
// pkg/blob/labels.go
type Backend string
const (
    BackendLocal   Backend = "local"
    BackendAWS     Backend = "aws"
    BackendSeaweed Backend = "seaweed"
)

type Labels struct {       // server subscription labels
    Backend Backend
    Region  string
    Tier    Tier
    Direct  string
}
func (l Labels) ToMap() map[string]string

type DiscoverFilter struct { // client discovery filter
    Backend Backend
    Region  string
    Tier    Tier
    Direct  bool
}
```

**2. Typed requests** (`request.go`) — strict per-operation structs:
```go
// pkg/blob/request.go
type PutReq struct{}
type GetReq struct{ CID [32]byte }
type WantReq struct{ CID [32]byte }

// Internal: compiles typed fields to wire format
func (r GetReq) config() capability.RequestConfig { ... }
```

**3. Typed client** (`client.go`) — public API, no labels exposed:
```go
// Consumer sees only typed methods:
client.Put(ctx, data) → *PutResult
client.Get(ctx, cid)  → *GetResult
client.Discover(ctx, blob.DiscoverFilter{Backend: blob.BackendAWS})
```

### Server Side

```go
// cmd/arc-blob/start.go — typed labels for subscription
labels := blob.Labels{
    Backend: blob.Backend(cfg.Storage.Backend),
    Direct:  externalAddr,
}
server, _ := capability.NewServer(capability.ServerConfig{
    Name:    blob.CapabilityName,
    Labels:  labels.ToMap(),
    Handler: handler,
    Runtime: rt,
})

// cmd/arc-blob/handler.go — typed op dispatch
op := blob.Op(env.Labels[blob.LabelOp])
switch op {
case blob.OpPut:  // ...
case blob.OpGet:  // ...
case blob.OpWant: // ...
}
```

## Relay Mental Model

**The relay has two jobs: route envelopes and gossip network state.**

**Data plane** — route envelopes by label-match:
- Routes by labels (exact subset match)
- Non-blocking send to subscriber buffers
- Full buffer = drop (protects the commons)
- No persistence, no replay, no guaranteed delivery
- Receipt means "queued in buffer", not "processed"

**Control plane** — gossip with peer relays:
- Observe local state (who's connected, what capabilities, what names)
- Propagate to peers via memberlist (SWIM protocol over UDP/TCP)
- Converge to full network view (every relay knows about every relay)
- Answer cross-relay discovery queries from local state

**Trust model** — the relay is a trust anchor in a federation chain:
- **Sender signs envelopes** → "I wrote this"
- **Relay signs envelopes** → "this came through me, I vouch for the sender"
- **Relay signs receipts** → "I saw this, I routed it to N subscribers"
- Gossip state is infrastructure, not user data — signed by originating relay

**What the relay knows** (and nothing more):
- Who's connected right now (subscriptions, identity, labels)
- How to route (label match, name resolution, pubkey addressing)
- Infrastructure health (latency, liveness, buffer state)
- Network state (other relays, remote capabilities, remote names — via gossip)

**What the relay does NOT know:**
- Capability-specific logic (blob, index, naming semantics)
- Message content or payload structure
- Persistence or history

See `.docs/RELAY.md` for full details.

## Naming — BIP-39 Petnames

All entities get deterministic 3-word names from their public key using BIP-39:

```
pubkey bytes → BIP-39 mnemonic → first 3 words → join with "-"
```

Example: `"leader-monkey-parrot"` (2048³ = ~8.5 billion combinations)

```go
names.Petname(pubkeyBytes)           // []byte → "leader-monkey-parrot"
names.PetnameFromHex(hexStr)         // hex string → petname
names.PetnameFromEncoded(encoded)    // "ed25519:hex..." → petname
```

Used everywhere: relay gossip node names, subscriber names, capability names.
Gossip defaults to petname from relay pubkey (`--gossip-name` overrides).

Dependency: `github.com/tyler-smith/go-bip39`

## CLI

The CLI uses plugin discovery. Core `arc` is minimal:

```bash
# Core commands (built-in)
arc keys generate                 # key management
arc keys list
arc names add alice 7f3a...       # local name resolution
arc names list

# Plugin commands (discovered from arc-* in PATH)
arc relay start                   # run relay server
arc relay send "hello"            # send envelope
arc relay listen                  # receive envelopes

arc blob start                    # run blob server
arc blob put file.txt             # store blob
arc blob get <cid>                # retrieve blob
```

Direct plugin invocation also works:
```bash
arc-relay send "hello"
arc-blob put file.txt
```

## Build

```bash
task build              # builds arc, arc-relay, arc-blob
task build:arc          # just arc
task build:arc-relay    # just arc-relay
task build:arc-blob     # just arc-blob
task lint               # run all linters
buf generate            # regenerate proto
go test ./...           # run tests
```

## Patterns & Conventions

### Constructors
Named `New()` or `NewXxx()`. Return `(*T, error)`.

### Context Keys
Unexported: `type ctxKey struct{}`.

### Errors
```go
// Use shared errors from pkg/errors
// import arcerrors "github.com/gezibash/arc/v2/pkg/errors"
var ErrNotFound = arcerrors.ErrNotFound

// Or wrap with context
return fmt.Errorf("route envelope: %w", err)

// gRPC errors
return status.Errorf(codes.NotFound, "...")
```

### Labels
```go
// Use pkg/labels for consistent parsing
parsed, err := labels.Parse([]string{"topic=news", "priority=high"})
formatted := labels.Format(parsed) // "priority=high, topic=news"
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

## Creating a New Capability

1. Create `pkg/<name>/` with typed interface:
   - `labels.go` — label constants, enums, `Labels`, `DiscoverFilter`
   - `request.go` — typed request structs per operation (`FooReq`, `BarReq`)
   - `client.go` — public typed client (no `map[string]string` exposed)
   - `constants.go` — capability-specific constants
   - `capability.go` — runtime extension (`Capability()`, `From()`)
2. Create `cmd/arc-<name>/`:
   - `handler.go` — implements `capability.Handler`, dispatches typed ops
   - `start.go` — server using `capability.NewServer` with typed `Labels`
   - CLI commands (put, get, etc.) using typed client
3. Add to `task build` in Taskfile.yml

**Rules:**
- `pkg/<name>` imports `pkg/capability` — NEVER `pkg/relay`
- `cmd/arc-<name>` imports `pkg/<name>` and `pkg/capability` — NEVER `pkg/relay`
- Only `pkg/capability` bridges to `pkg/relay`
- All labels are typed constants/enums — no string literals for label keys or values
- Discovery is at the capability layer, not duplicated per capability CLI

## Roadmap

Foundation → Relay Core → Gossip (backbone) → **{Naming, Direct Connection, SDK, Capabilities}**

Gossip backbone is implemented (memberlist, sections, cross-relay forwarding,
capability/name propagation). Current focus: naming system, direct connections,
capability SDK, and building out capabilities (blob, index, etc.).

See `.docs/INDEX.md` for the full dependency graph and issue links.
