# CLAUDE.md — arc-node

## Project Identity

Arc is a cryptographic messaging and storage protocol. **arc-node** is the
server, CLI, and SDK implementation. Every actor has an Ed25519 keypair; every
RPC is envelope-signed. Content is opaque bytes — encryption is client-side.

- Module: `github.com/gezibash/arc-node`
- Core primitives live in `github.com/gezibash/arc` (identity, message,
  reference)
- `pkg/client` is the public SDK that all apps (journal, DM, bitcoin feeds,
  etc.) will use
- Federation via streams between nodes (planned)

## Architecture

### Core Primitives (from `github.com/gezibash/arc`)

```go
identity.Keypair                    // Ed25519 keypair
identity.PublicKey  [32]byte        // Ed25519 public key
identity.Signature  [64]byte        // Ed25519 signature
message.Message                     // From, To, Content, ContentType, Timestamp, Signature
reference.Reference [32]byte        // SHA-256 content hash
reference.Compute(data) Reference   // SHA-256
message.Sign(msg, kp)               // Sign message with keypair
message.Verify(msg) (bool, error)   // Verify Ed25519 signature
message.CanonicalBytes(msg) []byte  // Deterministic serialization
```

### Signing Flow

1. Serialize proto request/response with
   `proto.MarshalOptions{Deterministic: true}`
2. `reference.Compute(payload)` → SHA-256 content hash
3. Build `message.Message{From, To, Content, ContentType, Timestamp}`
4. `message.Sign(&msg, kp)` → Ed25519 signature over canonical bytes

**Signature covers:**
`From(32) + To(32) + Content(32) + ContentType(var) + Timestamp(8)`

**Envelope fields NOT signed:** Origin, HopCount, Metadata — only the inner
message is signed.

### gRPC Metadata Keys

```
arc-from-bin, arc-to-bin, arc-timestamp, arc-signature-bin,
arc-content-type, arc-origin-bin, arc-hop-count, arc-meta-*
```

## Package Guide

### `internal/envelope/`

Wraps arc messages for gRPC transport. `Seal()` creates signed envelopes,
`Open()` verifies them. Provides `UnaryServerInterceptor` and
`StreamServerInterceptor`. Key types: `Envelope`, `Caller`.

### `internal/middleware/`

Pre/post hook chain for gRPC calls. `Chain{Pre, Post []Hook}` where
`Hook func(ctx, *CallInfo) (context.Context, error)`. Hooks can modify context
or reject calls via gRPC status errors.

### `internal/server/`

gRPC server and `NodeService` implementation. RPCs: `PutContent`, `GetContent`,
`SendMessage`, `QueryMessages`, `SubscribeMessages`, `Federate`. Interceptor
chain: observability → envelope.

### `internal/blobstore/`

Content-addressed blob storage. `Store(data) → Reference`, `Fetch(ref) → data`.
Verifies integrity on read. Pluggable backends via `physical.Backend` interface.

### `internal/blobstore/physical/`

Backend registry + implementations. Backends: `badger` (default), `memory`,
`fs`, `s3`, `seaweedfs`.

### `internal/indexstore/`

Label-indexed message storage with CEL query support and real-time
subscriptions. `Index()`, `Query(QueryOptions)`,
`Subscribe(expression) → Subscription`. Background cleanup of expired entries.

### `internal/indexstore/cel/`

CEL expression evaluator with compiled program cache. Variables: `labels` (map),
`timestamp`, `ref`, `expires_at`. Example:
`labels["priority"] == "high" && timestamp > 1234567890`.

### `internal/indexstore/physical/`

Index backend registry. Backends: `badger`, `memory`, `redis`, `sqlite`.

### `internal/keyring/`

Ed25519 keypair management with aliases. Keys stored at
`~/.arc/keys/<pubkey>.key` (seed, mode 0600) + `.json` (metadata).
`keyring.json` maps aliases and default key. Functions: `Generate`, `Import`,
`Load`, `LoadDefault`, `SetAlias`, `SetDefault`.

### `internal/config/`

Viper-based config. Priority: CLI flags > env vars (`ARC_` prefix) > config file
(HCL/YAML/JSON) > defaults. Defaults: addr `:50051`, data dir `~/.arc`, backends
`badger`, log level `info`.

### `internal/observability/`

slog + Prometheus + OpenTelemetry. `StartOperation(ctx, metrics, name)` returns
an `op` with deferred `op.End(err)` for span/histogram/counter tracking.
Metrics: `arc_operation_duration_seconds`, `arc_operation_total`,
`arc_bytes_processed_total`, `arc_errors_total`.

### `internal/node/`

Blank imports to trigger backend `init()` registration. Wires storage factory
creation.

### `internal/storage/`

Shared config helpers: `GetString`, `GetBool`, `GetInt64`, `GetDuration`,
`ExpandPath`, `MergeConfig`. Structured `ConfigError` type with `Unwrap()`.

### `pkg/client/`

Public SDK. `Dial(addr, ...Option) → *Client`. Options: `WithIdentity(kp)`,
`WithNodeKey(pub)`. Methods: `PutContent`, `GetContent`, `SendMessage`,
`QueryMessages`, `SubscribeMessages`, `Federate`. Client-side interceptors
handle envelope signing/verification automatically.

### `cmd/arc/`

CLI built with Cobra. Subcommands:

- `node start|put|get|publish|query|watch`
- `keys generate|import|export|list|show|alias|default|delete`

## Patterns & Conventions

### Storage Backend Registry

Backends self-register via `init()`:

```go
func init() { physical.Register("badger", NewFactory, Defaults) }
```

Activated by blank imports in `internal/node/`.

### Constructors

Named `New()` or `NewXxx()`. Return `(*T, error)`.

### Context Keys

Unexported: `type ctxKey struct{}`.

### Errors

- Sentinel vars: `var ErrNotFound = errors.New("blob not found")`
- Wrapping: `fmt.Errorf("store blob: %w", err)`
- gRPC: `status.Errorf(codes.NotFound, "...")`
- Structured: `ConfigError{Backend, Field, Value, Message, Cause}` with
  `Unwrap()`

### Tests

- `t.Helper()` on all test helpers
- `t.Cleanup(func() { backend.Close() })` for teardown
- gRPC status checking: `status.FromError(err)` → assert code
- Test helpers: `newTestServer(t)`, `newTestClient(t, addr, nodeKP)`,
  `makeMessage(t, kp, contentType)`

### Shutdown

`ShutdownCoordinator.Register(name, fn)` for ordered graceful shutdown.

### Atomic Close

```go
closed atomic.Bool
func (b *Backend) Close() error {
    if b.closed.Swap(true) { return nil }
    return b.db.Close()
}
```

## Build

```bash
task build          # go build -o bin/arc ./cmd/arc
buf generate        # required before build — regenerate proto (api/arc/node/v1/)
go test ./...       # run tests
```

## App Philosophy

Arc apps share three presentation tiers built on one SDK:

1. **SDK** (`pkg/<app>/`) — pure Go library. No UI, no CLI. This is what any
   client (web, mobile, CLI) would use.
2. **TUI** — interactive bubbletea UI when stdin is a TTY. Browse, read, write.
3. **Non-TTY CLI** — explicit subcommands with markdown or JSON output for
   scripting and LLM consumption.

`arc <app>` with no subcommand auto-detects: TTY → TUI, pipe → markdown list.

### Realtime by Default

Every long-lived Arc app (TUIs, daemons, watchers) **must** maintain a
persistent node connection and subscribe to real-time updates via
`client.SubscribeMessages()`. There is no polling. New entries, messages, or
events appear instantly through the node's streaming gRPC subscription. This is
a core architectural principle — if an app is running, it is live.

Implementation pattern:

- On launch, issue an initial `List()` / `Query()` to populate state
- Immediately open a `SubscribeMessages()` stream with the same label filter
- Feed subscription events into the TUI/app via channels or `tea.Cmd`
- The UI reflects new data the moment the node indexes it

### Dependency Direction

```
pkg/<app>/         →  pkg/client, github.com/gezibash/arc, x/crypto
cmd/arc/<app>/     →  pkg/<app>, internal/keyring, internal/config,
                      cobra, viper, bubbletea, lipgloss, go-isatty
```

SDKs never import internal packages. CLI/TUI layers never contain business
logic.

## Future Direction

- **Federation:** Nodes subscribe to each other, forward with Origin preserved,
  HopCount incremented
- **Apps as clients:** Journal = labels + content types, DM = e2e encryption,
  price feeds = server-side publishers
- **Middleware hooks:** Auth rules, rate limits, payments
- **MCP integration:** LLM interop via Model Context Protocol
