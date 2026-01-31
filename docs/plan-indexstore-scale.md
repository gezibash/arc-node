# IndexStore Scaling Plan

Target: millions of indexed messages per second through the arc-node indexstore.

**Current architecture:** `IndexStore` (in `internal/indexstore/indexstore.go`) wraps a `physical.Backend` with CEL evaluation (`internal/indexstore/cel/evaluator.go`) and a `subscriptionManager` (`internal/indexstore/subscription.go`). Backends (badger, redis, sqlite, memory) implement the `physical.Backend` interface defined in `internal/indexstore/physical/backend.go`. Labels are flat `map[string]string`. Queries flow through `physical.QueryOptions` with label AND-matching, time bounds, cursor pagination, and optional post-fetch CEL filtering.

---

## P0: Async Subscription Fan-out

### Problem

`subscriptionManager.Notify()` in `internal/indexstore/subscription.go:114` is called synchronously from `IndexStore.Index()` at `internal/indexstore/indexstore.go:54-56`. It holds `mu.RLock()` while iterating every subscription and evaluating CEL expressions via `m.eval.Match()`. With 1000 subscriptions each requiring CEL evaluation, a single `Index()` call blocks for milliseconds. At target throughput this is a hard bottleneck on the write path.

### Design

Decouple notification from indexing by introducing an async fan-out pipeline:

1. `Index()` sends the entry to a buffered intake channel (non-blocking).
2. A pool of worker goroutines drains the intake, evaluates CEL per subscription, and dispatches matches.
3. Slow subscribers are tracked and disconnected after a configurable drop threshold.

### Changes

#### `internal/indexstore/subscription.go`

Add drop tracking to `subscription`:

```go
type subscription struct {
	id              string
	expression      string
	entries         chan *physical.Entry
	cancel          context.CancelFunc
	err             error
	errMu           sync.RWMutex
	done            chan struct{}
	consecutiveDrops atomic.Int64
}
```

Replace `subscriptionManager` with an async version:

```go
const (
	defaultBufferSize      = 100
	defaultIntakeSize      = 65536
	defaultWorkerCount     = 4
	defaultMaxDrops        = 1000
)

type SubscriptionConfig struct {
	IntakeBufferSize int // Size of the intake ring buffer. Default 65536.
	WorkerCount      int // Number of fan-out goroutines. Default 4.
	MaxConsecutiveDrops int // Drops before disconnect. Default 1000. 0 = unlimited.
}

type subscriptionManager struct {
	mu     sync.RWMutex
	subs   map[string]*subscription
	eval   *celeval.Evaluator
	closed bool

	intake chan *physical.Entry // buffered intake channel
	wg     sync.WaitGroup      // tracks worker goroutines
	stop   chan struct{}        // signals workers to exit
	config SubscriptionConfig
}

func newSubscriptionManager(eval *celeval.Evaluator, cfg SubscriptionConfig) *subscriptionManager {
	if cfg.IntakeBufferSize <= 0 {
		cfg.IntakeBufferSize = defaultIntakeSize
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = defaultWorkerCount
	}
	if cfg.MaxConsecutiveDrops <= 0 {
		cfg.MaxConsecutiveDrops = defaultMaxDrops
	}

	m := &subscriptionManager{
		subs:   make(map[string]*subscription),
		eval:   eval,
		intake: make(chan *physical.Entry, cfg.IntakeBufferSize),
		stop:   make(chan struct{}),
		config: cfg,
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		m.wg.Add(1)
		go m.fanoutWorker()
	}

	return m
}
```

The fan-out worker:

```go
func (m *subscriptionManager) fanoutWorker() {
	defer m.wg.Done()
	for {
		select {
		case entry, ok := <-m.intake:
			if !ok {
				return
			}
			m.dispatchEntry(entry)
		case <-m.stop:
			return
		}
	}
}

func (m *subscriptionManager) dispatchEntry(entry *physical.Entry) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, sub := range m.subs {
		match, err := m.eval.Match(context.Background(), sub.expression, entry)
		if err != nil {
			slog.Warn("subscription filter evaluation failed",
				"subscription_id", sub.id, "error", err)
			continue
		}
		if !match {
			continue
		}

		select {
		case sub.entries <- entry:
			sub.consecutiveDrops.Store(0)
		default:
			drops := sub.consecutiveDrops.Add(1)
			if m.config.MaxConsecutiveDrops > 0 && drops >= int64(m.config.MaxConsecutiveDrops) {
				slog.Warn("disconnecting slow subscriber",
					"subscription_id", sub.id,
					"consecutive_drops", drops)
				sub.errMu.Lock()
				sub.err = fmt.Errorf("disconnected: %d consecutive drops", drops)
				sub.errMu.Unlock()
				sub.Cancel()
			} else {
				slog.Warn("subscription buffer full, entry dropped",
					"subscription_id", sub.id,
					"consecutive_drops", drops)
			}
		}
	}
}
```

Replace the synchronous `Notify()` with a non-blocking send:

```go
// Notify enqueues an entry for async fan-out. Non-blocking: if the intake
// buffer is full the entry is dropped and a warning is logged.
func (m *subscriptionManager) Notify(_ context.Context, entry *physical.Entry) {
	select {
	case m.intake <- entry:
	default:
		slog.Warn("subscription intake buffer full, entry dropped",
			"ref", reference.Hex(entry.Ref))
	}
}
```

Update `Close()` to drain workers:

```go
func (m *subscriptionManager) Close() {
	m.mu.Lock()
	m.closed = true
	for _, sub := range m.subs {
		sub.Cancel()
	}
	m.subs = nil
	m.mu.Unlock()

	close(m.stop)
	m.wg.Wait()
}
```

#### `internal/indexstore/indexstore.go`

Update `New()` to accept and forward `SubscriptionConfig`:

```go
type Options struct {
	SubscriptionConfig SubscriptionConfig
}

func New(backend physical.Backend, metrics *observability.Metrics, opts *Options) (*IndexStore, error) {
	eval, err := celeval.NewEvaluator()
	if err != nil {
		return nil, fmt.Errorf("create CEL evaluator: %w", err)
	}
	cfg := SubscriptionConfig{}
	if opts != nil {
		cfg = opts.SubscriptionConfig
	}
	return &IndexStore{
		backend: backend,
		metrics: metrics,
		eval:    eval,
		subs:    newSubscriptionManager(eval, cfg),
	}, nil
}
```

The `Index()` method stays the same -- `s.subs.Notify(ctx, entry)` is already a one-liner and now non-blocking.

### Testing

- Unit test: index 10k entries with 100 subscriptions, verify all matching entries arrive within 1 second.
- Benchmark: compare `Index()` latency before/after with 0, 100, 1000 active subscriptions.
- Slow subscriber test: create subscription that never reads, verify disconnect after `MaxConsecutiveDrops`.

---

## P0: Composite Label Indexes

### Problem

Multi-label queries (e.g., `app=dm` AND `thread=xyz`) use the `pickDrivingLabel()` heuristic in `internal/indexstore/physical/badger/backend.go:536` to choose the most selective single label, then validate remaining labels via point-get of the meta key (`queryByLabel` at line 579). For hot multi-label patterns this means scanning the entire driving label index and doing N point-gets per candidate entry.

### Design

Allow pre-registration of composite index definitions -- ordered lists of label keys. When an entry is written with labels matching a composite index, an additional composite key is written. On query, if the query labels exactly match (or are a prefix of) a registered composite index, the backend seeks directly on the composite key, eliminating intersection entirely.

### New Interface

#### `internal/indexstore/physical/backend.go`

```go
// CompositeIndexDef defines a composite index over an ordered set of label keys.
type CompositeIndexDef struct {
	Name string   // Unique name, e.g., "dm_thread"
	Keys []string // Ordered label keys, e.g., ["app", "thread"]
}

// CompositeIndexer is an optional interface for backends that support
// pre-materialized composite label indexes.
type CompositeIndexer interface {
	// RegisterCompositeIndex registers a composite index definition.
	// Must be called before any Put operations that should use this index.
	RegisterCompositeIndex(def CompositeIndexDef) error

	// CompositeIndexes returns all registered composite index definitions.
	CompositeIndexes() []CompositeIndexDef
}
```

### Badger Composite Key Layout

#### `internal/indexstore/physical/badger/backend.go`

New key prefix:

```go
const prefixComposite = "cidx/"
```

Composite key format:

```
cidx/{index_name}/{val1}/{val2}/.../{valN}/{tsHex(16)}/{refHex(64)}
```

Example for `CompositeIndexDef{Name: "dm_thread", Keys: ["app", "thread"]}` with an entry having `app=dm`, `thread=abc123`, timestamp `0x00188c3e4f2a0000`, ref `deadbeef...`:

```
cidx/dm_thread/dm/abc123/00188c3e4f2a0000/deadbeef...
```

Values are nil (same as label index keys). The composite key is sorted lexicographically, so a prefix scan on `cidx/dm_thread/dm/abc123/` returns all entries for that app+thread combination in timestamp order.

### Implementation

#### `internal/indexstore/physical/badger/backend.go`

Add composite index storage to `Backend`:

```go
type Backend struct {
	db              *badger.DB
	closed          atomic.Bool
	compositesMu    sync.RWMutex
	composites      []physical.CompositeIndexDef
	compositesByKey map[string]physical.CompositeIndexDef // keyed by sorted label key fingerprint
}

func NewWithDB(db *badger.DB) *Backend {
	return &Backend{
		db:              db,
		compositesByKey: make(map[string]physical.CompositeIndexDef),
	}
}

func (b *Backend) RegisterCompositeIndex(def physical.CompositeIndexDef) error {
	b.compositesMu.Lock()
	defer b.compositesMu.Unlock()
	fingerprint := strings.Join(def.Keys, "\x00")
	if _, exists := b.compositesByKey[fingerprint]; exists {
		return fmt.Errorf("composite index already registered for keys: %v", def.Keys)
	}
	b.composites = append(b.composites, def)
	b.compositesByKey[fingerprint] = def
	return nil
}

func (b *Backend) CompositeIndexes() []physical.CompositeIndexDef {
	b.compositesMu.RLock()
	defer b.compositesMu.RUnlock()
	out := make([]physical.CompositeIndexDef, len(b.composites))
	copy(out, b.composites)
	return out
}
```

Extend `putInTxn()` to write composite index entries:

```go
func (b *Backend) putInTxn(txn *badger.Txn, entry *physical.Entry) error {
	// ... existing code ...

	// Write composite index entries
	b.compositesMu.RLock()
	composites := b.composites
	b.compositesMu.RUnlock()

	for _, def := range composites {
		if key, ok := b.buildCompositeKey(def, entry, tsHex, refHex); ok {
			if ttl > 0 {
				if err := txn.SetEntry(badger.NewEntry(key, nil).WithTTL(ttl)); err != nil {
					return err
				}
			} else {
				if err := txn.Set(key, nil); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// buildCompositeKey returns the composite key if the entry has all required labels.
func (b *Backend) buildCompositeKey(def physical.CompositeIndexDef, entry *physical.Entry, tsHex, refHex string) ([]byte, bool) {
	var buf strings.Builder
	buf.WriteString(prefixComposite)
	buf.WriteString(def.Name)
	buf.WriteByte('/')
	for _, k := range def.Keys {
		v, ok := entry.Labels[k]
		if !ok {
			return nil, false
		}
		buf.WriteString(v)
		buf.WriteByte('/')
	}
	buf.WriteString(tsHex)
	buf.WriteByte('/')
	buf.WriteString(refHex)
	return []byte(buf.String()), true
}
```

Extend `queryByLabel()` to prefer composite indexes:

```go
func (b *Backend) queryByLabel(opts *physical.QueryOptions) (*physical.QueryResult, error) {
	// Try composite index first
	if def, vals, ok := b.findCompositeIndex(opts.Labels); ok {
		return b.queryByComposite(def, vals, opts)
	}

	// Fall back to existing single-label driving key approach
	// ... existing code ...
}

// findCompositeIndex checks if query labels match a registered composite index.
// Returns the best (most keys matched) composite index definition.
func (b *Backend) findCompositeIndex(labels map[string]string) (physical.CompositeIndexDef, []string, bool) {
	b.compositesMu.RLock()
	defer b.compositesMu.RUnlock()

	var best physical.CompositeIndexDef
	var bestVals []string

	for _, def := range b.composites {
		vals := make([]string, 0, len(def.Keys))
		match := true
		for _, k := range def.Keys {
			v, ok := labels[k]
			if !ok {
				match = false
				break
			}
			vals = append(vals, v)
		}
		if match && len(def.Keys) > len(best.Keys) {
			best = def
			bestVals = vals
		}
	}

	return best, bestVals, len(best.Keys) > 0
}

// queryByComposite scans a composite index prefix.
func (b *Backend) queryByComposite(def physical.CompositeIndexDef, vals []string, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	now := time.Now().UnixNano()
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	// Build composite prefix: cidx/{name}/{v1}/{v2}/.../
	var prefixBuf strings.Builder
	prefixBuf.WriteString(prefixComposite)
	prefixBuf.WriteString(def.Name)
	prefixBuf.WriteByte('/')
	for _, v := range vals {
		prefixBuf.WriteString(v)
		prefixBuf.WriteByte('/')
	}
	compositePrefix := []byte(prefixBuf.String())

	// ... iterator logic identical to queryByLabel but scanning compositePrefix
	// ... with time-range seek, cursor support, meta point-gets for extra labels
}
```

#### `internal/indexstore/physical/redis/backend.go`

Redis composite indexes use sorted sets: key = `cidx:{name}:{v1}:{v2}:...`, member = `{refHex}`, score = timestamp. On `Put`, `ZADD` to any matching composite sorted set. On query, `ZRANGEBYSCORE` with timestamp bounds.

#### `internal/indexstore/physical/sqlite/backend.go`

SQLite composite indexes are standard multi-column indexes. `RegisterCompositeIndex` runs:

```sql
CREATE INDEX IF NOT EXISTS idx_cidx_{name} ON entries ({key1}, {key2}, ..., timestamp);
```

Query uses `WHERE key1 = ? AND key2 = ? AND timestamp BETWEEN ? AND ?`.

### Registration

#### `internal/indexstore/indexstore.go`

```go
// RegisterCompositeIndex registers a composite label index if the backend supports it.
func (s *IndexStore) RegisterCompositeIndex(def physical.CompositeIndexDef) error {
	ci, ok := s.backend.(physical.CompositeIndexer)
	if !ok {
		return fmt.Errorf("backend %T does not support composite indexes", s.backend)
	}
	return ci.RegisterCompositeIndex(def)
}
```

Applications register composite indexes at startup:

```go
store.RegisterCompositeIndex(physical.CompositeIndexDef{
	Name: "dm_thread",
	Keys: []string{"app", "thread"},
})
```

### Testing

- Benchmark: 100k entries with `app=dm` + 1000 distinct threads. Compare query latency for `{app: "dm", thread: "xyz"}` with and without composite index.
- Correctness: verify composite query returns identical results to label intersection query.
- Test composite key cleanup on entry replacement and deletion in `putInTxn` and `deleteInTxn`.

---

## P1: CEL-to-Backend Pushdown

### Problem

CEL expressions are evaluated post-fetch in `IndexStore.Query()` at `internal/indexstore/indexstore.go:137-148`. Simple predicates like `labels["app"] == "dm"` or `timestamp > 1700000000` duplicate filtering the backend already supports natively. The current workaround is the 3x over-fetch multiplier (line 125: `physOpts.Limit = opts.Limit * 3`), which wastes bandwidth and CPU.

### Design

Build a CEL AST analyzer that walks the parsed expression tree, extracts predicates that map to `physical.QueryOptions` fields (label equality, timestamp comparisons), and separates them from non-pushable residual predicates. The extracted predicates are merged into the `QueryOptions` sent to the backend. The residual (if any) is compiled into a separate CEL program for post-fetch filtering.

### New File: `internal/indexstore/cel/analyzer.go`

```go
package cel

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

// AnalysisResult contains extracted backend-pushable predicates and any
// remaining expression that must be evaluated post-fetch.
type AnalysisResult struct {
	// Labels extracted from labels["k"] == "v" predicates.
	Labels map[string]string

	// After extracted from timestamp > N or timestamp >= N predicates.
	// Zero means no lower bound extracted.
	After int64

	// Before extracted from timestamp < N or timestamp <= N predicates.
	// Zero means no upper bound extracted.
	Before int64

	// Residual is the remaining CEL expression after extraction, or ""
	// if the entire expression was pushed down.
	Residual string

	// FullyPushed is true when the entire expression was converted to
	// backend-native filters and no post-fetch CEL evaluation is needed.
	FullyPushed bool
}

// Analyze walks a CEL AST and extracts predicates that can be pushed
// to the physical backend. Returns the analysis result.
// On any analysis error, returns a result with Residual set to the
// original expression (safe fallback).
func (e *Evaluator) Analyze(env *cel.Env, expression string) *AnalysisResult {
	fallback := &AnalysisResult{Residual: expression}

	parsed, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return fallback
	}

	checked, issues := env.Check(parsed)
	if issues != nil && issues.Err() != nil {
		return fallback
	}

	result := &AnalysisResult{
		Labels: make(map[string]string),
	}
	residualParts := extractPredicates(checked.Impl().Expr(), result)

	if len(residualParts) == 0 {
		result.FullyPushed = true
		result.Residual = ""
	} else {
		result.Residual = joinResidual(residualParts)
	}

	return result
}

// extractPredicates recursively walks AND-connected predicates.
// Returns sub-expressions that could not be pushed down.
func extractPredicates(expr ast.Expr, result *AnalysisResult) []ast.Expr {
	// If expr is a logical AND (&&), recurse into both sides
	if call, ok := asCall(expr); ok && call.FunctionName == "_&&_" {
		var residual []ast.Expr
		for _, arg := range call.Args {
			residual = append(residual, extractPredicates(arg, result)...)
		}
		return residual
	}

	// Try to extract: labels["key"] == "value"
	if tryExtractLabelEquality(expr, result) {
		return nil
	}

	// Try to extract: timestamp > N, timestamp >= N, timestamp < N, timestamp <= N
	if tryExtractTimestampBound(expr, result) {
		return nil
	}

	// Not pushable
	return []ast.Expr{expr}
}

// tryExtractLabelEquality matches:  labels["key"] == "value"
// where the map index and equality value are string literals.
func tryExtractLabelEquality(expr ast.Expr, result *AnalysisResult) bool {
	call, ok := asCall(expr)
	if !ok || call.FunctionName != "_==_" || len(call.Args) != 2 {
		return false
	}

	// One side must be labels["key"], the other a string literal
	var mapKey, litVal string
	if k, ok := asLabelIndex(call.Args[0]); ok {
		if v, ok := asStringLiteral(call.Args[1]); ok {
			mapKey, litVal = k, v
		}
	} else if k, ok := asLabelIndex(call.Args[1]); ok {
		if v, ok := asStringLiteral(call.Args[0]); ok {
			mapKey, litVal = k, v
		}
	}
	if mapKey == "" {
		return false
	}

	result.Labels[mapKey] = litVal
	return true
}

// tryExtractTimestampBound matches:
//   timestamp > N, timestamp >= N  -> result.After = N (or N+1 for >)
//   timestamp < N, timestamp <= N  -> result.Before = N (or N-1 for <)
func tryExtractTimestampBound(expr ast.Expr, result *AnalysisResult) bool {
	call, ok := asCall(expr)
	if !ok || len(call.Args) != 2 {
		return false
	}

	var ident string
	var lit int64
	var identLeft bool

	if id, ok := asIdent(call.Args[0]); ok && id == "timestamp" {
		if n, ok := asIntLiteral(call.Args[1]); ok {
			ident, lit, identLeft = id, n, true
		}
	} else if id, ok := asIdent(call.Args[1]); ok && id == "timestamp" {
		if n, ok := asIntLiteral(call.Args[0]); ok {
			ident, lit, identLeft = id, n, false
		}
	}
	if ident == "" {
		return false
	}

	// Normalize: identLeft means "timestamp {op} N"
	// !identLeft means "N {op} timestamp" which flips the comparison
	switch call.FunctionName {
	case "_>_":
		if identLeft {
			result.After = lit // timestamp > N  =>  After = N (exclusive, backend uses >)
		} else {
			result.Before = lit // N > timestamp  =>  Before = N
		}
	case "_>=_":
		if identLeft {
			result.After = lit
		} else {
			result.Before = lit
		}
	case "_<_":
		if identLeft {
			result.Before = lit
		} else {
			result.After = lit
		}
	case "_<=_":
		if identLeft {
			result.Before = lit
		} else {
			result.After = lit
		}
	default:
		return false
	}
	return true
}

// joinResidual reconstructs a CEL expression from residual AST nodes
// by joining them with " && ".
func joinResidual(parts []ast.Expr) string {
	// Use cel.FormatExpr or unparse each part and join with " && "
	// Implementation depends on cel-go unparser availability
	// ...
}

// Helper functions: asCall, asIdent, asStringLiteral, asIntLiteral, asLabelIndex
// These inspect CEL AST node types.
```

### Integration: `internal/indexstore/indexstore.go`

Update `Query()` to use analysis:

```go
func (s *IndexStore) Query(ctx context.Context, opts *QueryOptions) (result *QueryResult, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.query")
	defer op.End(err)

	if opts == nil {
		opts = &QueryOptions{}
	}

	// Analyze CEL expression for backend pushdown
	var residualPrg cel.Program
	physOpts := &physical.QueryOptions{
		Labels:         opts.Labels,
		After:          opts.After,
		Before:         opts.Before,
		Cursor:         opts.Cursor,
		Descending:     opts.Descending,
		IncludeExpired: opts.IncludeExpired,
		Limit:          opts.Limit,
	}

	if opts.Expression != "" {
		analysis := s.eval.Analyze(s.eval.Env(), opts.Expression)

		// Merge extracted predicates into physical options
		if physOpts.Labels == nil {
			physOpts.Labels = make(map[string]string)
		}
		for k, v := range analysis.Labels {
			physOpts.Labels[k] = v
		}
		if analysis.After > 0 && analysis.After > physOpts.After {
			physOpts.After = analysis.After
		}
		if analysis.Before > 0 && (physOpts.Before == 0 || analysis.Before < physOpts.Before) {
			physOpts.Before = analysis.Before
		}

		// Compile residual if not fully pushed
		if !analysis.FullyPushed {
			residualPrg, err = s.eval.Compile(ctx, analysis.Residual)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrInvalidExpression, err)
			}
			// Over-fetch for residual filtering
			if physOpts.Limit > 0 {
				physOpts.Limit = physOpts.Limit * 3
			}
		}
	}

	physResult, err := s.backend.Query(ctx, physOpts)
	if err != nil {
		return nil, fmt.Errorf("query entries: %w", err)
	}

	var entries []*physical.Entry
	if residualPrg != nil {
		matches, evalErr := s.eval.EvalBatch(ctx, residualPrg, physResult.Entries)
		if evalErr != nil {
			return nil, fmt.Errorf("evaluate CEL: %w", evalErr)
		}
		if opts.Limit > 0 && len(matches) > opts.Limit {
			entries = matches[:opts.Limit]
		} else {
			entries = matches
		}
	} else {
		entries = physResult.Entries
	}

	// ... rest unchanged ...
}
```

### Testing

- Test expression `labels["app"] == "dm" && timestamp > 1700000000`: verify `AnalysisResult` has `Labels: {"app": "dm"}`, `After: 1700000000`, `Residual: ""`, `FullyPushed: true`.
- Test mixed expression `labels["app"] == "dm" && ref != "abc"`: verify labels pushed, `ref != "abc"` remains as residual.
- Test non-pushable expression `labels["app"] != "dm"`: verify entire expression stays as residual.
- Benchmark: 1M entries, query `labels["app"] == "dm" && timestamp > X` -- compare pushed vs. non-pushed latency and number of entries scanned.

---

## P1: Time-Based Partitioning

### Problem

All entries live in a single backend keyspace. At millions of entries per second, scan ranges on time-ordered keys grow unbounded. Compaction pressure increases. Old data cannot be archived or dropped without full-table operations.

### Design

Insert a partitioning layer between `IndexStore` and `physical.Backend`. Each partition is a separate backend instance (same type) covering a fixed time window. A router sends writes to the current partition and queries to the relevant set of partitions based on time bounds.

### New File: `internal/indexstore/partition.go`

```go
package indexstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gezibash/arc/pkg/reference"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

// PartitionConfig configures time-based partitioning.
type PartitionConfig struct {
	// Window is the duration of each partition (e.g., 1 * time.Hour).
	Window time.Duration

	// Factory creates a new backend for a partition.
	// The partitionID is a string like "2024010100" (YYYYMMDDHH for hourly).
	Factory func(ctx context.Context, partitionID string) (physical.Backend, error)

	// MaxOpenPartitions limits how many partitions can be open simultaneously.
	// Older partitions are closed (but not deleted) when this limit is exceeded.
	MaxOpenPartitions int
}

// partitionedBackend wraps multiple time-partitioned backends behind
// the physical.Backend interface.
type partitionedBackend struct {
	config     PartitionConfig
	mu         sync.RWMutex
	partitions map[string]*partitionEntry // partitionID -> backend
	sorted     []string                   // sorted partition IDs for query planning
}

type partitionEntry struct {
	backend  physical.Backend
	start    int64 // partition start timestamp (nanos)
	end      int64 // partition end timestamp (nanos)
	lastUsed time.Time
}

// NewPartitionedBackend creates a partitioned backend.
func NewPartitionedBackend(config PartitionConfig) *partitionedBackend {
	return &partitionedBackend{
		config:     config,
		partitions: make(map[string]*partitionEntry),
	}
}

// partitionID returns the partition ID for a given timestamp.
func (pb *partitionedBackend) partitionID(tsNanos int64) string {
	t := time.Unix(0, tsNanos)
	truncated := t.Truncate(pb.config.Window)
	return truncated.Format("20060102T150405")
}

// getOrCreatePartition returns the backend for the given timestamp,
// creating it if necessary.
func (pb *partitionedBackend) getOrCreatePartition(ctx context.Context, tsNanos int64) (physical.Backend, error) {
	id := pb.partitionID(tsNanos)

	pb.mu.RLock()
	if p, ok := pb.partitions[id]; ok {
		p.lastUsed = time.Now()
		pb.mu.RUnlock()
		return p.backend, nil
	}
	pb.mu.RUnlock()

	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Double-check
	if p, ok := pb.partitions[id]; ok {
		p.lastUsed = time.Now()
		return p.backend, nil
	}

	backend, err := pb.config.Factory(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("create partition %s: %w", id, err)
	}

	t := time.Unix(0, tsNanos).Truncate(pb.config.Window)
	pb.partitions[id] = &partitionEntry{
		backend:  backend,
		start:    t.UnixNano(),
		end:      t.Add(pb.config.Window).UnixNano(),
		lastUsed: time.Now(),
	}

	// Maintain sorted list
	pb.sorted = append(pb.sorted, id)
	sort.Strings(pb.sorted)

	// Evict oldest if over limit
	pb.evictOldest()

	return backend, nil
}

// Put routes to the correct partition based on entry timestamp.
func (pb *partitionedBackend) Put(ctx context.Context, entry *physical.Entry) error {
	backend, err := pb.getOrCreatePartition(ctx, entry.Timestamp)
	if err != nil {
		return err
	}
	return backend.Put(ctx, entry)
}

// Query determines which partitions overlap [After, Before] and
// merges results.
func (pb *partitionedBackend) Query(ctx context.Context, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	partitions := pb.partitionsForRange(opts.After, opts.Before)

	if len(partitions) == 1 {
		return partitions[0].backend.Query(ctx, opts)
	}

	// Multi-partition merge-sort by timestamp
	// Order partitions based on Descending flag
	if opts.Descending {
		// Reverse order: newest partition first
		for i, j := 0, len(partitions)-1; i < j; i, j = i+1, j-1 {
			partitions[i], partitions[j] = partitions[j], partitions[i]
		}
	}

	var allEntries []*physical.Entry
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	for _, p := range partitions {
		partOpts := *opts
		partOpts.Limit = limit - len(allEntries)
		result, err := p.backend.Query(ctx, &partOpts)
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, result.Entries...)
		if len(allEntries) >= limit {
			break
		}
	}

	hasMore := len(allEntries) > limit
	if hasMore {
		allEntries = allEntries[:limit]
	}

	var nextCursor string
	if len(allEntries) > 0 && hasMore {
		last := allEntries[len(allEntries)-1]
		nextCursor = fmt.Sprintf("%016x/%s", last.Timestamp, reference.Hex(last.Ref))
	}

	return &physical.QueryResult{
		Entries:    allEntries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// partitionsForRange returns partitions overlapping [after, before].
func (pb *partitionedBackend) partitionsForRange(after, before int64) []*partitionEntry {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	var result []*partitionEntry
	for _, id := range pb.sorted {
		p := pb.partitions[id]
		// Partition range [p.start, p.end) overlaps query range [after, before]
		if before > 0 && p.start >= before {
			continue
		}
		if after > 0 && p.end <= after {
			continue
		}
		result = append(result, p)
	}

	if len(result) == 0 {
		// Return all partitions if no time bounds specified
		for _, id := range pb.sorted {
			result = append(result, pb.partitions[id])
		}
	}
	return result
}

// evictOldest closes the least-recently-used partitions if over limit.
func (pb *partitionedBackend) evictOldest() {
	if pb.config.MaxOpenPartitions <= 0 || len(pb.partitions) <= pb.config.MaxOpenPartitions {
		return
	}

	// Find LRU partition
	var oldestID string
	var oldestTime time.Time
	for id, p := range pb.partitions {
		if oldestID == "" || p.lastUsed.Before(oldestTime) {
			oldestID = id
			oldestTime = p.lastUsed
		}
	}

	if oldestID != "" {
		pb.partitions[oldestID].backend.Close()
		delete(pb.partitions, oldestID)
		// Rebuild sorted list
		newSorted := make([]string, 0, len(pb.partitions))
		for id := range pb.partitions {
			newSorted = append(newSorted, id)
		}
		sort.Strings(newSorted)
		pb.sorted = newSorted
	}
}

// Close closes all open partitions.
func (pb *partitionedBackend) Close() error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	var firstErr error
	for _, p := range pb.partitions {
		if err := p.backend.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	pb.partitions = nil
	pb.sorted = nil
	return firstErr
}

// Remaining Backend interface methods (PutBatch, Get, Delete, Count,
// DeleteExpired, Stats) follow the same routing pattern.
```

### Configuration

Add to `internal/indexstore/indexstore.go` Options:

```go
type Options struct {
	SubscriptionConfig SubscriptionConfig
	PartitionConfig    *PartitionConfig // nil = no partitioning
}
```

When `PartitionConfig` is set, `New()` wraps the provided backend factory in a `partitionedBackend` instead of using a single backend directly.

### Testing

- Create partitioned backend with 1-minute windows. Write entries spanning 5 minutes. Verify query with `After`/`Before` only opens relevant partitions.
- Test partition eviction: set `MaxOpenPartitions=2`, write to 5 partitions, verify only 2 are open.
- Benchmark: compare query latency with and without partitioning on 10M entries spanning 24 hours.

---

## P2: OR Support at Backend Level

### Problem

Only AND logic across labels is supported. A query like "entries in thread A OR thread B" requires two separate queries at the application layer, or a post-fetch CEL expression like `labels["thread"] == "a" || labels["thread"] == "b"` which cannot be pushed to the backend.

### Design

Extend `physical.QueryOptions` with a structured filter type that supports OR groups. Backends implement OR via their native union operations.

### Changes to `internal/indexstore/physical/backend.go`

```go
// LabelPredicate is a single label match condition.
type LabelPredicate struct {
	Key   string
	Value string
}

// LabelFilter represents a boolean combination of label predicates.
// Filters within an AND group are intersected.
// Multiple OR groups are unioned.
type LabelFilter struct {
	// OR contains groups of AND predicates.
	// Semantically: (group[0].AND) OR (group[1].AND) OR ...
	// Empty OR means no label filtering.
	OR []LabelFilterGroup
}

// LabelFilterGroup is a set of predicates that must all match (AND).
type LabelFilterGroup struct {
	Predicates []LabelPredicate
}

// QueryOptions specifies filtering and pagination for queries.
type QueryOptions struct {
	Labels         map[string]string  // Simple AND labels (existing, kept for compatibility)
	LabelFilter    *LabelFilter       // Structured filter with OR support (takes precedence over Labels)
	After          int64
	Before         int64
	Limit          int
	Cursor         string
	Descending     bool
	IncludeExpired bool
}
```

### Backend Implementations

#### Badger (`internal/indexstore/physical/badger/backend.go`)

OR is implemented as parallel iterators with merge-sort:

```go
func (b *Backend) queryByLabelFilter(filter *physical.LabelFilter, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	// For each OR group, run queryByLabel and merge-sort results by timestamp.
	// Deduplicate by reference (same entry may appear in multiple groups).

	seen := make(map[reference.Reference]bool)
	var allEntries []*physical.Entry
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	for _, group := range filter.OR {
		groupLabels := make(map[string]string, len(group.Predicates))
		for _, p := range group.Predicates {
			groupLabels[p.Key] = p.Value
		}
		groupOpts := *opts
		groupOpts.Labels = groupLabels
		groupOpts.LabelFilter = nil

		result, err := b.queryByLabel(&groupOpts)
		if err != nil {
			return nil, err
		}
		for _, e := range result.Entries {
			if !seen[e.Ref] {
				seen[e.Ref] = true
				allEntries = append(allEntries, e)
			}
		}
	}

	// Sort merged results by timestamp
	sort.Slice(allEntries, func(i, j int) bool {
		if opts.Descending {
			return allEntries[i].Timestamp > allEntries[j].Timestamp
		}
		return allEntries[i].Timestamp < allEntries[j].Timestamp
	})

	if len(allEntries) > limit {
		allEntries = allEntries[:limit]
	}

	// ... build QueryResult with cursor and hasMore ...
}
```

#### Redis (`internal/indexstore/physical/redis/backend.go`)

```go
// Use ZUNIONSTORE to create a temporary sorted set from multiple label sets,
// then ZRANGEBYSCORE on the union.
```

#### SQLite (`internal/indexstore/physical/sqlite/backend.go`)

```sql
-- Each OR group becomes a UNION subquery:
SELECT * FROM entries WHERE key1 = ? AND key2 = ?
UNION
SELECT * FROM entries WHERE key1 = ? AND key3 = ?
ORDER BY timestamp ASC LIMIT ?
```

### CEL Analyzer Integration

Extend `internal/indexstore/cel/analyzer.go` to extract OR predicates:

```go
// extractPredicates also handles || (logical OR)
// When all branches of an OR are pushable label equalities on the same key,
// extract as LabelFilter OR groups.
func extractPredicates(expr ast.Expr, result *AnalysisResult) []ast.Expr {
	if call, ok := asCall(expr); ok && call.FunctionName == "_||_" {
		groups, ok := extractORGroups(call)
		if ok {
			result.LabelFilter = &physical.LabelFilter{OR: groups}
			return nil
		}
	}
	// ... existing AND handling ...
}
```

### Testing

- Query `labels["thread"] == "a" || labels["thread"] == "b"`: verify returns entries from both threads, deduplicated, in timestamp order.
- Verify backward compatibility: queries using only `Labels` map still work.
- Benchmark: OR of 10 label values vs. 10 separate queries.

---

## P2: Subscription Backpressure and Observability

### Problem

Subscriptions silently drop entries when the 100-entry buffer is full (`internal/indexstore/subscription.go:136-138`). Clients have no way to know they are lagging or that data was lost. There are no metrics for subscription health.

### Design

Add configurable backpressure policies, health tracking, and observability metrics. The server can send a warning message to the client before disconnecting a slow subscriber.

### Changes to `internal/indexstore/subscription.go`

```go
// BackpressurePolicy controls behavior when a subscription's buffer is full.
type BackpressurePolicy int

const (
	// BackpressureDrop silently drops entries (current behavior).
	BackpressureDrop BackpressurePolicy = iota

	// BackpressureBlock blocks the dispatch goroutine for up to BlockTimeout.
	// If the timeout expires, the entry is dropped and counted.
	BackpressureBlock

	// BackpressureDisconnect disconnects the subscriber immediately on first drop.
	BackpressureDisconnect
)

// SubscriptionOptions configures a single subscription.
type SubscriptionOptions struct {
	BufferSize         int                // Channel buffer size. Default 100.
	BackpressurePolicy BackpressurePolicy // Default BackpressureDrop.
	BlockTimeout       time.Duration      // For BackpressureBlock. Default 1s.
}

// SubscriptionHealth contains real-time health metrics for a subscription.
type SubscriptionHealth struct {
	ID               string
	TotalDelivered   int64
	TotalDropped     int64
	ConsecutiveDrops int64
	BufferUsed       int
	BufferCapacity   int
	Lagging          bool // true when buffer > 80% full
}
```

Update `subscription` struct:

```go
type subscription struct {
	id               string
	expression       string
	entries          chan *physical.Entry
	cancel           context.CancelFunc
	err              error
	errMu            sync.RWMutex
	done             chan struct{}
	opts             SubscriptionOptions

	// Metrics
	totalDelivered   atomic.Int64
	totalDropped     atomic.Int64
	consecutiveDrops atomic.Int64
	lagWarningSent   atomic.Bool
}

func (s *subscription) Health() SubscriptionHealth {
	return SubscriptionHealth{
		ID:               s.id,
		TotalDelivered:   s.totalDelivered.Load(),
		TotalDropped:     s.totalDropped.Load(),
		ConsecutiveDrops: s.consecutiveDrops.Load(),
		BufferUsed:       len(s.entries),
		BufferCapacity:   cap(s.entries),
		Lagging:          len(s.entries) > cap(s.entries)*80/100,
	}
}
```

Update `Subscription` interface:

```go
type Subscription interface {
	ID() string
	Entries() <-chan *physical.Entry
	Cancel()
	Err() error
	Health() SubscriptionHealth
}
```

Update `Subscribe` to accept options:

```go
func (m *subscriptionManager) Subscribe(ctx context.Context, expression string, opts *SubscriptionOptions) (Subscription, error) {
	// ... validation ...

	if opts == nil {
		opts = &SubscriptionOptions{}
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultBufferSize
	}
	if opts.BlockTimeout <= 0 {
		opts.BlockTimeout = time.Second
	}

	sub := newSubscription(ctx, expression, *opts)
	// ...
}
```

Update dispatch in `dispatchEntry()` (from P0 async fan-out):

```go
func (m *subscriptionManager) dispatchToSub(sub *subscription, entry *physical.Entry) {
	switch sub.opts.BackpressurePolicy {
	case BackpressureDrop:
		select {
		case sub.entries <- entry:
			sub.consecutiveDrops.Store(0)
			sub.totalDelivered.Add(1)
		default:
			sub.totalDropped.Add(1)
			drops := sub.consecutiveDrops.Add(1)
			if m.config.MaxConsecutiveDrops > 0 && drops >= int64(m.config.MaxConsecutiveDrops) {
				m.disconnectSub(sub, fmt.Sprintf("exceeded %d consecutive drops", m.config.MaxConsecutiveDrops))
			}
		}

	case BackpressureBlock:
		timer := time.NewTimer(sub.opts.BlockTimeout)
		select {
		case sub.entries <- entry:
			timer.Stop()
			sub.consecutiveDrops.Store(0)
			sub.totalDelivered.Add(1)
		case <-timer.C:
			sub.totalDropped.Add(1)
			sub.consecutiveDrops.Add(1)
		}

	case BackpressureDisconnect:
		select {
		case sub.entries <- entry:
			sub.totalDelivered.Add(1)
		default:
			sub.totalDropped.Add(1)
			m.disconnectSub(sub, "buffer full")
		}
	}
}

func (m *subscriptionManager) disconnectSub(sub *subscription, reason string) {
	slog.Warn("disconnecting slow subscriber",
		"subscription_id", sub.id, "reason", reason,
		"total_delivered", sub.totalDelivered.Load(),
		"total_dropped", sub.totalDropped.Load())
	sub.errMu.Lock()
	sub.err = fmt.Errorf("disconnected: %s", reason)
	sub.errMu.Unlock()
	sub.Cancel()
}
```

### Observability: `internal/indexstore/subscription.go`

Expose aggregate metrics:

```go
func (m *subscriptionManager) Metrics() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var totalDelivered, totalDropped, totalLagging int64
	for _, sub := range m.subs {
		totalDelivered += sub.totalDelivered.Load()
		totalDropped += sub.totalDropped.Load()
		if sub.Health().Lagging {
			totalLagging++
		}
	}

	return map[string]int64{
		"subscriptions_active":          int64(len(m.subs)),
		"subscriptions_total_delivered": totalDelivered,
		"subscriptions_total_dropped":   totalDropped,
		"subscriptions_lagging":         totalLagging,
	}
}
```

Register Prometheus metrics in `internal/observability/`:

```go
// New gauges/counters:
// arc_subscriptions_active          gauge
// arc_subscription_entries_delivered counter (per subscription_id)
// arc_subscription_entries_dropped   counter (per subscription_id)
// arc_subscriptions_lagging          gauge
// arc_subscription_disconnects_total counter
```

### Server-Side Warning: `internal/server/service.go`

Before disconnecting, send a warning response on the stream:

```go
// In the SubscribeMessages handler, watch sub.Health().Lagging.
// When lagging transitions to true, send a response with a special
// system label indicating lag:
resp := &nodev1.SubscribeMessagesResponse{
	Labels: map[string]string{
		"_system":  "warning",
		"_message": "subscription lagging, may be disconnected",
	},
}
stream.Send(resp)
```

Alternatively, add a dedicated field to the proto response:

```protobuf
// api/arc/node/v1/node.proto
message SubscribeMessagesResponse {
  // ... existing fields ...

  // Server-side warning (e.g., "lagging", "disconnecting").
  // Empty when there is no warning.
  string warning = 10;
}
```

### Testing

- Test `BackpressureDrop`: fill buffer, verify drops counted, verify disconnect after threshold.
- Test `BackpressureBlock`: full buffer with short timeout, verify entry delivered after consumer drains.
- Test `BackpressureDisconnect`: full buffer, verify immediate disconnection.
- Test `Health()`: verify all counters accurately reflect delivery/drop state.
- Test Prometheus metrics: verify gauges and counters update correctly.

---

## Implementation Order

| Phase | Item | Effort | Dependencies |
|-------|------|--------|-------------|
| 1 | P0: Async Subscription Fan-out | 2-3 days | None |
| 1 | P0: Composite Label Indexes | 3-4 days | None |
| 2 | P1: CEL-to-Backend Pushdown | 3-4 days | None (but benefits from composite indexes) |
| 2 | P1: Time-Based Partitioning | 4-5 days | None |
| 3 | P2: OR Support at Backend Level | 3-4 days | CEL pushdown (for analyzer integration) |
| 3 | P2: Subscription Backpressure | 2-3 days | Async fan-out (builds on P0 worker model) |

Phase 1 and 2 items within each phase can be developed in parallel. Phase 3 items depend on earlier phases as noted.
