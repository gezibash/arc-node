package indexstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/cel-go/cel"

	"github.com/gezibash/arc/v2/pkg/reference"

	celeval "github.com/gezibash/arc-node/internal/indexstore/cel"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/observability"
)

// Options configures an IndexStore.
type Options struct {
	SubscriptionConfig SubscriptionConfig
}

// IndexStore provides indexed storage with CEL queries and subscriptions.
type IndexStore struct {
	backend          physical.Backend
	metrics          *observability.Metrics
	eval             *celeval.Evaluator
	subs             *subscriptionManager
	autoIdx          queryTracker
	delivery         *DeliveryTracker
	seq              atomic.Int64
	idempotencyCache *idempotencyLRU
}

// New creates a new IndexStore with the given backend.
func New(backend physical.Backend, metrics *observability.Metrics, opts ...*Options) (*IndexStore, error) {
	eval, err := celeval.NewEvaluator()
	if err != nil {
		return nil, fmt.Errorf("create CEL evaluator: %w", err)
	}

	cfg := SubscriptionConfig{}
	if len(opts) > 0 && opts[0] != nil {
		cfg = opts[0].SubscriptionConfig
	}

	s := &IndexStore{
		backend:          backend,
		metrics:          metrics,
		eval:             eval,
		subs:             newSubscriptionManager(eval, cfg),
		delivery:         NewDeliveryTracker(backend),
		idempotencyCache: newIdempotencyLRU(10000),
	}
	s.autoIdx = newQueryTracker(s, metrics)
	return s, nil
}

// Index stores an entry with the given labels.
func (s *IndexStore) Index(ctx context.Context, entry *physical.Entry) (err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.index")
	defer op.End(err)

	slog.DebugContext(ctx, "indexing entry", "ref", reference.Hex(entry.Ref), "label_count", len(entry.Labels))

	// Dedup check before any storage or notification.
	if s.checkDedup(ctx, entry) {
		slog.DebugContext(ctx, "duplicate entry skipped", "ref", reference.Hex(entry.Ref), "dedup_mode", entry.DedupMode)
		return nil
	}

	// Assign monotonic sequence if not already set (e.g. by federation).
	if entry.Sequence == 0 {
		entry.Sequence = s.seq.Add(1)
	} else {
		// Advance the counter past externally-set sequences to avoid collisions.
		for {
			cur := s.seq.Load()
			if cur >= entry.Sequence {
				break
			}
			if s.seq.CompareAndSwap(cur, entry.Sequence) {
				break
			}
		}
	}

	// STORE_ONLY pattern: index but never notify subscribers.
	storeOnly := entry.Pattern == 4 // PATTERN_STORE_ONLY

	// Ephemeral entries bypass disk entirely — fan out to subscribers only.
	if entry.Persistence == 0 {
		if !storeOnly && s.subs.Len() > 0 {
			s.subs.Notify(ctx, entry)
		}
		slog.DebugContext(ctx, "ephemeral entry fanned out", "ref", reference.Hex(entry.Ref))
		return nil
	}

	if err = s.backend.Put(ctx, entry); err != nil {
		return fmt.Errorf("index entry: %w", err)
	}

	// Cache idempotency key after successful store.
	s.cacheIdempotencyKey(entry)

	// Note: delivery tracking for at-least-once entries is handled
	// at the Channel layer via Deliver() when entries are sent to subscribers.

	// Notify subscribers (non-blocking async fan-out), unless STORE_ONLY.
	if !storeOnly && s.subs.Len() > 0 {
		s.subs.Notify(ctx, entry)
	}

	slog.InfoContext(ctx, "entry indexed", "ref", reference.Hex(entry.Ref))
	return nil
}

// checkDedup returns true if the entry is a duplicate that should be skipped.
func (s *IndexStore) checkDedup(ctx context.Context, entry *physical.Entry) bool {
	switch entry.DedupMode {
	case 1: // DEDUP_REF
		if _, err := s.backend.Get(ctx, entry.Ref); err == nil {
			return true
		}
	case 2: // DEDUP_IDEMPOTENCY_KEY
		if entry.IdempotencyKey != "" {
			if s.idempotencyCache.Contains(entry.IdempotencyKey) {
				return true
			}
			// Will be cached after successful store.
		}
	}
	return false
}

// cacheIdempotencyKey records the key after a successful index.
func (s *IndexStore) cacheIdempotencyKey(entry *physical.Entry) {
	if entry.DedupMode == 2 && entry.IdempotencyKey != "" {
		s.idempotencyCache.Add(entry.IdempotencyKey)
	}
}

// idempotencyLRU is a bounded set of recently-seen idempotency keys.
type idempotencyLRU struct {
	mu      sync.Mutex
	keys    map[string]struct{}
	order   []string
	maxSize int
}

func newIdempotencyLRU(maxSize int) *idempotencyLRU {
	return &idempotencyLRU{
		keys:    make(map[string]struct{}),
		maxSize: maxSize,
	}
}

func (c *idempotencyLRU) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.keys[key]
	return ok
}

func (c *idempotencyLRU) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.keys[key]; ok {
		return
	}
	if len(c.order) >= c.maxSize {
		evict := c.order[0]
		c.order = c.order[1:]
		delete(c.keys, evict)
	}
	c.keys[key] = struct{}{}
	c.order = append(c.order, key)
}

// Deliver assigns a delivery ID for an at-least-once entry being sent to a subscriber.
func (s *IndexStore) Deliver(entry *physical.Entry, subID string) int64 {
	return s.delivery.Deliver(entry, subID)
}

// AckDelivery acknowledges a delivery by its ID. Returns the entry for cursor updates.
func (s *IndexStore) AckDelivery(ctx context.Context, deliveryID int64) (*physical.Entry, error) {
	return s.delivery.Ack(ctx, deliveryID)
}

// PutCursor stores a durable subscription cursor.
func (s *IndexStore) PutCursor(ctx context.Context, key string, cursor physical.Cursor) error {
	return s.backend.PutCursor(ctx, key, cursor)
}

// GetCursor retrieves a durable subscription cursor.
func (s *IndexStore) GetCursor(ctx context.Context, key string) (physical.Cursor, error) {
	return s.backend.GetCursor(ctx, key)
}

// DeliveryTracker returns the delivery tracker for redelivery loop access.
func (s *IndexStore) DeliveryTracker() *DeliveryTracker {
	return s.delivery
}

// Get retrieves an entry by its reference.
func (s *IndexStore) Get(ctx context.Context, r reference.Reference) (entry *physical.Entry, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.get")
	defer op.End(err)

	entry, err = s.backend.Get(ctx, r)
	if errors.Is(err, physical.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get entry: %w", err)
	}
	return entry, nil
}

// QueryOptions configures a query operation.
type QueryOptions struct {
	Expression     string
	Labels         map[string]string
	After          int64 // unix milliseconds
	Before         int64 // unix milliseconds
	Limit          int
	Cursor         string
	Descending     bool
	IncludeExpired bool
}

// QueryResult contains the results of a query.
type QueryResult struct {
	Entries    []*physical.Entry
	NextCursor string
	HasMore    bool
}

// Query returns entries matching the given options.
func (s *IndexStore) Query(ctx context.Context, opts *QueryOptions) (result *QueryResult, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.query")
	defer op.End(err)

	if opts == nil {
		opts = &QueryOptions{}
	}

	// Build physical query options
	physOpts := &physical.QueryOptions{
		Labels:         opts.Labels,
		After:          opts.After,
		Before:         opts.Before,
		Cursor:         opts.Cursor,
		Descending:     opts.Descending,
		IncludeExpired: opts.IncludeExpired,
		Limit:          opts.Limit,
	}

	// Analyze CEL expression for backend pushdown
	var residualPrg cel.Program
	if opts.Expression != "" {
		analysis := s.eval.Analyze(opts.Expression)

		// Merge extracted labels into physical options
		if len(analysis.Labels) > 0 {
			if physOpts.Labels == nil {
				physOpts.Labels = make(map[string]string)
			}
			for k, v := range analysis.Labels {
				physOpts.Labels[k] = v
			}
		}

		// Merge extracted time bounds (take the tighter bound)
		if analysis.After > 0 && analysis.After > physOpts.After {
			physOpts.After = analysis.After
		}
		if analysis.Before > 0 && (physOpts.Before == 0 || analysis.Before < physOpts.Before) {
			physOpts.Before = analysis.Before
		}

		// Merge extracted OR label filter
		if analysis.LabelFilter != nil {
			physOpts.LabelFilter = analysis.LabelFilter
		}

		// Compile residual if not fully pushed
		if !analysis.FullyPushed {
			residualPrg, err = s.eval.Compile(ctx, analysis.Residual)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrInvalidExpression, err)
			}
			// Over-fetch for residual filtering
			if physOpts.Limit > 0 {
				physOpts.Limit = physOpts.Limit * 3
			}
		}
	}

	if len(physOpts.Labels) > 1 {
		s.autoIdx.Track(physOpts.Labels)
	}

	physResult, err := s.backend.Query(ctx, physOpts)
	if err != nil {
		return nil, fmt.Errorf("query entries: %w", err)
	}

	// Apply residual CEL filter if provided
	var entries []*physical.Entry
	residualHasMore := false
	if residualPrg != nil {
		matches, evalErr := s.eval.EvalBatch(ctx, residualPrg, physResult.Entries)
		if evalErr != nil {
			return nil, fmt.Errorf("evaluate CEL: %w", evalErr)
		}
		if opts.Limit > 0 && len(matches) > opts.Limit {
			residualHasMore = true
			entries = matches[:opts.Limit]
		} else {
			entries = matches
		}
	} else {
		entries = physResult.Entries
	}

	hasMore := physResult.HasMore || residualHasMore
	nextCursor := physResult.NextCursor
	if residualPrg != nil {
		if len(entries) > 0 {
			last := entries[len(entries)-1]
			nextCursor = fmt.Sprintf("%016x/%s", last.Timestamp, reference.Hex(last.Ref))
		} else if !hasMore {
			nextCursor = ""
		}
	}

	slog.DebugContext(ctx, "query completed", "result_count", len(entries), "has_more", hasMore)

	return &QueryResult{
		Entries:    entries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// Count returns the number of entries matching the given options without
// fetching full entries. CEL expressions are not supported for counting —
// only label and time-range filters are applied at the backend level.
func (s *IndexStore) Count(ctx context.Context, opts *QueryOptions) (count int64, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.count")
	defer op.End(err)

	if opts == nil {
		opts = &QueryOptions{}
	}

	physOpts := &physical.QueryOptions{
		Labels:         opts.Labels,
		After:          opts.After,
		Before:         opts.Before,
		IncludeExpired: opts.IncludeExpired,
	}

	if len(physOpts.Labels) > 1 {
		s.autoIdx.Track(physOpts.Labels)
	}

	count, err = s.backend.Count(ctx, physOpts)
	if err != nil {
		return 0, fmt.Errorf("count entries: %w", err)
	}

	slog.DebugContext(ctx, "count completed", "count", count)
	return count, nil
}

// Subscribe creates a subscription for entries matching the CEL expression.
// callerKey identifies the subscriber for visibility filtering and direct routing.
func (s *IndexStore) Subscribe(ctx context.Context, expression string, callerKey [32]byte, opts ...*SubscriptionOptions) (sub Subscription, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.subscribe")
	defer op.End(err)

	var subOpts *SubscriptionOptions
	if len(opts) > 0 {
		subOpts = opts[0]
	}

	sub, err = s.subs.Subscribe(ctx, expression, callerKey, subOpts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidExpression, err)
	}

	slog.InfoContext(ctx, "subscription created",
		"subscription_id", sub.ID(),
		"active_subscriptions", s.subs.Len(),
	)
	return sub, nil
}

// RegisterCompositeIndex registers a composite label index if the backend supports it.
func (s *IndexStore) RegisterCompositeIndex(def physical.CompositeIndexDef) error {
	ci, ok := s.backend.(physical.CompositeIndexer)
	if !ok {
		return fmt.Errorf("%w: backend %T does not support composite indexes", physical.ErrUnsupported, s.backend)
	}
	return ci.RegisterCompositeIndex(def)
}

// ResolvePrefix resolves a hex reference prefix to a single full reference.
func (s *IndexStore) ResolvePrefix(ctx context.Context, hexPrefix string) (ref reference.Reference, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.resolve_prefix")
	defer op.End(err)

	if len(hexPrefix) < 4 {
		return reference.Reference{}, ErrPrefixTooShort
	}
	scanner, ok := s.backend.(physical.PrefixScanner)
	if !ok {
		return reference.Reference{}, fmt.Errorf("backend does not support prefix resolution")
	}
	refs, err := scanner.ScanPrefix(ctx, hexPrefix, 2)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("scan prefix: %w", err)
	}
	switch len(refs) {
	case 0:
		return reference.Reference{}, ErrNotFound
	case 1:
		return refs[0], nil
	default:
		return reference.Reference{}, ErrAmbiguousPrefix
	}
}

// Delete removes an entry by its reference.
func (s *IndexStore) Delete(ctx context.Context, r reference.Reference) (err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.delete")
	defer op.End(err)

	if err = s.backend.Delete(ctx, r); err != nil {
		return fmt.Errorf("delete entry: %w", err)
	}
	return nil
}

// Cleanup removes expired entries from the store.
func (s *IndexStore) Cleanup(ctx context.Context) (count int, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.cleanup")
	defer op.End(err)

	count, err = s.backend.DeleteExpired(ctx, time.Now())
	if err != nil {
		return 0, fmt.Errorf("cleanup expired: %w", err)
	}

	slog.InfoContext(ctx, "cleanup complete", "deleted", count)
	return count, nil
}

// Stats returns storage statistics.
func (s *IndexStore) Stats(ctx context.Context) (stats *physical.Stats, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.stats")
	defer op.End(err)

	stats, err = s.backend.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// SetVisibilityFilter sets the function used to check whether an entry
// should be visible to a given subscriber.
func (s *IndexStore) SetVisibilityFilter(f VisibilityFilter) {
	s.subs.SetVisibilityFilter(f)
}

// SubscriptionCount returns the number of active subscriptions.
func (s *IndexStore) SubscriptionCount() int {
	return s.subs.Len()
}

// SubscriptionMetrics returns aggregate subscription health metrics.
func (s *IndexStore) SubscriptionMetrics() map[string]int64 {
	return s.subs.Metrics()
}

// StartCleanup launches a background goroutine that periodically removes
// expired entries. It stops when ctx is cancelled.
func (s *IndexStore) StartCleanup(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				slog.Info("cleanup goroutine stopped")
				return
			case <-ticker.C:
				count, err := s.Cleanup(ctx)
				if err != nil {
					slog.ErrorContext(ctx, "periodic cleanup failed", "error", err)
				}
				if count > 0 {
					slog.InfoContext(ctx, "periodic cleanup completed", "deleted", count)
				}

				// Clean up stale until-delivered entries.
				dCount, dErr := s.delivery.Cleanup(ctx, time.Hour)
				if dErr != nil {
					slog.ErrorContext(ctx, "delivery cleanup failed", "error", dErr)
				}
				if dCount > 0 {
					slog.InfoContext(ctx, "delivery cleanup completed", "deleted", dCount)
				}
			}
		}
	}()
}

// StartAutoIndex launches a background goroutine that periodically evaluates
// tracked query patterns and registers composite indexes for hot patterns.
// It stops when ctx is cancelled.
func (s *IndexStore) StartAutoIndex(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				slog.Info("auto-index goroutine stopped")
				return
			case <-ticker.C:
				created := s.autoIdx.evaluate(ctx)
				if created > 0 {
					slog.InfoContext(ctx, "auto-index tick", "new_indexes", created)
				}
			}
		}
	}()
}

// Close releases resources associated with the IndexStore.
func (s *IndexStore) Close() error {
	s.subs.Close()
	return s.backend.Close()
}
