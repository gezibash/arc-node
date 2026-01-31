package indexstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/cel-go/cel"

	"github.com/gezibash/arc/pkg/reference"

	celeval "github.com/gezibash/arc-node/internal/indexstore/cel"
	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/observability"
)

// IndexStore provides indexed storage with CEL queries and subscriptions.
type IndexStore struct {
	backend physical.Backend
	metrics *observability.Metrics
	eval    *celeval.Evaluator
	subs    *subscriptionManager
}

// New creates a new IndexStore with the given backend.
func New(backend physical.Backend, metrics *observability.Metrics) (*IndexStore, error) {
	eval, err := celeval.NewEvaluator()
	if err != nil {
		return nil, fmt.Errorf("create CEL evaluator: %w", err)
	}

	return &IndexStore{
		backend: backend,
		metrics: metrics,
		eval:    eval,
		subs:    newSubscriptionManager(eval),
	}, nil
}

// Index stores an entry with the given labels.
func (s *IndexStore) Index(ctx context.Context, entry *physical.Entry) (err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.index")
	defer op.End(err)

	slog.DebugContext(ctx, "indexing entry", "ref", reference.Hex(entry.Ref), "label_count", len(entry.Labels))

	if err = s.backend.Put(ctx, entry); err != nil {
		return fmt.Errorf("index entry: %w", err)
	}

	// Notify subscribers
	if s.subs.Len() > 0 {
		s.subs.Notify(ctx, entry)
	}

	slog.InfoContext(ctx, "entry indexed", "ref", reference.Hex(entry.Ref))
	return nil
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
	After          int64
	Before         int64
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

	// Validate CEL expression if provided
	var celPrg cel.Program
	if opts.Expression != "" {
		celPrg, err = s.eval.Compile(ctx, opts.Expression)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidExpression, err)
		}
	}

	// Build physical query options
	physOpts := &physical.QueryOptions{
		Labels:         opts.Labels,
		After:          opts.After,
		Before:         opts.Before,
		Cursor:         opts.Cursor,
		Descending:     opts.Descending,
		IncludeExpired: opts.IncludeExpired,
	}

	if celPrg != nil && opts.Limit > 0 {
		physOpts.Limit = opts.Limit * 3
	} else {
		physOpts.Limit = opts.Limit
	}

	physResult, err := s.backend.Query(ctx, physOpts)
	if err != nil {
		return nil, fmt.Errorf("query entries: %w", err)
	}

	// Apply CEL filter if provided
	var entries []*physical.Entry
	if celPrg != nil {
		matches, evalErr := s.eval.EvalBatch(ctx, celPrg, physResult.Entries)
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

	hasMore := physResult.HasMore
	nextCursor := physResult.NextCursor

	if celPrg != nil && opts.Limit > 0 && len(entries) < opts.Limit && physResult.HasMore {
		hasMore = true
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

	count, err = s.backend.Count(ctx, physOpts)
	if err != nil {
		return 0, fmt.Errorf("count entries: %w", err)
	}

	slog.DebugContext(ctx, "count completed", "count", count)
	return count, nil
}

// Subscribe creates a subscription for entries matching the CEL expression.
func (s *IndexStore) Subscribe(ctx context.Context, expression string) (sub Subscription, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "indexstore.subscribe")
	defer op.End(err)

	sub, err = s.subs.Subscribe(ctx, expression)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidExpression, err)
	}

	slog.InfoContext(ctx, "subscription created",
		"subscription_id", sub.ID(),
		"active_subscriptions", s.subs.Len(),
	)
	return sub, nil
}

// ResolvePrefix resolves a hex reference prefix to a single full reference.
// Returns ErrPrefixTooShort if the prefix is less than 4 characters,
// ErrAmbiguousPrefix if multiple entries match, or ErrNotFound if none match.
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

// SubscriptionCount returns the number of active subscriptions.
func (s *IndexStore) SubscriptionCount() int {
	return s.subs.Len()
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
			}
		}
	}()
}

// Close releases resources associated with the IndexStore.
func (s *IndexStore) Close() error {
	s.subs.Close()
	return s.backend.Close()
}
