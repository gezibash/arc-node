package indexstore

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/observability"
)

const defaultAutoIndexThreshold int64 = 100

// queryTracker records multi-label query patterns and evaluates whether to
// create composite indexes. Backends that don't support composite indexes
// get a no-op implementation — zero overhead on the hot path.
type queryTracker interface {
	Track(labels map[string]string)
	evaluate(ctx context.Context) int
}

// noopTracker is used when the backend doesn't support composite indexes.
type noopTracker struct{}

func (noopTracker) Track(map[string]string) {}
func (noopTracker) evaluate(context.Context) int { return 0 }

// autoIndexer tracks multi-label query patterns and automatically registers
// composite indexes when a pattern exceeds a frequency threshold.
type autoIndexer struct {
	mu        sync.Mutex
	patterns  map[string]*labelPattern
	threshold int64
	store     *IndexStore
	metrics   *observability.Metrics
}

type labelPattern struct {
	keys  []string // sorted label keys
	count int64
}

func newQueryTracker(store *IndexStore, metrics *observability.Metrics) queryTracker {
	if _, ok := store.backend.(physical.CompositeIndexer); !ok {
		return noopTracker{}
	}
	return &autoIndexer{
		patterns:  make(map[string]*labelPattern),
		threshold: defaultAutoIndexThreshold,
		store:     store,
		metrics:   metrics,
	}
}

// Track records a multi-label query pattern. Only call when len(labels) > 1.
func (a *autoIndexer) Track(labels map[string]string) {
	keys := sortedKeys(labels)
	fingerprint := strings.Join(keys, "\x00")

	// Skip if composite index already registered for this pattern.
	ci, ok := a.store.backend.(physical.CompositeIndexer)
	if ok {
		for _, def := range ci.CompositeIndexes() {
			if strings.Join(def.Keys, "\x00") == fingerprint {
				return
			}
		}
	}

	a.mu.Lock()
	p, exists := a.patterns[fingerprint]
	if !exists {
		p = &labelPattern{keys: keys}
		a.patterns[fingerprint] = p
	}
	p.count++
	a.mu.Unlock()

	if a.metrics != nil && a.metrics.AutoIndexPatterns != nil {
		a.metrics.AutoIndexPatterns.WithLabelValues(strings.Join(keys, "_")).Inc()
	}
}

// evaluate checks all tracked patterns and registers + backfills any that
// exceed the threshold. Returns the number of new indexes created.
func (a *autoIndexer) evaluate(ctx context.Context) int {
	a.mu.Lock()
	var hot []*labelPattern
	var hotFingerprints []string
	for fp, p := range a.patterns {
		if atomic.LoadInt64(&p.count) >= a.threshold {
			hot = append(hot, p)
			hotFingerprints = append(hotFingerprints, fp)
		}
	}
	// Reset counters for hot patterns so they don't fire again immediately.
	for _, fp := range hotFingerprints {
		delete(a.patterns, fp)
	}
	a.mu.Unlock()

	created := 0
	for _, p := range hot {
		name := strings.Join(p.keys, "_")
		def := physical.CompositeIndexDef{
			Name: name,
			Keys: p.keys,
		}

		if err := a.store.RegisterCompositeIndex(def); err != nil {
			slog.WarnContext(ctx, "auto-index register failed", "index", name, "error", err)
			if a.metrics != nil && a.metrics.AutoIndexReindexTotal != nil {
				a.metrics.AutoIndexReindexTotal.WithLabelValues(name, "error").Inc()
			}
			continue
		}

		slog.InfoContext(ctx, "auto-index registered", "index", name, "keys", p.keys)

		// Backfill existing entries.
		backfiller, ok := a.store.backend.(physical.Backfiller)
		if ok {
			n, err := backfiller.BackfillCompositeIndex(ctx, def)
			if err != nil {
				slog.ErrorContext(ctx, "auto-index backfill failed", "index", name, "error", err)
				if a.metrics != nil && a.metrics.AutoIndexReindexTotal != nil {
					a.metrics.AutoIndexReindexTotal.WithLabelValues(name, "error").Inc()
				}
				continue
			}
			slog.InfoContext(ctx, "auto-index backfill complete", "index", name, "entries", n)
		}

		if a.metrics != nil && a.metrics.AutoIndexReindexTotal != nil {
			a.metrics.AutoIndexReindexTotal.WithLabelValues(name, "ok").Inc()
		}
		created++
	}
	return created
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
