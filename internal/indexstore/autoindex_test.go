package indexstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	badgerbackend "github.com/gezibash/arc-node/internal/indexstore/physical/badger"
	"github.com/gezibash/arc-node/internal/observability"
)

func TestAutoIndexer(t *testing.T) {
	metrics := observability.NewMetrics()

	cfg := map[string]string{"in_memory": "true"}
	backend, err := badgerbackend.NewFactory(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	store, err := New(backend, metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	// Seed entries with two labels.
	for i := 0; i < 50; i++ {
		data := []byte(fmt.Sprintf("entry-%d", i))
		ref := reference.Compute(data)
		err := store.Index(ctx, &physical.Entry{
			Ref:         ref,
			Labels:      map[string]string{"app": "journal", "user": "alice"},
			Timestamp:   int64(1000 + i),
			Persistence: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	labels := map[string]string{"app": "journal", "user": "alice"}

	// Verify no composite index exists yet.
	ci := backend.(physical.CompositeIndexer)
	if len(ci.CompositeIndexes()) != 0 {
		t.Fatal("expected no composite indexes initially")
	}

	// Issue enough Count calls to exceed the threshold.
	store.autoIdx.(*autoIndexer).threshold = 10
	for i := 0; i < 11; i++ {
		_, err := store.Count(ctx, &QueryOptions{Labels: labels})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Trigger evaluation.
	created := store.autoIdx.evaluate(ctx)
	if created != 1 {
		t.Fatalf("expected 1 new index, got %d", created)
	}

	// Verify composite index was registered.
	indexes := ci.CompositeIndexes()
	if len(indexes) != 1 {
		t.Fatalf("expected 1 composite index, got %d", len(indexes))
	}
	if indexes[0].Name != "app_user" {
		t.Fatalf("expected index name app_user, got %s", indexes[0].Name)
	}

	// Verify Count now uses composite path (functional correctness).
	count, err := store.Count(ctx, &QueryOptions{Labels: labels})
	if err != nil {
		t.Fatal(err)
	}
	if count != 50 {
		t.Fatalf("expected count 50, got %d", count)
	}

	// A second evaluate should not create duplicates (pattern already indexed).
	for i := 0; i < 11; i++ {
		_, _ = store.Count(ctx, &QueryOptions{Labels: labels})
	}
	created = store.autoIdx.evaluate(ctx)
	if created != 0 {
		t.Fatalf("expected 0 new indexes on second evaluate, got %d", created)
	}
}

func TestNoopTracker(t *testing.T) {
	// Memory backend does not support CompositeIndexer, so newQueryTracker
	// should return a noopTracker.
	store := newTestStore(t)
	ctx := context.Background()

	// Track should be a no-op (no panic).
	store.autoIdx.Track(map[string]string{"a": "1", "b": "2"})

	// evaluate should return 0.
	created := store.autoIdx.evaluate(ctx)
	if created != 0 {
		t.Fatalf("noopTracker evaluate returned %d, want 0", created)
	}
}

func TestAutoIndexerTrackDirectly(t *testing.T) {
	metrics := observability.NewMetrics()
	cfg := map[string]string{"in_memory": "true"}
	backend, err := badgerbackend.NewFactory(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	store, err := New(backend, metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ai, ok := store.autoIdx.(*autoIndexer)
	if !ok {
		t.Fatal("expected autoIndexer for badger backend")
	}

	// Track a pattern below threshold — evaluate should not create index.
	ai.threshold = 100
	for i := 0; i < 5; i++ {
		ai.Track(map[string]string{"x": "1", "y": "2"})
	}

	ctx := context.Background()
	created := ai.evaluate(ctx)
	if created != 0 {
		t.Fatalf("expected 0 (below threshold), got %d", created)
	}

	// Track above threshold.
	for i := 0; i < 100; i++ {
		ai.Track(map[string]string{"x": "1", "y": "2"})
	}

	created = ai.evaluate(ctx)
	if created != 1 {
		t.Fatalf("expected 1, got %d", created)
	}
}

func TestAutoIndexerEvaluateBelowThreshold(t *testing.T) {
	metrics := observability.NewMetrics()
	cfg := map[string]string{"in_memory": "true"}
	backend, err := badgerbackend.NewFactory(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	store, err := New(backend, metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ai := store.autoIdx.(*autoIndexer)
	ai.threshold = 1000

	// Track a few times — well below threshold.
	for i := 0; i < 3; i++ {
		ai.Track(map[string]string{"foo": "1", "bar": "2"})
	}

	ctx := context.Background()
	created := ai.evaluate(ctx)
	if created != 0 {
		t.Fatalf("expected 0, got %d", created)
	}

	// Pattern should still be tracked (not deleted).
	ai.mu.Lock()
	count := len(ai.patterns)
	ai.mu.Unlock()
	if count != 1 {
		t.Fatalf("expected 1 tracked pattern, got %d", count)
	}
}
