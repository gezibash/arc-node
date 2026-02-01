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
	t.Cleanup(func() { backend.Close() })

	store, err := New(backend, metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

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
