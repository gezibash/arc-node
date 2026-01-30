package blobstore

import (
	"context"
	"errors"
	"testing"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/memory"
	"github.com/gezibash/arc-node/internal/observability"
)

func newTestStore(t *testing.T) *BlobStore {
	t.Helper()
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create memory backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })
	return New(backend, metrics)
}

func TestStoreAndFetch(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	data := []byte("hello world")

	ref, err := store.Store(ctx, data)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Fatal("Store returned zero reference")
	}

	got, err := store.Fetch(ctx, ref)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Fetch = %q, want %q", got, data)
	}
}

func TestFetchNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.Fetch(ctx, reference.Reference{})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Fetch = %v, want ErrNotFound", err)
	}
}

func TestExists(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref, _ := store.Store(ctx, []byte("test"))

	exists, err := store.Exists(ctx, ref)
	if err != nil || !exists {
		t.Errorf("Exists after store = %v, %v", exists, err)
	}

	exists, err = store.Exists(ctx, reference.Reference{})
	if err != nil || exists {
		t.Errorf("Exists for missing = %v, %v", exists, err)
	}
}

func TestDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref, _ := store.Store(ctx, []byte("test"))

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.Fetch(ctx, ref)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Fetch after delete = %v, want ErrNotFound", err)
	}
}

func TestStats(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.BackendType != "badger" {
		t.Errorf("BackendType = %q, want %q", stats.BackendType, "badger")
	}
}

func TestIntegrityCheck(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	data := []byte("test data")
	ref, err := store.Store(ctx, data)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Verify the computed reference matches what we'd expect
	expected := reference.Compute(data)
	if !reference.Equal(ref, expected) {
		t.Errorf("Store returned %s, expected %s", reference.Hex(ref), reference.Hex(expected))
	}
}
