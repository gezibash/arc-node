package blobstore

import (
	"context"
	"errors"
	"testing"

	"github.com/gezibash/arc/v2/pkg/reference"

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
	t.Cleanup(func() { _ = backend.Close() })
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

func TestStoreEmpty(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref, err := store.Store(ctx, []byte{})
	if err != nil {
		t.Fatalf("Store empty: %v", err)
	}
	if ref == (reference.Reference{}) {
		t.Fatal("Store empty returned zero reference")
	}

	got, err := store.Fetch(ctx, ref)
	if err != nil {
		t.Fatalf("Fetch empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Fetch empty = %q, want empty", got)
	}
}

func TestStoreDuplicate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	data := []byte("duplicate data")
	ref1, err := store.Store(ctx, data)
	if err != nil {
		t.Fatalf("Store first: %v", err)
	}
	ref2, err := store.Store(ctx, data)
	if err != nil {
		t.Fatalf("Store second: %v", err)
	}
	if !reference.Equal(ref1, ref2) {
		t.Errorf("duplicate store refs differ: %s vs %s", reference.Hex(ref1), reference.Hex(ref2))
	}

	// Data should still be fetchable.
	got, err := store.Fetch(ctx, ref1)
	if err != nil {
		t.Fatalf("Fetch after duplicate store: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Fetch = %q, want %q", got, data)
	}
}

func TestFetchIntegrityVerification(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	data := []byte("integrity test data")
	ref, err := store.Store(ctx, data)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Fetch and verify the returned data hashes to the same reference.
	got, err := store.Fetch(ctx, ref)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	computed := reference.Compute(got)
	if !reference.Equal(computed, ref) {
		t.Errorf("integrity mismatch: fetched data hashes to %s, expected %s", reference.Hex(computed), reference.Hex(ref))
	}
}

func TestDeleteNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Deleting a non-existent key should not error (idempotent).
	err := store.Delete(ctx, reference.Reference{})
	if err != nil {
		t.Errorf("Delete non-existent: %v", err)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref, err := store.Store(ctx, []byte("delete me"))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete first: %v", err)
	}
	if err := store.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete second: %v", err)
	}

	_, err = store.Fetch(ctx, ref)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Fetch after double delete = %v, want ErrNotFound", err)
	}
}

// mockBackend is a controllable backend for testing error paths.
type mockBackend struct {
	putErr    error
	getErr    error
	getData   []byte
	existsErr error
	existsVal bool
	deleteErr error
	statsErr  error
	statsVal  *physical.Stats
	closeErr  error

	// PrefixScanner support
	scanPrefixRefs []reference.Reference
	scanPrefixErr  error
}

func (m *mockBackend) Put(_ context.Context, _ reference.Reference, _ []byte) error {
	return m.putErr
}
func (m *mockBackend) Get(_ context.Context, _ reference.Reference) ([]byte, error) {
	return m.getData, m.getErr
}
func (m *mockBackend) Exists(_ context.Context, _ reference.Reference) (bool, error) {
	return m.existsVal, m.existsErr
}
func (m *mockBackend) Delete(_ context.Context, _ reference.Reference) error {
	return m.deleteErr
}
func (m *mockBackend) Stats(_ context.Context) (*physical.Stats, error) {
	return m.statsVal, m.statsErr
}
func (m *mockBackend) Close() error {
	return m.closeErr
}
func (m *mockBackend) ScanPrefix(_ context.Context, _ string, _ int) ([]reference.Reference, error) {
	return m.scanPrefixRefs, m.scanPrefixErr
}

func TestStoreError(t *testing.T) {
	m := &mockBackend{putErr: errors.New("disk full")}
	store := New(m, observability.NewMetrics())
	_, err := store.Store(context.Background(), []byte("data"))
	if err == nil || !errors.Is(err, m.putErr) {
		t.Fatalf("Store error = %v, want wrapping %v", err, m.putErr)
	}
}

func TestFetchBackendError(t *testing.T) {
	m := &mockBackend{getErr: errors.New("io error")}
	store := New(m, observability.NewMetrics())
	_, err := store.Fetch(context.Background(), reference.Reference{})
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("should not be ErrNotFound")
	}
}

func TestExistsError(t *testing.T) {
	m := &mockBackend{existsErr: errors.New("db error")}
	store := New(m, observability.NewMetrics())
	_, err := store.Exists(context.Background(), reference.Reference{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteError(t *testing.T) {
	m := &mockBackend{deleteErr: errors.New("permission denied")}
	store := New(m, observability.NewMetrics())
	err := store.Delete(context.Background(), reference.Reference{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestStatsError(t *testing.T) {
	m := &mockBackend{statsErr: errors.New("stats broken")}
	store := New(m, observability.NewMetrics())
	_, err := store.Stats(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClose(t *testing.T) {
	m := &mockBackend{}
	store := New(m, observability.NewMetrics())
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCloseError(t *testing.T) {
	m := &mockBackend{closeErr: errors.New("close failed")}
	store := New(m, observability.NewMetrics())
	if err := store.Close(); err == nil {
		t.Fatal("expected error")
	}
}

func TestResolvePrefixTooShort(t *testing.T) {
	m := &mockBackend{}
	store := New(m, observability.NewMetrics())
	_, err := store.ResolvePrefix(context.Background(), "abc")
	if !errors.Is(err, ErrPrefixTooShort) {
		t.Fatalf("ResolvePrefix short = %v, want ErrPrefixTooShort", err)
	}
}

func TestResolvePrefixNoScanner(t *testing.T) {
	// Use a backend that does not implement PrefixScanner via the interface assertion.
	b := &struct{ physical.Backend }{Backend: &mockBackend{}}
	store := New(b, observability.NewMetrics())
	_, err := store.ResolvePrefix(context.Background(), "abcd")
	if err == nil {
		t.Fatal("expected error for non-scanner backend")
	}
}

func TestResolvePrefixNotFound(t *testing.T) {
	m := &mockBackend{scanPrefixRefs: nil}
	store := New(m, observability.NewMetrics())
	_, err := store.ResolvePrefix(context.Background(), "abcd")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolvePrefix no match = %v, want ErrNotFound", err)
	}
}

func TestResolvePrefixSingleMatch(t *testing.T) {
	ref := reference.Compute([]byte("hello"))
	m := &mockBackend{scanPrefixRefs: []reference.Reference{ref}}
	store := New(m, observability.NewMetrics())
	got, err := store.ResolvePrefix(context.Background(), "abcd")
	if err != nil {
		t.Fatalf("ResolvePrefix: %v", err)
	}
	if !reference.Equal(got, ref) {
		t.Fatalf("ResolvePrefix = %s, want %s", reference.Hex(got), reference.Hex(ref))
	}
}

func TestResolvePrefixAmbiguous(t *testing.T) {
	ref1 := reference.Compute([]byte("a"))
	ref2 := reference.Compute([]byte("b"))
	m := &mockBackend{scanPrefixRefs: []reference.Reference{ref1, ref2}}
	store := New(m, observability.NewMetrics())
	_, err := store.ResolvePrefix(context.Background(), "abcd")
	if !errors.Is(err, ErrAmbiguousPrefix) {
		t.Fatalf("ResolvePrefix ambiguous = %v, want ErrAmbiguousPrefix", err)
	}
}

func TestResolvePrefixScanError(t *testing.T) {
	m := &mockBackend{scanPrefixErr: errors.New("scan failed")}
	store := New(m, observability.NewMetrics())
	_, err := store.ResolvePrefix(context.Background(), "abcd")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestStatsAfterOperations(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Store several items.
	for i := 0; i < 5; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		if _, err := store.Store(ctx, data); err != nil {
			t.Fatalf("Store[%d]: %v", i, err)
		}
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats == nil {
		t.Fatal("Stats returned nil")
	}
	if stats.BackendType == "" {
		t.Error("BackendType is empty")
	}
}
