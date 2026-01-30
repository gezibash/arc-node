package indexstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	"github.com/gezibash/arc-node/internal/observability"
)

func newTestStore(t *testing.T) *IndexStore {
	t.Helper()
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create memory backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	store, err := New(backend, metrics)
	if err != nil {
		t.Fatalf("create index store: %v", err)
	}
	return store
}

func testRef(b byte) reference.Reference {
	var r reference.Reference
	r[0] = b
	return r
}

func TestIndexAndGet(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("hello"))
	entry := &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"type": "text"},
		Timestamp: time.Now().UnixNano(),
	}

	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	got, err := store.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reference.Equal(got.Ref, ref) {
		t.Errorf("Get ref = %s, want %s", reference.Hex(got.Ref), reference.Hex(ref))
	}
	if got.Labels["type"] != "text" {
		t.Errorf("Get labels = %v", got.Labels)
	}
}

func TestGetNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.Get(ctx, testRef(0xFF))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
}

func TestDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("delete-me"))
	entry := &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"key": "val"},
		Timestamp: time.Now().UnixNano(),
	}
	store.Index(ctx, entry)

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.Get(ctx, ref)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
}

func TestQuery(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		ref := reference.Compute([]byte{byte(i)})
		store.Index(ctx, &physical.Entry{
			Ref:       ref,
			Labels:    map[string]string{"index": "true"},
			Timestamp: time.Now().UnixNano() + int64(i),
		})
	}

	result, err := store.Query(ctx, &QueryOptions{Limit: 3})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Errorf("Query result count = %d, want 3", len(result.Entries))
	}
	if !result.HasMore {
		t.Error("Query HasMore = false, want true")
	}
}

func TestSubscribe(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	if store.SubscriptionCount() != 1 {
		t.Errorf("SubscriptionCount = %d, want 1", store.SubscriptionCount())
	}

	ref := reference.Compute([]byte("notify-me"))
	store.Index(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"type": "test"},
		Timestamp: time.Now().UnixNano(),
	})

	select {
	case entry := <-sub.Entries():
		if !reference.Equal(entry.Ref, ref) {
			t.Errorf("subscription received wrong ref")
		}
	case <-time.After(time.Second):
		t.Error("subscription did not receive entry within timeout")
	}
}
