package indexstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/reference"

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
		Ref:         ref,
		Labels:      map[string]string{"type": "text"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
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
		Ref:         ref,
		Labels:      map[string]string{"key": "val"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
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
			Ref:         ref,
			Labels:      map[string]string{"index": "true"},
			Timestamp:   time.Now().UnixMilli() + int64(i),
			Persistence: 1,
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

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	if store.SubscriptionCount() != 1 {
		t.Errorf("SubscriptionCount = %d, want 1", store.SubscriptionCount())
	}

	ref := reference.Compute([]byte("notify-me"))
	store.Index(ctx, &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
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

func TestQueryFiltersExpiredEntriesMillis(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("expired")),
		Labels:      map[string]string{"type": "a"},
		Timestamp:   now,
		ExpiresAt:   now - int64(time.Second/time.Millisecond),
		Persistence: 1,
	}

	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	res, err := store.Query(ctx, &QueryOptions{IncludeExpired: false})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(res.Entries))
	}
}

func TestEphemeralEntryNotStored(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	ref := reference.Compute([]byte("ephemeral"))
	entry := &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "ephemeral"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 0, // ephemeral
	}

	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Subscriber should receive it.
	select {
	case got := <-sub.Entries():
		if !reference.Equal(got.Ref, ref) {
			t.Errorf("subscriber got wrong ref")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive ephemeral entry")
	}

	// Backend should not have it.
	_, err = store.Get(ctx, ref)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get ephemeral entry = %v, want ErrNotFound", err)
	}
}

func TestUntilDeliveredEntryDeletedAfterAck(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	ref := reference.Compute([]byte("until-delivered"))
	entry := &physical.Entry{
		Ref:          ref,
		Labels:       map[string]string{"type": "ud"},
		Timestamp:    time.Now().UnixMilli(),
		Persistence:  1, // durable
		DeliveryMode: 1, // until-delivered
	}

	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Wait for subscriber to receive it.
	select {
	case <-sub.Entries():
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive entry")
	}

	// Entry should exist in backend.
	if _, err := store.Get(ctx, ref); err != nil {
		t.Fatalf("Get before ack: %v", err)
	}

	// Deliver and ack via delivery ID.
	deliveryID := store.Deliver(entry, sub.ID())
	if deliveryID <= 0 {
		t.Fatalf("Deliver returned non-positive ID: %d", deliveryID)
	}
	if _, err := store.AckDelivery(ctx, deliveryID); err != nil {
		t.Fatalf("AckDelivery: %v", err)
	}

	// Entry should be deleted after ack (1 delivery = 1 target).
	_, err = store.Get(ctx, ref)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after ack = %v, want ErrNotFound", err)
	}
}

func TestCursorPutGetRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	key := "test-sub/cursor1"
	cursor := physical.Cursor{Timestamp: 12345, Sequence: 42}

	if err := store.PutCursor(ctx, key, cursor); err != nil {
		t.Fatalf("PutCursor: %v", err)
	}

	got, err := store.GetCursor(ctx, key)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if got.Timestamp != cursor.Timestamp || got.Sequence != cursor.Sequence {
		t.Errorf("GetCursor = %+v, want %+v", got, cursor)
	}
}

func TestCursorNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.GetCursor(ctx, "nonexistent")
	if !errors.Is(err, physical.ErrCursorNotFound) {
		t.Errorf("GetCursor = %v, want ErrCursorNotFound", err)
	}
}

func TestSequenceAssignment(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entry1 := &physical.Entry{
		Ref:         reference.Compute([]byte("seq-1")),
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	}
	entry2 := &physical.Entry{
		Ref:         reference.Compute([]byte("seq-2")),
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	}

	if err := store.Index(ctx, entry1); err != nil {
		t.Fatalf("Index 1: %v", err)
	}
	if err := store.Index(ctx, entry2); err != nil {
		t.Fatalf("Index 2: %v", err)
	}

	if entry1.Sequence <= 0 {
		t.Errorf("entry1.Sequence = %d, want > 0", entry1.Sequence)
	}
	if entry2.Sequence <= entry1.Sequence {
		t.Errorf("entry2.Sequence = %d, should be > entry1.Sequence = %d", entry2.Sequence, entry1.Sequence)
	}
}

func TestDedupByRef(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("dedup-ref"))
	entry := &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "dedup"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		DedupMode:   1, // DEDUP_REF
	}

	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index first: %v", err)
	}

	// Second index with same ref should be silently deduped.
	entry2 := &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "dedup", "extra": "should-not-appear"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		DedupMode:   1,
	}
	if err := store.Index(ctx, entry2); err != nil {
		t.Fatalf("Index second: %v", err)
	}

	got, err := store.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["extra"] != "" {
		t.Error("dedup should have prevented second index")
	}
}

func TestDedupByIdempotencyKey(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	entry1 := &physical.Entry{
		Ref:            reference.Compute([]byte("idem-1")),
		Labels:         map[string]string{"type": "idem"},
		Timestamp:      time.Now().UnixMilli(),
		Persistence:    1,
		DedupMode:      2, // DEDUP_IDEMPOTENCY_KEY
		IdempotencyKey: "key-abc",
	}
	if err := store.Index(ctx, entry1); err != nil {
		t.Fatalf("Index first: %v", err)
	}

	// Second entry with same idempotency key but different ref should be deduped.
	entry2 := &physical.Entry{
		Ref:            reference.Compute([]byte("idem-2")),
		Labels:         map[string]string{"type": "idem"},
		Timestamp:      time.Now().UnixMilli(),
		Persistence:    1,
		DedupMode:      2,
		IdempotencyKey: "key-abc",
	}
	if err := store.Index(ctx, entry2); err != nil {
		t.Fatalf("Index second: %v", err)
	}

	// Only the first entry should exist.
	_, err := store.Get(ctx, entry1.Ref)
	if err != nil {
		t.Fatalf("first entry should exist: %v", err)
	}
	_, err = store.Get(ctx, entry2.Ref)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("second entry should not exist, got: %v", err)
	}
}

func TestDedupNoneAllowsDuplicates(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("no-dedup"))
	entry := &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "nodup"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		DedupMode:   0, // DEDUP_NONE
	}
	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index first: %v", err)
	}

	// Second index with same ref should succeed (overwrite).
	entry2 := &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "nodup", "extra": "yes"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		DedupMode:   0,
	}
	if err := store.Index(ctx, entry2); err != nil {
		t.Fatalf("Index second: %v", err)
	}

	got, err := store.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["extra"] != "yes" {
		t.Error("DEDUP_NONE should allow overwrite")
	}
}

func TestQueryResidualPaginationHasMore(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("entry-%d", i))),
			Labels:      map[string]string{"type": "a", "user": "keep"},
			Timestamp:   int64(1000 + i),
			Persistence: 1,
		}
		if err := store.Index(ctx, entry); err != nil {
			t.Fatalf("Index entry %d: %v", i, err)
		}
	}

	res, err := store.Query(ctx, &QueryOptions{
		Expression: `labels["type"] == "a" && labels["user"] != "skip"`,
		Limit:      2,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(res.Entries))
	}
	if !res.HasMore {
		t.Fatalf("expected HasMore=true for residual pagination")
	}
	if res.NextCursor == "" {
		t.Fatalf("expected NextCursor to be set for residual pagination")
	}
}
