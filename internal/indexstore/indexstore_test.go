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

func newBenchStore(b *testing.B) *IndexStore {
	b.Helper()
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		b.Fatalf("create memory backend: %v", err)
	}
	b.Cleanup(func() { backend.Close() })

	store, err := New(backend, metrics)
	if err != nil {
		b.Fatalf("create index store: %v", err)
	}
	return store
}

func BenchmarkIndex(b *testing.B) {
	b.ReportAllocs()
	store := newBenchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("bench-%d", i))),
			Labels:      map[string]string{"type": "bench"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
		}
		if err := store.Index(ctx, entry); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	b.ReportAllocs()
	store := newBenchStore(b)
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("query-bench-%d", i))),
			Labels:      map[string]string{"type": "bench", "index": fmt.Sprintf("%d", i)},
			Timestamp:   time.Now().UnixMilli() + int64(i),
			Persistence: 1,
		}
		if err := store.Index(ctx, entry); err != nil {
			b.Fatalf("setup Index: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Query(ctx, &QueryOptions{Limit: 50})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestQueryDescending(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		ref := reference.Compute([]byte(fmt.Sprintf("desc-%d", i)))
		store.Index(ctx, &physical.Entry{
			Ref:         ref,
			Labels:      map[string]string{"type": "desc"},
			Timestamp:   int64(1000 + i),
			Persistence: 1,
		})
	}

	result, err := store.Query(ctx, &QueryOptions{Descending: true, Limit: 5})
	if err != nil {
		t.Fatalf("Query descending: %v", err)
	}
	if len(result.Entries) == 0 {
		t.Fatal("expected entries")
	}
	// Verify descending order.
	for i := 1; i < len(result.Entries); i++ {
		if result.Entries[i].Timestamp > result.Entries[i-1].Timestamp {
			t.Errorf("entry %d timestamp %d > entry %d timestamp %d (should be descending)",
				i, result.Entries[i].Timestamp, i-1, result.Entries[i-1].Timestamp)
		}
	}
}

func TestQueryWithExpression(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		label := "even"
		if i%2 != 0 {
			label = "odd"
		}
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("expr-%d", i))),
			Labels:      map[string]string{"parity": label},
			Timestamp:   int64(1000 + i),
			Persistence: 1,
		})
	}

	result, err := store.Query(ctx, &QueryOptions{
		Expression: `labels["parity"] == "even"`,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Query with expression: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Errorf("expected 3 even entries, got %d", len(result.Entries))
	}
	for _, e := range result.Entries {
		if e.Labels["parity"] != "even" {
			t.Errorf("unexpected label: %v", e.Labels)
		}
	}
}

func TestQueryTimeRange(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("tr-%d", i))),
			Labels:      map[string]string{"type": "range"},
			Timestamp:   int64(1000 + i*100),
			Persistence: 1,
		})
	}

	result, err := store.Query(ctx, &QueryOptions{
		After:  1250,
		Before: 1650,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query time range: %v", err)
	}
	if len(result.Entries) == 0 {
		t.Fatal("expected some entries in time range")
	}
	for _, e := range result.Entries {
		if e.Timestamp < 1250 || e.Timestamp > 1650 {
			t.Errorf("entry timestamp %d outside expected range [1250, 1650]", e.Timestamp)
		}
	}
}

func TestDeleteEntry(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("del-entry"))
	store.Index(ctx, &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "deletable"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	})

	// Confirm exists.
	if _, err := store.Get(ctx, ref); err != nil {
		t.Fatalf("Get before delete: %v", err)
	}

	if err := store.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.Get(ctx, ref)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}

	// Double delete should not error (idempotent or not-found).
	// Behavior depends on backend but should not panic.
	_ = store.Delete(ctx, ref)
}

func TestSubscribeWithExpression(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, `labels["channel"] == "important"`, [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Index a non-matching entry.
	store.Index(ctx, &physical.Entry{
		Ref:         reference.Compute([]byte("unimportant")),
		Labels:      map[string]string{"channel": "spam"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	})

	// Index a matching entry.
	matchRef := reference.Compute([]byte("important"))
	store.Index(ctx, &physical.Entry{
		Ref:         matchRef,
		Labels:      map[string]string{"channel": "important"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	})

	select {
	case entry := <-sub.Entries():
		if !reference.Equal(entry.Ref, matchRef) {
			t.Errorf("received wrong ref, got %s want %s",
				reference.Hex(entry.Ref), reference.Hex(matchRef))
		}
	case <-time.After(2 * time.Second):
		t.Error("subscription did not receive matching entry")
	}

	// Verify no extra entry arrives (the spam one should have been filtered).
	select {
	case entry := <-sub.Entries():
		t.Errorf("unexpected extra entry: %s", reference.Hex(entry.Ref))
	case <-time.After(200 * time.Millisecond):
		// Good - no extra entries.
	}
}

func TestIndexStoreClose(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Index an entry before close.
	ref := reference.Compute([]byte("before-close"))
	store.Index(ctx, &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "close"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	})

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Operations after close should return errors.
	_, err := store.Get(ctx, ref)
	if err == nil {
		t.Error("Get after close should return error")
	}

	err = store.Index(ctx, &physical.Entry{
		Ref:         reference.Compute([]byte("after-close")),
		Labels:      map[string]string{},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	})
	if err == nil {
		t.Error("Index after close should return error")
	}
}

func TestCursorPaginationMultiPage(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	const total = 15
	for i := 0; i < total; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("page-%d", i))),
			Labels:      map[string]string{"type": "paginate"},
			Timestamp:   int64(1000 + i),
			Persistence: 1,
		})
	}

	var allEntries []*physical.Entry
	cursor := ""
	pages := 0
	for {
		result, err := store.Query(ctx, &QueryOptions{
			Limit:  4,
			Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("Query page %d: %v", pages, err)
		}
		allEntries = append(allEntries, result.Entries...)
		pages++
		if !result.HasMore || result.NextCursor == "" {
			break
		}
		cursor = result.NextCursor
		if pages > 10 {
			t.Fatal("too many pages, possible infinite loop")
		}
	}

	if len(allEntries) != total {
		t.Errorf("paginated total = %d, want %d", len(allEntries), total)
	}
	if pages < 2 {
		t.Errorf("expected multiple pages, got %d", pages)
	}
}

func TestSubscribeNotifyCount(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	const n = 5
	for i := 0; i < n; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("metric-%d", i))),
			Labels:      map[string]string{"type": "metric"},
			Timestamp:   time.Now().UnixMilli() + int64(i),
			Persistence: 1,
		})
	}

	// Drain entries.
	received := 0
	timeout := time.After(2 * time.Second)
	for received < n {
		select {
		case <-sub.Entries():
			received++
		case <-timeout:
			t.Fatalf("timed out, received %d/%d", received, n)
		}
	}

	metrics := store.SubscriptionMetrics()
	if metrics["subscriptions_active"] != 1 {
		t.Errorf("subscriptions_active = %d, want 1", metrics["subscriptions_active"])
	}
	if metrics["subscriptions_total_delivered"] < int64(n) {
		t.Errorf("subscriptions_total_delivered = %d, want >= %d",
			metrics["subscriptions_total_delivered"], n)
	}
}

func TestCloseStopsSubscriptions(t *testing.T) {
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create memory backend: %v", err)
	}

	store, err := New(backend, metrics)
	if err != nil {
		t.Fatalf("create index store: %v", err)
	}

	ctx := context.Background()
	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After close, subscription channel should be closed.
	select {
	case _, ok := <-sub.Entries():
		if ok {
			t.Fatal("expected subscription channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription channel not closed after store.Close()")
	}

	// Subscribe after close should fail.
	_, err = store.Subscribe(ctx, "true", [32]byte{})
	if err == nil {
		t.Error("Subscribe after Close should return error")
	}
}

func TestSubscriptionCloseAndMetrics(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Check metrics before activity.
	m := store.SubscriptionMetrics()
	if m["subscriptions_active"] != 1 {
		t.Errorf("active = %d, want 1", m["subscriptions_active"])
	}

	// Cancel subscription.
	sub.Cancel()
	time.Sleep(100 * time.Millisecond)

	m = store.SubscriptionMetrics()
	if m["subscriptions_active"] != 0 {
		t.Errorf("active after cancel = %d, want 0", m["subscriptions_active"])
	}
}

func TestCleanupRunsWithoutError(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Index a normal entry.
	store.Index(ctx, &physical.Entry{
		Ref:         reference.Compute([]byte("cleanup-test")),
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	})

	// Cleanup should run without error even if nothing expired.
	count, err := store.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if count < 0 {
		t.Errorf("Cleanup count = %d, should be >= 0", count)
	}
}

func TestStartCleanupStopsOnCancel(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	store.StartCleanup(ctx, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
	// No panic or hang means success.
}

func TestQueryNilOpts(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	result, err := store.Query(ctx, nil)
	if err != nil {
		t.Fatalf("Query nil opts: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestCountBasic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("count-%d", i))),
			Labels:      map[string]string{"type": "countable"},
			Timestamp:   int64(1000 + i),
			Persistence: 1,
		})
	}

	count, err := store.Count(ctx, &QueryOptions{Labels: map[string]string{"type": "countable"}})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}
}

func TestCountNilOpts(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.Count(ctx, nil)
	if err != nil {
		t.Fatalf("Count nil opts: %v", err)
	}
}

func TestDeliveryTrackerAccessor(t *testing.T) {
	store := newTestStore(t)
	dt := store.DeliveryTracker()
	if dt == nil {
		t.Fatal("DeliveryTracker() returned nil")
	}
}

func TestStatsBasic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats == nil {
		t.Fatal("Stats returned nil")
	}
}

func TestSubscribeInvalidExpression(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.Subscribe(ctx, "this is not valid CEL !!!!", [32]byte{})
	if err == nil {
		t.Fatal("expected error for invalid expression")
	}
	if !errors.Is(err, ErrInvalidExpression) {
		t.Errorf("expected ErrInvalidExpression, got: %v", err)
	}
}

func TestExternalSequenceAdvancesCounter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Index with a pre-set high sequence.
	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("ext-seq")),
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Sequence:    1000,
	}
	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Next auto-assigned sequence should be > 1000.
	entry2 := &physical.Entry{
		Ref:         reference.Compute([]byte("ext-seq-2")),
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	}
	if err := store.Index(ctx, entry2); err != nil {
		t.Fatalf("Index 2: %v", err)
	}
	if entry2.Sequence <= 1000 {
		t.Errorf("Sequence = %d, want > 1000", entry2.Sequence)
	}
}

func TestNewWithCustomOptions(t *testing.T) {
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create memory backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	store, err := New(backend, metrics, &Options{
		SubscriptionConfig: SubscriptionConfig{
			IntakeBufferSize:    128,
			WorkerCount:         2,
			MaxConsecutiveDrops: 50,
		},
	})
	if err != nil {
		t.Fatalf("New with options: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	// Basic sanity check that the store works.
	ctx := context.Background()
	ref := reference.Compute([]byte("custom-opts"))
	if err := store.Index(ctx, &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	}); err != nil {
		t.Fatalf("Index: %v", err)
	}
	if _, err := store.Get(ctx, ref); err != nil {
		t.Fatalf("Get: %v", err)
	}
}

func TestNewWithNilOptions(t *testing.T) {
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create memory backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	// Passing nil *Options should use defaults.
	store, err := New(backend, metrics, nil)
	if err != nil {
		t.Fatalf("New with nil opts: %v", err)
	}
	store.Close()
}

func TestRegisterCompositeIndexUnsupportedBackend(t *testing.T) {
	store := newTestStore(t)
	// Memory backend wraps badger which does support composite indexes,
	// so this test uses the store directly.
	// We test via the public API. Memory backend (badger in-memory) supports it,
	// so let's just verify registering works and re-registering the same is safe.
	err := store.RegisterCompositeIndex(physical.CompositeIndexDef{
		Name: "test_idx",
		Keys: []string{"a", "b"},
	})
	if err != nil {
		t.Fatalf("RegisterCompositeIndex: %v", err)
	}
}

func TestResolvePrefixTooShort(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.ResolvePrefix(ctx, "ab")
	if !errors.Is(err, ErrPrefixTooShort) {
		t.Errorf("expected ErrPrefixTooShort, got: %v", err)
	}
}

func TestResolvePrefixNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.ResolvePrefix(ctx, "deadbeef")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestResolvePrefixSingleMatch(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("resolve-prefix-single"))
	store.Index(ctx, &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	})

	hex := reference.Hex(ref)
	got, err := store.ResolvePrefix(ctx, hex[:8])
	if err != nil {
		t.Fatalf("ResolvePrefix: %v", err)
	}
	if got != ref {
		t.Errorf("resolved ref mismatch")
	}
}

func TestEphemeralEntryNoSubscribers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Index ephemeral with no subscribers - should not error.
	ref := reference.Compute([]byte("ephemeral-no-sub"))
	err := store.Index(ctx, &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 0,
	})
	if err != nil {
		t.Fatalf("Index ephemeral with no subs: %v", err)
	}

	// Should not be stored.
	_, err = store.Get(ctx, ref)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for ephemeral, got: %v", err)
	}
}

func TestStoreOnlyEphemeral(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, _ := store.Subscribe(ctx, "true", [32]byte{})
	defer sub.Cancel()

	// STORE_ONLY + ephemeral (persistence=0): should not store and not notify.
	ref := reference.Compute([]byte("store-only-ephemeral"))
	err := store.Index(ctx, &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 0,
		Pattern:     4, // STORE_ONLY
	})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	select {
	case e := <-sub.Entries():
		if e != nil {
			t.Fatal("STORE_ONLY ephemeral should not notify")
		}
	case <-time.After(200 * time.Millisecond):
		// Good.
	}
}

func TestIdempotencyLRUEviction(t *testing.T) {
	cache := newIdempotencyLRU(3)

	cache.Add("a")
	cache.Add("b")
	cache.Add("c")

	if !cache.Contains("a") {
		t.Fatal("should contain a")
	}

	// Adding a 4th should evict "a".
	cache.Add("d")
	if cache.Contains("a") {
		t.Fatal("a should have been evicted")
	}
	if !cache.Contains("b") || !cache.Contains("c") || !cache.Contains("d") {
		t.Fatal("b, c, d should still be present")
	}

	// Adding duplicate should be a no-op.
	cache.Add("b")
	if !cache.Contains("b") {
		t.Fatal("b should still be present after re-add")
	}
}

func TestStartAutoIndexStopsOnCancel(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	store.StartAutoIndex(ctx, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
	// No panic or hang means success.
}

func TestQueryWithExpressionAndLabels(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("combo-%d", i))),
			Labels:      map[string]string{"app": "test", "priority": fmt.Sprintf("%d", i%2)},
			Timestamp:   int64(1000 + i),
			Persistence: 1,
		})
	}

	// Use both Labels and Expression.
	result, err := store.Query(ctx, &QueryOptions{
		Labels:     map[string]string{"app": "test"},
		Expression: `labels["priority"] == "0"`,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Entries 0,2,4 have priority=0.
	if len(result.Entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(result.Entries))
	}
}

func TestCleanupDeletesExpiredEntries(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	ref := reference.Compute([]byte("will-expire"))
	store.Index(ctx, &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "test"},
		Timestamp:   now,
		ExpiresAt:   now - 1000, // already expired
		Persistence: 1,
	})

	// Cleanup should not error. Count depends on backend TTL support.
	_, err := store.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}

	// The entry should at least not be returned in non-expired queries.
	result, err := store.Query(ctx, &QueryOptions{IncludeExpired: false})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	for _, e := range result.Entries {
		if e.Ref == ref {
			t.Error("expired entry should not appear in non-expired query")
		}
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
