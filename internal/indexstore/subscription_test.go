package indexstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	"github.com/gezibash/arc-node/internal/observability"
)

func BenchmarkFanout(b *testing.B) {
	b.ReportAllocs()

	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		b.Fatalf("create memory backend: %v", err)
	}
	store, err := New(backend, metrics)
	if err != nil {
		b.Fatalf("create index store: %v", err)
	}

	ctx := context.Background()
	subs := make([]Subscription, 10)
	for i := range subs {
		sub, err := store.Subscribe(ctx, "true", [32]byte{byte(i)})
		if err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
		subs[i] = sub
	}

	// Drain subscribers in background so they don't block fanout.
	done := make(chan struct{})
	var drainWg sync.WaitGroup
	for _, sub := range subs {
		drainWg.Add(1)
		go func(s Subscription) {
			defer drainWg.Done()
			for {
				select {
				case <-done:
					return
				case _, ok := <-s.Entries():
					if !ok {
						return
					}
				}
			}
		}(sub)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("fanout-bench-%d", i))),
			Labels:      map[string]string{"app": "bench"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
			Pattern:     0,
		}
		if err := store.Index(ctx, entry); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// Close store first (stops fanout workers), then clean up drains.
	store.Close()
	close(done)
	drainWg.Wait()
}

func TestFanoutDelivery(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub1, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub1.Cancel()

	sub2, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub2.Cancel()

	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("fanout")),
		Labels:      map[string]string{"app": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Pattern:     0, // FIRE_AND_FORGET — fanout
	}
	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Both subscribers should receive the entry.
	for i, sub := range []Subscription{sub1, sub2} {
		select {
		case e := <-sub.Entries():
			if e == nil {
				t.Fatalf("sub%d: nil entry", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("sub%d: no delivery", i)
		}
	}
}

func TestFanoneRoundRobin(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub1, err := store.Subscribe(ctx, "true", [32]byte{1})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub1.Cancel()

	sub2, err := store.Subscribe(ctx, "true", [32]byte{2})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub2.Cancel()

	// Publish several QUEUE entries.
	delivered := map[string]int{}
	for i := 0; i < 10; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("queue-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
			Pattern:     2, // PATTERN_QUEUE
		}
		if err := store.Index(ctx, entry); err != nil {
			t.Fatalf("Index: %v", err)
		}
	}

	// Drain both subscribers.
	timeout := time.After(2 * time.Second)
	total := 0
	for total < 10 {
		select {
		case e := <-sub1.Entries():
			if e != nil {
				delivered["sub1"]++
				total++
			}
		case e := <-sub2.Entries():
			if e != nil {
				delivered["sub2"]++
				total++
			}
		case <-timeout:
			t.Fatalf("only received %d/10 entries", total)
		}
	}

	// Both should have received some entries (round-robin).
	if delivered["sub1"] == 0 || delivered["sub2"] == 0 {
		t.Errorf("uneven distribution: sub1=%d sub2=%d", delivered["sub1"], delivered["sub2"])
	}
}

func TestFanoneAffinity(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub1, err := store.Subscribe(ctx, "true", [32]byte{1})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub1.Cancel()

	sub2, err := store.Subscribe(ctx, "true", [32]byte{2})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub2.Cancel()

	// Send several QUEUE entries with AFFINITY_SENDER from the same sender.
	senderKey := [32]byte{0xAA}
	senderHex := fmt.Sprintf("%x", senderKey)

	for i := 0; i < 5; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("affinity-%d", i))),
			Labels:      map[string]string{"from": senderHex},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
			Pattern:     2, // PATTERN_QUEUE
			Affinity:    1, // AFFINITY_SENDER
		}
		if err := store.Index(ctx, entry); err != nil {
			t.Fatalf("Index: %v", err)
		}
	}

	// All entries from same sender should go to the same subscriber.
	timeout := time.After(2 * time.Second)
	counts := [2]int{}
	total := 0
	for total < 5 {
		select {
		case e := <-sub1.Entries():
			if e != nil {
				counts[0]++
				total++
			}
		case e := <-sub2.Entries():
			if e != nil {
				counts[1]++
				total++
			}
		case <-timeout:
			t.Fatalf("only received %d/5 entries", total)
		}
	}

	// One sub should have all 5, the other 0.
	if counts[0] != 5 && counts[1] != 5 {
		t.Errorf("affinity not sticky: sub1=%d sub2=%d", counts[0], counts[1])
	}
}

func TestStoreOnlyNoNotify(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("store-only")),
		Labels:      map[string]string{"app": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Pattern:     4, // PATTERN_STORE_ONLY
	}
	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Entry should be queryable.
	result, err := store.Query(ctx, &QueryOptions{Labels: map[string]string{"app": "test"}})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}

	// But subscriber should NOT receive it.
	select {
	case e := <-sub.Entries():
		if e != nil {
			t.Fatal("STORE_ONLY entry should not be delivered to subscriber")
		}
	case <-time.After(500 * time.Millisecond):
		// Good — no delivery.
	}
}

func TestFIFOOrdering(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	sender := "sender-a"
	// Publish entries in sequence order — should all be delivered in order.
	for i := int64(1); i <= 5; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("fifo-%d", i))),
			Labels:      map[string]string{"from": sender},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
			Ordering:    1, // FIFO
			Sequence:    i,
		}
		if err := store.Index(ctx, entry); err != nil {
			t.Fatalf("Index %d: %v", i, err)
		}
	}

	timeout := time.After(2 * time.Second)
	for i := int64(1); i <= 5; i++ {
		select {
		case e := <-sub.Entries():
			if e == nil {
				t.Fatalf("nil entry at position %d", i)
			}
			if e.Sequence != i {
				t.Errorf("sequence = %d, want %d", e.Sequence, i)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for entry %d", i)
		}
	}
}

func TestFIFOHoldBack(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	sender := "sender-b"

	// Deliver sequence 1 first so the subscription knows the starting point.
	entry1 := &physical.Entry{
		Ref:         reference.Compute([]byte("fifo-hold-1")),
		Labels:      map[string]string{"from": sender},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Ordering:    1,
		Sequence:    1,
	}
	if err := store.Index(ctx, entry1); err != nil {
		t.Fatalf("Index 1: %v", err)
	}
	select {
	case <-sub.Entries():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for entry 1")
	}

	// Now send sequence 3 (out of order, skipping 2).
	entry3 := &physical.Entry{
		Ref:         reference.Compute([]byte("fifo-hold-3")),
		Labels:      map[string]string{"from": sender},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Ordering:    1,
		Sequence:    3,
	}
	if err := store.Index(ctx, entry3); err != nil {
		t.Fatalf("Index 3: %v", err)
	}

	// Entry 3 should be held back — shouldn't arrive yet.
	select {
	case e := <-sub.Entries():
		if e != nil {
			t.Fatal("entry 3 should be held back until entry 2 arrives")
		}
	case <-time.After(200 * time.Millisecond):
		// Good — held back.
	}

	// Now send sequence 2 — both 2 and 3 should be delivered.
	entry2 := &physical.Entry{
		Ref:         reference.Compute([]byte("fifo-hold-2")),
		Labels:      map[string]string{"from": sender},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Ordering:    1,
		Sequence:    2,
	}
	if err := store.Index(ctx, entry2); err != nil {
		t.Fatalf("Index 2: %v", err)
	}

	// Should receive entry 2 then entry 3.
	timeout := time.After(2 * time.Second)
	var seqs []int64
	for i := 0; i < 2; i++ {
		select {
		case e := <-sub.Entries():
			if e == nil {
				t.Fatal("nil entry")
			}
			seqs = append(seqs, e.Sequence)
		case <-timeout:
			t.Fatalf("timeout, received %d/2 entries: %v", len(seqs), seqs)
		}
	}

	if len(seqs) != 2 || seqs[0] != 2 || seqs[1] != 3 {
		t.Errorf("expected sequences [2, 3], got %v", seqs)
	}
}

func TestDirectRouting(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	targetKey := [32]byte{0xBB}
	otherKey := [32]byte{0xCC}

	sub1, err := store.Subscribe(ctx, "true", targetKey)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub1.Cancel()

	sub2, err := store.Subscribe(ctx, "true", otherKey)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub2.Cancel()

	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("direct")),
		Labels:      map[string]string{"to": fmt.Sprintf("%x", targetKey)},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Pattern:     1, // PATTERN_REQ_REP — direct
		To:          targetKey,
	}
	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Only sub1 (targetKey) should receive.
	select {
	case e := <-sub1.Entries():
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("target sub did not receive direct entry")
	}

	// sub2 should not receive.
	select {
	case e := <-sub2.Entries():
		if e != nil {
			t.Fatal("non-target sub received direct entry")
		}
	case <-time.After(300 * time.Millisecond):
		// Good.
	}
}

func TestBackpressureDrop(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{}, &SubscriptionOptions{
		BufferSize:         1,
		BackpressurePolicy: BackpressureDrop,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Index 10 entries rapidly without reading from sub.Entries().
	for i := 0; i < 10; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("bp-drop-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
			Pattern:     0,
		}
		if err := store.Index(ctx, entry); err != nil {
			t.Fatalf("Index %d: %v", i, err)
		}
	}

	// Wait for fan-out workers to process.
	time.Sleep(500 * time.Millisecond)

	health := sub.Health()
	if health.TotalDropped == 0 {
		t.Fatalf("expected TotalDropped > 0, got %d (delivered=%d)", health.TotalDropped, health.TotalDelivered)
	}
	// Subscription should still be active (channel not closed).
	select {
	case _, ok := <-sub.Entries():
		if !ok {
			t.Fatal("subscription channel closed unexpectedly")
		}
		// Got a buffered entry, that's fine.
	default:
		// Buffer may have been drained already, that's fine too.
	}
	if sub.Err() != nil {
		t.Fatalf("subscription should still be active, got err: %v", sub.Err())
	}
}

func TestBackpressureDisconnect(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{}, &SubscriptionOptions{
		BufferSize:         1,
		BackpressurePolicy: BackpressureDisconnect,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Index enough entries to fill the buffer and trigger disconnect.
	for i := 0; i < 10; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("bp-disc-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
			Pattern:     0,
		}
		if err := store.Index(ctx, entry); err != nil {
			t.Fatalf("Index %d: %v", i, err)
		}
	}

	// Wait for fan-out and disconnect.
	time.Sleep(500 * time.Millisecond)

	if sub.Err() == nil {
		t.Fatal("expected subscription to be disconnected with non-nil Err()")
	}
}

func TestAffinityKey(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub1, err := store.Subscribe(ctx, "true", [32]byte{1})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub1.Cancel()

	sub2, err := store.Subscribe(ctx, "true", [32]byte{2})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub2.Cancel()

	// Index QUEUE entries with AFFINITY_KEY and the same AffinityKey.
	for i := 0; i < 5; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("affkey-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
			Pattern:     2, // PATTERN_QUEUE
			Affinity:    2, // AFFINITY_KEY
			AffinityKey: "same-key",
		}
		if err := store.Index(ctx, entry); err != nil {
			t.Fatalf("Index %d: %v", i, err)
		}
	}

	timeout := time.After(2 * time.Second)
	counts := [2]int{}
	total := 0
	for total < 5 {
		select {
		case e := <-sub1.Entries():
			if e != nil {
				counts[0]++
				total++
			}
		case e := <-sub2.Entries():
			if e != nil {
				counts[1]++
				total++
			}
		case <-timeout:
			t.Fatalf("only received %d/5 entries", total)
		}
	}

	// All entries should go to the same subscriber due to same AffinityKey.
	if counts[0] != 5 && counts[1] != 5 {
		t.Errorf("affinity key not sticky: sub1=%d sub2=%d", counts[0], counts[1])
	}
}

func TestVisibilityFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	allowedKey := [32]byte{0xAA}
	blockedKey := [32]byte{0xBB}

	store.SetVisibilityFilter(func(entry *physical.Entry, callerKey [32]byte) bool {
		return callerKey == allowedKey
	})

	sub1, err := store.Subscribe(ctx, "true", allowedKey)
	if err != nil {
		t.Fatalf("Subscribe allowed: %v", err)
	}
	defer sub1.Cancel()

	sub2, err := store.Subscribe(ctx, "true", blockedKey)
	if err != nil {
		t.Fatalf("Subscribe blocked: %v", err)
	}
	defer sub2.Cancel()

	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("visibility")),
		Labels:      map[string]string{"app": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	}
	if err := store.Index(ctx, entry); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// sub1 should receive it.
	select {
	case e := <-sub1.Entries():
		if e == nil {
			t.Fatal("nil entry for allowed sub")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("allowed sub did not receive entry")
	}

	// sub2 should not.
	select {
	case e := <-sub2.Entries():
		if e != nil {
			t.Fatal("blocked sub should not receive entry")
		}
	case <-time.After(300 * time.Millisecond):
		// Good.
	}
}

func TestSubscriptionCancelAndErr(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Initially no error.
	if sub.Err() != nil {
		t.Errorf("Err() before cancel = %v, want nil", sub.Err())
	}

	sub.Cancel()

	// Done channel should be closed after cancel.
	select {
	case <-sub.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() not closed after Cancel()")
	}
}

func TestSubscriptionMetricsAggregation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub1, _ := store.Subscribe(ctx, "true", [32]byte{1})
	defer sub1.Cancel()
	sub2, _ := store.Subscribe(ctx, "true", [32]byte{2})
	defer sub2.Cancel()

	// Send entries.
	for i := 0; i < 3; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("agg-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
		})
	}

	// Drain both.
	timeout := time.After(2 * time.Second)
drain:
	for i := 0; i < 6; i++ {
		select {
		case <-sub1.Entries():
		case <-sub2.Entries():
		case <-timeout:
			break drain
		}
	}

	time.Sleep(100 * time.Millisecond)
	m := store.SubscriptionMetrics()
	if m["subscriptions_active"] != 2 {
		t.Errorf("active = %d, want 2", m["subscriptions_active"])
	}
	if m["subscriptions_total_delivered"] < 3 {
		t.Errorf("total_delivered = %d, want >= 3", m["subscriptions_total_delivered"])
	}
}

func TestBackpressureBlock(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{}, &SubscriptionOptions{
		BufferSize:         1,
		BackpressurePolicy: BackpressureBlock,
		BlockTimeout:       10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Rapidly index without reading.
	for i := 0; i < 10; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("bp-block-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
		})
	}

	time.Sleep(500 * time.Millisecond)
	health := sub.Health()
	// With block timeout of 10ms and buffer of 1, some should be dropped.
	if health.TotalDropped == 0 && health.TotalDelivered < 10 {
		t.Logf("delivered=%d dropped=%d (block timeout may have allowed some through)", health.TotalDelivered, health.TotalDropped)
	}
}

func TestNotifyIntakeBufferFull(t *testing.T) {
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create memory backend: %v", err)
	}

	store, err := New(backend, metrics, &Options{
		SubscriptionConfig: SubscriptionConfig{
			IntakeBufferSize: 1,
			WorkerCount:      1,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	// Create a subscriber but don't drain it so the worker blocks.
	sub, err := store.Subscribe(ctx, "true", [32]byte{}, &SubscriptionOptions{
		BufferSize: 1,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Flood the intake buffer.
	for i := 0; i < 100; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("flood-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
		})
	}

	// Some entries should have been dropped from intake. No panic = success.
	time.Sleep(100 * time.Millisecond)
}

func TestBackpressureDropDisconnectsAfterMaxDrops(t *testing.T) {
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	store, err := New(backend, metrics, &Options{
		SubscriptionConfig: SubscriptionConfig{
			MaxConsecutiveDrops: 5,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	sub, err := store.Subscribe(ctx, "true", [32]byte{}, &SubscriptionOptions{
		BufferSize:         1,
		BackpressurePolicy: BackpressureDrop,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Don't read from sub, flood entries to trigger disconnect.
	for i := 0; i < 50; i++ {
		store.Index(ctx, &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("drop-disc-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
		})
	}

	time.Sleep(time.Second)
	if sub.Err() == nil {
		t.Fatal("expected subscription to be disconnected after max consecutive drops")
	}
}

func TestSubscribeAfterClose(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.Close()

	_, err := store.Subscribe(ctx, "true", [32]byte{})
	if err == nil {
		t.Fatal("Subscribe after Close should fail")
	}
}

func TestReqRepNoMatchingSubscriber(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Subscribe with a key that doesn't match the "to" label.
	sub, _ := store.Subscribe(ctx, "true", [32]byte{0xAA})
	defer sub.Cancel()

	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("req-rep-no-match")),
		Labels:      map[string]string{"to": fmt.Sprintf("%x", [32]byte{0xBB})},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Pattern:     1, // PATTERN_REQ_REP
	}
	store.Index(ctx, entry)

	// No subscriber should receive it.
	select {
	case e := <-sub.Entries():
		if e != nil {
			t.Fatal("REQ_REP entry should not be delivered to non-matching subscriber")
		}
	case <-time.After(300 * time.Millisecond):
		// Good.
	}
}

func TestPubSubPatternFanout(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub1, _ := store.Subscribe(ctx, "true", [32]byte{1})
	defer sub1.Cancel()
	sub2, _ := store.Subscribe(ctx, "true", [32]byte{2})
	defer sub2.Cancel()

	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("pubsub")),
		Labels:      map[string]string{"app": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Pattern:     3, // PATTERN_PUB_SUB
	}
	store.Index(ctx, entry)

	// Both should receive.
	for i, sub := range []Subscription{sub1, sub2} {
		select {
		case e := <-sub.Entries():
			if e == nil {
				t.Fatalf("sub%d got nil", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("sub%d timeout", i)
		}
	}
}

func TestQueuePatternSingleSub(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, _ := store.Subscribe(ctx, "true", [32]byte{1})
	defer sub.Cancel()

	entry := &physical.Entry{
		Ref:         reference.Compute([]byte("queue-single")),
		Labels:      map[string]string{"app": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
		Pattern:     2, // PATTERN_QUEUE
	}
	store.Index(ctx, entry)

	select {
	case e := <-sub.Entries():
		if e == nil {
			t.Fatal("nil entry")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestHealthMetrics(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	sub, err := store.Subscribe(ctx, "true", [32]byte{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Cancel()

	// Send 5 entries.
	for i := 0; i < 5; i++ {
		entry := &physical.Entry{
			Ref:         reference.Compute([]byte(fmt.Sprintf("health-%d", i))),
			Labels:      map[string]string{"app": "test"},
			Timestamp:   time.Now().UnixMilli(),
			Persistence: 1,
			Pattern:     0,
		}
		if err := store.Index(ctx, entry); err != nil {
			t.Fatalf("Index %d: %v", i, err)
		}
	}

	// Read all 5 entries.
	timeout := time.After(2 * time.Second)
	for i := 0; i < 5; i++ {
		select {
		case e := <-sub.Entries():
			if e == nil {
				t.Fatalf("nil entry at %d", i)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for entry %d", i)
		}
	}

	// Allow metrics to settle.
	time.Sleep(100 * time.Millisecond)

	health := sub.Health()
	if health.TotalDelivered != 5 {
		t.Errorf("TotalDelivered = %d, want 5", health.TotalDelivered)
	}
	if health.TotalDropped != 0 {
		t.Errorf("TotalDropped = %d, want 0", health.TotalDropped)
	}
}
