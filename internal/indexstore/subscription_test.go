package indexstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
)

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
