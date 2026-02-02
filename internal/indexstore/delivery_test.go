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

func newTestBackend(t *testing.T) physical.Backend {
	t.Helper()
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create memory backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })
	return backend
}

func newBenchBackend(b *testing.B) physical.Backend {
	b.Helper()
	metrics := observability.NewMetrics()
	backend, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		b.Fatalf("create memory backend: %v", err)
	}
	b.Cleanup(func() { backend.Close() })
	return backend
}

func BenchmarkDeliver(b *testing.B) {
	b.ReportAllocs()
	backend := newBenchBackend(b)
	tracker := NewDeliveryTracker(backend)
	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("bench")),
		Labels:       map[string]string{"type": "bench"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.Deliver(entry, "sub-1")
	}
}

func BenchmarkDeliverAndAck(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()
	backend := newBenchBackend(b)
	tracker := NewDeliveryTracker(backend)
	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("bench-ack")),
		Labels:       map[string]string{"type": "bench"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 0, // no auto-delete to avoid backend interaction
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := tracker.Deliver(entry, "sub-1")
		if _, err := tracker.Ack(ctx, id); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDeliveryTracker_DeliverAssignsMonotonicIDs(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("test")),
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}

	id1 := tracker.Deliver(entry, "sub-1")
	id2 := tracker.Deliver(entry, "sub-2")
	id3 := tracker.Deliver(entry, "sub-3")

	if id1 >= id2 || id2 >= id3 {
		t.Fatalf("IDs not monotonic: %d, %d, %d", id1, id2, id3)
	}
	if tracker.Pending() != 3 {
		t.Fatalf("Pending = %d, want 3", tracker.Pending())
	}
}

func TestDeliveryTracker_AckRemovesFromInflight(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	ref := reference.Compute([]byte("ack-me"))
	entry := &physical.Entry{
		Ref:          ref,
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id := tracker.Deliver(entry, "sub-1")

	got, err := tracker.Ack(ctx, id)
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got == nil {
		t.Fatal("Ack returned nil entry")
	}
	if got.Ref != ref {
		t.Errorf("Ack entry ref mismatch")
	}
	if tracker.Pending() != 0 {
		t.Fatalf("Pending after ack = %d, want 0", tracker.Pending())
	}

	// Entry should be deleted from backend (DeliveryMode==1, all deliveries acked).
	_, getErr := backend.Get(ctx, ref)
	if getErr == nil {
		t.Fatal("expected entry to be deleted from backend after ack")
	}
}

func TestDeliveryTracker_AckUnknownID(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	got, err := tracker.Ack(context.Background(), 999)
	if err != nil {
		t.Fatalf("Ack unknown ID should be no-op, got: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil entry for unknown ID")
	}
}

func TestDeliveryTracker_Expired(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = 10 * time.Millisecond

	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("expire-me")),
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}

	tracker.Deliver(entry, "sub-1")

	// Should not be expired yet.
	expired := tracker.Expired()
	if len(expired) != 0 {
		t.Fatalf("expected 0 expired, got %d", len(expired))
	}

	time.Sleep(20 * time.Millisecond)

	expired = tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}
}

func TestDeliveryTracker_RedeliverAndDeadLetter(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = 10 * time.Millisecond
	tracker.maxRedeliver = 2

	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("redeliver-me")),
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id := tracker.Deliver(entry, "sub-1")
	time.Sleep(20 * time.Millisecond)

	expired := tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}

	// First redeliver.
	newID := tracker.Redeliver(expired[0])
	if newID == id {
		t.Fatal("expected new delivery ID")
	}
	if tracker.Pending() != 1 {
		t.Fatalf("Pending after redeliver = %d, want 1", tracker.Pending())
	}

	time.Sleep(20 * time.Millisecond)
	expired = tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired after redeliver, got %d", len(expired))
	}

	// Attempt is 2 now (started at 1, incremented in Redeliver), at max.
	if expired[0].Attempt < tracker.maxRedeliver {
		t.Fatalf("attempt = %d, want >= %d", expired[0].Attempt, tracker.maxRedeliver)
	}

	// Dead letter.
	if err := tracker.DeadLetter(ctx, expired[0]); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	if tracker.Pending() != 0 {
		t.Fatalf("Pending after dead letter = %d, want 0", tracker.Pending())
	}
}

func TestDeliveryCompleteAll(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	ref := reference.Compute([]byte("complete-all"))
	entry := &physical.Entry{
		Ref:              ref,
		Labels:           map[string]string{"type": "test"},
		Timestamp:        time.Now().UnixMilli(),
		DeliveryMode:     1,
		DeliveryComplete: 2, // DELIVERY_COMPLETE_ALL
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id1 := tracker.Deliver(entry, "sub-1")
	id2 := tracker.Deliver(entry, "sub-2")

	// Ack first — entry should NOT be deleted yet.
	if _, err := tracker.Ack(ctx, id1); err != nil {
		t.Fatalf("Ack 1: %v", err)
	}
	if _, err := backend.Get(ctx, ref); err != nil {
		t.Fatal("entry should still exist after partial ack")
	}

	// Ack second — all acked, entry should be deleted.
	if _, err := tracker.Ack(ctx, id2); err != nil {
		t.Fatalf("Ack 2: %v", err)
	}
	if _, err := backend.Get(ctx, ref); err == nil {
		t.Fatal("entry should be deleted after all acks")
	}
}

func TestDeliveryCompleteNOfM(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	ref := reference.Compute([]byte("complete-n"))
	entry := &physical.Entry{
		Ref:              ref,
		Labels:           map[string]string{"type": "test"},
		Timestamp:        time.Now().UnixMilli(),
		DeliveryMode:     1,
		DeliveryComplete: 1, // DELIVERY_COMPLETE_N_OF_M
		CompleteN:        2,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id1 := tracker.Deliver(entry, "sub-1")
	id2 := tracker.Deliver(entry, "sub-2")
	id3 := tracker.Deliver(entry, "sub-3")

	// Ack 1 — not enough.
	if _, err := tracker.Ack(ctx, id1); err != nil {
		t.Fatalf("Ack 1: %v", err)
	}
	if _, err := backend.Get(ctx, ref); err != nil {
		t.Fatal("entry should still exist after 1 ack (need 2)")
	}

	// Ack 2 — N reached, should delete.
	if _, err := tracker.Ack(ctx, id2); err != nil {
		t.Fatalf("Ack 2: %v", err)
	}
	if _, err := backend.Get(ctx, ref); err == nil {
		t.Fatal("entry should be deleted after N acks")
	}

	// Ack 3 — idempotent, no error.
	if _, err := tracker.Ack(ctx, id3); err != nil {
		t.Fatalf("Ack 3: %v", err)
	}
}

func TestDeliveryCompleteNone(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	ref := reference.Compute([]byte("complete-none"))
	entry := &physical.Entry{
		Ref:              ref,
		Labels:           map[string]string{"type": "test"},
		Timestamp:        time.Now().UnixMilli(),
		DeliveryMode:     1,
		DeliveryComplete: 0, // DELIVERY_COMPLETE_NONE — but DeliveryMode==1 defaults to ALL
	}
	// Explicitly set DeliveryComplete to NONE to override the default.
	entry.DeliveryComplete = 0
	entry.DeliveryMode = 0 // not at-least-once, so no default ALL
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id := tracker.Deliver(entry, "sub-1")
	if _, err := tracker.Ack(ctx, id); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Entry should NOT be deleted (NONE mode, DeliveryMode==0).
	if _, err := backend.Get(ctx, ref); err != nil {
		t.Fatal("entry should NOT be deleted with DELIVERY_COMPLETE_NONE")
	}
}

func TestPerEntryAckTimeout(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = time.Hour // global timeout is huge

	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("per-entry-timeout")),
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
		AckTimeoutMs: 10, // 10ms per-entry override
	}

	tracker.Deliver(entry, "sub-1")

	// Should not be expired immediately.
	if len(tracker.Expired()) != 0 {
		t.Fatal("should not be expired yet")
	}

	time.Sleep(20 * time.Millisecond)

	expired := tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}
}

func TestPerEntryMaxRedelivery(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = time.Millisecond
	tracker.maxRedeliver = 10 // global is high

	entry := &physical.Entry{
		Ref:           reference.Compute([]byte("per-entry-redeliver")),
		Labels:        map[string]string{"type": "test"},
		Timestamp:     time.Now().UnixMilli(),
		DeliveryMode:  1,
		MaxRedelivery: 1, // per-entry max is low
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	tracker.Deliver(entry, "sub-1")
	time.Sleep(5 * time.Millisecond)

	expired := tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}

	// Redeliver once — attempt becomes 2, which exceeds MaxRedelivery=1.
	tracker.Redeliver(expired[0])

	time.Sleep(5 * time.Millisecond)
	expired = tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired after redeliver, got %d", len(expired))
	}
	if expired[0].Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", expired[0].Attempt)
	}
}

func TestDeliveryTracker_DoubleAck(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	ref := reference.Compute([]byte("double-ack"))
	entry := &physical.Entry{
		Ref:          ref,
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id := tracker.Deliver(entry, "sub-1")

	// First ack should return the entry.
	got, err := tracker.Ack(ctx, id)
	if err != nil {
		t.Fatalf("first Ack: %v", err)
	}
	if got == nil {
		t.Fatal("first Ack returned nil entry")
	}

	// Second ack of the same ID should be idempotent: nil entry, no error.
	got2, err2 := tracker.Ack(ctx, id)
	if err2 != nil {
		t.Fatalf("second Ack: %v", err2)
	}
	if got2 != nil {
		t.Fatal("second Ack should return nil entry (idempotent)")
	}
}

func TestDeliveryTracker_DeadLetterLabels(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = time.Millisecond
	tracker.maxRedeliver = 1

	ref := reference.Compute([]byte("dl-labels"))
	entry := &physical.Entry{
		Ref:          ref,
		Labels:       map[string]string{"type": "test", "app": "myapp"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	tracker.Deliver(entry, "sub-dl")
	time.Sleep(5 * time.Millisecond)

	expired := tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}

	// Dead letter the entry.
	if err := tracker.DeadLetter(ctx, expired[0]); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	// Fetch from backend and verify dead letter labels.
	got, err := backend.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get after dead letter: %v", err)
	}
	if got.Labels["_dead_letter"] != "true" {
		t.Errorf("expected _dead_letter=true, got %q", got.Labels["_dead_letter"])
	}
	if got.Labels["_dead_letter_sub"] != "sub-dl" {
		t.Errorf("expected _dead_letter_sub=sub-dl, got %q", got.Labels["_dead_letter_sub"])
	}
	if got.Labels["_dead_letter_attempts"] != "1" {
		t.Errorf("expected _dead_letter_attempts=1, got %q", got.Labels["_dead_letter_attempts"])
	}
}

func TestDeliveryTracker_ConcurrentDeliverAck(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	const goroutines = 50

	// 50 goroutines delivering, 50 goroutines acking.
	ids := make(chan int64, goroutines)
	var wg sync.WaitGroup

	// Deliver goroutines.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			entry := &physical.Entry{
				Ref:          reference.Compute([]byte(fmt.Sprintf("concurrent-%d", i))),
				Labels:       map[string]string{"type": "test"},
				Timestamp:    time.Now().UnixMilli(),
				DeliveryMode: 0, // no auto-delete
			}
			id := tracker.Deliver(entry, fmt.Sprintf("sub-%d", i))
			ids <- id
		}(i)
	}

	// Ack goroutines — read IDs as they come in.
	var ackWg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		ackWg.Add(1)
		go func() {
			defer ackWg.Done()
			id := <-ids
			_, _ = tracker.Ack(ctx, id)
		}()
	}

	wg.Wait()
	ackWg.Wait()

	// No corruption — Pending should be >= 0 (likely 0 since all acked).
	pending := tracker.Pending()
	if pending < 0 {
		t.Fatalf("Pending() = %d, should be >= 0", pending)
	}
}

func TestDeliveryTracker_MaxRedeliver(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	if tracker.MaxRedeliver() != 3 {
		t.Fatalf("MaxRedeliver() = %d, want 3", tracker.MaxRedeliver())
	}

	tracker.maxRedeliver = 7
	if tracker.MaxRedeliver() != 7 {
		t.Fatalf("MaxRedeliver() = %d, want 7", tracker.MaxRedeliver())
	}
}

func TestDeliveryTracker_RedeliverExceedsMax(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = time.Millisecond
	tracker.maxRedeliver = 1

	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("redeliver-max")),
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	tracker.Deliver(entry, "sub-1")
	time.Sleep(5 * time.Millisecond)

	expired := tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}

	// Redeliver — attempt becomes 2 which exceeds maxRedeliver=1.
	tracker.Redeliver(expired[0])
	time.Sleep(5 * time.Millisecond)

	expired = tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}
	if expired[0].Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", expired[0].Attempt)
	}

	// Should dead letter since attempt > maxRedeliver.
	if expired[0].Attempt <= tracker.MaxRedeliver() {
		t.Fatal("attempt should exceed max redeliver")
	}

	if err := tracker.DeadLetter(ctx, expired[0]); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	if tracker.Pending() != 0 {
		t.Fatalf("Pending = %d, want 0", tracker.Pending())
	}
}

func TestDeliveryTracker_DeadLetterNilEntry(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	d := &InflightDelivery{
		id:    999,
		ref:   reference.Compute([]byte("nil-entry")),
		Entry: nil,
		subID: "sub-1",
	}

	// DeadLetter with nil Entry should return nil (no-op).
	if err := tracker.DeadLetter(context.Background(), d); err != nil {
		t.Fatalf("DeadLetter nil entry: %v", err)
	}
}

func TestDeliveryTracker_RedeliverWithPerEntryTimeout(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = time.Hour // global is huge

	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("redeliver-per-entry")),
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
		AckTimeoutMs: 10, // 10ms per-entry
	}

	id := tracker.Deliver(entry, "sub-1")

	// Wait for per-entry timeout.
	time.Sleep(20 * time.Millisecond)
	expired := tracker.Expired()
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}

	// Redeliver should also use per-entry timeout.
	newID := tracker.Redeliver(expired[0])
	if newID == id {
		t.Fatal("expected different ID")
	}

	// Should not be expired immediately after redeliver.
	if len(tracker.Expired()) != 0 {
		t.Fatal("should not be expired immediately after redeliver")
	}

	// Wait for per-entry timeout again.
	time.Sleep(20 * time.Millisecond)
	if len(tracker.Expired()) != 1 {
		t.Fatal("should be expired after per-entry timeout")
	}
}

func TestDeliveryTracker_PendingAfterMultipleDeliveries(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	ref := reference.Compute([]byte("multi-deliver"))
	entry := &physical.Entry{
		Ref:          ref,
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}

	tracker.Deliver(entry, "sub-1")
	tracker.Deliver(entry, "sub-2")
	tracker.Deliver(entry, "sub-3")

	if tracker.Pending() != 3 {
		t.Fatalf("Pending = %d, want 3", tracker.Pending())
	}
}

func TestDeliveryTracker_NackRedeliver(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	ref := reference.Compute([]byte("nack-redeliver"))
	entry := &physical.Entry{
		Ref:          ref,
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id := tracker.Deliver(entry, "sub-1")
	if tracker.Pending() != 1 {
		t.Fatalf("Pending = %d, want 1", tracker.Pending())
	}

	// Nack without dead_letter — should redeliver immediately.
	d, err := tracker.Nack(ctx, id, false, "retry please")
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if d == nil {
		t.Fatal("Nack returned nil")
	}
	// Should still have 1 pending (the redelivered entry).
	if tracker.Pending() != 1 {
		t.Fatalf("Pending after nack = %d, want 1", tracker.Pending())
	}
}

func TestDeliveryTracker_NackDeadLetter(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	ref := reference.Compute([]byte("nack-dl"))
	entry := &physical.Entry{
		Ref:          ref,
		Labels:       map[string]string{"type": "test", "app": "myapp"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id := tracker.Deliver(entry, "sub-nack")

	// Nack with dead_letter=true and a reason.
	d, err := tracker.Nack(ctx, id, true, "bad payload")
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if d == nil {
		t.Fatal("Nack returned nil")
	}
	if tracker.Pending() != 0 {
		t.Fatalf("Pending after nack dead letter = %d, want 0", tracker.Pending())
	}

	// Verify dead letter labels in backend.
	got, err := backend.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get after nack dead letter: %v", err)
	}
	if got.Labels["_dead_letter"] != "true" {
		t.Errorf("expected _dead_letter=true, got %q", got.Labels["_dead_letter"])
	}
	if got.Labels["_dead_letter_reason"] != "bad payload" {
		t.Errorf("expected _dead_letter_reason='bad payload', got %q", got.Labels["_dead_letter_reason"])
	}
	if got.Labels["_dead_letter_sub"] != "sub-nack" {
		t.Errorf("expected _dead_letter_sub=sub-nack, got %q", got.Labels["_dead_letter_sub"])
	}
}

func TestDeliveryTracker_NackUnknownID(t *testing.T) {
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)

	d, err := tracker.Nack(context.Background(), 999, false, "")
	if err != nil {
		t.Fatalf("Nack unknown ID: %v", err)
	}
	if d != nil {
		t.Fatal("expected nil for unknown ID")
	}
}

func TestDeliveryTracker_CleanupNotExpiredEnough(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = time.Millisecond

	entry := &physical.Entry{
		Ref:          reference.Compute([]byte("not-stale")),
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}
	tracker.Deliver(entry, "sub-1")

	// Expired but not old enough for cleanup (maxAge=1h, expired only ms ago).
	time.Sleep(5 * time.Millisecond)
	cleaned, err := tracker.Cleanup(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if cleaned != 0 {
		t.Fatalf("expected 0 cleaned (not stale enough), got %d", cleaned)
	}
	if tracker.Pending() != 1 {
		t.Fatalf("Pending = %d, want 1", tracker.Pending())
	}
}

func TestDeliveryTracker_CleanupMultiple(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)
	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = time.Millisecond

	for i := 0; i < 3; i++ {
		entry := &physical.Entry{
			Ref:          reference.Compute([]byte(fmt.Sprintf("stale-%d", i))),
			Labels:       map[string]string{"type": "test"},
			Timestamp:    time.Now().UnixMilli(),
			DeliveryMode: 1,
		}
		if err := backend.Put(ctx, entry); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		tracker.Deliver(entry, fmt.Sprintf("sub-%d", i))
	}

	// Backdate all deliveries.
	tracker.mu.Lock()
	for _, d := range tracker.inflight {
		d.expiresAt = time.Now().Add(-2 * time.Hour)
	}
	tracker.mu.Unlock()

	cleaned, err := tracker.Cleanup(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if cleaned != 3 {
		t.Fatalf("Cleanup cleaned = %d, want 3", cleaned)
	}
	if tracker.Pending() != 0 {
		t.Fatalf("Pending = %d, want 0", tracker.Pending())
	}
}

func TestDeliveryTracker_Cleanup(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend(t)

	ref := reference.Compute([]byte("stale"))
	entry := &physical.Entry{
		Ref:          ref,
		Labels:       map[string]string{"type": "test"},
		Timestamp:    time.Now().UnixMilli(),
		DeliveryMode: 1,
	}
	if err := backend.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	tracker := NewDeliveryTracker(backend)
	tracker.ackTimeout = time.Millisecond
	tracker.Deliver(entry, "sub-1")

	// Backdate the expiry.
	tracker.mu.Lock()
	for _, d := range tracker.inflight {
		d.expiresAt = time.Now().Add(-2 * time.Hour)
	}
	tracker.mu.Unlock()

	cleaned, err := tracker.Cleanup(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("Cleanup cleaned = %d, want 1", cleaned)
	}
	if tracker.Pending() != 0 {
		t.Fatalf("Pending after cleanup = %d, want 0", tracker.Pending())
	}
}
