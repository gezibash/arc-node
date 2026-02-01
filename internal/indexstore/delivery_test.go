package indexstore

import (
	"context"
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
