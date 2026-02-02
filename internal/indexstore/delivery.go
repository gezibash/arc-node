package indexstore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

// DeliveryTracker tracks per-entry, per-subscriber delivery and ack state
// for at-least-once entries using delivery IDs.
type DeliveryTracker struct {
	mu           sync.Mutex
	nextID       atomic.Int64
	inflight     map[int64]*InflightDelivery
	byRef        map[reference.Reference][]*InflightDelivery
	ackCounts    map[reference.Reference]int32
	backend      physical.Backend
	ackTimeout   time.Duration
	maxRedeliver int
}

// InflightDelivery represents an in-flight delivery awaiting acknowledgement.
type InflightDelivery struct {
	id               int64
	ref              reference.Reference
	Entry            *physical.Entry
	subID            string
	Attempt          int
	FirstDeliveredAt time.Time
	expiresAt        time.Time
}

// NewDeliveryTracker creates a new DeliveryTracker.
func NewDeliveryTracker(backend physical.Backend) *DeliveryTracker {
	return &DeliveryTracker{
		inflight:     make(map[int64]*InflightDelivery),
		byRef:        make(map[reference.Reference][]*InflightDelivery),
		ackCounts:    make(map[reference.Reference]int32),
		backend:      backend,
		ackTimeout:   30 * time.Second,
		maxRedeliver: 3,
	}
}

// Deliver assigns a delivery ID and records the entry as inflight.
// Uses per-entry ack timeout if set, otherwise falls back to the global default.
func (t *DeliveryTracker) Deliver(entry *physical.Entry, subID string) int64 {
	timeout := t.ackTimeout
	if entry.AckTimeoutMs > 0 {
		timeout = time.Duration(entry.AckTimeoutMs) * time.Millisecond
	}

	id := t.nextID.Add(1)
	now := time.Now()
	d := &InflightDelivery{
		id:               id,
		ref:              entry.Ref,
		Entry:            entry,
		subID:            subID,
		Attempt:          1,
		FirstDeliveredAt: now,
		expiresAt:        now.Add(timeout),
	}
	t.mu.Lock()
	t.inflight[id] = d
	t.byRef[entry.Ref] = append(t.byRef[entry.Ref], d)
	t.mu.Unlock()
	return id
}

// Ack removes a delivery from inflight by delivery ID. Returns the entry
// for cursor updates. Deletion behavior depends on DeliveryComplete mode:
//   - DELIVERY_COMPLETE_NONE (0): never auto-delete
//   - DELIVERY_COMPLETE_N_OF_M (1): delete after CompleteN acks
//   - DELIVERY_COMPLETE_ALL (2): delete when all inflight acked (default for DeliveryMode==1)
func (t *DeliveryTracker) Ack(ctx context.Context, deliveryID int64) (*physical.Entry, error) {
	t.mu.Lock()
	d, ok := t.inflight[deliveryID]
	if !ok {
		t.mu.Unlock()
		return nil, nil // idempotent
	}
	delete(t.inflight, deliveryID)

	// Remove from byRef slice.
	ref := d.ref
	entries := t.byRef[ref]
	for i, e := range entries {
		if e.id == deliveryID {
			entries[i] = entries[len(entries)-1]
			entries = entries[:len(entries)-1]
			break
		}
	}
	if len(entries) == 0 {
		delete(t.byRef, ref)
	} else {
		t.byRef[ref] = entries
	}

	shouldDelete := false
	entry := d.Entry

	if entry != nil {
		completeMode := entry.DeliveryComplete
		// Default: DeliveryMode==1 with no explicit complete mode uses ALL semantics.
		if completeMode == 0 && entry.DeliveryMode == 1 {
			completeMode = 2 // DELIVERY_COMPLETE_ALL
		}

		switch completeMode {
		case 1: // DELIVERY_COMPLETE_N_OF_M
			t.ackCounts[ref]++
			if t.ackCounts[ref] >= entry.CompleteN {
				shouldDelete = true
				delete(t.ackCounts, ref)
			}
		case 2: // DELIVERY_COMPLETE_ALL
			shouldDelete = len(entries) == 0
		}
		// case 0: DELIVERY_COMPLETE_NONE — never auto-delete
	}

	t.mu.Unlock()

	if shouldDelete {
		if err := t.backend.Delete(ctx, ref); err != nil {
			return entry, fmt.Errorf("delete delivered entry: %w", err)
		}
		slog.DebugContext(ctx, "delivery-complete entry deleted",
			"ref", reference.Hex(ref))
	}

	return entry, nil
}

// Nack explicitly rejects a delivery. If deadLetter is true, the entry is
// sent straight to the dead letter queue with the given reason. Otherwise
// the entry is immediately redelivered.
func (t *DeliveryTracker) Nack(ctx context.Context, deliveryID int64, deadLetter bool, reason string) (*InflightDelivery, error) {
	t.mu.Lock()
	d, ok := t.inflight[deliveryID]
	if !ok {
		t.mu.Unlock()
		return nil, nil // idempotent
	}
	t.mu.Unlock()

	if deadLetter {
		if err := t.deadLetterWithReason(ctx, d, reason); err != nil {
			return d, fmt.Errorf("nack dead letter: %w", err)
		}
		return d, nil
	}

	// Immediate redelivery.
	t.Redeliver(d)
	return d, nil
}

// deadLetterWithReason is like DeadLetter but also adds a _dead_letter_reason label.
func (t *DeliveryTracker) deadLetterWithReason(ctx context.Context, d *InflightDelivery, reason string) error {
	t.mu.Lock()
	delete(t.inflight, d.id)
	entries := t.byRef[d.ref]
	for i, e := range entries {
		if e.id == d.id {
			entries[i] = entries[len(entries)-1]
			entries = entries[:len(entries)-1]
			break
		}
	}
	if len(entries) == 0 {
		delete(t.byRef, d.ref)
	} else {
		t.byRef[d.ref] = entries
	}
	t.mu.Unlock()

	if d.Entry == nil {
		return nil
	}

	dlEntry := &physical.Entry{
		Ref:         d.Entry.Ref,
		Labels:      make(map[string]string),
		Timestamp:   d.Entry.Timestamp,
		Persistence: 1,
	}
	for k, v := range d.Entry.Labels {
		dlEntry.Labels[k] = v
	}
	dlEntry.Labels["_dead_letter"] = "true"
	dlEntry.Labels["_dead_letter_sub"] = d.subID
	dlEntry.Labels["_dead_letter_attempts"] = fmt.Sprintf("%d", d.Attempt)
	if reason != "" {
		dlEntry.Labels["_dead_letter_reason"] = reason
	}

	if err := t.backend.Put(ctx, dlEntry); err != nil {
		return fmt.Errorf("dead letter: %w", err)
	}

	slog.WarnContext(ctx, "entry dead-lettered via nack",
		"ref", reference.Hex(d.ref),
		"sub", d.subID,
		"attempts", d.Attempt,
		"reason", reason,
	)
	return nil
}

// Expired returns inflight deliveries that have exceeded their ack timeout.
func (t *DeliveryTracker) Expired() []*InflightDelivery {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	var expired []*InflightDelivery
	for _, d := range t.inflight {
		if d.expiresAt.Before(now) {
			expired = append(expired, d)
		}
	}
	return expired
}

// Redeliver increments the attempt counter and re-adds with a new delivery ID.
// Uses per-entry ack timeout if set.
// Returns the new delivery ID.
func (t *DeliveryTracker) Redeliver(d *InflightDelivery) int64 {
	t.mu.Lock()
	// Remove old ID.
	delete(t.inflight, d.id)
	entries := t.byRef[d.ref]
	for i, e := range entries {
		if e.id == d.id {
			entries[i] = entries[len(entries)-1]
			entries = entries[:len(entries)-1]
			break
		}
	}
	if len(entries) == 0 {
		delete(t.byRef, d.ref)
	} else {
		t.byRef[d.ref] = entries
	}
	t.mu.Unlock()

	timeout := t.ackTimeout
	if d.Entry != nil && d.Entry.AckTimeoutMs > 0 {
		timeout = time.Duration(d.Entry.AckTimeoutMs) * time.Millisecond
	}

	newID := t.nextID.Add(1)
	newD := &InflightDelivery{
		id:               newID,
		ref:              d.ref,
		Entry:            d.Entry,
		subID:            d.subID,
		Attempt:          d.Attempt + 1,
		FirstDeliveredAt: d.FirstDeliveredAt,
		expiresAt:        time.Now().Add(timeout),
	}
	t.mu.Lock()
	t.inflight[newID] = newD
	t.byRef[d.ref] = append(t.byRef[d.ref], newD)
	t.mu.Unlock()

	return newID
}

// DeadLetter re-indexes the entry with a _dead_letter label and removes from inflight.
func (t *DeliveryTracker) DeadLetter(ctx context.Context, d *InflightDelivery) error {
	t.mu.Lock()
	delete(t.inflight, d.id)
	entries := t.byRef[d.ref]
	for i, e := range entries {
		if e.id == d.id {
			entries[i] = entries[len(entries)-1]
			entries = entries[:len(entries)-1]
			break
		}
	}
	if len(entries) == 0 {
		delete(t.byRef, d.ref)
	} else {
		t.byRef[d.ref] = entries
	}
	t.mu.Unlock()

	if d.Entry == nil {
		return nil
	}

	// Re-index with dead letter label.
	dlEntry := &physical.Entry{
		Ref:         d.Entry.Ref,
		Labels:      make(map[string]string),
		Timestamp:   d.Entry.Timestamp,
		Persistence: 1,
	}
	for k, v := range d.Entry.Labels {
		dlEntry.Labels[k] = v
	}
	dlEntry.Labels["_dead_letter"] = "true"
	dlEntry.Labels["_dead_letter_sub"] = d.subID
	dlEntry.Labels["_dead_letter_attempts"] = fmt.Sprintf("%d", d.Attempt)

	if err := t.backend.Put(ctx, dlEntry); err != nil {
		return fmt.Errorf("dead letter: %w", err)
	}

	slog.WarnContext(ctx, "entry dead-lettered",
		"ref", reference.Hex(d.ref),
		"sub", d.subID,
		"attempts", d.Attempt,
	)
	return nil
}

// MaxRedeliver returns the max redelivery count.
func (t *DeliveryTracker) MaxRedeliver() int {
	return t.maxRedeliver
}

// Pending returns the number of inflight deliveries.
func (t *DeliveryTracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.inflight)
}

// Cleanup removes stale unacked entries that have exceeded maxAge from both
// the tracker and the backend. Returns the number of entries cleaned up.
func (t *DeliveryTracker) Cleanup(ctx context.Context, maxAge time.Duration) (int, error) {
	t.mu.Lock()
	var stale []*InflightDelivery
	cutoff := time.Now().Add(-maxAge)
	for _, d := range t.inflight {
		if d.expiresAt.Before(cutoff) {
			stale = append(stale, d)
		}
	}
	t.mu.Unlock()

	var cleaned int
	for _, d := range stale {
		if err := t.backend.Delete(ctx, d.ref); err != nil {
			slog.WarnContext(ctx, "failed to delete stale delivery entry",
				"ref", reference.Hex(d.ref), "error", err)
			continue
		}

		t.mu.Lock()
		delete(t.inflight, d.id)
		entries := t.byRef[d.ref]
		for i, e := range entries {
			if e.id == d.id {
				entries[i] = entries[len(entries)-1]
				entries = entries[:len(entries)-1]
				break
			}
		}
		if len(entries) == 0 {
			delete(t.byRef, d.ref)
		} else {
			t.byRef[d.ref] = entries
		}
		t.mu.Unlock()
		cleaned++
	}

	return cleaned, nil
}
