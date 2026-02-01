package indexstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	"github.com/gezibash/arc-node/internal/observability"
)

func newMemoryBackend(t *testing.T) physical.Backend {
	t.Helper()
	metrics := observability.NewMetrics()
	be, err := physical.New(context.Background(), "memory", nil, metrics)
	if err != nil {
		t.Fatalf("create memory backend: %v", err)
	}
	t.Cleanup(func() { be.Close() })
	return be
}

func newTestPartitioned(t *testing.T, window time.Duration, maxOpen int) *PartitionedBackend {
	t.Helper()
	pb := NewPartitionedBackend(PartitionConfig{
		Window: window,
		Factory: func(ctx context.Context, partitionID string) (physical.Backend, error) {
			metrics := observability.NewMetrics()
			return physical.New(ctx, "memory", nil, metrics)
		},
		MaxOpenPartitions: maxOpen,
	})
	t.Cleanup(func() { pb.Close() })
	return pb
}

func TestPartitionedPutAndGet(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	ref := reference.Compute([]byte("partition-test"))
	entry := &physical.Entry{
		Ref:         ref,
		Labels:      map[string]string{"type": "test"},
		Timestamp:   time.Now().UnixMilli(),
		Persistence: 1,
	}

	if err := pb.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := pb.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Ref != ref {
		t.Errorf("ref mismatch")
	}
}

func TestPartitionedGetNotFound(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	// Put one entry so there's at least one partition.
	pb.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("exists")),
		Labels:    map[string]string{"type": "test"},
		Timestamp: time.Now().UnixMilli(),
	})

	_, err := pb.Get(ctx, reference.Compute([]byte("nope")))
	if err != physical.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestPartitionedDelete(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	ref := reference.Compute([]byte("del-part"))
	pb.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"type": "test"},
		Timestamp: time.Now().UnixMilli(),
	})

	if err := pb.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := pb.Get(ctx, ref)
	if err != physical.ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestPartitionedQueryNoBounds(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		pb.Put(ctx, &physical.Entry{
			Ref:       reference.Compute([]byte(fmt.Sprintf("q-%d", i))),
			Labels:    map[string]string{"type": "test"},
			Timestamp: now + int64(i),
		})
	}

	result, err := pb.Query(ctx, &physical.QueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(result.Entries))
	}
}

func TestPartitionedQueryWithTimeBounds(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	// Put entries in different hours.
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 3; i++ {
		pb.Put(ctx, &physical.Entry{
			Ref:       reference.Compute([]byte(fmt.Sprintf("range-%d", i))),
			Labels:    map[string]string{"type": "test"},
			Timestamp: base + int64(i)*int64(time.Hour/time.Millisecond),
		})
	}

	// Query only the middle hour.
	after := base + int64(30*time.Minute/time.Millisecond)
	before := base + int64(90*time.Minute/time.Millisecond)
	result, err := pb.Query(ctx, &physical.QueryOptions{
		After:  after,
		Before: before,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Entries) != 1 {
		t.Errorf("expected 1 entry in range, got %d", len(result.Entries))
	}
}

func TestPartitionedQueryDescending(t *testing.T) {
	pb := newTestPartitioned(t, 24*time.Hour, 0)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		pb.Put(ctx, &physical.Entry{
			Ref:       reference.Compute([]byte(fmt.Sprintf("desc-%d", i))),
			Labels:    map[string]string{"type": "test"},
			Timestamp: now + int64(i)*1000,
		})
	}

	result, err := pb.Query(ctx, &physical.QueryOptions{
		Limit:      5,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	for i := 1; i < len(result.Entries); i++ {
		if result.Entries[i].Timestamp > result.Entries[i-1].Timestamp {
			t.Errorf("not descending at position %d", i)
		}
	}
}

func TestPartitionedQueryMultiPartitionPagination(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
	hourMs := int64(time.Hour / time.Millisecond)

	// Put 3 entries in 3 different hours.
	for i := 0; i < 3; i++ {
		pb.Put(ctx, &physical.Entry{
			Ref:       reference.Compute([]byte(fmt.Sprintf("multi-%d", i))),
			Labels:    map[string]string{"type": "test"},
			Timestamp: base + int64(i)*hourMs + 100,
		})
	}

	result, err := pb.Query(ctx, &physical.QueryOptions{Limit: 2})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Entries) != 2 {
		t.Errorf("expected 2, got %d", len(result.Entries))
	}
	if !result.HasMore {
		t.Error("expected HasMore")
	}
	if result.NextCursor == "" {
		t.Error("expected NextCursor")
	}
}

func TestPartitionedCount(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
	hourMs := int64(time.Hour / time.Millisecond)

	for i := 0; i < 3; i++ {
		pb.Put(ctx, &physical.Entry{
			Ref:       reference.Compute([]byte(fmt.Sprintf("count-%d", i))),
			Labels:    map[string]string{"type": "test"},
			Timestamp: base + int64(i)*hourMs,
		})
	}

	count, err := pb.Count(ctx, &physical.QueryOptions{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}
}

func TestPartitionedDeleteExpired(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	pb.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("expired-part")),
		Labels:    map[string]string{"type": "test"},
		Timestamp: now,
		ExpiresAt: now - 1000,
	})

	// Should not error; count depends on backend TTL support.
	_, err := pb.DeleteExpired(ctx, time.Now())
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
}

func TestPartitionedStats(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	pb.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("stats")),
		Labels:    map[string]string{"type": "test"},
		Timestamp: time.Now().UnixMilli(),
	})

	stats, err := pb.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.BackendType != "partitioned" {
		t.Errorf("BackendType = %s, want partitioned", stats.BackendType)
	}
}

func TestPartitionedClose(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	pb.Put(ctx, &physical.Entry{
		Ref:       reference.Compute([]byte("close")),
		Labels:    map[string]string{"type": "test"},
		Timestamp: time.Now().UnixMilli(),
	})

	if err := pb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPartitionedPutBatch(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
	hourMs := int64(time.Hour / time.Millisecond)

	entries := make([]*physical.Entry, 3)
	for i := range entries {
		entries[i] = &physical.Entry{
			Ref:       reference.Compute([]byte(fmt.Sprintf("batch-%d", i))),
			Labels:    map[string]string{"type": "test"},
			Timestamp: base + int64(i)*hourMs,
		}
	}

	if err := pb.PutBatch(ctx, entries); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}

	// Verify all entries exist.
	for _, e := range entries {
		if _, err := pb.Get(ctx, e.Ref); err != nil {
			t.Fatalf("Get after PutBatch: %v", err)
		}
	}
}

func TestPartitionedEviction(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 2)
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
	hourMs := int64(time.Hour / time.Millisecond)

	// Put entries in 3 different hours; max open is 2.
	refs := make([]reference.Reference, 3)
	for i := 0; i < 3; i++ {
		refs[i] = reference.Compute([]byte(fmt.Sprintf("evict-%d", i)))
		pb.Put(ctx, &physical.Entry{
			Ref:       refs[i],
			Labels:    map[string]string{"type": "test"},
			Timestamp: base + int64(i)*hourMs,
		})
	}

	pb.mu.RLock()
	numPartitions := len(pb.partitions)
	pb.mu.RUnlock()

	if numPartitions > 2 {
		t.Errorf("expected <= 2 open partitions, got %d", numPartitions)
	}
}

func TestPartitionedQueryNoPartitions(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	result, err := pb.Query(ctx, &physical.QueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(result.Entries))
	}
}

func TestPartitionedGetNoPartitions(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	_, err := pb.Get(ctx, reference.Compute([]byte("nope")))
	if err != physical.ErrNotFound {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestPartitionedDeleteNoPartitions(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	// Delete on empty should not error.
	if err := pb.Delete(ctx, reference.Compute([]byte("nope"))); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestPartitionedCountNoPartitions(t *testing.T) {
	pb := newTestPartitioned(t, time.Hour, 0)
	ctx := context.Background()

	count, err := pb.Count(ctx, &physical.QueryOptions{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Errorf("Count = %d, want 0", count)
	}
}
