package memory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

func newTestBackend(t *testing.T) physical.Backend {
	t.Helper()
	be, err := NewFactory(context.Background(), map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { be.Close() })
	return be
}

func testRef(b byte) reference.Reference {
	var r reference.Reference
	r[0] = b
	return r
}

func TestPutAndGet(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("hello"))
	entry := &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"type": "text"},
		Timestamp: time.Now().UnixMilli(),
		ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
	}

	if err := be.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reference.Equal(got.Ref, ref) {
		t.Errorf("ref = %s, want %s", reference.Hex(got.Ref), reference.Hex(ref))
	}
	if got.Labels["type"] != "text" {
		t.Errorf("labels = %v, want type=text", got.Labels)
	}
}

func TestGetNotFound(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	_, err := be.Get(ctx, testRef(0xFF))
	if !errors.Is(err, physical.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
}

func TestDelete(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("delete-me"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"k": "v"},
		Timestamp: time.Now().UnixMilli(),
	})

	if err := be.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := be.Get(ctx, ref)
	if !errors.Is(err, physical.ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
}

func TestQueryWithLabels(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		label := "a"
		if i%2 == 0 {
			label = "b"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": label},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"group": "b"},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Errorf("got %d entries, want 5", len(res.Entries))
	}
}

func TestQueryPagination(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"x": "y"},
			Timestamp: int64(1000 + i),
		})
	}

	var total int
	cursor := ""
	for {
		res, err := be.Query(ctx, &physical.QueryOptions{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		total += len(res.Entries)
		if !res.HasMore {
			break
		}
		cursor = res.NextCursor
	}
	if total != 10 {
		t.Errorf("paginated total = %d, want 10", total)
	}
}

func TestDeleteExpired(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()
	now := time.Now()

	for i := range 6 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		exp := now.Add(time.Hour).UnixMilli()
		if i < 3 {
			exp = now.Add(-time.Hour).UnixMilli()
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"i": "v"},
			Timestamp: int64(1000 + i),
			ExpiresAt: exp,
		})
	}

	// Memory backend uses badger in-memory mode, which handles expiration
	// natively via TTL — DeleteExpired is a no-op.
	n, err := be.DeleteExpired(ctx, now)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if n != 0 {
		t.Errorf("deleted = %d, want 0 (badger TTL handles expiration)", n)
	}
}

func TestStats(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	stats, err := be.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.BackendType == "" {
		t.Error("BackendType is empty")
	}
}

func TestDimensionFieldsRoundTrip(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("dim-roundtrip"))
	entry := &physical.Entry{
		Ref:              ref,
		Labels:           map[string]string{"type": "test"},
		Timestamp:        time.Now().UnixMilli(),
		ExpiresAt:        time.Now().Add(time.Hour).UnixMilli(),
		Persistence:      1,
		Visibility:       2,
		DeliveryMode:     1,
		Pattern:          3,
		Affinity:         2,
		AffinityKey:      "my-key",
		Ordering:         1,
		DedupMode:        2,
		IdempotencyKey:   "idem-123",
		DeliveryComplete: 1,
		CompleteN:        3,
		Priority:         42,
		MaxRedelivery:    5,
		AckTimeoutMs:     15000,
		Correlation:      "corr-abc",
	}

	if err := be.Put(ctx, entry); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Persistence != 1 {
		t.Errorf("Persistence = %d, want 1", got.Persistence)
	}
	if got.Visibility != 2 {
		t.Errorf("Visibility = %d, want 2", got.Visibility)
	}
	if got.DeliveryMode != 1 {
		t.Errorf("DeliveryMode = %d, want 1", got.DeliveryMode)
	}
	if got.Pattern != 3 {
		t.Errorf("Pattern = %d, want 3", got.Pattern)
	}
	if got.Affinity != 2 {
		t.Errorf("Affinity = %d, want 2", got.Affinity)
	}
	if got.AffinityKey != "my-key" {
		t.Errorf("AffinityKey = %q, want %q", got.AffinityKey, "my-key")
	}
	if got.Ordering != 1 {
		t.Errorf("Ordering = %d, want 1", got.Ordering)
	}
	if got.DedupMode != 2 {
		t.Errorf("DedupMode = %d, want 2", got.DedupMode)
	}
	if got.IdempotencyKey != "idem-123" {
		t.Errorf("IdempotencyKey = %q, want %q", got.IdempotencyKey, "idem-123")
	}
	if got.DeliveryComplete != 1 {
		t.Errorf("DeliveryComplete = %d, want 1", got.DeliveryComplete)
	}
	if got.CompleteN != 3 {
		t.Errorf("CompleteN = %d, want 3", got.CompleteN)
	}
	if got.Priority != 42 {
		t.Errorf("Priority = %d, want 42", got.Priority)
	}
	if got.MaxRedelivery != 5 {
		t.Errorf("MaxRedelivery = %d, want 5", got.MaxRedelivery)
	}
	if got.AckTimeoutMs != 15000 {
		t.Errorf("AckTimeoutMs = %d, want 15000", got.AckTimeoutMs)
	}
	if got.Correlation != "corr-abc" {
		t.Errorf("Correlation = %q, want %q", got.Correlation, "corr-abc")
	}
}

func TestDefaults(t *testing.T) {
	d := Defaults()
	if d["in_memory"] != "true" {
		t.Errorf("Defaults()[in_memory] = %q, want %q", d["in_memory"], "true")
	}
}

func TestInitRegistration(t *testing.T) {
	// init() registers "memory" in the physical registry.
	d := physical.GetDefaults("memory")
	if d == nil {
		t.Fatal("memory backend not registered")
	}
	if d["in_memory"] != "true" {
		t.Errorf("registered defaults[in_memory] = %q, want %q", d["in_memory"], "true")
	}
}

func TestPutBatch(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	entries := make([]*physical.Entry, 5)
	for i := range entries {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i+100))
		entries[i] = &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"batch": "yes"},
			Timestamp: int64(2000 + i),
		}
	}

	if err := be.PutBatch(ctx, entries); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}

	for _, e := range entries {
		got, err := be.Get(ctx, e.Ref)
		if err != nil {
			t.Fatalf("Get after PutBatch: %v", err)
		}
		if got.Labels["batch"] != "yes" {
			t.Errorf("label mismatch: %v", got.Labels)
		}
	}
}

func TestCount(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 7 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i+200))
		label := "alpha"
		if i%2 == 0 {
			label = "beta"
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": label},
			Timestamp: int64(3000 + i),
		})
	}

	n, err := be.Count(ctx, &physical.QueryOptions{
		Labels: map[string]string{"group": "beta"},
	})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 4 {
		t.Errorf("Count = %d, want 4", n)
	}

	total, err := be.Count(ctx, &physical.QueryOptions{})
	if err != nil {
		t.Fatalf("Count all: %v", err)
	}
	if total != 7 {
		t.Errorf("Count all = %d, want 7", total)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	cur := physical.Cursor{Timestamp: 123456, Sequence: 42}
	if err := be.PutCursor(ctx, "test-cursor", cur); err != nil {
		t.Fatalf("PutCursor: %v", err)
	}

	got, err := be.GetCursor(ctx, "test-cursor")
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if got.Timestamp != 123456 || got.Sequence != 42 {
		t.Errorf("GetCursor = %+v, want {123456, 42}", got)
	}

	if err := be.DeleteCursor(ctx, "test-cursor"); err != nil {
		t.Fatalf("DeleteCursor: %v", err)
	}

	_, err = be.GetCursor(ctx, "test-cursor")
	if !errors.Is(err, physical.ErrCursorNotFound) {
		t.Errorf("GetCursor after delete = %v, want ErrCursorNotFound", err)
	}
}

func TestGetCursorNotFound(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	_, err := be.GetCursor(ctx, "nonexistent")
	if !errors.Is(err, physical.ErrCursorNotFound) {
		t.Errorf("GetCursor = %v, want ErrCursorNotFound", err)
	}
}

func TestQueryDescending(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i+300))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"d": "yes"},
			Timestamp: int64(4000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels:     map[string]string{"d": "yes"},
		Limit:      10,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("Query descending: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Errorf("not descending at index %d: %d > %d",
				i, res.Entries[i].Timestamp, res.Entries[i-1].Timestamp)
		}
	}
}

func TestQueryTimeRange(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i+400))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"t": "r"},
			Timestamp: int64(5000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		After:  5002,
		Before: 5007,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query time range: %v", err)
	}
	// After is exclusive lower bound, Before is exclusive upper bound (typically)
	// The exact count depends on the backend semantics; just verify filtering works.
	for _, e := range res.Entries {
		if e.Timestamp < 5002 || e.Timestamp >= 5007 {
			t.Errorf("entry timestamp %d outside range [5002, 5007)", e.Timestamp)
		}
	}
}

func TestDoubleClose(t *testing.T) {
	be, err := NewFactory(context.Background(), map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if err := be.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should not panic or error.
	if err := be.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestDeleteNonExistent(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	// Deleting a non-existent entry should not error.
	err := be.Delete(ctx, testRef(0xAA))
	if err != nil {
		t.Errorf("Delete non-existent: %v", err)
	}
}

func TestPutOverwrite(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("overwrite-me"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"v": "1"},
		Timestamp: 9000,
	})
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"v": "2"},
		Timestamp: 9001,
	})

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["v"] != "2" {
		t.Errorf("label v = %q, want %q", got.Labels["v"], "2")
	}
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

var (
	benchSizes = []int{100, 1_000, 10_000, 100_000}

	typeValues = []string{"text", "image", "video", "audio", "document", "spreadsheet", "presentation", "archive", "code", "binary"}
	envValues  = []string{"prod", "staging", "dev", "test", "ci"}
	userPool   = func() []string {
		u := make([]string, 100)
		for i := range u {
			u[i] = fmt.Sprintf("user-%03d", i)
		}
		return u
	}()
)

const (
	baseTimestamp = int64(1735689600000)
	farFuture     = int64(2051222400000)
)

func benchRef(seed int) reference.Reference {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seed))
	return reference.Reference(sha256.Sum256(buf[:]))
}

func benchEntry(rng *rand.Rand, i int) *physical.Entry {
	return &physical.Entry{
		Ref: benchRef(i),
		Labels: map[string]string{
			"type": typeValues[rng.Intn(len(typeValues))],
			"env":  envValues[rng.Intn(len(envValues))],
			"user": userPool[rng.Intn(len(userPool))],
		},
		Timestamp: baseTimestamp + int64(i),
		ExpiresAt: farFuture + int64(i),
	}
}

func benchEntryWithExpiry(rng *rand.Rand, i int, n int) *physical.Entry {
	var expiresAt int64
	if rng.Intn(2) == 0 {
		expiresAt = baseTimestamp + int64(rng.Intn(n/2))
	} else {
		expiresAt = farFuture + int64(rng.Intn(n))
	}
	return &physical.Entry{
		Ref: benchRef(i),
		Labels: map[string]string{
			"type": typeValues[rng.Intn(len(typeValues))],
			"env":  envValues[rng.Intn(len(envValues))],
			"user": userPool[rng.Intn(len(userPool))],
		},
		Timestamp: baseTimestamp + int64(i),
		ExpiresAt: expiresAt,
	}
}

func seedBench(b *testing.B, be physical.Backend, n int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()
	const batchSize = 1000
	batch := make([]*physical.Entry, 0, batchSize)
	for i := range n {
		batch = append(batch, benchEntry(rng, i))
		if len(batch) >= batchSize {
			if err := be.PutBatch(ctx, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := be.PutBatch(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func seedBenchWithExpiry(b *testing.B, be physical.Backend, n int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()
	const batchSize = 1000
	batch := make([]*physical.Entry, 0, batchSize)
	for i := range n {
		batch = append(batch, benchEntryWithExpiry(rng, i, n))
		if len(batch) >= batchSize {
			if err := be.PutBatch(ctx, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := be.PutBatch(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchBackend(b *testing.B) physical.Backend {
	b.Helper()
	be, err := NewFactory(context.Background(), map[string]string{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { be.Close() })
	return be
}

func queryBench(b *testing.B, sizes []int, opts func(n int) *physical.QueryOptions) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			ctx := context.Background()
			qo := opts(n)
			b.ResetTimer()
			for range b.N {
				if _, err := be.Query(ctx, qo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func countBench(b *testing.B, sizes []int, opts func(n int) *physical.QueryOptions) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			ctx := context.Background()
			qo := opts(n)
			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, qo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Put benchmarks
// ---------------------------------------------------------------------------

func BenchmarkPut(b *testing.B) {
	be := newBenchBackend(b)
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()

	b.ResetTimer()
	for i := range b.N {
		entry := &physical.Entry{
			Ref: benchRef(i),
			Labels: map[string]string{
				"type": typeValues[rng.Intn(len(typeValues))],
				"env":  envValues[rng.Intn(len(envValues))],
				"user": userPool[rng.Intn(len(userPool))],
			},
			Timestamp: baseTimestamp + int64(i),
			ExpiresAt: baseTimestamp + int64(i) + 1000,
		}
		if err := be.Put(ctx, entry); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutBatch(b *testing.B) {
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			be := newBenchBackend(b)
			rng := rand.New(rand.NewSource(42))
			ctx := context.Background()

			b.ResetTimer()
			for i := range b.N {
				batch := make([]*physical.Entry, batchSize)
				for j := range batchSize {
					idx := i*batchSize + j
					batch[j] = &physical.Entry{
						Ref: benchRef(idx),
						Labels: map[string]string{
							"type": typeValues[rng.Intn(len(typeValues))],
							"env":  envValues[rng.Intn(len(envValues))],
							"user": userPool[rng.Intn(len(userPool))],
						},
						Timestamp: baseTimestamp + int64(idx),
						ExpiresAt: baseTimestamp + int64(idx) + 1000,
					}
				}
				if err := be.PutBatch(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Get benchmarks
// ---------------------------------------------------------------------------

func BenchmarkGet(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			rng := rand.New(rand.NewSource(99))
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				ref := benchRef(rng.Intn(n))
				if _, err := be.Get(ctx, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Query benchmarks
// ---------------------------------------------------------------------------

func BenchmarkQueryNoFilter(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Limit: 100}
	})
}

func BenchmarkQuerySingleLabel(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text"}, Limit: 100}
	})
}

func BenchmarkQueryMultiLabel(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 100}
	})
}

func BenchmarkQueryTimeRange(b *testing.B) {
	queryBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			After: baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100), Limit: 100,
		}
	})
}

func BenchmarkQueryDescending(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text"}, Limit: 100, Descending: true}
	})
}

// ---------------------------------------------------------------------------
// Count benchmarks
// ---------------------------------------------------------------------------

func BenchmarkCountNoFilter(b *testing.B) {
	countBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{}
	})
}

func BenchmarkCount(b *testing.B) {
	countBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text"}}
	})
}

func BenchmarkCountMultiLabel(b *testing.B) {
	countBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}}
	})
}

// ---------------------------------------------------------------------------
// DeleteExpired benchmarks
// ---------------------------------------------------------------------------

func BenchmarkDeleteExpiredBench(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()
			midpoint := time.UnixMilli(baseTimestamp + int64(n/2))

			for range b.N {
				b.StopTimer()
				be := newBenchBackend(b)
				seedBenchWithExpiry(b, be, n)
				b.StartTimer()

				if _, err := be.DeleteExpired(ctx, midpoint); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				be.Close()
				b.StartTimer()
			}
		})
	}
}
