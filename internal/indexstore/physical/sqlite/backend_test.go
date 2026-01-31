package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

func newTestBackend(t *testing.T) physical.Backend {
	t.Helper()
	cfg := map[string]string{"path": filepath.Join(t.TempDir(), "test.db")}
	be, err := NewFactory(context.Background(), cfg)
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
	if got.Timestamp != entry.Timestamp {
		t.Errorf("timestamp = %d, want %d", got.Timestamp, entry.Timestamp)
	}
	if got.ExpiresAt != entry.ExpiresAt {
		t.Errorf("expires_at = %d, want %d", got.ExpiresAt, entry.ExpiresAt)
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

func TestPutReplacement(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	ref := reference.Compute([]byte("replace-me"))
	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"v": "1"},
		Timestamp: 1000,
	})

	be.Put(ctx, &physical.Entry{
		Ref:       ref,
		Labels:    map[string]string{"v": "2"},
		Timestamp: 2000,
	})

	got, err := be.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Labels["v"] != "2" {
		t.Errorf("label v = %s, want 2", got.Labels["v"])
	}
	if got.Timestamp != 2000 {
		t.Errorf("timestamp = %d, want 2000", got.Timestamp)
	}
}

func TestDeleteIdempotent(t *testing.T) {
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

	// Second delete should not error.
	if err := be.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete (idempotent): %v", err)
	}
}

func TestQueryNoFilter(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 5 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"x": "y"},
			Timestamp: int64(1000 + i),
		})
	}

	// Ascending
	res, err := be.Query(ctx, &physical.QueryOptions{Limit: 100})
	if err != nil {
		t.Fatalf("Query ASC: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp < res.Entries[i-1].Timestamp {
			t.Error("entries not in ascending order")
		}
	}

	// Descending
	res, err = be.Query(ctx, &physical.QueryOptions{Limit: 100, Descending: true})
	if err != nil {
		t.Fatalf("Query DESC: %v", err)
	}
	if len(res.Entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(res.Entries))
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Error("entries not in descending order")
		}
	}
}

func TestQuerySingleLabel(t *testing.T) {
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

func TestQueryMultiLabel(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		grp := "a"
		if i%2 == 0 {
			grp = "b"
		}
		env := "dev"
		if i%3 == 0 {
			env = "prod"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": grp, "env": env},
			Timestamp: int64(1000 + i),
		})
	}

	// group=b AND env=prod -> indices 0, 6 (even AND divisible by 3)
	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"group": "b", "env": "prod"},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Entries) != 2 {
		t.Errorf("got %d entries, want 2", len(res.Entries))
	}
}

func TestQueryTimeRange(t *testing.T) {
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

	res, err := be.Query(ctx, &physical.QueryOptions{
		After:  1002,
		Before: 1007,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// timestamps 1003, 1004, 1005, 1006
	if len(res.Entries) != 4 {
		t.Errorf("got %d entries, want 4", len(res.Entries))
	}
}

func TestQueryLabelAndTimeRange(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	for i := range 10 {
		grp := "a"
		if i%2 == 0 {
			grp = "b"
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"group": grp},
			Timestamp: int64(1000 + i),
		})
	}

	res, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"group": "b"},
		After:  1001,
		Before: 1008,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// group=b (even): 0,2,4,6,8. After 1001 and before 1008: 2,4,6
	if len(res.Entries) != 3 {
		t.Errorf("got %d entries, want 3", len(res.Entries))
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
		exp := now.Add(time.Hour).UnixNano()
		if i < 3 {
			exp = now.Add(-time.Hour).UnixNano()
		}
		be.Put(ctx, &physical.Entry{
			Ref:       reference.Reference(sha256.Sum256(buf[:])),
			Labels:    map[string]string{"i": "v"},
			Timestamp: int64(1000 + i),
			ExpiresAt: exp,
		})
	}

	n, err := be.DeleteExpired(ctx, now)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if n != 3 {
		t.Errorf("deleted = %d, want 3", n)
	}
}

func TestClosedBackend(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	be.Close()

	if err := be.Put(ctx, &physical.Entry{}); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Put = %v, want ErrClosed", err)
	}
	if _, err := be.Get(ctx, testRef(0)); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Get = %v, want ErrClosed", err)
	}
	if err := be.Delete(ctx, testRef(0)); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Delete = %v, want ErrClosed", err)
	}
	if _, err := be.Query(ctx, nil); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Query = %v, want ErrClosed", err)
	}
	if _, err := be.DeleteExpired(ctx, time.Now()); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("DeleteExpired = %v, want ErrClosed", err)
	}
	if _, err := be.Stats(ctx); !errors.Is(err, physical.ErrClosed) {
		t.Errorf("Stats = %v, want ErrClosed", err)
	}
}

func TestStats(t *testing.T) {
	be := newTestBackend(t)
	ctx := context.Background()

	stats, err := be.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.BackendType != "sqlite" {
		t.Errorf("BackendType = %s, want sqlite", stats.BackendType)
	}
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

var (
	benchSizes = []int{100, 1_000, 10_000}

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
	cfg := map[string]string{"path": filepath.Join(b.TempDir(), "bench.db")}
	be, err := NewFactory(context.Background(), cfg)
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

func BenchmarkQueryLabelAndTime(b *testing.B) {
	queryBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100), Limit: 100,
		}
	})
}

func BenchmarkQueryMultiLabelAndTime(b *testing.B) {
	queryBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100), Limit: 100,
		}
	})
}

func BenchmarkQueryDescending(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text"}, Limit: 100, Descending: true}
	})
}

func BenchmarkQueryMultiLabelDescending(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 100, Descending: true}
	})
}

func BenchmarkQueryThreeLabel(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"}, Limit: 100}
	})
}

func BenchmarkQueryPaginationBench(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				cursor := ""
				for {
					res, err := be.Query(ctx, &physical.QueryOptions{Limit: 100, Cursor: cursor})
					if err != nil {
						b.Fatal(err)
					}
					if !res.HasMore {
						break
					}
					cursor = res.NextCursor
				}
			}
		})
	}
}

func BenchmarkQueryPaginationWithLabel(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBench(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				cursor := ""
				for {
					res, err := be.Query(ctx, &physical.QueryOptions{
						Labels: map[string]string{"type": "text"}, Limit: 100, Cursor: cursor,
					})
					if err != nil {
						b.Fatal(err)
					}
					if !res.HasMore {
						break
					}
					cursor = res.NextCursor
				}
			}
		})
	}
}

func BenchmarkQueryLabelFilter(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			LabelFilter: &physical.LabelFilter{
				OR: []physical.LabelFilterGroup{
					{Predicates: []physical.LabelPredicate{{Key: "type", Value: "text"}}},
					{Predicates: []physical.LabelPredicate{{Key: "type", Value: "image"}}},
				},
			},
			Limit: 100,
		}
	})
}

func BenchmarkQueryLabelFilterMultiLabel(b *testing.B) {
	queryBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			LabelFilter: &physical.LabelFilter{
				OR: []physical.LabelFilterGroup{
					{Predicates: []physical.LabelPredicate{{Key: "type", Value: "text"}, {Key: "env", Value: "prod"}}},
					{Predicates: []physical.LabelPredicate{{Key: "type", Value: "image"}, {Key: "env", Value: "staging"}}},
				},
			},
			Limit: 100,
		}
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

func BenchmarkCountThreeLabel(b *testing.B) {
	countBench(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"}}
	})
}

func BenchmarkCountTimeRange(b *testing.B) {
	countBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			After: baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100),
		}
	})
}

func BenchmarkCountLabelAndTime(b *testing.B) {
	countBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100),
		}
	})
}

func BenchmarkCountMultiLabelAndTime(b *testing.B) {
	countBench(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100),
		}
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
