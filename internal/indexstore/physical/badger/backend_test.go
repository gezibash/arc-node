package badger

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
	cfg := map[string]string{"path": t.TempDir()}
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

	// Insert 6 entries: 3 already expired (TTL ~1ms), 3 expiring in 1h.
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

	// Wait for the 1ms TTL to fire.
	time.Sleep(5 * time.Millisecond)

	// Badger handles expiration natively via WithTTL — DeleteExpired is a no-op.
	n, err := be.DeleteExpired(ctx, now)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if n != 0 {
		t.Errorf("deleted = %d, want 0 (badger TTL handles expiration)", n)
	}

	// Expired entries should be invisible to Get.
	for i := range 3 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		ref := reference.Reference(sha256.Sum256(buf[:]))
		_, err := be.Get(ctx, ref)
		if !errors.Is(err, physical.ErrNotFound) {
			t.Errorf("Get(expired ref %d) = %v, want ErrNotFound", i, err)
		}
	}

	// Non-expired entries should still be visible via Get.
	for i := 3; i < 6; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(i))
		ref := reference.Reference(sha256.Sum256(buf[:]))
		got, err := be.Get(ctx, ref)
		if err != nil {
			t.Fatalf("Get(valid ref %d): %v", i, err)
		}
		if got.Labels["i"] != "v" {
			t.Errorf("Get(valid ref %d) labels = %v, want i=v", i, got.Labels)
		}
	}

	// Expired entries should be invisible to queries.
	result, err := be.Query(ctx, &physical.QueryOptions{
		Labels: map[string]string{"i": "v"},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Errorf("visible entries = %d, want 3 (expired entries should be hidden)", len(result.Entries))
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
	baseTimestamp = int64(1735689600000) // 2025-01-01T00:00:00Z in ms
	farFuture     = int64(2051222400000) // ~2035-01-01
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
	cfg := map[string]string{"path": b.TempDir()}
	be, err := NewFactory(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { be.Close() })
	return be
}

func registerComposite2(b *testing.B, be physical.Backend) bool {
	b.Helper()
	ci, ok := be.(physical.CompositeIndexer)
	if !ok {
		return false
	}
	if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
		Name: "type_env",
		Keys: []string{"type", "env"},
	}); err != nil {
		b.Fatal(err)
	}
	return true
}

func registerComposite3(b *testing.B, be physical.Backend) bool {
	b.Helper()
	ci, ok := be.(physical.CompositeIndexer)
	if !ok {
		return false
	}
	if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
		Name: "type_env_user",
		Keys: []string{"type", "env", "user"},
	}); err != nil {
		b.Fatal(err)
	}
	return true
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

func queryBenchComposite(b *testing.B, sizes []int, opts func(n int) *physical.QueryOptions) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			if !registerComposite2(b, be) {
				b.Skip("backend does not support composite indexes")
			}
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

func countBenchComposite(b *testing.B, sizes []int, opts func(n int) *physical.QueryOptions) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			if !registerComposite2(b, be) {
				b.Skip("backend does not support composite indexes")
			}
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

func BenchmarkPutWithComposite(b *testing.B) {
	for _, name := range []string{"NoComposite", "Composite2", "Composite3"} {
		b.Run(name, func(b *testing.B) {
			be := newBenchBackend(b)
			switch name {
			case "Composite2":
				if !registerComposite2(b, be) {
					b.Skip("no composite support")
				}
			case "Composite3":
				if !registerComposite3(b, be) {
					b.Skip("no composite support")
				}
			}

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
		})
	}
}

func BenchmarkPutCompositeScaling(b *testing.B) {
	labelKeys := make([]string, 10)
	for i := range labelKeys {
		labelKeys[i] = fmt.Sprintf("lbl%02d", i)
	}
	labelVals := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	makeEntry := func(rng *rand.Rand, i int) *physical.Entry {
		labels := make(map[string]string, len(labelKeys))
		for _, k := range labelKeys {
			labels[k] = labelVals[rng.Intn(len(labelVals))]
		}
		return &physical.Entry{
			Ref:       benchRef(i),
			Labels:    labels,
			Timestamp: baseTimestamp + int64(i),
			ExpiresAt: baseTimestamp + int64(i) + 1000,
		}
	}

	type pair struct{ a, b int }
	var allPairs []pair
	for i := 0; i < len(labelKeys); i++ {
		for j := i + 1; j < len(labelKeys); j++ {
			allPairs = append(allPairs, pair{i, j})
		}
	}

	for _, count := range []int{0, 10, 45, 100, 500} {
		b.Run(fmt.Sprintf("composites=%d", count), func(b *testing.B) {
			be := newBenchBackend(b)
			ci, ok := be.(physical.CompositeIndexer)
			if !ok && count > 0 {
				b.Skip("backend does not support composite indexes")
			}

			registered := 0
			for registered < count && registered < len(allPairs) {
				p := allPairs[registered]
				if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
					Name: fmt.Sprintf("c2_%d_%d", p.a, p.b),
					Keys: []string{labelKeys[p.a], labelKeys[p.b]},
				}); err != nil {
					b.Fatal(err)
				}
				registered++
			}
			for i := 0; registered < count && i < len(labelKeys); i++ {
				for j := i + 1; registered < count && j < len(labelKeys); j++ {
					for k := j + 1; registered < count && k < len(labelKeys); k++ {
						if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
							Name: fmt.Sprintf("c3_%d_%d_%d", i, j, k),
							Keys: []string{labelKeys[i], labelKeys[j], labelKeys[k]},
						}); err != nil {
							b.Fatal(err)
						}
						registered++
					}
				}
			}
			for i := 0; registered < count && i < len(labelKeys); i++ {
				for j := i + 1; registered < count && j < len(labelKeys); j++ {
					for k := j + 1; registered < count && k < len(labelKeys); k++ {
						for l := k + 1; registered < count && l < len(labelKeys); l++ {
							if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
								Name: fmt.Sprintf("c4_%d_%d_%d_%d", i, j, k, l),
								Keys: []string{labelKeys[i], labelKeys[j], labelKeys[k], labelKeys[l]},
							}); err != nil {
								b.Fatal(err)
							}
							registered++
						}
					}
				}
			}

			rng := rand.New(rand.NewSource(42))
			ctx := context.Background()
			b.ResetTimer()
			for i := range b.N {
				if err := be.Put(ctx, makeEntry(rng, i)); err != nil {
					b.Fatal(err)
				}
			}
		})
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
// Query — no composite
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

// ---------------------------------------------------------------------------
// Query — composite
// ---------------------------------------------------------------------------

func BenchmarkQueryMultiLabelComposite(b *testing.B) {
	queryBenchComposite(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 100}
	})
}

func BenchmarkQueryMultiLabelAndTimeComposite(b *testing.B) {
	queryBenchComposite(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100), Limit: 100,
		}
	})
}

func BenchmarkQueryMultiLabelDescendingComposite(b *testing.B) {
	queryBenchComposite(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}, Limit: 100, Descending: true}
	})
}

func BenchmarkQueryLabelFilterMultiLabelComposite(b *testing.B) {
	queryBenchComposite(b, benchSizes, func(_ int) *physical.QueryOptions {
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

func BenchmarkQueryThreeLabelComposite(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			if !registerComposite3(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			seedBench(b, be, n)
			ctx := context.Background()

			qo := &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"}, Limit: 100}
			b.ResetTimer()
			for range b.N {
				if _, err := be.Query(ctx, qo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Count — no composite
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
// Count — composite
// ---------------------------------------------------------------------------

func BenchmarkCountMultiLabelComposite(b *testing.B) {
	countBenchComposite(b, benchSizes, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}}
	})
}

func BenchmarkCountThreeLabelComposite(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			if !registerComposite3(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			seedBench(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"},
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCountMultiLabelAndTimeComposite(b *testing.B) {
	countBenchComposite(b, benchSizes, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  baseTimestamp + int64(n*45/100), Before: baseTimestamp + int64(n*55/100),
		}
	})
}

// ---------------------------------------------------------------------------
// Count vs Query
// ---------------------------------------------------------------------------

func BenchmarkCountVsQuery(b *testing.B) {
	opts := &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}}
	n := 10_000

	for _, variant := range []string{"NoComposite", "Composite"} {
		b.Run(variant, func(b *testing.B) {
			runCountVsQueryVariant(b, opts, n, variant == "Composite")
		})
	}
}

func BenchmarkCountVsQueryScaling(b *testing.B) {
	opts := &physical.QueryOptions{Labels: map[string]string{"type": "text", "env": "prod"}}

	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d/NoComposite", n), func(b *testing.B) {
			runCountVsQueryVariant(b, opts, n, false)
		})
		b.Run(fmt.Sprintf("n=%d/Composite", n), func(b *testing.B) {
			runCountVsQueryVariant(b, opts, n, true)
		})
	}
}

func runCountVsQueryVariant(b *testing.B, opts *physical.QueryOptions, n int, composite bool) {
	b.Helper()

	setup := func(b *testing.B) physical.Backend {
		b.Helper()
		be := newBenchBackend(b)
		if composite {
			if ci, ok := be.(physical.CompositeIndexer); ok {
				if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
					Name: "type_env", Keys: []string{"type", "env"},
				}); err != nil {
					b.Fatal(err)
				}
			} else {
				b.Skip("backend does not support composite indexes")
			}
		}
		seedBench(b, be, n)
		return be
	}

	b.Run("Count", func(b *testing.B) {
		be := setup(b)
		ctx := context.Background()
		b.ResetTimer()
		for range b.N {
			if _, err := be.Count(ctx, opts); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Query", func(b *testing.B) {
		be := setup(b)
		ctx := context.Background()
		b.ResetTimer()
		for range b.N {
			if _, err := be.Query(ctx, &physical.QueryOptions{Labels: opts.Labels, Limit: 1_000_000}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Backfill
// ---------------------------------------------------------------------------

func BenchmarkBackfill(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()

			for range b.N {
				b.StopTimer()
				be := newBenchBackend(b)
				seedBench(b, be, n)

				ci, ok := be.(physical.CompositeIndexer)
				if !ok {
					b.Skip("backend does not support composite indexes")
				}
				if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
					Name: "type_env", Keys: []string{"type", "env"},
				}); err != nil {
					b.Fatal(err)
				}

				bf, ok := be.(physical.Backfiller)
				if !ok {
					b.Skip("backend does not support backfill")
				}
				b.StartTimer()

				if _, err := bf.BackfillCompositeIndex(ctx, physical.CompositeIndexDef{
					Name: "type_env", Keys: []string{"type", "env"},
				}); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				be.Close()
				b.StartTimer()
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DeleteExpired
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
