// Package benchhelper provides shared benchmark scaffolding for indexstore backends.
package benchhelper

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

// Config controls benchmark parameters per backend.
type Config struct {
	// Sizes is the list of dataset sizes to benchmark. Defaults to DefaultSizes.
	Sizes []int
}

var (
	DefaultSizes = []int{100, 1_000, 10_000, 100_000}

	// BenchSizes is the active sizes for the current run. Set via RunAll config
	// or defaults to DefaultSizes.
	BenchSizes = DefaultSizes

	TypeValues = []string{"text", "image", "video", "audio", "document", "spreadsheet", "presentation", "archive", "code", "binary"}
	EnvValues  = []string{"prod", "staging", "dev", "test", "ci"}
	UserPool   = func() []string {
		u := make([]string, 100)
		for i := range u {
			u[i] = fmt.Sprintf("user-%03d", i)
		}
		return u
	}()
)

const BaseTimestamp = int64(1735689600000) // 2025-01-01T00:00:00Z in ms

func MakeRef(seed int) reference.Reference {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seed))
	return reference.Reference(sha256.Sum256(buf[:]))
}

// FarFuture is a millisecond timestamp far enough in the future that entries
// won't expire during benchmarks (~2035-01-01).
const FarFuture = int64(2051222400000)

// MakeEntry creates a benchmark entry. All entries have far-future expiry
// so they survive in backends with native TTL (e.g. badger).
func MakeEntry(rng *rand.Rand, i int) *physical.Entry {
	return &physical.Entry{
		Ref: MakeRef(i),
		Labels: map[string]string{
			"type": TypeValues[rng.Intn(len(TypeValues))],
			"env":  EnvValues[rng.Intn(len(EnvValues))],
			"user": UserPool[rng.Intn(len(UserPool))],
		},
		Timestamp: BaseTimestamp + int64(i),
		ExpiresAt: FarFuture + int64(i),
	}
}

// MakeEntryWithExpiry creates an entry where half expire at midpoint (for DeleteExpired benchmarks).
func MakeEntryWithExpiry(rng *rand.Rand, i int, n int) *physical.Entry {
	var expiresAt int64
	if rng.Intn(2) == 0 {
		// Expires at midpoint — will be cleaned up by DeleteExpired.
		expiresAt = BaseTimestamp + int64(rng.Intn(n/2))
	} else {
		// Far future.
		expiresAt = FarFuture + int64(rng.Intn(n))
	}

	return &physical.Entry{
		Ref: MakeRef(i),
		Labels: map[string]string{
			"type": TypeValues[rng.Intn(len(TypeValues))],
			"env":  EnvValues[rng.Intn(len(EnvValues))],
			"user": UserPool[rng.Intn(len(UserPool))],
		},
		Timestamp: BaseTimestamp + int64(i),
		ExpiresAt: expiresAt,
	}
}

func SeedBackend(b *testing.B, be physical.Backend, n int) {
	b.Helper()
	seedBackendWith(b, be, n, false)
}

func SeedBackendWithExpiry(b *testing.B, be physical.Backend, n int) {
	b.Helper()
	seedBackendWith(b, be, n, true)
}

func seedBackendWith(b *testing.B, be physical.Backend, n int, withExpiry bool) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()

	const batchSize = 1000
	batch := make([]*physical.Entry, 0, batchSize)

	for i := range n {
		if withExpiry {
			batch = append(batch, MakeEntryWithExpiry(rng, i, n))
		} else {
			batch = append(batch, MakeEntry(rng, i))
		}

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

// registerComposite2 registers the type+env composite index if the backend supports it.
// Returns true if registration succeeded.
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

// registerComposite3 registers the type+env+user composite index.
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

// ---------------------------------------------------------------------------
// Put benchmarks
// ---------------------------------------------------------------------------

func RunPut(b *testing.B, be physical.Backend) {
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()

	b.ResetTimer()
	for i := range b.N {
		entry := &physical.Entry{
			Ref: MakeRef(i),
			Labels: map[string]string{
				"type": TypeValues[rng.Intn(len(TypeValues))],
				"env":  EnvValues[rng.Intn(len(EnvValues))],
				"user": UserPool[rng.Intn(len(UserPool))],
			},
			Timestamp: BaseTimestamp + int64(i),
			ExpiresAt: BaseTimestamp + int64(i) + 1000,
		}
		if err := be.Put(ctx, entry); err != nil {
			b.Fatal(err)
		}
	}
}

func RunPutWithComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	b.Run("NoComposite", func(b *testing.B) {
		be := newBackend(b)
		RunPut(b, be)
	})
	b.Run("Composite2", func(b *testing.B) {
		be := newBackend(b)
		if !registerComposite2(b, be) {
			b.Skip("backend does not support composite indexes")
		}
		RunPut(b, be)
	})
	b.Run("Composite3", func(b *testing.B) {
		be := newBackend(b)
		if !registerComposite3(b, be) {
			b.Skip("backend does not support composite indexes")
		}
		RunPut(b, be)
	})
}

// RunPutCompositeScaling measures Put overhead as the number of registered
// composite indexes grows. Each entry has 10 label keys; composites are
// 2-key combinations that all match, so every Put writes N cidx keys.
func RunPutCompositeScaling(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	// 10 label keys used by entries in this benchmark.
	labelKeys := make([]string, 10)
	for i := range labelKeys {
		labelKeys[i] = fmt.Sprintf("lbl%02d", i)
	}
	// 5 possible values per key.
	labelVals := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	makeEntry := func(rng *rand.Rand, i int) *physical.Entry {
		labels := make(map[string]string, len(labelKeys))
		for _, k := range labelKeys {
			labels[k] = labelVals[rng.Intn(len(labelVals))]
		}
		return &physical.Entry{
			Ref:       MakeRef(i),
			Labels:    labels,
			Timestamp: BaseTimestamp + int64(i),
			ExpiresAt: BaseTimestamp + int64(i) + 1000,
		}
	}

	// Generate all unique 2-key combinations from labelKeys.
	// C(10,2) = 45 pairs. We take the first N for each sub-benchmark.
	type pair struct{ a, b int }
	var allPairs []pair
	for i := 0; i < len(labelKeys); i++ {
		for j := i + 1; j < len(labelKeys); j++ {
			allPairs = append(allPairs, pair{i, j})
		}
	}

	for _, count := range []int{0, 10, 45, 100, 500} {
		b.Run(fmt.Sprintf("composites=%d", count), func(b *testing.B) {
			be := newBackend(b)
			ci, ok := be.(physical.CompositeIndexer)
			if !ok && count > 0 {
				b.Skip("backend does not support composite indexes")
			}

			// Register composites. For counts > 45 we generate
			// synthetic wider combinations.
			registered := 0
			// First use the real 2-key pairs.
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
			// Then 3-key combos.
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
			// Then 4-key combos if we still need more.
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

func RunPutBatch(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			be := newBackend(b)
			rng := rand.New(rand.NewSource(42))
			ctx := context.Background()

			b.ResetTimer()
			for i := range b.N {
				batch := make([]*physical.Entry, batchSize)
				for j := range batchSize {
					idx := i*batchSize + j
					batch[j] = &physical.Entry{
						Ref: MakeRef(idx),
						Labels: map[string]string{
							"type": TypeValues[rng.Intn(len(TypeValues))],
							"env":  EnvValues[rng.Intn(len(EnvValues))],
							"user": UserPool[rng.Intn(len(UserPool))],
						},
						Timestamp: BaseTimestamp + int64(idx),
						ExpiresAt: BaseTimestamp + int64(idx) + 1000,
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

func RunGet(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			rng := rand.New(rand.NewSource(99))
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				ref := MakeRef(rng.Intn(n))
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

func runQueryBench(b *testing.B, newBackend func(b *testing.B) physical.Backend, opts func(n int) *physical.QueryOptions) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
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

func runQueryBenchComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend, opts func(n int) *physical.QueryOptions) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			if !registerComposite2(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			SeedBackend(b, be, n)
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

func RunQueryNoFilter(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{Limit: 100}
	})
}

func RunQuerySingleLabel(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text"},
			Limit:  100,
		}
	})
}

func RunQueryMultiLabel(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			Limit:  100,
		}
	})
}

func RunQueryMultiLabelComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBenchComposite(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			Limit:  100,
		}
	})
}

func RunQueryTimeRange(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			After:  BaseTimestamp + int64(n*45/100),
			Before: BaseTimestamp + int64(n*55/100),
			Limit:  100,
		}
	})
}

func RunQueryLabelAndTime(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text"},
			After:  BaseTimestamp + int64(n*45/100),
			Before: BaseTimestamp + int64(n*55/100),
			Limit:  100,
		}
	})
}

func RunQueryMultiLabelAndTime(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  BaseTimestamp + int64(n*45/100),
			Before: BaseTimestamp + int64(n*55/100),
			Limit:  100,
		}
	})
}

func RunQueryMultiLabelAndTimeComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBenchComposite(b, newBackend, func(n int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod"},
			After:  BaseTimestamp + int64(n*45/100),
			Before: BaseTimestamp + int64(n*55/100),
			Limit:  100,
		}
	})
}

func RunQueryDescending(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels:     map[string]string{"type": "text"},
			Limit:      100,
			Descending: true,
		}
	})
}

func RunQueryMultiLabelDescending(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels:     map[string]string{"type": "text", "env": "prod"},
			Limit:      100,
			Descending: true,
		}
	})
}

func RunQueryMultiLabelDescendingComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBenchComposite(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels:     map[string]string{"type": "text", "env": "prod"},
			Limit:      100,
			Descending: true,
		}
	})
}

func RunQueryLabelFilter(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
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

func RunQueryLabelFilterMultiLabel(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			LabelFilter: &physical.LabelFilter{
				OR: []physical.LabelFilterGroup{
					{Predicates: []physical.LabelPredicate{
						{Key: "type", Value: "text"},
						{Key: "env", Value: "prod"},
					}},
					{Predicates: []physical.LabelPredicate{
						{Key: "type", Value: "image"},
						{Key: "env", Value: "staging"},
					}},
				},
			},
			Limit: 100,
		}
	})
}

func RunQueryLabelFilterMultiLabelComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBenchComposite(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			LabelFilter: &physical.LabelFilter{
				OR: []physical.LabelFilterGroup{
					{Predicates: []physical.LabelPredicate{
						{Key: "type", Value: "text"},
						{Key: "env", Value: "prod"},
					}},
					{Predicates: []physical.LabelPredicate{
						{Key: "type", Value: "image"},
						{Key: "env", Value: "staging"},
					}},
				},
			},
			Limit: 100,
		}
	})
}

func RunQueryThreeLabel(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"},
			Limit:  100,
		}
	})
}

func RunQueryThreeLabelComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			if !registerComposite3(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			SeedBackend(b, be, n)
			ctx := context.Background()

			qo := &physical.QueryOptions{
				Labels: map[string]string{"type": "text", "env": "prod", "user": "user-001"},
				Limit:  100,
			}
			b.ResetTimer()
			for range b.N {
				if _, err := be.Query(ctx, qo); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func RunQueryPagination(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				cursor := ""
				for {
					res, err := be.Query(ctx, &physical.QueryOptions{
						Limit:  100,
						Cursor: cursor,
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

func RunQueryPaginationWithLabel(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				cursor := ""
				for {
					res, err := be.Query(ctx, &physical.QueryOptions{
						Labels: map[string]string{"type": "text"},
						Limit:  100,
						Cursor: cursor,
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
// Count benchmarks
// ---------------------------------------------------------------------------

func RunCountNoFilter(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func RunCount(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text"},
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func RunCountMultiLabel(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text", "env": "prod"},
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func RunCountMultiLabelComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			if !registerComposite2(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text", "env": "prod"},
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func RunCountThreeLabel(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
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

func RunCountThreeLabelComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			if !registerComposite3(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			SeedBackend(b, be, n)
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

func RunCountTimeRange(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					After:  BaseTimestamp + int64(n*45/100),
					Before: BaseTimestamp + int64(n*55/100),
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func RunCountLabelAndTime(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text"},
					After:  BaseTimestamp + int64(n*45/100),
					Before: BaseTimestamp + int64(n*55/100),
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func RunCountMultiLabelAndTime(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text", "env": "prod"},
					After:  BaseTimestamp + int64(n*45/100),
					Before: BaseTimestamp + int64(n*55/100),
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func RunCountMultiLabelAndTimeComposite(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBackend(b)
			if !registerComposite2(b, be) {
				b.Skip("backend does not support composite indexes")
			}
			SeedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				if _, err := be.Count(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text", "env": "prod"},
					After:  BaseTimestamp + int64(n*45/100),
					Before: BaseTimestamp + int64(n*55/100),
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CountVsQuery benchmarks (composite vs non-composite, multiple sizes)
// ---------------------------------------------------------------------------

func RunCountVsQuery(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	opts := &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
	}
	n := 10_000

	b.Run("NoComposite", func(b *testing.B) {
		runCountVsQueryVariant(b, newBackend, opts, n, false)
	})
	b.Run("Composite", func(b *testing.B) {
		runCountVsQueryVariant(b, newBackend, opts, n, true)
	})
}

func RunCountVsQueryScaling(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	opts := &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
	}

	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d/NoComposite", n), func(b *testing.B) {
			runCountVsQueryVariant(b, newBackend, opts, n, false)
		})
		b.Run(fmt.Sprintf("n=%d/Composite", n), func(b *testing.B) {
			runCountVsQueryVariant(b, newBackend, opts, n, true)
		})
	}
}

func runCountVsQueryVariant(b *testing.B, newBackend func(b *testing.B) physical.Backend, opts *physical.QueryOptions, n int, composite bool) {
	b.Helper()

	setup := func(b *testing.B) physical.Backend {
		b.Helper()
		be := newBackend(b)
		if composite {
			if ci, ok := be.(physical.CompositeIndexer); ok {
				if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
					Name: "type_env",
					Keys: []string{"type", "env"},
				}); err != nil {
					b.Fatal(err)
				}
			} else {
				b.Skip("backend does not support composite indexes")
			}
		}
		SeedBackend(b, be, n)
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
			if _, err := be.Query(ctx, &physical.QueryOptions{
				Labels: opts.Labels,
				Limit:  1_000_000,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Backfill benchmarks
// ---------------------------------------------------------------------------

func RunBackfill(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()

			for range b.N {
				b.StopTimer()
				be := newBackend(b)
				SeedBackend(b, be, n)

				ci, ok := be.(physical.CompositeIndexer)
				if !ok {
					b.Skip("backend does not support composite indexes")
				}
				if err := ci.RegisterCompositeIndex(physical.CompositeIndexDef{
					Name: "type_env",
					Keys: []string{"type", "env"},
				}); err != nil {
					b.Fatal(err)
				}

				bf, ok := be.(physical.Backfiller)
				if !ok {
					b.Skip("backend does not support backfill")
				}
				b.StartTimer()

				if _, err := bf.BackfillCompositeIndex(ctx, physical.CompositeIndexDef{
					Name: "type_env",
					Keys: []string{"type", "env"},
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
// DeleteExpired benchmarks
// ---------------------------------------------------------------------------

func RunDeleteExpired(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	for _, n := range BenchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()
			midpoint := time.UnixMilli(BaseTimestamp + int64(n/2))

			for range b.N {
				b.StopTimer()
				be := newBackend(b)
				SeedBackendWithExpiry(b, be, n)
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

// ---------------------------------------------------------------------------
// RunAll
// ---------------------------------------------------------------------------

// RunAll runs all benchmark suites against the given backend constructor.
// An optional Config controls dataset sizes; nil uses DefaultSizes.
func RunAll(b *testing.B, newBackend func(b *testing.B) physical.Backend, cfgs ...*Config) {
	if len(cfgs) > 0 && cfgs[0] != nil && len(cfgs[0].Sizes) > 0 {
		BenchSizes = cfgs[0].Sizes
	} else {
		BenchSizes = DefaultSizes
	}
	// Write
	b.Run("Put", func(b *testing.B) { RunPut(b, newBackend(b)) })
	b.Run("PutWithComposite", func(b *testing.B) { RunPutWithComposite(b, newBackend) })
	b.Run("PutCompositeScaling", func(b *testing.B) { RunPutCompositeScaling(b, newBackend) })
	b.Run("PutBatch", func(b *testing.B) { RunPutBatch(b, newBackend) })

	// Read
	b.Run("Get", func(b *testing.B) { RunGet(b, newBackend) })

	// Query — no composite
	b.Run("QueryNoFilter", func(b *testing.B) { RunQueryNoFilter(b, newBackend) })
	b.Run("QuerySingleLabel", func(b *testing.B) { RunQuerySingleLabel(b, newBackend) })
	b.Run("QueryMultiLabel", func(b *testing.B) { RunQueryMultiLabel(b, newBackend) })
	b.Run("QueryTimeRange", func(b *testing.B) { RunQueryTimeRange(b, newBackend) })
	b.Run("QueryLabelAndTime", func(b *testing.B) { RunQueryLabelAndTime(b, newBackend) })
	b.Run("QueryMultiLabelAndTime", func(b *testing.B) { RunQueryMultiLabelAndTime(b, newBackend) })
	b.Run("QueryDescending", func(b *testing.B) { RunQueryDescending(b, newBackend) })
	b.Run("QueryMultiLabelDescending", func(b *testing.B) { RunQueryMultiLabelDescending(b, newBackend) })
	b.Run("QueryLabelFilter", func(b *testing.B) { RunQueryLabelFilter(b, newBackend) })
	b.Run("QueryLabelFilterMultiLabel", func(b *testing.B) { RunQueryLabelFilterMultiLabel(b, newBackend) })
	b.Run("QueryThreeLabel", func(b *testing.B) { RunQueryThreeLabel(b, newBackend) })
	b.Run("QueryPagination", func(b *testing.B) { RunQueryPagination(b, newBackend) })
	b.Run("QueryPaginationWithLabel", func(b *testing.B) { RunQueryPaginationWithLabel(b, newBackend) })

	// Query — composite
	b.Run("QueryMultiLabelComposite", func(b *testing.B) { RunQueryMultiLabelComposite(b, newBackend) })
	b.Run("QueryMultiLabelAndTimeComposite", func(b *testing.B) { RunQueryMultiLabelAndTimeComposite(b, newBackend) })
	b.Run("QueryMultiLabelDescendingComposite", func(b *testing.B) { RunQueryMultiLabelDescendingComposite(b, newBackend) })
	b.Run("QueryLabelFilterMultiLabelComposite", func(b *testing.B) { RunQueryLabelFilterMultiLabelComposite(b, newBackend) })
	b.Run("QueryThreeLabelComposite", func(b *testing.B) { RunQueryThreeLabelComposite(b, newBackend) })

	// Count — no composite
	b.Run("CountNoFilter", func(b *testing.B) { RunCountNoFilter(b, newBackend) })
	b.Run("Count", func(b *testing.B) { RunCount(b, newBackend) })
	b.Run("CountMultiLabel", func(b *testing.B) { RunCountMultiLabel(b, newBackend) })
	b.Run("CountThreeLabel", func(b *testing.B) { RunCountThreeLabel(b, newBackend) })
	b.Run("CountTimeRange", func(b *testing.B) { RunCountTimeRange(b, newBackend) })
	b.Run("CountLabelAndTime", func(b *testing.B) { RunCountLabelAndTime(b, newBackend) })
	b.Run("CountMultiLabelAndTime", func(b *testing.B) { RunCountMultiLabelAndTime(b, newBackend) })

	// Count — composite
	b.Run("CountMultiLabelComposite", func(b *testing.B) { RunCountMultiLabelComposite(b, newBackend) })
	b.Run("CountThreeLabelComposite", func(b *testing.B) { RunCountThreeLabelComposite(b, newBackend) })
	b.Run("CountMultiLabelAndTimeComposite", func(b *testing.B) { RunCountMultiLabelAndTimeComposite(b, newBackend) })

	// Count vs Query head-to-head
	b.Run("CountVsQuery", func(b *testing.B) { RunCountVsQuery(b, newBackend) })
	b.Run("CountVsQueryScaling", func(b *testing.B) { RunCountVsQueryScaling(b, newBackend) })

	// Backfill
	b.Run("Backfill", func(b *testing.B) { RunBackfill(b, newBackend) })

	// Cleanup
	b.Run("DeleteExpired", func(b *testing.B) { RunDeleteExpired(b, newBackend) })
}
