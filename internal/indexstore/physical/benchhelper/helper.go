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

var (
	BenchSizes = []int{100, 1_000, 10_000, 100_000}

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

func RunQueryDescending(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	runQueryBench(b, newBackend, func(_ int) *physical.QueryOptions {
		return &physical.QueryOptions{
			Labels:     map[string]string{"type": "text"},
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

func RunCountVsQuery(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	opts := &physical.QueryOptions{
		Labels: map[string]string{"type": "text", "env": "prod"},
	}

	n := 10_000
	b.Run("Count", func(b *testing.B) {
		be := newBackend(b)
		SeedBackend(b, be, n)
		ctx := context.Background()

		b.ResetTimer()
		for range b.N {
			if _, err := be.Count(ctx, opts); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Query", func(b *testing.B) {
		be := newBackend(b)
		SeedBackend(b, be, n)
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

// RunAll runs all benchmark suites against the given backend constructor.
func RunAll(b *testing.B, newBackend func(b *testing.B) physical.Backend) {
	b.Run("Put", func(b *testing.B) { RunPut(b, newBackend(b)) })
	b.Run("PutBatch", func(b *testing.B) { RunPutBatch(b, newBackend) })
	b.Run("Get", func(b *testing.B) { RunGet(b, newBackend) })
	b.Run("QueryNoFilter", func(b *testing.B) { RunQueryNoFilter(b, newBackend) })
	b.Run("QuerySingleLabel", func(b *testing.B) { RunQuerySingleLabel(b, newBackend) })
	b.Run("QueryMultiLabel", func(b *testing.B) { RunQueryMultiLabel(b, newBackend) })
	b.Run("QueryTimeRange", func(b *testing.B) { RunQueryTimeRange(b, newBackend) })
	b.Run("QueryLabelAndTime", func(b *testing.B) { RunQueryLabelAndTime(b, newBackend) })
	b.Run("QueryDescending", func(b *testing.B) { RunQueryDescending(b, newBackend) })
	b.Run("QueryLabelFilter", func(b *testing.B) { RunQueryLabelFilter(b, newBackend) })
	b.Run("QueryPagination", func(b *testing.B) { RunQueryPagination(b, newBackend) })
	b.Run("Count", func(b *testing.B) { RunCount(b, newBackend) })
	b.Run("CountVsQuery", func(b *testing.B) { RunCountVsQuery(b, newBackend) })
	b.Run("DeleteExpired", func(b *testing.B) { RunDeleteExpired(b, newBackend) })
}
