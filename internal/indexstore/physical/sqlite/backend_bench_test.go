package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
)

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

const baseTimestamp = int64(1735689600000) // 2025-01-01T00:00:00Z in ms

func makeRef(seed int) reference.Reference {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seed))
	return reference.Reference(sha256.Sum256(buf[:]))
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

func seedBackend(b *testing.B, be physical.Backend, n int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()

	const batchSize = 1000
	batch := make([]*physical.Entry, 0, batchSize)

	for i := range n {
		var expiresAt int64
		if rng.Intn(2) == 0 {
			expiresAt = baseTimestamp + int64(rng.Intn(n/2))
		} else {
			expiresAt = baseTimestamp + int64(n) + int64(rng.Intn(n))
		}

		batch = append(batch, &physical.Entry{
			Ref: makeRef(i),
			Labels: map[string]string{
				"type": typeValues[rng.Intn(len(typeValues))],
				"env":  envValues[rng.Intn(len(envValues))],
				"user": userPool[rng.Intn(len(userPool))],
			},
			Timestamp: baseTimestamp + int64(i),
			ExpiresAt: expiresAt,
		})

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

func BenchmarkPut(b *testing.B) {
	be := newBenchBackend(b)
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &physical.Entry{
			Ref: makeRef(i),
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

func BenchmarkGet(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBackend(b, be, n)
			rng := rand.New(rand.NewSource(99))
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref := makeRef(rng.Intn(n))
				if _, err := be.Get(ctx, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkQueryNoFilter(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := be.Query(ctx, &physical.QueryOptions{Limit: 100}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkQuerySingleLabel(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := be.Query(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text"},
					Limit:  100,
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkQueryMultiLabel(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := be.Query(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text", "env": "prod"},
					Limit:  100,
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkQueryTimeRange(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBackend(b, be, n)
			ctx := context.Background()

			lo := baseTimestamp + int64(n*45/100)
			hi := baseTimestamp + int64(n*55/100)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := be.Query(ctx, &physical.QueryOptions{
					After:  lo,
					Before: hi,
					Limit:  100,
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkQueryLabelAndTime(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBackend(b, be, n)
			ctx := context.Background()

			lo := baseTimestamp + int64(n*45/100)
			hi := baseTimestamp + int64(n*55/100)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := be.Query(ctx, &physical.QueryOptions{
					Labels: map[string]string{"type": "text"},
					After:  lo,
					Before: hi,
					Limit:  100,
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkQueryPagination(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBackend(b, be, n)
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
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

func BenchmarkDeleteExpired(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()
			midpoint := time.UnixMilli(baseTimestamp + int64(n/2))

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				be := newBenchBackend(b)
				seedBackend(b, be, n)
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
