package badger

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
)

var (
	benchSizes = []int{100, 1_000, 10_000, 100_000}
	blobSizes  = []int{1 << 10, 10 << 10, 100 << 10, 1 << 20} // 1KB, 10KB, 100KB, 1MB
)

func makeRef(seed int) reference.Reference {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seed))
	return reference.Reference(sha256.Sum256(buf[:]))
}

func makeBlob(seed, size int) []byte {
	rng := rand.New(rand.NewSource(int64(seed)))
	data := make([]byte, size)
	rng.Read(data)
	return data
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

func seedBackend(b *testing.B, be physical.Backend, n int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	ctx := context.Background()

	for i := range n {
		size := blobSizes[rng.Intn(len(blobSizes))]
		if err := be.Put(ctx, makeRef(i), makeBlob(i, size)); err != nil {
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
		size := blobSizes[rng.Intn(len(blobSizes))]
		if err := be.Put(ctx, makeRef(i), makeBlob(i, size)); err != nil {
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

func BenchmarkExists(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			be := newBenchBackend(b)
			seedBackend(b, be, n)
			rng := rand.New(rand.NewSource(99))
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Alternate between hits (known ref) and misses (ref beyond seeded range).
				var ref reference.Reference
				if i%2 == 0 {
					ref = makeRef(rng.Intn(n))
				} else {
					ref = makeRef(n + i)
				}
				if _, err := be.Exists(ctx, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				be := newBenchBackend(b)
				seedBackend(b, be, n)
				b.StartTimer()

				for j := range n {
					if err := be.Delete(ctx, makeRef(j)); err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()
				be.Close()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkPutOverwrite(b *testing.B) {
	be := newBenchBackend(b)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	// Pre-populate keys that will be overwritten.
	const pool = 1000
	for i := range pool {
		size := blobSizes[rng.Intn(len(blobSizes))]
		if err := be.Put(ctx, makeRef(i), makeBlob(i, size)); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % pool
		size := blobSizes[rng.Intn(len(blobSizes))]
		if err := be.Put(ctx, makeRef(idx), makeBlob(idx+i, size)); err != nil {
			b.Fatal(err)
		}
	}
}
