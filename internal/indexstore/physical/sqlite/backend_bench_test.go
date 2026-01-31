package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/indexstore/physical/benchhelper"
)

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

func BenchmarkAll(b *testing.B) {
	benchhelper.RunAll(b, newBenchBackend)
}
