package memory

import (
	"context"
	"testing"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/indexstore/physical/benchhelper"
)

func newBenchBackend(b *testing.B) physical.Backend {
	b.Helper()
	be, err := NewFactory(context.Background(), map[string]string{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { be.Close() })
	return be
}

func BenchmarkAll(b *testing.B) {
	benchhelper.RunAll(b, newBenchBackend)
}
