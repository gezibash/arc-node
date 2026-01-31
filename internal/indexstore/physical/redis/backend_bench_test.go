//go:build integration

package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/indexstore/physical/benchhelper"
)

func newBenchBackend(b *testing.B) physical.Backend {
	b.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	prefix := fmt.Sprintf("bench-%d-", time.Now().UnixNano())
	cfg := map[string]string{
		"addr":       addr,
		"db":         "15",
		"key_prefix": prefix,
	}
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
