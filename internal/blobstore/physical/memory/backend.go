// Package memory provides an in-memory blob storage backend for testing.
package memory

import (
	"context"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
	"github.com/gezibash/arc-node/internal/blobstore/physical/badger"
)

func init() {
	physical.Register("memory", NewFactory, Defaults)
}

// Defaults returns the default configuration for the memory backend.
func Defaults() map[string]string {
	return map[string]string{
		badger.KeyInMemory: "true",
	}
}

// NewFactory creates a new in-memory backend using BadgerDB's in-memory mode.
func NewFactory(ctx context.Context, config map[string]string) (physical.Backend, error) {
	config[badger.KeyInMemory] = "true"
	return badger.NewFactory(ctx, config)
}
