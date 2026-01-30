// Package node provides initialization helpers for the arc node.
package node

import (
	"context"
	"fmt"

	"github.com/gezibash/arc-node/internal/blobstore"
	blobphysical "github.com/gezibash/arc-node/internal/blobstore/physical"
	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/indexstore"
	indexphysical "github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/observability"

	// Register blob backends
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/badger"
	_ "github.com/gezibash/arc-node/internal/blobstore/physical/memory"

	// Register index backends
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/badger"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/memory"
	_ "github.com/gezibash/arc-node/internal/indexstore/physical/redis"
)

// NewBlobStore creates a BlobStore from configuration.
func NewBlobStore(ctx context.Context, cfg *config.BackendConfig, metrics *observability.Metrics) (*blobstore.BlobStore, error) {
	backend, err := blobphysical.New(ctx, cfg.Backend, cfg.Config, metrics)
	if err != nil {
		return nil, fmt.Errorf("create blob backend: %w", err)
	}
	return blobstore.New(backend, metrics), nil
}

// NewIndexStore creates an IndexStore from configuration.
func NewIndexStore(ctx context.Context, cfg *config.BackendConfig, metrics *observability.Metrics) (*indexstore.IndexStore, error) {
	backend, err := indexphysical.New(ctx, cfg.Backend, cfg.Config, metrics)
	if err != nil {
		return nil, fmt.Errorf("create index backend: %w", err)
	}
	store, err := indexstore.New(backend, metrics)
	if err != nil {
		_ = backend.Close()
		return nil, fmt.Errorf("create index store: %w", err)
	}
	return store, nil
}
