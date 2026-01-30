package blobstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
	"github.com/gezibash/arc-node/internal/observability"
)

// BlobStore provides content-addressed binary storage.
type BlobStore struct {
	backend physical.Backend
	metrics *observability.Metrics
}

// New creates a new BlobStore with the given backend.
func New(backend physical.Backend, metrics *observability.Metrics) *BlobStore {
	return &BlobStore{
		backend: backend,
		metrics: metrics,
	}
}

// Store computes the content hash and stores the data.
// Returns the reference (content address) of the stored data.
func (s *BlobStore) Store(ctx context.Context, data []byte) (r reference.Reference, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "blobstore.store")
	defer op.End(err)

	r = reference.Compute(data)

	slog.DebugContext(ctx, "storing blob", "ref", reference.Hex(r), "size_bytes", len(data))

	if err = s.backend.Put(ctx, r, data); err != nil {
		return reference.Reference{}, fmt.Errorf("store blob: %w", err)
	}

	slog.InfoContext(ctx, "blob stored", "ref", reference.Hex(r), "size_bytes", len(data))
	return r, nil
}

// Fetch retrieves data by its reference.
// Returns ErrNotFound if the blob does not exist.
// Verifies data integrity on retrieval.
func (s *BlobStore) Fetch(ctx context.Context, r reference.Reference) (data []byte, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "blobstore.fetch")
	defer op.End(err)

	data, err = s.backend.Get(ctx, r)
	if errors.Is(err, physical.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("fetch blob: %w", err)
	}

	// Verify integrity
	computed := reference.Compute(data)
	if !reference.Equal(computed, r) {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrIntegrityMismatch, reference.Hex(r), reference.Hex(computed))
	}

	return data, nil
}

// Exists checks if a blob exists.
func (s *BlobStore) Exists(ctx context.Context, r reference.Reference) (exists bool, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "blobstore.exists")
	defer op.End(err)

	exists, err = s.backend.Exists(ctx, r)
	if err != nil {
		return false, fmt.Errorf("check blob exists: %w", err)
	}
	return exists, nil
}

// Delete removes a blob by its reference.
// Idempotent - returns nil if blob doesn't exist.
func (s *BlobStore) Delete(ctx context.Context, r reference.Reference) (err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "blobstore.delete")
	defer op.End(err)

	if err = s.backend.Delete(ctx, r); err != nil {
		return fmt.Errorf("delete blob: %w", err)
	}
	return nil
}

// Stats returns storage statistics.
func (s *BlobStore) Stats(ctx context.Context) (stats *physical.Stats, err error) {
	op, ctx := observability.StartOperation(ctx, s.metrics, "blobstore.stats")
	defer op.End(err)

	stats, err = s.backend.Stats(ctx)
	if err != nil {
		return nil, fmt.Errorf("get blob stats: %w", err)
	}
	return stats, nil
}

// ResolvePrefix resolves a hex reference prefix to a single full reference.
// Returns ErrPrefixTooShort if the prefix is less than 4 characters,
// ErrAmbiguousPrefix if multiple blobs match, or ErrNotFound if none match.
func (s *BlobStore) ResolvePrefix(ctx context.Context, hexPrefix string) (reference.Reference, error) {
	if len(hexPrefix) < 4 {
		return reference.Reference{}, ErrPrefixTooShort
	}
	scanner, ok := s.backend.(physical.PrefixScanner)
	if !ok {
		return reference.Reference{}, fmt.Errorf("backend does not support prefix resolution")
	}
	refs, err := scanner.ScanPrefix(ctx, strings.ToLower(hexPrefix), 2)
	if err != nil {
		return reference.Reference{}, fmt.Errorf("scan prefix: %w", err)
	}
	switch len(refs) {
	case 0:
		return reference.Reference{}, ErrNotFound
	case 1:
		return refs[0], nil
	default:
		return reference.Reference{}, ErrAmbiguousPrefix
	}
}

// Close releases resources associated with the BlobStore.
func (s *BlobStore) Close() error {
	slog.Info("closing blobstore")
	return s.backend.Close()
}
