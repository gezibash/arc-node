package blobstore

import (
	"context"
	"errors"
	"io"
)

// BlobStore is the interface for content-addressed blob storage.
type BlobStore interface {
	// Put stores a blob and returns its CID (SHA-256 hash).
	Put(ctx context.Context, data []byte) ([32]byte, error)

	// PutStream stores a blob from a reader and returns its CID and size.
	PutStream(ctx context.Context, r io.Reader) (cid [32]byte, size int64, err error)

	// Get retrieves a blob by CID.
	Get(ctx context.Context, cid []byte) ([]byte, error)

	// GetReader returns a reader for a blob and its size.
	GetReader(ctx context.Context, cid []byte) (io.ReadCloser, int64, error)

	// Has checks if a blob exists.
	Has(ctx context.Context, cid []byte) (bool, error)

	// Size returns the size of a blob.
	Size(ctx context.Context, cid []byte) (int64, error)

	// Delete removes a blob.
	Delete(ctx context.Context, cid []byte) error

	// Close releases any resources.
	Close() error
}

// StoreStats contains storage usage metrics.
type StoreStats struct {
	BytesUsed int64 // total bytes stored
	BlobCount int64 // number of blobs
}

// Stater is optionally implemented by stores that can report usage stats.
type Stater interface {
	Stats(ctx context.Context) (StoreStats, error)
}

// Common errors.
var (
	ErrNotFound     = errors.New("blob not found")
	ErrInvalidCID   = errors.New("invalid content ID")
	ErrHashMismatch = errors.New("content hash mismatch")
	ErrClosed       = errors.New("store is closed")
)

// Backend names.
const (
	BackendFile   = "file"
	BackendMemory = "memory"
	BackendS3     = "s3"
	BackendBadger = "badger"
)
