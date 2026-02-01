// Package physical provides the physical storage backend interface for blob storage.
package physical

import (
	"context"
	"errors"

	"github.com/gezibash/arc/v2/pkg/reference"
)

var (
	// ErrNotFound indicates the requested blob was not found.
	ErrNotFound = errors.New("blob not found")

	// ErrClosed indicates the backend has been closed.
	ErrClosed = errors.New("backend closed")
)

// Stats contains storage statistics.
type Stats struct {
	SizeBytes   int64
	BackendType string
}

// PrefixScanner is an optional interface for backends that support
// scanning blobs by hex reference prefix.
type PrefixScanner interface {
	ScanPrefix(ctx context.Context, hexPrefix string, limit int) ([]reference.Reference, error)
}

// Backend is the physical storage interface for blob storage.
// All implementations must be thread-safe.
type Backend interface {
	Put(ctx context.Context, r reference.Reference, data []byte) error
	Get(ctx context.Context, r reference.Reference) ([]byte, error)
	Exists(ctx context.Context, r reference.Reference) (bool, error)
	Delete(ctx context.Context, r reference.Reference) error
	Stats(ctx context.Context) (*Stats, error)
	Close() error
}
