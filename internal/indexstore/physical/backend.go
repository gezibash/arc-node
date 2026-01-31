// Package physical provides the physical storage backend interface for index storage.
package physical

import (
	"context"
	"errors"
	"time"

	"github.com/gezibash/arc/pkg/reference"
)

var (
	// ErrNotFound indicates the requested entry was not found.
	ErrNotFound = errors.New("entry not found")

	// ErrClosed indicates the backend has been closed.
	ErrClosed = errors.New("backend closed")
)

// Entry represents an indexed entry in storage.
type Entry struct {
	Ref       reference.Reference
	Labels    map[string]string
	Timestamp int64
	ExpiresAt int64
}

// QueryOptions specifies filtering and pagination for queries.
type QueryOptions struct {
	Labels         map[string]string
	After          int64
	Before         int64
	Limit          int
	Cursor         string
	Descending     bool
	IncludeExpired bool
}

// QueryResult contains the results of a query.
type QueryResult struct {
	Entries    []*Entry
	NextCursor string
	HasMore    bool
}

// Stats contains storage statistics.
type Stats struct {
	SizeBytes   int64
	BackendType string
}

// PrefixScanner is an optional interface for backends that support
// scanning indexed entries by hex reference prefix.
type PrefixScanner interface {
	ScanPrefix(ctx context.Context, hexPrefix string, limit int) ([]reference.Reference, error)
}

// Backend is the physical storage interface for index storage.
// All implementations must be thread-safe.
type Backend interface {
	Put(ctx context.Context, entry *Entry) error
	PutBatch(ctx context.Context, entries []*Entry) error
	Get(ctx context.Context, r reference.Reference) (*Entry, error)
	Delete(ctx context.Context, r reference.Reference) error
	Query(ctx context.Context, opts *QueryOptions) (*QueryResult, error)
	Count(ctx context.Context, opts *QueryOptions) (int64, error)
	DeleteExpired(ctx context.Context, now time.Time) (int, error)
	Stats(ctx context.Context) (*Stats, error)
	Close() error
}
