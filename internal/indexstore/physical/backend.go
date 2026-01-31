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

	// ErrUnsupported indicates the backend does not support the requested operation.
	ErrUnsupported = errors.New("backend does not support operation")
)

// Entry represents an indexed entry in storage.
type Entry struct {
	Ref       reference.Reference
	Labels    map[string]string
	Timestamp int64 // unix milliseconds
	ExpiresAt int64 // unix milliseconds
}

// LabelPredicate is a single label match condition (exact key=value).
type LabelPredicate struct {
	Key   string
	Value string
}

// LabelFilterGroup is a set of predicates that must all match (AND).
type LabelFilterGroup struct {
	Predicates []LabelPredicate
}

// LabelFilter represents a boolean combination of label predicates.
// Semantically: (group[0].AND) OR (group[1].AND) OR ...
// Empty OR means no label filtering.
type LabelFilter struct {
	OR []LabelFilterGroup
}

// QueryOptions specifies filtering and pagination for queries.
type QueryOptions struct {
	Labels         map[string]string // Simple AND labels (existing, kept for compatibility)
	LabelFilter    *LabelFilter      // Structured filter with OR support (takes precedence over Labels when set)
	After          int64             // unix milliseconds
	Before         int64             // unix milliseconds
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

// CompositeIndexDef defines a composite index over an ordered set of label keys.
type CompositeIndexDef struct {
	Name string   // Unique name, e.g., "dm_thread"
	Keys []string // Ordered label keys, e.g., ["app", "thread"]
}

// CompositeIndexer is an optional interface for backends that support
// pre-materialized composite label indexes.
type CompositeIndexer interface {
	// RegisterCompositeIndex registers a composite index definition.
	// Must be called before any Put operations that should use this index.
	RegisterCompositeIndex(def CompositeIndexDef) error

	// CompositeIndexes returns all registered composite index definitions.
	CompositeIndexes() []CompositeIndexDef
}

// Backfiller is an optional interface for backends that support backfilling
// composite indexes for existing entries.
type Backfiller interface {
	// BackfillCompositeIndex iterates all entries and writes composite index
	// keys for the given definition. Returns the number of entries backfilled.
	BackfillCompositeIndex(ctx context.Context, def CompositeIndexDef) (int64, error)
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
