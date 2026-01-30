package journal

import "github.com/gezibash/arc/pkg/reference"

// Entry represents a journal entry.
type Entry struct {
	Ref       reference.Reference
	Labels    map[string]string
	Timestamp int64
	Content   []byte // populated by Read; nil in List results
}

// WriteResult is returned by Write.
type WriteResult struct{ Ref reference.Reference }

// EditResult is returned by Edit.
type EditResult struct{ Ref reference.Reference }

// ListResult is returned by List.
type ListResult struct {
	Entries    []Entry
	NextCursor string
	HasMore    bool
}

// ListOptions controls list queries.
type ListOptions struct {
	Expression string
	Labels     map[string]string
	Limit      int
	Cursor     string
	Descending bool
}
