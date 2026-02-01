package journal

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/gezibash/arc/pkg/reference"
)

const contentType = "application/x-arc-journal+encrypted"

// Journal provides encrypted journal operations over an Arc node.
type Journal struct {
	client  *client.Client
	kp      *identity.Keypair
	nodeKey *identity.PublicKey
	symKey  [32]byte
	search  *SearchIndex
}

// Option configures a Journal.
type Option func(*Journal)

// WithNodeKey sets the node public key used as the default message recipient.
func WithNodeKey(pub identity.PublicKey) Option {
	return func(j *Journal) { j.nodeKey = &pub }
}

// WithSearchIndex attaches a local full-text search index.
func WithSearchIndex(idx *SearchIndex) Option {
	return func(j *Journal) { j.search = idx }
}

// New creates a Journal backed by the given client and keypair.
func New(c *client.Client, kp *identity.Keypair, opts ...Option) *Journal {
	j := &Journal{
		client: c,
		kp:     kp,
		symKey: deriveKey(kp),
	}
	for _, o := range opts {
		o(j)
	}
	return j
}

// SearchIndex returns the attached search index, or nil if none.
func (j *Journal) SearchIndex() *SearchIndex { return j.search }

// SetSearchIndex replaces the attached search index.
func (j *Journal) SetSearchIndex(idx *SearchIndex) { j.search = idx }

// ComputeEntryRef computes a deterministic journal-level entry ID from the
// content reference, message timestamp, and owner public key.
func ComputeEntryRef(contentRef reference.Reference, timestamp int64, owner identity.PublicKey) reference.Reference {
	h := sha256.New()
	h.Write(contentRef[:])
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(timestamp))
	h.Write(buf[:])
	h.Write(owner[:])
	var out reference.Reference
	copy(out[:], h.Sum(nil))
	return out
}

// Search queries the local full-text search index and returns matching entries.
func (j *Journal) Search(ctx context.Context, query string, opts SearchOptions) (*SearchResponse, error) {
	if j.search == nil {
		return nil, fmt.Errorf("no search index configured")
	}
	return j.search.Search(query, opts)
}

// IndexEntry indexes a single entry's plaintext in the search index. This is
// called automatically after Write when a search index is attached. The index
// is keyed on entryRef (deterministic journal-level ID) for dedup.
func (j *Journal) IndexEntry(contentRef, entryRef reference.Reference, plaintext string, timestamp int64) {
	if j.search != nil {
		_ = j.search.Index(contentRef, entryRef, plaintext, timestamp)
	}
}

// Reindex rebuilds the search index from all existing journal entries.
// It filters out replaced entries and deduplicates by message ref so each
// journal entry appears exactly once.
func (j *Journal) Reindex(ctx context.Context) error {
	if j.search == nil {
		return fmt.Errorf("no search index configured")
	}

	if err := j.search.Clear(); err != nil {
		return fmt.Errorf("clear search index: %w", err)
	}

	pub := j.kp.PublicKey()

	// First pass: collect all entries and track which entryRefs have been replaced.
	type entry struct {
		ContentHex string
		EntryRef   reference.Reference
		Timestamp  int64
	}
	var all []entry
	replaced := map[string]bool{}

	var cursor string
	for {
		result, err := j.List(ctx, ListOptions{
			Limit:      50,
			Cursor:     cursor,
			Descending: false,
			Labels:     map[string]string{"app": "journal", "type": "entry"},
		})
		if err != nil {
			return fmt.Errorf("list entries: %w", err)
		}

		for _, e := range result.Entries {
			if rep := e.Labels["replaces"]; rep != "" {
				replaced[rep] = true
			}
			contentHex := e.Labels["content"]
			if contentHex == "" {
				continue
			}
			// Compute entryRef deterministically from content+timestamp+owner.
			contentRef, err := reference.FromHex(contentHex)
			if err != nil {
				continue
			}
			entryRef := ComputeEntryRef(contentRef, e.Timestamp, pub)
			all = append(all, entry{
				ContentHex: contentHex,
				EntryRef:   entryRef,
				Timestamp:  e.Timestamp,
			})
		}

		if !result.HasMore {
			break
		}
		cursor = result.NextCursor
	}

	// Second pass: index only non-replaced entries, dedup by entryRef.
	seen := map[string]bool{}
	for _, e := range all {
		entryHex := reference.Hex(e.EntryRef)
		if replaced[entryHex] {
			continue
		}
		if seen[entryHex] {
			continue
		}
		seen[entryHex] = true

		contentRef, err := reference.FromHex(e.ContentHex)
		if err != nil {
			continue
		}
		entry, err := j.Read(ctx, contentRef)
		if err != nil {
			continue
		}
		_ = j.search.Index(contentRef, e.EntryRef, string(entry.Content), e.Timestamp)
	}
	return nil
}
