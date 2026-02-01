package journal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gezibash/arc/v2/pkg/reference"
	_ "modernc.org/sqlite"
)

// SearchIndex provides full-text search over decrypted journal entries using
// SQLite FTS5. The index lives on the local filesystem because entries are
// E2E encrypted and can only be indexed after client-side decryption.
type SearchIndex struct {
	db *sql.DB
}

// SearchResult is a single full-text search match.
type SearchResult struct {
	Ref       reference.Reference
	Snippet   string
	Timestamp int64
}

// OpenSearchIndex opens (or creates) a search index at the given path.
func OpenSearchIndex(ctx context.Context, path string) (*SearchIndex, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create search index dir: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open search db: %w", err)
	}

	// Schema versioning. Version 5 keys the FTS on entry_ref (deterministic
	// journal-level ID) instead of msg_ref. Any older or missing version
	// triggers a full drop-and-recreate + reindex.
	const currentSchemaVersion = 5
	var version int
	_ = db.QueryRowContext(ctx, `SELECT version FROM search_schema_version LIMIT 1`).Scan(&version)
	if version != currentSchemaVersion {
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS entries_fts`)
		_, _ = db.ExecContext(ctx, `DROP TRIGGER IF EXISTS entries_ai`)
		_, _ = db.ExecContext(ctx, `DROP TRIGGER IF EXISTS entries_ad`)
		_, _ = db.ExecContext(ctx, `DROP TRIGGER IF EXISTS entries_au`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS entries`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS search_schema_version`)
		_, _ = db.ExecContext(ctx, `CREATE TABLE search_schema_version (version INTEGER)`)
		_, _ = db.ExecContext(ctx, `INSERT INTO search_schema_version (version) VALUES (?)`, currentSchemaVersion)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
			content_ref UNINDEXED,
			entry_ref UNINDEXED,
			content,
			timestamp UNINDEXED
		);
	`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init search schema: %w", err)
	}

	return &SearchIndex{db: db}, nil
}

// Index adds or replaces an entry in the search index. The index is keyed on
// entryRef (deterministic journal-level ID) so each journal entry is unique.
// contentRef is stored for lookup and debugging.
func (s *SearchIndex) Index(ctx context.Context, contentRef, entryRef reference.Reference, plaintext string, timestamp int64) error {
	contentHex := reference.Hex(contentRef)
	entryHex := reference.Hex(entryRef)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM entries_fts WHERE entry_ref = ?`, entryHex); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO entries_fts (content_ref, entry_ref, content, timestamp) VALUES (?, ?, ?, ?)`,
		contentHex, entryHex, plaintext, timestamp,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteByEntryRef removes an entry from the search index by entry reference.
func (s *SearchIndex) DeleteByEntryRef(ctx context.Context, entryRef reference.Reference) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM entries_fts WHERE entry_ref = ?`, reference.Hex(entryRef))
	return err
}

// SearchOptions controls pagination for full-text search.
type SearchOptions struct {
	Limit  int // 0 means unlimited
	Offset int
}

// SearchResponse wraps paginated search results.
type SearchResponse struct {
	Results    []SearchResult
	TotalCount int
}

// Search performs a full-text search and returns matching entries with snippets.
// Query terms are automatically prefix-matched (e.g. "hel" matches "hello").
func (s *SearchIndex) Search(ctx context.Context, query string, opts SearchOptions) (*SearchResponse, error) {
	ftsQuery := prefixQuery(query)

	// Get total count.
	var totalCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries_fts WHERE entries_fts MATCH ?`, ftsQuery).Scan(&totalCount); err != nil {
		return nil, fmt.Errorf("search count: %w", err)
	}

	// Build paginated query.
	q := `
		SELECT entry_ref, snippet(entries_fts, 2, '**', '**', '...', 32), timestamp
		FROM entries_fts
		WHERE entries_fts MATCH ?
		ORDER BY rank`
	args := []any{ftsQuery}
	if opts.Limit > 0 {
		q += ` LIMIT ?`
		args = append(args, opts.Limit)
		if opts.Offset > 0 {
			q += ` OFFSET ?`
			args = append(args, opts.Offset)
		}
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []SearchResult
	for rows.Next() {
		var refHex, snippet string
		var ts int64
		if err := rows.Scan(&refHex, &snippet, &ts); err != nil {
			return nil, fmt.Errorf("scan search result: %w", err)
		}
		ref, err := reference.FromHex(refHex)
		if err != nil {
			continue
		}
		results = append(results, SearchResult{
			Ref:       ref,
			Snippet:   snippet,
			Timestamp: ts,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &SearchResponse{Results: results, TotalCount: totalCount}, nil
}

// LastIndexedTimestamp returns the maximum timestamp in the index, or 0 if empty.
func (s *SearchIndex) LastIndexedTimestamp(ctx context.Context) (int64, error) {
	var ts sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT MAX(timestamp) FROM entries_fts`).Scan(&ts)
	if err != nil {
		return 0, err
	}
	if !ts.Valid {
		return 0, nil
	}
	return ts.Int64, nil
}

// ContentHash computes a deterministic SHA-256 over all indexed entries,
// ordered by message ref. This is stable across SQLite file rewrites — same
// logical content always produces the same hash.
func (s *SearchIndex) ContentHash(ctx context.Context) (reference.Reference, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT entry_ref, timestamp FROM entries_fts ORDER BY entry_ref`)
	if err != nil {
		return reference.Reference{}, err
	}
	defer func() { _ = rows.Close() }()

	h := sha256.New()
	var buf [8]byte
	for rows.Next() {
		var refHex string
		var ts int64
		if err := rows.Scan(&refHex, &ts); err != nil {
			return reference.Reference{}, err
		}
		h.Write([]byte(refHex))
		binary.BigEndian.PutUint64(buf[:], uint64(ts))
		h.Write(buf[:])
	}
	if err := rows.Err(); err != nil {
		return reference.Reference{}, err
	}
	var out reference.Reference
	copy(out[:], h.Sum(nil))
	return out, nil
}

// Count stores the number of indexed entries into n.
func (s *SearchIndex) Count(ctx context.Context, n *int) error {
	return s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries_fts`).Scan(n)
}

// Clear removes all entries from the search index.
func (s *SearchIndex) Clear(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM entries_fts`)
	return err
}

// ResolvePrefix finds an entry by entry_ref prefix. Returns an error if zero
// or more than one entry matches.
func (s *SearchIndex) ResolvePrefix(ctx context.Context, prefix string) (reference.Reference, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT entry_ref FROM entries_fts WHERE entry_ref LIKE ?`, prefix+"%")
	if err != nil {
		return reference.Reference{}, fmt.Errorf("resolve prefix: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var matches []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return reference.Reference{}, err
		}
		matches = append(matches, h)
	}
	if err := rows.Err(); err != nil {
		return reference.Reference{}, err
	}
	switch len(matches) {
	case 0:
		return reference.Reference{}, fmt.Errorf("no entry matching prefix %q", prefix)
	case 1:
		return reference.FromHex(matches[0])
	default:
		return reference.Reference{}, fmt.Errorf("ambiguous prefix %q: %d matches", prefix, len(matches))
	}
}

// Close closes the search index database.
func (s *SearchIndex) Close() error {
	return s.db.Close()
}

// DBPath returns the filesystem path of the underlying database.
func (s *SearchIndex) DBPath() (string, error) {
	var path string
	err := s.db.QueryRowContext(context.Background(), `PRAGMA database_list`).Scan(new(int), new(string), &path)
	return path, err
}

// prefixQuery converts a plain query like "hel wor" into the FTS5 prefix
// query `"hel" * "wor" *` so that partial words match.
func prefixQuery(q string) string {
	words := strings.Fields(q)
	if len(words) == 0 {
		return q
	}
	var parts []string
	for _, w := range words {
		// Escape double quotes within the word.
		w = strings.ReplaceAll(w, `"`, `""`)
		parts = append(parts, `"`+w+`"*`)
	}
	return strings.Join(parts, " ")
}
