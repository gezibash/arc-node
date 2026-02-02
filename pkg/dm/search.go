package dm

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

// SearchIndex provides full-text search over decrypted DM messages using
// SQLite FTS5. The index lives on the local filesystem because messages are
// E2E encrypted and can only be indexed after client-side decryption.
type SearchIndex struct {
	db *sql.DB
}

// SearchResult is a single full-text search match.
type SearchResult struct {
	Ref       reference.Reference // message ref
	Snippet   string
	Timestamp int64
}

// SearchOptions controls pagination for full-text search.
type SearchOptions struct {
	Limit  int
	Offset int
}

// SearchResponse wraps paginated search results.
type SearchResponse struct {
	Results    []SearchResult
	TotalCount int
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

	const currentSchemaVersion = 2
	var version int
	_ = db.QueryRowContext(ctx, `SELECT version FROM search_schema_version LIMIT 1`).Scan(&version)
	if version != currentSchemaVersion {
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS messages_fts`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS last_read`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS search_schema_version`)
		_, _ = db.ExecContext(ctx, `CREATE TABLE search_schema_version (version INTEGER)`)
		_, _ = db.ExecContext(ctx, `INSERT INTO search_schema_version (version) VALUES (?)`, currentSchemaVersion)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
			content_ref UNINDEXED,
			msg_ref UNINDEXED,
			content,
			timestamp UNINDEXED
		);
	`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init search schema: %w", err)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS last_read (
			conv_id TEXT PRIMARY KEY,
			timestamp INTEGER NOT NULL
		);
	`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init last_read schema: %w", err)
	}

	return &SearchIndex{db: db}, nil
}

// Index adds or replaces a message in the search index, keyed on msg_ref.
func (s *SearchIndex) Index(ctx context.Context, contentRef, msgRef reference.Reference, plaintext string, timestamp int64) error {
	contentHex := reference.Hex(contentRef)
	msgHex := reference.Hex(msgRef)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM messages_fts WHERE msg_ref = ?`, msgHex); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO messages_fts (content_ref, msg_ref, content, timestamp) VALUES (?, ?, ?, ?)`,
		contentHex, msgHex, plaintext, timestamp,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// Search performs a full-text search and returns matching messages with snippets.
func (s *SearchIndex) Search(ctx context.Context, query string, opts SearchOptions) (*SearchResponse, error) {
	ftsQuery := prefixQuery(query)

	var totalCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH ?`, ftsQuery).Scan(&totalCount); err != nil {
		return nil, fmt.Errorf("search count: %w", err)
	}

	q := `
		SELECT msg_ref, snippet(messages_fts, 2, '**', '**', '...', 32), timestamp
		FROM messages_fts
		WHERE messages_fts MATCH ?
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
	err := s.db.QueryRowContext(ctx, `SELECT MAX(timestamp) FROM messages_fts`).Scan(&ts)
	if err != nil {
		return 0, err
	}
	if !ts.Valid {
		return 0, nil
	}
	return ts.Int64, nil
}

// ContentHash computes a deterministic SHA-256 over all indexed messages.
func (s *SearchIndex) ContentHash(ctx context.Context) (reference.Reference, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT msg_ref, timestamp FROM messages_fts ORDER BY msg_ref`)
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

// Count stores the number of indexed messages into n.
func (s *SearchIndex) Count(ctx context.Context, n *int) error {
	return s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages_fts`).Scan(n)
}

// Clear removes all messages from the search index.
func (s *SearchIndex) Clear(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM messages_fts`)
	return err
}

// ResolvePrefix finds a message by msg_ref prefix. Returns an error if zero
// or more than one message matches.
func (s *SearchIndex) ResolvePrefix(ctx context.Context, prefix string) (reference.Reference, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT msg_ref FROM messages_fts WHERE msg_ref LIKE ?`, prefix+"%")
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
		return reference.Reference{}, fmt.Errorf("no message matching prefix %q", prefix)
	case 1:
		return reference.FromHex(matches[0])
	default:
		return reference.Reference{}, fmt.Errorf("ambiguous prefix %q: %d matches", prefix, len(matches))
	}
}

// MarkRead records the latest read timestamp for a conversation.
func (s *SearchIndex) MarkRead(ctx context.Context, convID string, timestamp int64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO last_read (conv_id, timestamp) VALUES (?, ?)
		 ON CONFLICT(conv_id) DO UPDATE SET timestamp = excluded.timestamp
		 WHERE excluded.timestamp > last_read.timestamp`,
		convID, timestamp)
	return err
}

// LastRead returns the last-read timestamp for a conversation, or 0 if unread.
func (s *SearchIndex) LastRead(ctx context.Context, convID string) (int64, error) {
	var ts int64
	err := s.db.QueryRowContext(ctx, `SELECT timestamp FROM last_read WHERE conv_id = ?`, convID).Scan(&ts)
	if err != nil {
		return 0, nil //nolint:nilerr // missing row means never read
	}
	return ts, nil
}

// AllLastRead returns all last-read timestamps keyed by conversation ID.
func (s *SearchIndex) AllLastRead(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT conv_id, timestamp FROM last_read`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	m := make(map[string]int64)
	for rows.Next() {
		var convID string
		var ts int64
		if err := rows.Scan(&convID, &ts); err != nil {
			return nil, err
		}
		m[convID] = ts
	}
	return m, rows.Err()
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

// prefixQuery converts a plain query into FTS5 prefix query.
func prefixQuery(q string) string {
	words := strings.Fields(q)
	if len(words) == 0 {
		return q
	}
	var parts []string
	for _, w := range words {
		w = strings.ReplaceAll(w, `"`, `""`)
		parts = append(parts, `"`+w+`"*`)
	}
	return strings.Join(parts, " ")
}
