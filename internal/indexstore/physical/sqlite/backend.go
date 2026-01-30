// Package sqlite provides a SQLite-backed index storage backend.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/storage"
)

const (
	KeyPath        = "path"
	KeyJournalMode = "journal_mode"
	KeyBusyTimeout = "busy_timeout"
	KeyCacheSize   = "cache_size"
)

func init() {
	physical.Register("sqlite", NewFactory, Defaults)
}

// Defaults returns the default configuration for the SQLite backend.
func Defaults() map[string]string {
	return map[string]string{
		KeyPath:        "~/.arc/index.db",
		KeyJournalMode: "wal",
		KeyBusyTimeout: "5000",
		KeyCacheSize:   "-64000",
	}
}

const schema = `
CREATE TABLE IF NOT EXISTS entries (
    ref_hex     TEXT PRIMARY KEY,
    ref_bytes   BLOB NOT NULL,
    timestamp   INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS labels (
    ref_hex     TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT NOT NULL,
    timestamp   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (ref_hex, key),
    FOREIGN KEY (ref_hex) REFERENCES entries(ref_hex) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_entries_timestamp ON entries(timestamp);
CREATE INDEX IF NOT EXISTS idx_entries_expires ON entries(expires_at) WHERE expires_at > 0;
CREATE INDEX IF NOT EXISTS idx_labels_kv ON labels(key, value);
CREATE INDEX IF NOT EXISTS idx_labels_kvt ON labels(key, value, timestamp, ref_hex);
CREATE INDEX IF NOT EXISTS idx_entries_ts_ref ON entries(timestamp, ref_hex);
`

// NewFactory creates a new SQLite backend from a configuration map.
func NewFactory(_ context.Context, config map[string]string) (physical.Backend, error) {
	path := storage.GetString(config, KeyPath, "")
	if path == "" {
		return nil, storage.NewConfigError("sqlite", KeyPath, "cannot be empty")
	}
	path = storage.ExpandPath(path)

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, storage.NewConfigErrorWithCause("sqlite", KeyPath, "failed to create directory", err)
	}

	journalMode := storage.GetString(config, KeyJournalMode, "wal")
	busyTimeout := storage.GetString(config, KeyBusyTimeout, "5000")
	cacheSize := storage.GetString(config, KeyCacheSize, "-64000")

	dsn := fmt.Sprintf("file:%s?_journal_mode=%s&_busy_timeout=%s&_cache_size=%s&_foreign_keys=on",
		path, journalMode, busyTimeout, cacheSize)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, storage.NewConfigErrorWithCause("sqlite", KeyPath, "failed to open database", err)
	}

	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, storage.NewConfigErrorWithCause("sqlite", KeyPath, "failed to initialize schema", err)
	}

	slog.Info("sqlite indexstore initialized", "path", path, "journal_mode", journalMode)
	return &Backend{db: db}, nil
}

// Backend is a SQLite implementation of physical.Backend.
type Backend struct {
	db     *sql.DB
	closed atomic.Bool
}

// Put stores an entry.
func (b *Backend) Put(ctx context.Context, entry *physical.Entry) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite put: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	if err := b.putInTx(ctx, tx, entry); err != nil {
		return err
	}
	return tx.Commit()
}

// PutBatch stores multiple entries in a single transaction.
func (b *Backend) PutBatch(ctx context.Context, entries []*physical.Entry) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite put batch: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for _, entry := range entries {
		if err := b.putInTx(ctx, tx, entry); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (b *Backend) putInTx(ctx context.Context, tx *sql.Tx, entry *physical.Entry) error {
	refHex := reference.Hex(entry.Ref)

	if _, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO entries (ref_hex, ref_bytes, timestamp, expires_at) VALUES (?, ?, ?, ?)`,
		refHex, entry.Ref[:], entry.Timestamp, entry.ExpiresAt,
	); err != nil {
		return fmt.Errorf("sqlite put: insert entry: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM labels WHERE ref_hex = ?`, refHex); err != nil {
		return fmt.Errorf("sqlite put: delete labels: %w", err)
	}

	if len(entry.Labels) > 0 {
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO labels (ref_hex, key, value, timestamp) VALUES (?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("sqlite put: prepare label insert: %w", err)
		}
		defer stmt.Close()

		for k, v := range entry.Labels {
			if _, err := stmt.ExecContext(ctx, refHex, k, v, entry.Timestamp); err != nil {
				return fmt.Errorf("sqlite put: insert label: %w", err)
			}
		}
	}

	return nil
}

// Get retrieves an entry by reference.
func (b *Backend) Get(ctx context.Context, r reference.Reference) (*physical.Entry, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	refHex := reference.Hex(r)

	var refBytes []byte
	var ts, expiresAt int64
	err := b.db.QueryRowContext(ctx,
		`SELECT ref_bytes, timestamp, expires_at FROM entries WHERE ref_hex = ?`, refHex,
	).Scan(&refBytes, &ts, &expiresAt)
	if err == sql.ErrNoRows {
		return nil, physical.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite get: %w", err)
	}

	entry := &physical.Entry{
		Timestamp: ts,
		ExpiresAt: expiresAt,
		Labels:    make(map[string]string),
	}
	copy(entry.Ref[:], refBytes)

	rows, err := b.db.QueryContext(ctx, `SELECT key, value FROM labels WHERE ref_hex = ?`, refHex)
	if err != nil {
		return nil, fmt.Errorf("sqlite get labels: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("sqlite get: scan label: %w", err)
		}
		entry.Labels[k] = v
	}

	return entry, rows.Err()
}

// Delete removes an entry by reference.
func (b *Backend) Delete(ctx context.Context, r reference.Reference) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	refHex := reference.Hex(r)
	_, err := b.db.ExecContext(ctx, `DELETE FROM entries WHERE ref_hex = ?`, refHex)
	if err != nil {
		return fmt.Errorf("sqlite delete: %w", err)
	}
	return nil
}

// Query returns entries matching the given options.
func (b *Backend) Query(ctx context.Context, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	if opts == nil {
		opts = &physical.QueryOptions{}
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	now := time.Now().UnixNano()

	// Parse cursor: "{timestamp}/{refHex}"
	var cursorTS int64
	var cursorRef string
	if opts.Cursor != "" {
		parts := strings.SplitN(opts.Cursor, "/", 2)
		if len(parts) == 2 {
			fmt.Sscanf(parts[0], "%x", &cursorTS)
			cursorRef = parts[1]
		}
	}

	// Build query — when labels + time range are both present, drive from the
	// labels table using idx_labels_kvt (key, value, timestamp, ref_hex) which
	// satisfies the label match, time range filter, AND sort in one index scan.
	var qb strings.Builder
	var args []any

	hasLabels := len(opts.Labels) > 0
	hasTime := opts.After > 0 || opts.Before > 0

	if hasLabels && hasTime {
		// Pick one label to drive the scan from (the covering index handles it).
		// Additional labels are verified via extra JOINs.
		first := true
		var driverKey, driverValue string
		i := 0
		for k, v := range opts.Labels {
			if first {
				driverKey, driverValue = k, v
				first = false
				continue
			}
			_ = i // used below
			i++
		}

		qb.WriteString("SELECT e.ref_bytes, e.timestamp, e.expires_at, e.ref_hex FROM labels d")
		qb.WriteString(" JOIN entries e ON e.ref_hex = d.ref_hex")

		// Additional label JOINs for multi-label queries
		i = 0
		for k, v := range opts.Labels {
			if k == driverKey && v == driverValue {
				continue
			}
			alias := fmt.Sprintf("l%d", i)
			qb.WriteString(fmt.Sprintf(" JOIN labels %s ON %s.ref_hex = d.ref_hex AND %s.key = ? AND %s.value = ?",
				alias, alias, alias, alias))
			args = append(args, k, v)
			i++
		}

		qb.WriteString(" WHERE d.key = ? AND d.value = ?")
		args = append(args, driverKey, driverValue)

		if opts.After > 0 {
			qb.WriteString(" AND d.timestamp > ?")
			args = append(args, opts.After)
		}
		if opts.Before > 0 {
			qb.WriteString(" AND d.timestamp < ?")
			args = append(args, opts.Before)
		}
		if !opts.IncludeExpired {
			qb.WriteString(" AND (e.expires_at = 0 OR e.expires_at > ?)")
			args = append(args, now)
		}
		if opts.Cursor != "" {
			if opts.Descending {
				qb.WriteString(" AND (d.timestamp < ? OR (d.timestamp = ? AND d.ref_hex < ?))")
			} else {
				qb.WriteString(" AND (d.timestamp > ? OR (d.timestamp = ? AND d.ref_hex > ?))")
			}
			args = append(args, cursorTS, cursorTS, cursorRef)
		}
		if opts.Descending {
			qb.WriteString(" ORDER BY d.timestamp DESC, d.ref_hex DESC")
		} else {
			qb.WriteString(" ORDER BY d.timestamp ASC, d.ref_hex ASC")
		}
	} else {
		qb.WriteString("SELECT e.ref_bytes, e.timestamp, e.expires_at, e.ref_hex FROM entries e")

		if hasLabels {
			i := 0
			for k, v := range opts.Labels {
				alias := fmt.Sprintf("l%d", i)
				qb.WriteString(fmt.Sprintf(" JOIN labels %s ON %s.ref_hex = e.ref_hex AND %s.key = ? AND %s.value = ?",
					alias, alias, alias, alias))
				args = append(args, k, v)
				i++
			}
		}

		qb.WriteString(" WHERE 1=1")

		if opts.After > 0 {
			qb.WriteString(" AND e.timestamp > ?")
			args = append(args, opts.After)
		}
		if opts.Before > 0 {
			qb.WriteString(" AND e.timestamp < ?")
			args = append(args, opts.Before)
		}
		if !opts.IncludeExpired {
			qb.WriteString(" AND (e.expires_at = 0 OR e.expires_at > ?)")
			args = append(args, now)
		}
		if opts.Cursor != "" {
			if opts.Descending {
				qb.WriteString(" AND (e.timestamp < ? OR (e.timestamp = ? AND e.ref_hex < ?))")
			} else {
				qb.WriteString(" AND (e.timestamp > ? OR (e.timestamp = ? AND e.ref_hex > ?))")
			}
			args = append(args, cursorTS, cursorTS, cursorRef)
		}
		if opts.Descending {
			qb.WriteString(" ORDER BY e.timestamp DESC, e.ref_hex DESC")
		} else {
			qb.WriteString(" ORDER BY e.timestamp ASC, e.ref_hex ASC")
		}
	}

	qb.WriteString(" LIMIT ?")
	args = append(args, limit+1)

	rows, err := b.db.QueryContext(ctx, qb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite query: %w", err)
	}
	defer rows.Close()

	var entries []*physical.Entry
	for rows.Next() {
		var refBytes []byte
		var ts, expiresAt int64
		var rh string
		if err := rows.Scan(&refBytes, &ts, &expiresAt, &rh); err != nil {
			return nil, fmt.Errorf("sqlite query: scan: %w", err)
		}
		e := &physical.Entry{
			Timestamp: ts,
			ExpiresAt: expiresAt,
		}
		copy(e.Ref[:], refBytes)
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite query: rows: %w", err)
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	// Fetch labels for results
	if len(entries) > 0 {
		if err := b.fetchLabels(ctx, entries); err != nil {
			return nil, err
		}
	}

	var nextCursor string
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		nextCursor = fmt.Sprintf("%016x/%s", last.Timestamp, reference.Hex(last.Ref))
	}

	return &physical.QueryResult{
		Entries:    entries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (b *Backend) fetchLabels(ctx context.Context, entries []*physical.Entry) error {
	// Build a map for quick lookup
	byRef := make(map[string]*physical.Entry, len(entries))
	placeholders := make([]string, len(entries))
	args := make([]any, len(entries))
	for i, e := range entries {
		h := reference.Hex(e.Ref)
		byRef[h] = e
		e.Labels = make(map[string]string)
		placeholders[i] = "?"
		args[i] = h
	}

	q := fmt.Sprintf("SELECT ref_hex, key, value FROM labels WHERE ref_hex IN (%s)",
		strings.Join(placeholders, ","))

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("sqlite fetch labels: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var rh, k, v string
		if err := rows.Scan(&rh, &k, &v); err != nil {
			return fmt.Errorf("sqlite fetch labels: scan: %w", err)
		}
		if e, ok := byRef[rh]; ok {
			e.Labels[k] = v
		}
	}
	return rows.Err()
}

// DeleteExpired removes all expired entries.
func (b *Backend) DeleteExpired(ctx context.Context, now time.Time) (int, error) {
	if b.closed.Load() {
		return 0, physical.ErrClosed
	}

	result, err := b.db.ExecContext(ctx,
		`DELETE FROM entries WHERE expires_at > 0 AND expires_at <= ?`, now.UnixNano())
	if err != nil {
		return 0, fmt.Errorf("sqlite delete expired: %w", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite delete expired: rows affected: %w", err)
	}
	return int(n), nil
}

// Stats returns storage statistics.
func (b *Backend) Stats(ctx context.Context) (*physical.Stats, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	var sizeBytes int64
	err := b.db.QueryRowContext(ctx,
		`SELECT page_count * page_size FROM pragma_page_count, pragma_page_size`).Scan(&sizeBytes)
	if err != nil {
		return nil, fmt.Errorf("sqlite stats: %w", err)
	}

	return &physical.Stats{
		SizeBytes:   sizeBytes,
		BackendType: "sqlite",
	}, nil
}

// Close closes the SQLite database.
func (b *Backend) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	return b.db.Close()
}
