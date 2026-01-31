// Package badger provides a BadgerDB-backed index storage backend.
package badger

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/storage"
)

const (
	prefixLabel = "label/"
	prefixRef   = "ref/"
	prefixMeta  = "meta/"
)

const (
	KeyPath             = "path"
	KeySyncWrites       = "sync_writes"
	KeyValueLogFileSize = "value_log_file_size"
	KeyMemTableSize     = "mem_table_size"
	KeyInMemory         = "in_memory"
)

func init() {
	physical.Register("badger", NewFactory, Defaults)
}

// Defaults returns the default configuration for the BadgerDB backend.
func Defaults() map[string]string {
	return map[string]string{
		KeyPath:             "~/.arc/index",
		KeySyncWrites:       "false",
		KeyValueLogFileSize: strconv.FormatInt(1<<30, 10),
		KeyMemTableSize:     strconv.FormatInt(64<<20, 10),
		KeyInMemory:         "false",
	}
}

// NewFactory creates a new BadgerDB backend from a configuration map.
func NewFactory(_ context.Context, config map[string]string) (physical.Backend, error) {
	inMemory, err := storage.GetBool(config, KeyInMemory, false)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("badger", KeyInMemory, config[KeyInMemory], err.Error())
	}

	if inMemory {
		return newInMemory()
	}

	path := storage.GetString(config, KeyPath, "")
	if path == "" {
		return nil, storage.NewConfigError("badger", KeyPath, "cannot be empty")
	}
	path = storage.ExpandPath(path)

	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, storage.NewConfigErrorWithCause("badger", KeyPath, "failed to create directory", err)
	}

	syncWrites, err := storage.GetBool(config, KeySyncWrites, false)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("badger", KeySyncWrites, config[KeySyncWrites], err.Error())
	}

	valueLogFileSize, err := storage.GetInt64(config, KeyValueLogFileSize, 1<<30)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("badger", KeyValueLogFileSize, config[KeyValueLogFileSize], err.Error())
	}

	memTableSize, err := storage.GetInt64(config, KeyMemTableSize, 64<<20)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("badger", KeyMemTableSize, config[KeyMemTableSize], err.Error())
	}

	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	opts.SyncWrites = syncWrites
	if valueLogFileSize > 0 {
		opts.ValueLogFileSize = valueLogFileSize
	}
	if memTableSize > 0 {
		opts.MemTableSize = memTableSize
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, storage.NewConfigErrorWithCause("badger", KeyPath, "failed to open database", err)
	}

	slog.Info("badger indexstore initialized", "path", path, "sync_writes", syncWrites)
	return NewWithDB(db), nil
}

func newInMemory() (*Backend, error) {
	opts := badger.DefaultOptions("").
		WithInMemory(true).
		WithLogger(nil)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, storage.NewConfigErrorWithCause("badger", KeyInMemory, "failed to open in-memory database", err)
	}

	slog.Info("badger indexstore initialized (in-memory)")
	return NewWithDB(db), nil
}

// Backend is a BadgerDB implementation of physical.Backend.
type Backend struct {
	db     *badger.DB
	closed atomic.Bool
}

// NewWithDB creates a new backend with an existing BadgerDB instance.
func NewWithDB(db *badger.DB) *Backend {
	return &Backend{db: db}
}

// Put stores an entry.
func (b *Backend) Put(_ context.Context, entry *physical.Entry) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	return b.db.Update(func(txn *badger.Txn) error {
		return b.putInTxn(txn, entry)
	})
}

// PutBatch stores multiple entries in a single transaction.
func (b *Backend) PutBatch(_ context.Context, entries []*physical.Entry) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	return b.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			if err := b.putInTxn(txn, entry); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *Backend) putInTxn(txn *badger.Txn, entry *physical.Entry) error {
	refHex := reference.Hex(entry.Ref)
	tsHex := timestampToHex(entry.Timestamp)

	// Check if entry already exists (for replacement)
	refKey := []byte(prefixRef + refHex)
	item, getErr := txn.Get(refKey)
	if getErr == nil {
		// Entry exists - delete old indexes
		var oldTsHex string
		if valErr := item.Value(func(val []byte) error {
			oldTsHex = string(val)
			return nil
		}); valErr != nil {
			return valErr
		}

		oldSuffix := oldTsHex + "/" + refHex
		oldMetaKey := []byte(prefixMeta + oldSuffix)

		// Read old meta to get labels for index cleanup
		oldMetaItem, oldMetaErr := txn.Get(oldMetaKey)
		if oldMetaErr == nil {
			if metaErr := oldMetaItem.Value(func(val []byte) error {
				_, oldLabels, decErr := decodeMeta(val)
				if decErr != nil {
					return decErr
				}
				for k, v := range oldLabels {
					labelKey := []byte(prefixLabel + k + "/" + v + "/" + oldTsHex + "/" + refHex)
					if delErr := txn.Delete(labelKey); delErr != nil {
						return delErr
					}
				}
				return nil
			}); metaErr != nil {
				return metaErr
			}
		} else if oldMetaErr != badger.ErrKeyNotFound {
			return oldMetaErr
		}

		if delErr := txn.Delete(oldMetaKey); delErr != nil && delErr != badger.ErrKeyNotFound {
			return delErr
		}
	} else if getErr != badger.ErrKeyNotFound {
		return getErr
	}

	suffix := tsHex + "/" + refHex

	// Compute TTL for entries with expiration.
	var ttl time.Duration
	if entry.ExpiresAt > 0 {
		ttl = time.Duration(entry.ExpiresAt - time.Now().UnixNano())
		if ttl <= 0 {
			ttl = time.Millisecond
		}
	}

	// Store meta (contains all entry fields needed for queries and Get)
	metaKey := []byte(prefixMeta + suffix)
	if ttl > 0 {
		if err := txn.SetEntry(badger.NewEntry(metaKey, encodeMeta(entry)).WithTTL(ttl)); err != nil {
			return err
		}
	} else {
		if err := txn.Set(metaKey, encodeMeta(entry)); err != nil {
			return err
		}
	}

	// Store ref -> timestamp lookup
	if ttl > 0 {
		if err := txn.SetEntry(badger.NewEntry(refKey, []byte(tsHex)).WithTTL(ttl)); err != nil {
			return err
		}
	} else {
		if err := txn.Set(refKey, []byte(tsHex)); err != nil {
			return err
		}
	}

	// Store label indexes
	for k, v := range entry.Labels {
		lk := []byte(prefixLabel + k + "/" + v + "/" + tsHex + "/" + refHex)
		if ttl > 0 {
			if err := txn.SetEntry(badger.NewEntry(lk, nil).WithTTL(ttl)); err != nil {
				return err
			}
		} else {
			if err := txn.Set(lk, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

// Get retrieves an entry by reference.
func (b *Backend) Get(_ context.Context, r reference.Reference) (*physical.Entry, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	refHex := reference.Hex(r)
	var entry physical.Entry

	err := b.db.View(func(txn *badger.Txn) error {
		refKey := []byte(prefixRef + refHex)
		item, getErr := txn.Get(refKey)
		if getErr == badger.ErrKeyNotFound {
			return physical.ErrNotFound
		}
		if getErr != nil {
			return getErr
		}

		var tsHex string
		if valErr := item.Value(func(val []byte) error {
			tsHex = string(val)
			return nil
		}); valErr != nil {
			return valErr
		}

		suffix := tsHex + "/" + refHex
		metaKey := []byte(prefixMeta + suffix)
		metaItem, metaErr := txn.Get(metaKey)
		if metaErr != nil {
			return metaErr
		}

		return metaItem.Value(func(val []byte) error {
			expiresAt, labels, decErr := decodeMeta(val)
			if decErr != nil {
				return decErr
			}
			built, buildErr := entryFromSuffix(suffix, expiresAt, labels)
			if buildErr != nil {
				return buildErr
			}
			entry = *built
			return nil
		})
	})
	if err == physical.ErrNotFound {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("badger get: %w", err)
	}
	return &entry, nil
}

// Delete removes an entry by reference.
func (b *Backend) Delete(_ context.Context, r reference.Reference) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	return b.db.Update(func(txn *badger.Txn) error {
		return deleteInTxn(txn, r)
	})
}

func deleteInTxn(txn *badger.Txn, r reference.Reference) error {
	refHex := reference.Hex(r)
	refKey := []byte(prefixRef + refHex)
	item, getErr := txn.Get(refKey)
	if getErr == badger.ErrKeyNotFound {
		return nil
	}
	if getErr != nil {
		return getErr
	}

	var tsHex string
	if valErr := item.Value(func(val []byte) error {
		tsHex = string(val)
		return nil
	}); valErr != nil {
		return valErr
	}

	suffix := tsHex + "/" + refHex
	metaKey := []byte(prefixMeta + suffix)

	metaItem, metaErr := txn.Get(metaKey)
	if metaErr == nil {
		if valErr := metaItem.Value(func(val []byte) error {
			_, labels, decErr := decodeMeta(val)
			if decErr != nil {
				return decErr
			}
			for k, v := range labels {
				labelKey := []byte(prefixLabel + k + "/" + v + "/" + tsHex + "/" + refHex)
				if delErr := txn.Delete(labelKey); delErr != nil {
					return delErr
				}
			}
			return nil
		}); valErr != nil {
			return valErr
		}
	} else if metaErr != badger.ErrKeyNotFound {
		return metaErr
	}

	if delErr := txn.Delete(metaKey); delErr != nil && delErr != badger.ErrKeyNotFound {
		return delErr
	}

	return txn.Delete(refKey)
}

// Query returns entries matching the given options.
func (b *Backend) Query(_ context.Context, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	if opts == nil {
		opts = &physical.QueryOptions{}
	}

	if len(opts.Labels) > 0 {
		return b.queryByLabel(opts)
	}

	return b.queryByEntry(opts)
}

func (b *Backend) queryByEntry(opts *physical.QueryOptions) (*physical.QueryResult, error) {
	now := time.Now().UnixNano()
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	// Pre-compute hex bounds for seek and early termination.
	var afterHex, beforeHex string
	if opts.After > 0 {
		afterHex = timestampToHex(opts.After)
	}
	if opts.Before > 0 {
		beforeHex = timestampToHex(opts.Before)
	}

	var results []*physical.Entry
	var nextCursor string
	hasMore := false

	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte(prefixMeta)

		iterOpts := badger.DefaultIteratorOptions
		iterOpts.PrefetchValues = true
		iterOpts.PrefetchSize = 20
		iterOpts.Prefix = prefix
		iterOpts.Reverse = opts.Descending

		it := txn.NewIterator(iterOpts)
		defer it.Close()

		var seekKey []byte
		switch {
		case opts.Cursor != "":
			seekKey = []byte(prefixMeta + opts.Cursor)
			if opts.Descending {
				seekKey = append(seekKey, 0xFF)
			}
		case opts.Descending:
			if beforeHex != "" {
				seekKey = []byte(prefixMeta + beforeHex)
				seekKey = append(seekKey, 0xFF)
			} else {
				seekKey = prefixEndKey(prefix)
			}
		default:
			if afterHex != "" {
				seekKey = []byte(prefixMeta + afterHex)
			} else {
				seekKey = prefix
			}
		}

		// queryByEntry never has label filters — only expiry check needed.
		needExpiry := !opts.IncludeExpired
		skipFirst := opts.Cursor != ""

		for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
			if skipFirst {
				skipFirst = false
				continue
			}

			key := it.Item().Key()
			suffix := string(key[len(prefixMeta):])
			// suffix = {tsHex(16)}/{refHex(64)}
			if len(suffix) < 81 {
				continue
			}

			tsHex := suffix[:16]

			// Early termination: keys are sorted by tsHex.
			if !opts.Descending && beforeHex != "" && tsHex >= beforeHex {
				break
			}
			if opts.Descending && afterHex != "" && tsHex <= afterHex {
				break
			}

			// Fast path: only read expiresAt (8 bytes, zero label alloc) for filtering.
			// Full decode only for entries that become results.
			var skip bool
			var entry *physical.Entry
			err := it.Item().Value(func(val []byte) error {
				if needExpiry {
					ea, eaErr := decodeMetaExpiresAt(val)
					if eaErr != nil {
						return eaErr
					}
					if ea > 0 && ea <= now {
						skip = true
						return nil
					}
				}
				if len(results) >= limit {
					// We've hit the limit — just need to know there's more.
					return nil
				}
				var buildErr error
				entry, buildErr = entryFromMetaBytes(suffix, val)
				return buildErr
			})
			if err != nil {
				slog.Warn("failed to decode meta", "key", string(key), "error", err)
				continue
			}
			if skip {
				continue
			}

			if len(results) >= limit {
				hasMore = true
				break
			}

			results = append(results, entry)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("badger query: %w", err)
	}

	if hasMore && len(results) > 0 {
		last := results[len(results)-1]
		nextCursor = timestampToHex(last.Timestamp) + "/" + reference.Hex(last.Ref)
	}

	return &physical.QueryResult{
		Entries:    results,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// pickDrivingLabel selects the label with the fewest index entries (most selective)
// by sampling up to a small number of keys per label prefix. For single-label queries
// this is a no-op. For multi-label, we peek at each label/{k}/{v}/ prefix and pick
// the one with the smallest estimated cardinality.
func pickDrivingLabel(db *badger.DB, labels map[string]string) (string, string) {
	if len(labels) <= 1 {
		for k, v := range labels {
			return k, v
		}
		return "", ""
	}

	// Sample up to sampleLimit keys per prefix to estimate cardinality.
	const sampleLimit = 101
	bestKey, bestVal := "", ""
	bestCount := int(^uint(0) >> 1) // max int

	_ = db.View(func(txn *badger.Txn) error {
		for k, v := range labels {
			prefix := []byte(prefixLabel + k + "/" + v + "/")
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = prefix

			it := txn.NewIterator(opts)
			count := 0
			for it.Seek(prefix); it.ValidForPrefix(prefix) && count < sampleLimit; it.Next() {
				count++
			}
			it.Close()

			if count < bestCount {
				bestCount = count
				bestKey = k
				bestVal = v
			}
			// If we find one with 0 entries, no need to check others.
			if bestCount == 0 {
				break
			}
		}
		return nil
	})

	return bestKey, bestVal
}

func (b *Backend) queryByLabel(opts *physical.QueryOptions) (*physical.QueryResult, error) {
	now := time.Now().UnixNano()
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	// Pick the most selective label as the driving index key.
	// We estimate selectivity by peeking at how many entries each label prefix
	// contains (fewer = more selective = better driver).
	dk, dv := pickDrivingLabel(b.db, opts.Labels)

	var results []*physical.Entry
	var nextCursor string
	hasMore := false

	// Pre-compute hex bounds for time-range early termination.
	var afterHex, beforeHex string
	if opts.After > 0 {
		afterHex = timestampToHex(opts.After)
	}
	if opts.Before > 0 {
		beforeHex = timestampToHex(opts.Before)
	}

	err := b.db.View(func(txn *badger.Txn) error {
		labelPrefix := []byte(prefixLabel + dk + "/" + dv + "/")

		iterOpts := badger.DefaultIteratorOptions
		iterOpts.PrefetchValues = false // label keys have nil values
		iterOpts.Prefix = labelPrefix
		iterOpts.Reverse = opts.Descending

		it := txn.NewIterator(iterOpts)
		defer it.Close()

		var seekKey []byte
		switch {
		case opts.Cursor != "":
			seekKey = []byte(prefixLabel + dk + "/" + dv + "/" + opts.Cursor)
			if opts.Descending {
				seekKey = append(seekKey, 0xFF)
			}
		case opts.Descending:
			if beforeHex != "" {
				seekKey = []byte(prefixLabel + dk + "/" + dv + "/" + beforeHex)
				seekKey = append(seekKey, 0xFF)
			} else {
				seekKey = prefixEndKey(labelPrefix)
			}
		default:
			if afterHex != "" {
				seekKey = []byte(prefixLabel + dk + "/" + dv + "/" + afterHex)
			} else {
				seekKey = labelPrefix
			}
		}

		skipFirst := opts.Cursor != ""

		for it.Seek(seekKey); it.ValidForPrefix(labelPrefix); it.Next() {
			if skipFirst {
				skipFirst = false
				continue
			}

			key := it.Item().Key()
			// key = label/{dk}/{dv}/{tsHex}/{refHex}
			suffix := string(key[len(labelPrefix):])
			if len(suffix) < 17 { // tsHex(16) + "/" minimum
				continue
			}

			tsHex := suffix[:16]

			// Early termination based on time bounds.
			if !opts.Descending && beforeHex != "" && tsHex >= beforeHex {
				break
			}
			if opts.Descending && afterHex != "" && tsHex <= afterHex {
				break
			}

			// Point-get meta for filtering and reconstruction.
			metaKey := []byte(prefixMeta + suffix)
			metaItem, metaGetErr := txn.Get(metaKey)
			if metaGetErr != nil {
				continue
			}

			var skip bool
			var entry *physical.Entry
			if metaDecErr := metaItem.Value(func(val []byte) error {
				expiresAt, labels, decErr := decodeMeta(val)
				if decErr != nil {
					return decErr
				}
				if !matchesMetaFilter(expiresAt, labels, opts, now) {
					skip = true
					return nil
				}
				if len(results) >= limit {
					return nil
				}
				var buildErr error
				entry, buildErr = entryFromSuffix(suffix, expiresAt, labels)
				return buildErr
			}); metaDecErr != nil {
				slog.Warn("failed to decode meta", "key", string(metaKey), "error", metaDecErr)
				continue
			}
			if skip {
				continue
			}

			if len(results) >= limit {
				hasMore = true
				break
			}

			results = append(results, entry)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("badger query: %w", err)
	}

	if hasMore && len(results) > 0 {
		last := results[len(results)-1]
		nextCursor = timestampToHex(last.Timestamp) + "/" + reference.Hex(last.Ref)
	}

	return &physical.QueryResult{
		Entries:    results,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// Count returns the number of entries matching the given options without
// deserializing full entries. For label queries it iterates the label index;
// for unfiltered queries it iterates the meta prefix. Only expiry is checked.
func (b *Backend) Count(_ context.Context, opts *physical.QueryOptions) (int64, error) {
	if b.closed.Load() {
		return 0, physical.ErrClosed
	}
	if opts == nil {
		opts = &physical.QueryOptions{}
	}

	now := time.Now().UnixNano()
	var afterHex, beforeHex string
	if opts.After > 0 {
		afterHex = timestampToHex(opts.After)
	}
	if opts.Before > 0 {
		beforeHex = timestampToHex(opts.Before)
	}

	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		if len(opts.Labels) > 0 {
			return b.countByLabel(txn, opts, now, afterHex, beforeHex, &count)
		}
		return b.countByEntry(txn, opts, now, afterHex, beforeHex, &count)
	})
	if err != nil {
		return 0, fmt.Errorf("badger count: %w", err)
	}
	return count, nil
}

func (b *Backend) countByEntry(txn *badger.Txn, opts *physical.QueryOptions, now int64, afterHex, beforeHex string, count *int64) error {
	prefix := []byte(prefixMeta)
	iterOpts := badger.DefaultIteratorOptions
	iterOpts.PrefetchValues = !opts.IncludeExpired // only need values to check expiry
	iterOpts.PrefetchSize = 100
	iterOpts.Prefix = prefix

	it := txn.NewIterator(iterOpts)
	defer it.Close()

	var seekKey []byte
	if afterHex != "" {
		seekKey = []byte(prefixMeta + afterHex)
	} else {
		seekKey = prefix
	}

	for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		suffix := string(key[len(prefixMeta):])
		if len(suffix) < 17 {
			continue
		}
		tsHex := suffix[:16]

		if beforeHex != "" && tsHex >= beforeHex {
			break
		}

		if !opts.IncludeExpired {
			var skip bool
			if err := it.Item().Value(func(val []byte) error {
				ea, err := decodeMetaExpiresAt(val)
				if err != nil {
					return err
				}
				if ea > 0 && ea <= now {
					skip = true
				}
				return nil
			}); err != nil {
				continue
			}
			if skip {
				continue
			}
		}

		*count++
	}
	return nil
}

func (b *Backend) countByLabel(txn *badger.Txn, opts *physical.QueryOptions, now int64, afterHex, beforeHex string, count *int64) error {
	dk, dv := pickDrivingLabel(b.db, opts.Labels)
	labelPrefix := []byte(prefixLabel + dk + "/" + dv + "/")

	iterOpts := badger.DefaultIteratorOptions
	iterOpts.PrefetchValues = false
	iterOpts.Prefix = labelPrefix

	it := txn.NewIterator(iterOpts)
	defer it.Close()

	var seekKey []byte
	if afterHex != "" {
		seekKey = []byte(prefixLabel + dk + "/" + dv + "/" + afterHex)
	} else {
		seekKey = labelPrefix
	}

	needValidate := len(opts.Labels) > 1 || !opts.IncludeExpired

	for it.Seek(seekKey); it.ValidForPrefix(labelPrefix); it.Next() {
		key := it.Item().Key()
		suffix := string(key[len(labelPrefix):])
		if len(suffix) < 17 {
			continue
		}
		tsHex := suffix[:16]

		if beforeHex != "" && tsHex >= beforeHex {
			break
		}

		if needValidate {
			metaKey := []byte(prefixMeta + suffix)
			metaItem, err := txn.Get(metaKey)
			if err != nil {
				continue
			}
			var skip bool
			if err := metaItem.Value(func(val []byte) error {
				expiresAt, labels, decErr := decodeMeta(val)
				if decErr != nil {
					return decErr
				}
				if !matchesMetaFilter(expiresAt, labels, opts, now) {
					skip = true
				}
				return nil
			}); err != nil || skip {
				continue
			}
		}

		*count++
	}
	return nil
}

// DeleteExpired is a no-op for badger. Expired keys are handled natively
// via WithTTL: iterators skip them automatically, and badger's GC/compaction
// reclaims the space.
func (b *Backend) DeleteExpired(_ context.Context, _ time.Time) (int, error) {
	if b.closed.Load() {
		return 0, physical.ErrClosed
	}
	return 0, nil
}

// Stats returns storage statistics.
func (b *Backend) Stats(_ context.Context) (*physical.Stats, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	lsm, vlog := b.db.Size()
	return &physical.Stats{
		SizeBytes:   lsm + vlog,
		BackendType: "badger",
	}, nil
}

// RunGC triggers value log garbage collection. It rewrites vlog files where
// at least the given fraction (0.0–1.0) of space is reclaimable. A
// discardRatio of 0.5 is a reasonable default. Returns nil when there is
// nothing left to collect.
func (b *Backend) RunGC(discardRatio float64) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}
	for {
		if err := b.db.RunValueLogGC(discardRatio); err != nil {
			if errors.Is(err, badger.ErrNoRewrite) {
				return nil
			}
			return fmt.Errorf("value log gc: %w", err)
		}
	}
}

// ScanPrefix returns references matching the given hex prefix.
// It scans the ref/ keyspace where keys are ref/{refHex}.
func (b *Backend) ScanPrefix(_ context.Context, hexPrefix string, limit int) ([]reference.Reference, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	var refs []reference.Reference
	prefix := []byte(prefixRef + hexPrefix)

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().Key())
			refHex := key[len(prefixRef):]
			ref, err := reference.FromHex(refHex)
			if err != nil {
				continue
			}
			refs = append(refs, ref)
			if len(refs) >= limit {
				break
			}
		}
		return nil
	})
	return refs, err
}

// Close closes the BadgerDB database.
func (b *Backend) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	return b.db.Close()
}

// encodeMeta encodes ExpiresAt and Labels into a compact binary format.
// Layout: ExpiresAt(8) + LabelCount(2) + for each label: keyLen(2) + key + valLen(2) + val
func encodeMeta(entry *physical.Entry) []byte {
	// Calculate size
	size := 8 + 2
	for k, v := range entry.Labels {
		size += 2 + len(k) + 2 + len(v)
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint64(buf[0:8], uint64(entry.ExpiresAt)) //nolint:gosec // expiresAt can be 0 or positive
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(entry.Labels)))
	off := 10
	for k, v := range entry.Labels {
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(k)))
		off += 2
		copy(buf[off:], k)
		off += len(k)
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(v)))
		off += 2
		copy(buf[off:], v)
		off += len(v)
	}
	return buf
}

// decodeMeta decodes the compact binary metadata format.
func decodeMeta(data []byte) (expiresAt int64, labels map[string]string, err error) {
	if len(data) < 10 {
		return 0, nil, fmt.Errorf("meta too short: %d", len(data))
	}
	expiresAt = int64(binary.BigEndian.Uint64(data[0:8]))
	count := int(binary.BigEndian.Uint16(data[8:10]))
	labels = make(map[string]string, count)
	off := 10
	for i := 0; i < count; i++ {
		if off+2 > len(data) {
			return 0, nil, fmt.Errorf("meta truncated at label %d key length", i)
		}
		kl := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if off+kl > len(data) {
			return 0, nil, fmt.Errorf("meta truncated at label %d key", i)
		}
		k := string(data[off : off+kl])
		off += kl
		if off+2 > len(data) {
			return 0, nil, fmt.Errorf("meta truncated at label %d value length", i)
		}
		vl := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if off+vl > len(data) {
			return 0, nil, fmt.Errorf("meta truncated at label %d value", i)
		}
		v := string(data[off : off+vl])
		off += vl
		labels[k] = v
	}
	return expiresAt, labels, nil
}

// decodeMetaExpiresAt reads only the first 8 bytes of a meta value. Zero allocations.
func decodeMetaExpiresAt(data []byte) (int64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("meta too short: %d", len(data))
	}
	return int64(binary.BigEndian.Uint64(data[0:8])), nil
}

// entryFromMetaBytes reconstructs an Entry from a key suffix and raw meta value bytes.
func entryFromMetaBytes(suffix string, data []byte) (*physical.Entry, error) {
	expiresAt, labels, err := decodeMeta(data)
	if err != nil {
		return nil, err
	}
	return entryFromSuffix(suffix, expiresAt, labels)
}

// matchesMetaFilter checks filter conditions using only metadata fields (no full Entry needed).
func matchesMetaFilter(expiresAt int64, labels map[string]string, opts *physical.QueryOptions, now int64) bool {
	if !opts.IncludeExpired && expiresAt > 0 && expiresAt <= now {
		return false
	}
	for k, v := range opts.Labels {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// prefixEndKey returns a key just past the end of the given prefix,
// suitable for Badger reverse-seek to land on the last key under prefix.
func prefixEndKey(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	// Increment the last byte; works for all ASCII prefixes used here.
	end[len(end)-1]++
	return end
}

func timestampToHex(ts int64) string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(ts)) //nolint:gosec // timestamp is always positive
	return fmt.Sprintf("%016x", buf)
}

func hexToTimestamp(h string) (int64, error) {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 8 {
		return 0, fmt.Errorf("invalid timestamp hex: %s", h)
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// entryFromSuffix reconstructs a full Entry from a meta key suffix and already-decoded meta fields.
// suffix format: {tsHex(16)}/{refHex(64)}
func entryFromSuffix(suffix string, expiresAt int64, labels map[string]string) (*physical.Entry, error) {
	tsHex := suffix[:16]
	refHex := suffix[17:]

	ts, err := hexToTimestamp(tsHex)
	if err != nil {
		return nil, err
	}

	ref, err := reference.FromHex(refHex)
	if err != nil {
		return nil, err
	}

	return &physical.Entry{
		Ref:       ref,
		Timestamp: ts,
		ExpiresAt: expiresAt,
		Labels:    labels,
	}, nil
}
