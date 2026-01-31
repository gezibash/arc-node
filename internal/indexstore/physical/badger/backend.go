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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/indexstore/physical"
	"github.com/gezibash/arc-node/internal/storage"
)

const (
	prefixLabel     = "label/"
	prefixRef       = "ref/"
	prefixMeta      = "meta/"
	prefixComposite = "cidx/"
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
	db              *badger.DB
	closed          atomic.Bool
	compositesMu    sync.RWMutex
	composites      []physical.CompositeIndexDef
	compositesByKey map[string]physical.CompositeIndexDef
}

// NewWithDB creates a new backend with an existing BadgerDB instance.
func NewWithDB(db *badger.DB) *Backend {
	return &Backend{
		db:              db,
		compositesByKey: make(map[string]physical.CompositeIndexDef),
	}
}

// RegisterCompositeIndex registers a composite index definition.
func (b *Backend) RegisterCompositeIndex(def physical.CompositeIndexDef) error {
	b.compositesMu.Lock()
	defer b.compositesMu.Unlock()
	fingerprint := strings.Join(def.Keys, "\x00")
	if _, exists := b.compositesByKey[fingerprint]; exists {
		return fmt.Errorf("composite index already registered for keys: %v", def.Keys)
	}
	b.composites = append(b.composites, def)
	b.compositesByKey[fingerprint] = def
	return nil
}

// CompositeIndexes returns all registered composite index definitions.
func (b *Backend) CompositeIndexes() []physical.CompositeIndexDef {
	b.compositesMu.RLock()
	defer b.compositesMu.RUnlock()
	out := make([]physical.CompositeIndexDef, len(b.composites))
	copy(out, b.composites)
	return out
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
				// Delete old composite index entries
				b.deleteCompositeKeys(txn, oldLabels, oldTsHex, refHex)
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
		ttl = time.Until(time.UnixMilli(entry.ExpiresAt))
		if ttl <= 0 {
			ttl = time.Millisecond
		}
	}

	// Store meta
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

	// Store composite index entries
	b.compositesMu.RLock()
	composites := b.composites
	b.compositesMu.RUnlock()

	for _, def := range composites {
		if key, ok := buildCompositeKey(def, entry.Labels, tsHex, refHex); ok {
			if ttl > 0 {
				if err := txn.SetEntry(badger.NewEntry(key, nil).WithTTL(ttl)); err != nil {
					return err
				}
			} else {
				if err := txn.Set(key, nil); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// buildCompositeKey returns the composite key if the labels have all required keys.
func buildCompositeKey(def physical.CompositeIndexDef, labels map[string]string, tsHex, refHex string) ([]byte, bool) {
	var buf strings.Builder
	buf.WriteString(prefixComposite)
	buf.WriteString(def.Name)
	buf.WriteByte('/')
	for _, k := range def.Keys {
		v, ok := labels[k]
		if !ok {
			return nil, false
		}
		buf.WriteString(v)
		buf.WriteByte('/')
	}
	buf.WriteString(tsHex)
	buf.WriteByte('/')
	buf.WriteString(refHex)
	return []byte(buf.String()), true
}

func (b *Backend) deleteCompositeKeys(txn *badger.Txn, labels map[string]string, tsHex, refHex string) {
	b.compositesMu.RLock()
	composites := b.composites
	b.compositesMu.RUnlock()

	for _, def := range composites {
		if key, ok := buildCompositeKey(def, labels, tsHex, refHex); ok {
			_ = txn.Delete(key)
		}
	}
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
		return b.deleteInTxn(txn, r)
	})
}

func (b *Backend) deleteInTxn(txn *badger.Txn, r reference.Reference) error {
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
			b.deleteCompositeKeys(txn, labels, tsHex, refHex)
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

	// Check for OR label filter
	if opts.LabelFilter != nil && len(opts.LabelFilter.OR) > 0 {
		return b.queryByLabelFilter(opts)
	}

	if len(opts.Labels) > 0 {
		return b.queryByLabel(opts)
	}

	return b.queryByEntry(opts)
}

func (b *Backend) queryByEntry(opts *physical.QueryOptions) (*physical.QueryResult, error) {
	now := time.Now().UnixMilli()
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

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

		needExpiry := !opts.IncludeExpired
		skipFirst := opts.Cursor != ""

		for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
			if skipFirst {
				skipFirst = false
				continue
			}

			key := it.Item().Key()
			suffix := string(key[len(prefixMeta):])
			if len(suffix) < 81 {
				continue
			}

			tsHex := suffix[:16]

			if !opts.Descending && beforeHex != "" && tsHex >= beforeHex {
				break
			}
			if opts.Descending && afterHex != "" && tsHex <= afterHex {
				break
			}

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

// pickDrivingLabel selects the label with the fewest index entries.
func pickDrivingLabel(db *badger.DB, labels map[string]string) (string, string) {
	if len(labels) <= 1 {
		for k, v := range labels {
			return k, v
		}
		return "", ""
	}

	const sampleLimit = 101
	bestKey, bestVal := "", ""
	bestCount := int(^uint(0) >> 1)

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
			if bestCount == 0 {
				break
			}
		}
		return nil
	})

	return bestKey, bestVal
}

func (b *Backend) queryByLabel(opts *physical.QueryOptions) (*physical.QueryResult, error) {
	// Try composite index first
	if def, vals, ok := b.findCompositeIndex(opts.Labels); ok {
		return b.queryByComposite(def, vals, opts)
	}

	now := time.Now().UnixMilli()
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	dk, dv := pickDrivingLabel(b.db, opts.Labels)

	var results []*physical.Entry
	var nextCursor string
	hasMore := false

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
		iterOpts.PrefetchValues = false
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
			suffix := string(key[len(labelPrefix):])
			if len(suffix) < 17 {
				continue
			}

			tsHex := suffix[:16]

			if !opts.Descending && beforeHex != "" && tsHex >= beforeHex {
				break
			}
			if opts.Descending && afterHex != "" && tsHex <= afterHex {
				break
			}

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

// findCompositeIndex checks if query labels match a registered composite index.
func (b *Backend) findCompositeIndex(labels map[string]string) (physical.CompositeIndexDef, []string, bool) {
	b.compositesMu.RLock()
	defer b.compositesMu.RUnlock()

	var best physical.CompositeIndexDef
	var bestVals []string

	for _, def := range b.composites {
		vals := make([]string, 0, len(def.Keys))
		match := true
		for _, k := range def.Keys {
			v, ok := labels[k]
			if !ok {
				match = false
				break
			}
			vals = append(vals, v)
		}
		if match && len(def.Keys) > len(best.Keys) {
			best = def
			bestVals = vals
		}
	}

	return best, bestVals, len(best.Keys) > 0
}

// queryByComposite scans a composite index prefix.
func (b *Backend) queryByComposite(def physical.CompositeIndexDef, vals []string, opts *physical.QueryOptions) (*physical.QueryResult, error) {
	now := time.Now().UnixMilli()
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	var prefixBuf strings.Builder
	prefixBuf.WriteString(prefixComposite)
	prefixBuf.WriteString(def.Name)
	prefixBuf.WriteByte('/')
	for _, v := range vals {
		prefixBuf.WriteString(v)
		prefixBuf.WriteByte('/')
	}
	compositePrefix := []byte(prefixBuf.String())

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
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.PrefetchValues = false
		iterOpts.Prefix = compositePrefix
		iterOpts.Reverse = opts.Descending

		it := txn.NewIterator(iterOpts)
		defer it.Close()

		var seekKey []byte
		switch {
		case opts.Cursor != "":
			seekKey = append(compositePrefix, []byte(opts.Cursor)...)
			if opts.Descending {
				seekKey = append(seekKey, 0xFF)
			}
		case opts.Descending:
			if beforeHex != "" {
				seekKey = append(compositePrefix, []byte(beforeHex)...)
				seekKey = append(seekKey, 0xFF)
			} else {
				seekKey = prefixEndKey(compositePrefix)
			}
		default:
			if afterHex != "" {
				seekKey = append(compositePrefix, []byte(afterHex)...)
			} else {
				seekKey = compositePrefix
			}
		}

		skipFirst := opts.Cursor != ""

		for it.Seek(seekKey); it.ValidForPrefix(compositePrefix); it.Next() {
			if skipFirst {
				skipFirst = false
				continue
			}

			key := it.Item().Key()
			// Extract suffix (tsHex/refHex) from the end of the composite key
			rest := string(key[len(compositePrefix):])
			// rest = tsHex(16)/refHex(64)
			if len(rest) < 81 {
				continue
			}

			tsHex := rest[:16]

			if !opts.Descending && beforeHex != "" && tsHex >= beforeHex {
				break
			}
			if opts.Descending && afterHex != "" && tsHex <= afterHex {
				break
			}

			suffix := rest // tsHex/refHex
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
				// Check expiry and any extra labels not in the composite index
				if !opts.IncludeExpired && expiresAt > 0 && expiresAt <= now {
					skip = true
					return nil
				}
				// Validate extra labels beyond the composite index
				for k, v := range opts.Labels {
					if labels[k] != v {
						skip = true
						return nil
					}
				}
				if len(results) >= limit {
					return nil
				}
				var buildErr error
				entry, buildErr = entryFromSuffix(suffix, expiresAt, labels)
				return buildErr
			}); metaDecErr != nil {
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
		return nil, fmt.Errorf("badger composite query: %w", err)
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

// queryByLabelFilter handles OR label filter queries.
func (b *Backend) queryByLabelFilter(opts *physical.QueryOptions) (*physical.QueryResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	seen := make(map[reference.Reference]bool)
	var allEntries []*physical.Entry

	for _, group := range opts.LabelFilter.OR {
		groupLabels := make(map[string]string, len(group.Predicates))
		for _, p := range group.Predicates {
			groupLabels[p.Key] = p.Value
		}
		groupOpts := *opts
		groupOpts.Labels = groupLabels
		groupOpts.LabelFilter = nil
		groupOpts.Limit = limit + 1

		result, err := b.queryByLabel(&groupOpts)
		if err != nil {
			return nil, err
		}
		for _, e := range result.Entries {
			if !seen[e.Ref] {
				seen[e.Ref] = true
				allEntries = append(allEntries, e)
			}
		}
	}

	// Sort merged results by timestamp
	sort.Slice(allEntries, func(i, j int) bool {
		if opts.Descending {
			if allEntries[i].Timestamp == allEntries[j].Timestamp {
				return reference.Hex(allEntries[i].Ref) > reference.Hex(allEntries[j].Ref)
			}
			return allEntries[i].Timestamp > allEntries[j].Timestamp
		}
		if allEntries[i].Timestamp == allEntries[j].Timestamp {
			return reference.Hex(allEntries[i].Ref) < reference.Hex(allEntries[j].Ref)
		}
		return allEntries[i].Timestamp < allEntries[j].Timestamp
	})

	hasMore := len(allEntries) > limit
	if hasMore {
		allEntries = allEntries[:limit]
	}

	var nextCursor string
	if hasMore && len(allEntries) > 0 {
		last := allEntries[len(allEntries)-1]
		nextCursor = timestampToHex(last.Timestamp) + "/" + reference.Hex(last.Ref)
	}

	return &physical.QueryResult{
		Entries:    allEntries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// Count returns the number of entries matching the given options.
func (b *Backend) Count(_ context.Context, opts *physical.QueryOptions) (int64, error) {
	if b.closed.Load() {
		return 0, physical.ErrClosed
	}
	if opts == nil {
		opts = &physical.QueryOptions{}
	}

	now := time.Now().UnixMilli()
	var afterHex, beforeHex string
	if opts.After > 0 {
		afterHex = timestampToHex(opts.After)
	}
	if opts.Before > 0 {
		beforeHex = timestampToHex(opts.Before)
	}

	var count int64

	// Use composite index when available for multi-label counts.
	if len(opts.Labels) > 1 {
		if def, vals, ok := b.findCompositeIndex(opts.Labels); ok {
			return b.countByComposite(def, vals, opts, afterHex, beforeHex)
		}
	}

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

func (b *Backend) countByEntry(txn *badger.Txn, _ *physical.QueryOptions, _ int64, afterHex, beforeHex string, count *int64) error {
	prefix := []byte(prefixMeta)
	iterOpts := badger.DefaultIteratorOptions
	// Badger handles TTL natively — expired keys are invisible to iterators.
	// No need to fetch values just to check ExpiresAt.
	iterOpts.PrefetchValues = false
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

		*count++
	}
	return nil
}

func (b *Backend) countByLabel(txn *badger.Txn, opts *physical.QueryOptions, _ int64, afterHex, beforeHex string, count *int64) error {
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

	// Only need meta validation for multi-label AND queries (to verify
	// additional labels beyond the driving label). Badger handles TTL
	// natively, so expiry checks are unnecessary.
	needValidate := len(opts.Labels) > 1

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
				_, labels, decErr := decodeMeta(val)
				if decErr != nil {
					return decErr
				}
				for k, v := range opts.Labels {
					if labels[k] != v {
						skip = true
						return nil
					}
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

// DeleteExpired is a no-op for badger.
func (b *Backend) DeleteExpired(_ context.Context, _ time.Time) (int, error) {
	if b.closed.Load() {
		return 0, physical.ErrClosed
	}
	return 0, nil
}

func (b *Backend) countByComposite(def physical.CompositeIndexDef, vals []string, opts *physical.QueryOptions, afterHex, beforeHex string) (int64, error) {
	var prefixBuf strings.Builder
	prefixBuf.WriteString(prefixComposite)
	prefixBuf.WriteString(def.Name)
	prefixBuf.WriteByte('/')
	for _, v := range vals {
		prefixBuf.WriteString(v)
		prefixBuf.WriteByte('/')
	}
	compositePrefix := []byte(prefixBuf.String())

	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.PrefetchValues = false
		iterOpts.Prefix = compositePrefix

		it := txn.NewIterator(iterOpts)
		defer it.Close()

		var seekKey []byte
		if afterHex != "" {
			seekKey = append(compositePrefix, []byte(afterHex)...)
		} else {
			seekKey = compositePrefix
		}

		// Extra labels beyond the composite index keys need validation.
		extraLabels := make(map[string]string)
		defKeySet := make(map[string]struct{}, len(def.Keys))
		for _, k := range def.Keys {
			defKeySet[k] = struct{}{}
		}
		for k, v := range opts.Labels {
			if _, ok := defKeySet[k]; !ok {
				extraLabels[k] = v
			}
		}
		needValidate := len(extraLabels) > 0

		for it.Seek(seekKey); it.ValidForPrefix(compositePrefix); it.Next() {
			key := it.Item().Key()
			rest := string(key[len(compositePrefix):])
			if len(rest) < 17 {
				continue
			}
			tsHex := rest[:16]

			if beforeHex != "" && tsHex >= beforeHex {
				break
			}

			if needValidate {
				suffix := rest
				metaKey := []byte(prefixMeta + suffix)
				metaItem, err := txn.Get(metaKey)
				if err != nil {
					continue
				}
				var skip bool
				if err := metaItem.Value(func(val []byte) error {
					_, labels, decErr := decodeMeta(val)
					if decErr != nil {
						return decErr
					}
					for k, v := range extraLabels {
						if labels[k] != v {
							skip = true
							return nil
						}
					}
					return nil
				}); err != nil || skip {
					continue
				}
			}

			count++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("badger composite count: %w", err)
	}
	return count, nil
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

// RunGC triggers value log garbage collection.
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
func encodeMeta(entry *physical.Entry) []byte {
	size := 8 + 2
	for k, v := range entry.Labels {
		size += 2 + len(k) + 2 + len(v)
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint64(buf[0:8], uint64(entry.ExpiresAt)) //nolint:gosec
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

func decodeMetaExpiresAt(data []byte) (int64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("meta too short: %d", len(data))
	}
	return int64(binary.BigEndian.Uint64(data[0:8])), nil
}

func entryFromMetaBytes(suffix string, data []byte) (*physical.Entry, error) {
	expiresAt, labels, err := decodeMeta(data)
	if err != nil {
		return nil, err
	}
	return entryFromSuffix(suffix, expiresAt, labels)
}

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

func prefixEndKey(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	end[len(end)-1]++
	return end
}

func timestampToHex(ts int64) string {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(ts)) //nolint:gosec
	return fmt.Sprintf("%016x", buf)
}

func hexToTimestamp(h string) (int64, error) {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 8 {
		return 0, fmt.Errorf("invalid timestamp hex: %s", h)
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

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
