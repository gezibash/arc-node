// Package badger provides a BadgerDB-backed blob storage backend.
package badger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
	"github.com/gezibash/arc-node/internal/storage"
)

const keyPrefix = "blob/"

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
		KeyPath:             "~/.arc/blobs",
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

	slog.Info("badger blobstore initialized", "path", path, "sync_writes", syncWrites)
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

	slog.Info("badger blobstore initialized (in-memory)")
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

// Put stores data at the given reference.
func (b *Backend) Put(_ context.Context, r reference.Reference, data []byte) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	key := keyPrefix + reference.Hex(r)
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
	if err != nil {
		return fmt.Errorf("badger put: %w", err)
	}
	return nil
}

// Get retrieves data by reference.
func (b *Backend) Get(_ context.Context, r reference.Reference) ([]byte, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	key := keyPrefix + reference.Hex(r)
	var data []byte

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, physical.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("badger get: %w", err)
	}
	return data, nil
}

// Exists checks if a blob exists.
func (b *Backend) Exists(_ context.Context, r reference.Reference) (bool, error) {
	if b.closed.Load() {
		return false, physical.ErrClosed
	}

	key := keyPrefix + reference.Hex(r)
	var exists bool

	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			exists = false
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("badger exists: %w", err)
	}
	return exists, nil
}

// Delete removes a blob by reference.
func (b *Backend) Delete(_ context.Context, r reference.Reference) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	key := keyPrefix + reference.Hex(r)
	err := b.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return txn.Delete([]byte(key))
	})
	if err != nil {
		return fmt.Errorf("badger delete: %w", err)
	}
	return nil
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

// ScanPrefix returns references matching the given hex prefix.
// limit controls the maximum number of results returned.
func (b *Backend) ScanPrefix(_ context.Context, hexPrefix string, limit int) ([]reference.Reference, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	var refs []reference.Reference
	prefix := []byte(keyPrefix + hexPrefix)

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			hex := strings.TrimPrefix(string(it.Item().Key()), keyPrefix)
			ref, err := reference.FromHex(hex)
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
