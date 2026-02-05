// Package badger provides a BadgerDB-based BlobStore backend.
package badger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/gezibash/arc/v2/internal/blobstore"
	"github.com/gezibash/arc/v2/internal/storage"
)

const keyPrefix = "blob/"

func init() {
	blobstore.Register(blobstore.BackendBadger, factory, defaults)
}

func defaults() map[string]string {
	return map[string]string{
		"sync_writes":         "false",
		"value_log_file_size": "256",
		"mem_table_size":      "64",
		"in_memory":           "false",
	}
}

func factory(_ context.Context, config map[string]string) (blobstore.BlobStore, error) {
	path := storage.GetString(config, "path", "")
	inMemory, err := storage.GetBool(config, "in_memory", false)
	if err != nil {
		return nil, err
	}

	if path == "" && !inMemory {
		return nil, storage.NewConfigError("badger", "path", "required (unless in_memory=true)")
	}

	if path != "" {
		path = storage.ExpandPath(path)
	}

	syncWrites, err := storage.GetBool(config, "sync_writes", false)
	if err != nil {
		return nil, err
	}

	vlogSize, err := storage.GetInt64(config, "value_log_file_size", 256)
	if err != nil {
		return nil, err
	}

	memTableSize, err := storage.GetInt64(config, "mem_table_size", 64)
	if err != nil {
		return nil, err
	}

	opts := badgerdb.DefaultOptions(path)
	opts.SyncWrites = syncWrites
	opts.ValueLogFileSize = vlogSize * 1024 * 1024 // MB to bytes
	opts.MemTableSize = memTableSize * 1024 * 1024
	opts.Logger = nil // Suppress badger's internal logging

	if inMemory {
		opts = opts.WithInMemory(true)
	}

	db, err := badgerdb.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badger: open: %w", err)
	}

	return &Store{db: db}, nil
}

// Store provides content-addressed blob storage backed by BadgerDB.
type Store struct {
	db     *badgerdb.DB
	closed atomic.Bool
}

func (s *Store) checkClosed() error {
	if s.closed.Load() {
		return blobstore.ErrClosed
	}
	return nil
}

func blobKey(cid []byte) []byte {
	return []byte(keyPrefix + fmt.Sprintf("%x", cid))
}

// Put stores a blob and returns its CID (SHA-256 hash).
func (s *Store) Put(_ context.Context, data []byte) ([32]byte, error) {
	if err := s.checkClosed(); err != nil {
		return [32]byte{}, err
	}

	cid := sha256.Sum256(data)
	key := blobKey(cid[:])

	err := s.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Set(key, data)
	})
	if err != nil {
		return [32]byte{}, fmt.Errorf("badger: put: %w", err)
	}

	return cid, nil
}

// PutStream stores a blob from a reader. Reads all into memory then delegates to Put.
func (s *Store) PutStream(ctx context.Context, r io.Reader) ([32]byte, int64, error) {
	if err := s.checkClosed(); err != nil {
		return [32]byte{}, 0, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("badger: read stream: %w", err)
	}

	cid, err := s.Put(ctx, data)
	return cid, int64(len(data)), err
}

// Get retrieves a blob by CID.
func (s *Store) Get(_ context.Context, cid []byte) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if len(cid) != 32 {
		return nil, blobstore.ErrInvalidCID
	}

	key := blobKey(cid)
	var val []byte

	err := s.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return nil, blobstore.ErrNotFound
		}
		return nil, fmt.Errorf("badger: get: %w", err)
	}

	return val, nil
}

// GetReader returns a reader for a blob and its size.
func (s *Store) GetReader(ctx context.Context, cid []byte) (io.ReadCloser, int64, error) {
	data, err := s.Get(ctx, cid)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

// Has checks if a blob exists.
func (s *Store) Has(_ context.Context, cid []byte) (bool, error) {
	if err := s.checkClosed(); err != nil {
		return false, err
	}
	if len(cid) != 32 {
		return false, blobstore.ErrInvalidCID
	}

	key := blobKey(cid)
	err := s.db.View(func(txn *badgerdb.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if err != nil {
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("badger: has: %w", err)
	}
	return true, nil
}

// Size returns the size of a blob using item.ValueSize() to avoid loading the full value.
func (s *Store) Size(_ context.Context, cid []byte) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}
	if len(cid) != 32 {
		return 0, blobstore.ErrInvalidCID
	}

	key := blobKey(cid)
	var size int64

	err := s.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		size = item.ValueSize()
		return nil
	})
	if err != nil {
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return 0, blobstore.ErrNotFound
		}
		return 0, fmt.Errorf("badger: size: %w", err)
	}

	return size, nil
}

// Delete removes a blob.
func (s *Store) Delete(_ context.Context, cid []byte) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if len(cid) != 32 {
		return blobstore.ErrInvalidCID
	}

	key := blobKey(cid)
	err := s.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Delete(key)
	})
	if err != nil {
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return nil // Delete is idempotent
		}
		return fmt.Errorf("badger: delete: %w", err)
	}
	return nil
}

// Stats returns storage usage metrics by iterating keys.
func (s *Store) Stats(_ context.Context) (blobstore.StoreStats, error) {
	if err := s.checkClosed(); err != nil {
		return blobstore.StoreStats{}, err
	}
	var stats blobstore.StoreStats
	err := s.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			stats.BytesUsed += item.ValueSize()
			stats.BlobCount++
		}
		return nil
	})
	if err != nil {
		return blobstore.StoreStats{}, fmt.Errorf("badger: stats: %w", err)
	}
	return stats, nil
}

// Close closes the BadgerDB database.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}
	return s.db.Close()
}
