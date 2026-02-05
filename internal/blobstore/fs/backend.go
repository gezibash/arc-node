// Package fs provides a filesystem-based BlobStore backend.
package fs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/gezibash/arc/v2/internal/blobstore"
	"github.com/gezibash/arc/v2/internal/storage"
)

func init() {
	blobstore.Register(blobstore.BackendFile, factory, defaults)
}

func defaults() map[string]string {
	return map[string]string{
		"dir_permissions":  "0700",
		"file_permissions": "0600",
	}
}

func factory(_ context.Context, config map[string]string) (blobstore.BlobStore, error) {
	path := storage.GetString(config, "path", "")
	if path == "" {
		return nil, storage.NewConfigError("file", "path", "required")
	}
	path = storage.ExpandPath(path)

	dirPerm, err := parseFileMode(config, "dir_permissions", 0700)
	if err != nil {
		return nil, err
	}

	filePerm, err := parseFileMode(config, "file_permissions", 0600)
	if err != nil {
		return nil, err
	}

	return New(path, dirPerm, filePerm)
}

func parseFileMode(config map[string]string, key string, def fs.FileMode) (fs.FileMode, error) {
	v := storage.GetString(config, key, "")
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(v, 8, 32)
	if err != nil {
		return 0, storage.NewConfigErrorWithValue("file", key, v, "must be an octal file mode (e.g., 0700)")
	}
	return fs.FileMode(n), nil
}

// Store provides content-addressed blob storage using the filesystem.
// Blobs are stored in a two-level sharded directory: ab/cdef...
type Store struct {
	dir      string
	dirPerm  fs.FileMode
	filePerm fs.FileMode
	closed   atomic.Bool
}

// New creates a filesystem-based store at the given directory.
func New(dir string, dirPerm, filePerm fs.FileMode) (*Store, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("fs: create store dir: %w", err)
	}
	return &Store{
		dir:      dir,
		dirPerm:  dirPerm,
		filePerm: filePerm,
	}, nil
}

func (s *Store) checkClosed() error {
	if s.closed.Load() {
		return blobstore.ErrClosed
	}
	return nil
}

// Put stores a blob and returns its CID (SHA-256 hash).
func (s *Store) Put(_ context.Context, data []byte) ([32]byte, error) {
	if err := s.checkClosed(); err != nil {
		return [32]byte{}, err
	}

	cid := sha256.Sum256(data)
	path := s.path(cid[:])

	// Check if already exists
	if _, err := os.Stat(path); err == nil {
		return cid, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), s.dirPerm); err != nil {
		return [32]byte{}, fmt.Errorf("fs: create dir: %w", err)
	}

	// Write atomically via temp file
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, s.filePerm); err != nil {
		return [32]byte{}, fmt.Errorf("fs: write temp: %w", err)
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return [32]byte{}, fmt.Errorf("fs: rename: %w", err)
	}

	return cid, nil
}

// PutStream stores a blob from a reader and returns its CID and size.
func (s *Store) PutStream(_ context.Context, r io.Reader) ([32]byte, int64, error) {
	if err := s.checkClosed(); err != nil {
		return [32]byte{}, 0, err
	}

	// Write to temp file while computing hash
	tmp, err := os.CreateTemp(s.dir, "blob-*.tmp")
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("fs: create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) // Clean up on error path
	}()

	h := sha256.New()
	w := io.MultiWriter(tmp, h)

	n, err := io.Copy(w, r)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("fs: copy: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return [32]byte{}, 0, fmt.Errorf("fs: close temp: %w", err)
	}

	// Set file permissions
	if err := os.Chmod(tmpPath, s.filePerm); err != nil {
		return [32]byte{}, 0, fmt.Errorf("fs: chmod temp: %w", err)
	}

	var cid [32]byte
	copy(cid[:], h.Sum(nil))
	path := s.path(cid[:])

	// Check if already exists
	if _, err := os.Stat(path); err == nil {
		return cid, n, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), s.dirPerm); err != nil {
		return [32]byte{}, 0, fmt.Errorf("fs: create dir: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return [32]byte{}, 0, fmt.Errorf("fs: rename: %w", err)
	}

	return cid, n, nil
}

// Get retrieves a blob by CID.
func (s *Store) Get(_ context.Context, cid []byte) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if len(cid) != 32 {
		return nil, blobstore.ErrInvalidCID
	}

	data, err := os.ReadFile(s.path(cid))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blobstore.ErrNotFound
		}
		return nil, fmt.Errorf("fs: read blob: %w", err)
	}

	return data, nil
}

// GetReader returns a reader for a blob and its size.
func (s *Store) GetReader(_ context.Context, cid []byte) (io.ReadCloser, int64, error) {
	if err := s.checkClosed(); err != nil {
		return nil, 0, err
	}
	if len(cid) != 32 {
		return nil, 0, blobstore.ErrInvalidCID
	}

	f, err := os.Open(s.path(cid))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, blobstore.ErrNotFound
		}
		return nil, 0, fmt.Errorf("fs: open blob: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("fs: stat blob: %w", err)
	}

	return f, info.Size(), nil
}

// Has checks if a blob exists.
func (s *Store) Has(_ context.Context, cid []byte) (bool, error) {
	if err := s.checkClosed(); err != nil {
		return false, err
	}
	if len(cid) != 32 {
		return false, blobstore.ErrInvalidCID
	}
	_, err := os.Stat(s.path(cid))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("fs: stat: %w", err)
	}
	return true, nil
}

// Size returns the size of a blob.
func (s *Store) Size(_ context.Context, cid []byte) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}
	if len(cid) != 32 {
		return 0, blobstore.ErrInvalidCID
	}
	info, err := os.Stat(s.path(cid))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, blobstore.ErrNotFound
		}
		return 0, fmt.Errorf("fs: stat: %w", err)
	}
	return info.Size(), nil
}

// Delete removes a blob.
func (s *Store) Delete(_ context.Context, cid []byte) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if len(cid) != 32 {
		return blobstore.ErrInvalidCID
	}
	err := os.Remove(s.path(cid))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs: remove blob: %w", err)
	}
	return nil
}

// Close marks the store as closed.
func (s *Store) Close() error {
	s.closed.Swap(true)
	return nil
}

// Stats returns storage usage metrics by walking the blob directory.
func (s *Store) Stats(_ context.Context) (blobstore.StoreStats, error) {
	if err := s.checkClosed(); err != nil {
		return blobstore.StoreStats{}, err
	}
	var stats blobstore.StoreStats
	err := filepath.WalkDir(s.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Skip temp files
		if filepath.Ext(path) == ".tmp" {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil // skip files we can't stat
		}
		stats.BytesUsed += info.Size()
		stats.BlobCount++
		return nil
	})
	if err != nil {
		return blobstore.StoreStats{}, fmt.Errorf("fs: walk stats: %w", err)
	}
	return stats, nil
}

// path returns the file path for a CID using two-level sharding.
func (s *Store) path(cid []byte) string {
	hex := fmt.Sprintf("%x", cid)
	return filepath.Join(s.dir, hex[:2], hex[2:])
}
