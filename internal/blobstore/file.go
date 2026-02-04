package blobstore

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileStore provides content-addressed blob storage using the filesystem.
// Blobs are stored as files named by their SHA-256 hash.
// Implements BlobStore interface.
type FileStore struct {
	dir string
}

// NewFileStore creates a file-based store using the given directory.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

// Put stores a blob and returns its CID (SHA-256 hash).
func (s *FileStore) Put(data []byte) ([32]byte, error) {
	cid := sha256.Sum256(data)
	path := s.path(cid[:])

	// Check if already exists
	if _, err := os.Stat(path); err == nil {
		return cid, nil // Already stored
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return [32]byte{}, fmt.Errorf("create dir: %w", err)
	}

	// Write atomically via temp file
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return [32]byte{}, fmt.Errorf("write temp: %w", err)
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return [32]byte{}, fmt.Errorf("rename: %w", err)
	}

	return cid, nil
}

// PutStream stores a blob from a reader and returns its CID.
func (s *FileStore) PutStream(r io.Reader) ([32]byte, int64, error) {
	// Write to temp file while computing hash
	tmp, err := os.CreateTemp(s.dir, "blob-*.tmp")
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath) // Clean up on error
	}()

	h := sha256.New()
	w := io.MultiWriter(tmp, h)

	n, err := io.Copy(w, r)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("copy: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return [32]byte{}, 0, fmt.Errorf("close temp: %w", err)
	}

	var cid [32]byte
	copy(cid[:], h.Sum(nil))
	path := s.path(cid[:])

	// Check if already exists
	if _, err := os.Stat(path); err == nil {
		return cid, n, nil // Already stored
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return [32]byte{}, 0, fmt.Errorf("create dir: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return [32]byte{}, 0, fmt.Errorf("rename: %w", err)
	}

	return cid, n, nil
}

// Get retrieves a blob by CID.
func (s *FileStore) Get(cid []byte) ([]byte, error) {
	if len(cid) != 32 {
		return nil, ErrInvalidCID
	}

	data, err := os.ReadFile(s.path(cid))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("read blob: %w", err)
	}

	return data, nil
}

// GetReader returns a reader for a blob.
func (s *FileStore) GetReader(cid []byte) (io.ReadCloser, int64, error) {
	if len(cid) != 32 {
		return nil, 0, ErrInvalidCID
	}

	f, err := os.Open(s.path(cid))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("open blob: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("stat blob: %w", err)
	}

	return f, info.Size(), nil
}

// Has checks if a blob exists.
func (s *FileStore) Has(cid []byte) bool {
	if len(cid) != 32 {
		return false
	}
	_, err := os.Stat(s.path(cid))
	return err == nil
}

// Size returns the size of a blob, or -1 if not found.
func (s *FileStore) Size(cid []byte) int64 {
	if len(cid) != 32 {
		return -1
	}
	info, err := os.Stat(s.path(cid))
	if err != nil {
		return -1
	}
	return info.Size()
}

// Delete removes a blob.
func (s *FileStore) Delete(cid []byte) error {
	if len(cid) != 32 {
		return ErrInvalidCID
	}
	err := os.Remove(s.path(cid))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove blob: %w", err)
	}
	return nil
}

// Close is a no-op for file store.
func (s *FileStore) Close() error {
	return nil
}

// path returns the file path for a CID.
// Uses two-level directory structure: ab/cdef... for better filesystem performance.
func (s *FileStore) path(cid []byte) string {
	hex := fmt.Sprintf("%x", cid)
	return filepath.Join(s.dir, hex[:2], hex[2:])
}
