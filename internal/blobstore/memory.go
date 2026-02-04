package blobstore

import (
	"bytes"
	"crypto/sha256"
	"io"
	"sync"
)

// MemoryStore provides an in-memory blob store for testing.
// Implements BlobStore interface.
type MemoryStore struct {
	mu    sync.RWMutex
	blobs map[[32]byte][]byte
}

// NewMemoryStore creates an in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		blobs: make(map[[32]byte][]byte),
	}
}

// Put stores a blob and returns its CID.
func (s *MemoryStore) Put(data []byte) ([32]byte, error) {
	cid := sha256.Sum256(data)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Store a copy
	cp := make([]byte, len(data))
	copy(cp, data)
	s.blobs[cid] = cp

	return cid, nil
}

// PutStream stores a blob from a reader.
func (s *MemoryStore) PutStream(r io.Reader) ([32]byte, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return [32]byte{}, 0, err
	}

	cid, err := s.Put(data)
	return cid, int64(len(data)), err
}

// Get retrieves a blob by CID.
func (s *MemoryStore) Get(cid []byte) ([]byte, error) {
	if len(cid) != 32 {
		return nil, ErrInvalidCID
	}

	var key [32]byte
	copy(key[:], cid)

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.blobs[key]
	if !ok {
		return nil, ErrNotFound
	}

	// Return a copy
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

// GetReader returns a reader for a blob.
func (s *MemoryStore) GetReader(cid []byte) (io.ReadCloser, int64, error) {
	data, err := s.Get(cid)
	if err != nil {
		return nil, 0, err
	}
	return &readSeekCloser{bytes.NewReader(data)}, int64(len(data)), nil
}

// readSeekCloser wraps a bytes.Reader to implement io.ReadSeekCloser.
type readSeekCloser struct {
	*bytes.Reader
}

func (r *readSeekCloser) Close() error {
	return nil
}

// Has checks if a blob exists.
func (s *MemoryStore) Has(cid []byte) bool {
	if len(cid) != 32 {
		return false
	}

	var key [32]byte
	copy(key[:], cid)

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.blobs[key]
	return ok
}

// Size returns the size of a blob, or -1 if not found.
func (s *MemoryStore) Size(cid []byte) int64 {
	if len(cid) != 32 {
		return -1
	}

	var key [32]byte
	copy(key[:], cid)

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.blobs[key]
	if !ok {
		return -1
	}
	return int64(len(data))
}

// Delete removes a blob.
func (s *MemoryStore) Delete(cid []byte) error {
	if len(cid) != 32 {
		return ErrInvalidCID
	}

	var key [32]byte
	copy(key[:], cid)

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.blobs, key)
	return nil
}

// Close is a no-op for memory store.
func (s *MemoryStore) Close() error {
	return nil
}

// Len returns the number of blobs stored (for testing).
func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.blobs)
}
