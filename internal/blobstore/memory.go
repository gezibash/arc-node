package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"sync"
)

func init() {
	Register(BackendMemory, func(_ context.Context, _ map[string]string) (BlobStore, error) {
		return NewMemoryStore(), nil
	}, nil)
}

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
func (s *MemoryStore) Put(_ context.Context, data []byte) ([32]byte, error) {
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
func (s *MemoryStore) PutStream(ctx context.Context, r io.Reader) ([32]byte, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return [32]byte{}, 0, err
	}

	cid, err := s.Put(ctx, data)
	return cid, int64(len(data)), err
}

// Get retrieves a blob by CID.
func (s *MemoryStore) Get(_ context.Context, cid []byte) ([]byte, error) {
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
func (s *MemoryStore) GetReader(ctx context.Context, cid []byte) (io.ReadCloser, int64, error) {
	data, err := s.Get(ctx, cid)
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
func (s *MemoryStore) Has(_ context.Context, cid []byte) (bool, error) {
	if len(cid) != 32 {
		return false, ErrInvalidCID
	}

	var key [32]byte
	copy(key[:], cid)

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.blobs[key]
	return ok, nil
}

// Size returns the size of a blob.
func (s *MemoryStore) Size(_ context.Context, cid []byte) (int64, error) {
	if len(cid) != 32 {
		return 0, ErrInvalidCID
	}

	var key [32]byte
	copy(key[:], cid)

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.blobs[key]
	if !ok {
		return 0, ErrNotFound
	}
	return int64(len(data)), nil
}

// Delete removes a blob.
func (s *MemoryStore) Delete(_ context.Context, cid []byte) error {
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

// Stats returns storage usage metrics.
func (s *MemoryStore) Stats(_ context.Context) (StoreStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var used int64
	for _, data := range s.blobs {
		used += int64(len(data))
	}
	return StoreStats{BytesUsed: used, BlobCount: int64(len(s.blobs))}, nil
}

// Len returns the number of blobs stored (for testing).
func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.blobs)
}
