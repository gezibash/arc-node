package blobstore

import (
	"errors"
	"fmt"
	"io"
)

// BlobStore is the interface for content-addressed blob storage.
type BlobStore interface {
	// Put stores a blob and returns its CID (SHA-256 hash).
	Put(data []byte) ([32]byte, error)

	// PutStream stores a blob from a reader and returns its CID and size.
	PutStream(r io.Reader) (cid [32]byte, size int64, err error)

	// Get retrieves a blob by CID.
	Get(cid []byte) ([]byte, error)

	// GetReader returns a reader for a blob and its size.
	GetReader(cid []byte) (io.ReadCloser, int64, error)

	// Has checks if a blob exists.
	Has(cid []byte) bool

	// Size returns the size of a blob, or -1 if not found.
	Size(cid []byte) int64

	// Delete removes a blob.
	Delete(cid []byte) error

	// Close releases any resources.
	Close() error
}

// Common errors.
var (
	ErrNotFound     = errors.New("blob not found")
	ErrInvalidCID   = errors.New("invalid content ID")
	ErrHashMismatch = errors.New("content hash mismatch")
)

// Backend names.
const (
	BackendFile   = "file"
	BackendMemory = "memory" // for testing
)

// New creates a BlobStore based on the backend config.
func New(backend string, cfg map[string]string) (BlobStore, error) {
	switch backend {
	case BackendFile, "": // default to file
		dir := cfg["dir"]
		if dir == "" {
			return nil, fmt.Errorf("file backend requires 'dir' config")
		}
		return NewFileStore(dir)

	case BackendMemory:
		return NewMemoryStore(), nil

	default:
		return nil, fmt.Errorf("unknown blob backend: %s", backend)
	}
}
