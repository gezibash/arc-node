package blobstore

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
)

// testBlobStore runs the standard test suite against any BlobStore implementation.
func testBlobStore(t *testing.T, store BlobStore) {
	t.Helper()

	t.Run("PutGet", func(t *testing.T) {
		data := []byte("hello world")
		expectedCID := sha256.Sum256(data)

		cid, err := store.Put(data)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if cid != expectedCID {
			t.Errorf("CID mismatch: got %x, want %x", cid, expectedCID)
		}

		got, err := store.Get(cid[:])
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("Get returned %q, want %q", got, data)
		}
	})

	t.Run("Has", func(t *testing.T) {
		data := []byte("has test")
		cid, _ := store.Put(data)

		if !store.Has(cid[:]) {
			t.Error("Has returned false for stored blob")
		}

		// Non-existent
		fakeCID := sha256.Sum256([]byte("does not exist"))
		if store.Has(fakeCID[:]) {
			t.Error("Has returned true for non-existent blob")
		}
	})

	t.Run("Size", func(t *testing.T) {
		data := []byte("size test data")
		cid, _ := store.Put(data)

		size := store.Size(cid[:])
		if size != int64(len(data)) {
			t.Errorf("Size returned %d, want %d", size, len(data))
		}

		// Non-existent
		fakeCID := sha256.Sum256([]byte("does not exist"))
		if store.Size(fakeCID[:]) != -1 {
			t.Error("Size returned non-negative for non-existent blob")
		}
	})

	t.Run("GetReader", func(t *testing.T) {
		data := []byte("reader test data")
		cid, _ := store.Put(data)

		r, size, err := store.GetReader(cid[:])
		if err != nil {
			t.Fatalf("GetReader failed: %v", err)
		}
		defer func() { _ = r.Close() }()

		if size != int64(len(data)) {
			t.Errorf("GetReader size: got %d, want %d", size, len(data))
		}

		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("GetReader data mismatch")
		}
	})

	t.Run("PutStream", func(t *testing.T) {
		data := []byte("streaming data test")
		expectedCID := sha256.Sum256(data)

		cid, n, err := store.PutStream(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("PutStream failed: %v", err)
		}
		if cid != expectedCID {
			t.Errorf("CID mismatch: got %x, want %x", cid, expectedCID)
		}
		if n != int64(len(data)) {
			t.Errorf("size mismatch: got %d, want %d", n, len(data))
		}

		got, _ := store.Get(cid[:])
		if !bytes.Equal(got, data) {
			t.Errorf("Get returned wrong data after PutStream")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		data := []byte("delete test")
		cid, _ := store.Put(data)

		if err := store.Delete(cid[:]); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if store.Has(cid[:]) {
			t.Error("Has returned true after delete")
		}

		_, err := store.Get(cid[:])
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Get after delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("Idempotent", func(t *testing.T) {
		data := []byte("idempotent test")

		cid1, _ := store.Put(data)
		cid2, _ := store.Put(data)

		if cid1 != cid2 {
			t.Errorf("CIDs differ: %x vs %x", cid1, cid2)
		}
	})

	t.Run("InvalidCID", func(t *testing.T) {
		_, err := store.Get([]byte("short"))
		if !errors.Is(err, ErrInvalidCID) {
			t.Errorf("Get with short CID: got %v, want ErrInvalidCID", err)
		}

		if store.Has([]byte("short")) {
			t.Error("Has returned true for invalid CID")
		}

		if store.Size([]byte("short")) != -1 {
			t.Error("Size returned non-negative for invalid CID")
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		fakeCID := sha256.Sum256([]byte("this blob does not exist"))

		_, err := store.Get(fakeCID[:])
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Get non-existent: got %v, want ErrNotFound", err)
		}

		_, _, err = store.GetReader(fakeCID[:])
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("GetReader non-existent: got %v, want ErrNotFound", err)
		}
	})
}

// TestFileStoreInterface runs the interface test suite against FileStore.
func TestFileStoreInterface(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	testBlobStore(t, store)
}

// TestMemoryStoreInterface runs the interface test suite against MemoryStore.
func TestMemoryStoreInterface(t *testing.T) {
	store := NewMemoryStore()
	defer func() { _ = store.Close() }()

	testBlobStore(t, store)
}

// TestNewFactory tests the factory function.
func TestNewFactory(t *testing.T) {
	t.Run("File", func(t *testing.T) {
		dir := t.TempDir()
		store, err := New(BackendFile, map[string]string{"dir": dir})
		if err != nil {
			t.Fatalf("New(file) failed: %v", err)
		}
		defer func() { _ = store.Close() }()

		// Verify it works
		cid, err := store.Put([]byte("test"))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if !store.Has(cid[:]) {
			t.Error("Has returned false")
		}
	})

	t.Run("FileDefaultBackend", func(t *testing.T) {
		dir := t.TempDir()
		store, err := New("", map[string]string{"dir": dir})
		if err != nil {
			t.Fatalf("New('') failed: %v", err)
		}
		defer func() { _ = store.Close() }()

		_, err = store.Put([]byte("test"))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	t.Run("Memory", func(t *testing.T) {
		store, err := New(BackendMemory, nil)
		if err != nil {
			t.Fatalf("New(memory) failed: %v", err)
		}
		defer func() { _ = store.Close() }()

		cid, err := store.Put([]byte("test"))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if !store.Has(cid[:]) {
			t.Error("Has returned false")
		}
	})

	t.Run("FileMissingDir", func(t *testing.T) {
		_, err := New(BackendFile, nil)
		if err == nil {
			t.Error("expected error for file backend without dir")
		}
	})

	t.Run("UnknownBackend", func(t *testing.T) {
		_, err := New("unknown", nil)
		if err == nil {
			t.Error("expected error for unknown backend")
		}
	})
}
