package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
)

// testBlobStore runs the standard test suite against any BlobStore implementation.
func testBlobStore(t *testing.T, store BlobStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("PutGet", func(t *testing.T) {
		data := []byte("hello world")
		expectedCID := sha256.Sum256(data)

		cid, err := store.Put(ctx, data)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if cid != expectedCID {
			t.Errorf("CID mismatch: got %x, want %x", cid, expectedCID)
		}

		got, err := store.Get(ctx, cid[:])
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("Get returned %q, want %q", got, data)
		}
	})

	t.Run("Has", func(t *testing.T) {
		data := []byte("has test")
		cid, _ := store.Put(ctx, data)

		exists, err := store.Has(ctx, cid[:])
		if err != nil {
			t.Fatalf("Has failed: %v", err)
		}
		if !exists {
			t.Error("Has returned false for stored blob")
		}

		// Non-existent
		fakeCID := sha256.Sum256([]byte("does not exist"))
		exists, err = store.Has(ctx, fakeCID[:])
		if err != nil {
			t.Fatalf("Has failed: %v", err)
		}
		if exists {
			t.Error("Has returned true for non-existent blob")
		}
	})

	t.Run("Size", func(t *testing.T) {
		data := []byte("size test data")
		cid, _ := store.Put(ctx, data)

		size, err := store.Size(ctx, cid[:])
		if err != nil {
			t.Fatalf("Size failed: %v", err)
		}
		if size != int64(len(data)) {
			t.Errorf("Size returned %d, want %d", size, len(data))
		}

		// Non-existent
		fakeCID := sha256.Sum256([]byte("does not exist"))
		_, err = store.Size(ctx, fakeCID[:])
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Size for non-existent: got %v, want ErrNotFound", err)
		}
	})

	t.Run("GetReader", func(t *testing.T) {
		data := []byte("reader test data")
		cid, _ := store.Put(ctx, data)

		r, size, err := store.GetReader(ctx, cid[:])
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

		cid, n, err := store.PutStream(ctx, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("PutStream failed: %v", err)
		}
		if cid != expectedCID {
			t.Errorf("CID mismatch: got %x, want %x", cid, expectedCID)
		}
		if n != int64(len(data)) {
			t.Errorf("size mismatch: got %d, want %d", n, len(data))
		}

		got, _ := store.Get(ctx, cid[:])
		if !bytes.Equal(got, data) {
			t.Errorf("Get returned wrong data after PutStream")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		data := []byte("delete test")
		cid, _ := store.Put(ctx, data)

		if err := store.Delete(ctx, cid[:]); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		exists, _ := store.Has(ctx, cid[:])
		if exists {
			t.Error("Has returned true after delete")
		}

		_, err := store.Get(ctx, cid[:])
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Get after delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("Idempotent", func(t *testing.T) {
		data := []byte("idempotent test")

		cid1, _ := store.Put(ctx, data)
		cid2, _ := store.Put(ctx, data)

		if cid1 != cid2 {
			t.Errorf("CIDs differ: %x vs %x", cid1, cid2)
		}
	})

	t.Run("InvalidCID", func(t *testing.T) {
		_, err := store.Get(ctx, []byte("short"))
		if !errors.Is(err, ErrInvalidCID) {
			t.Errorf("Get with short CID: got %v, want ErrInvalidCID", err)
		}

		_, err = store.Has(ctx, []byte("short"))
		if !errors.Is(err, ErrInvalidCID) {
			t.Errorf("Has with short CID: got %v, want ErrInvalidCID", err)
		}

		_, err = store.Size(ctx, []byte("short"))
		if !errors.Is(err, ErrInvalidCID) {
			t.Errorf("Size with short CID: got %v, want ErrInvalidCID", err)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		fakeCID := sha256.Sum256([]byte("this blob does not exist"))

		_, err := store.Get(ctx, fakeCID[:])
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Get non-existent: got %v, want ErrNotFound", err)
		}

		_, _, err = store.GetReader(ctx, fakeCID[:])
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("GetReader non-existent: got %v, want ErrNotFound", err)
		}
	})
}

// TestMemoryStoreInterface runs the interface test suite against MemoryStore.
func TestMemoryStoreInterface(t *testing.T) {
	store := NewMemoryStore()
	defer func() { _ = store.Close() }()

	testBlobStore(t, store)
}

// TestRegistryMemory tests the registry with the memory backend.
func TestRegistryMemory(t *testing.T) {
	if !IsRegistered(BackendMemory) {
		t.Fatal("memory backend not registered")
	}

	store, err := New(context.Background(), BackendMemory, nil)
	if err != nil {
		t.Fatalf("New(memory) failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	cid, err := store.Put(context.Background(), []byte("test"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	exists, err := store.Has(context.Background(), cid[:])
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	if !exists {
		t.Error("Has returned false")
	}
}

// TestRegistryUnknownBackend tests that unknown backends return an error.
func TestRegistryUnknownBackend(t *testing.T) {
	_, err := New(context.Background(), "unknown", nil)
	if err == nil {
		t.Error("expected error for unknown backend")
	}
}

// TestListBackends tests that ListBackends returns registered backends.
func TestListBackends(t *testing.T) {
	backends := ListBackends()
	found := false
	for _, b := range backends {
		if b == BackendMemory {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("ListBackends() = %v, want to contain %q", backends, BackendMemory)
	}
}
