package badger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/gezibash/arc/v2/internal/blobstore"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()

	store, err := blobstore.New(context.Background(), blobstore.BackendBadger, map[string]string{
		"path": t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New(badger) failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	return store.(*Store)
}

func TestPutGet(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

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
}

func TestHas(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	data := []byte("has test")
	cid, _ := store.Put(ctx, data)

	exists, err := store.Has(ctx, cid[:])
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	if !exists {
		t.Error("Has returned false for stored blob")
	}

	fakeCID := sha256.Sum256([]byte("does not exist"))
	exists, err = store.Has(ctx, fakeCID[:])
	if err != nil {
		t.Fatalf("Has failed: %v", err)
	}
	if exists {
		t.Error("Has returned true for non-existent blob")
	}
}

func TestSize(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	data := []byte("size test data")
	cid, _ := store.Put(ctx, data)

	size, err := store.Size(ctx, cid[:])
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("Size returned %d, want %d", size, len(data))
	}

	fakeCID := sha256.Sum256([]byte("does not exist"))
	_, err = store.Size(ctx, fakeCID[:])
	if !errors.Is(err, blobstore.ErrNotFound) {
		t.Errorf("Size for non-existent: got %v, want ErrNotFound", err)
	}
}

func TestGetReader(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

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
}

func TestPutStream(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

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
}

func TestDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

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
	if !errors.Is(err, blobstore.ErrNotFound) {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestInvalidCID(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.Get(ctx, []byte("short"))
	if !errors.Is(err, blobstore.ErrInvalidCID) {
		t.Errorf("Get with short CID: got %v, want ErrInvalidCID", err)
	}

	_, err = store.Has(ctx, []byte("short"))
	if !errors.Is(err, blobstore.ErrInvalidCID) {
		t.Errorf("Has with short CID: got %v, want ErrInvalidCID", err)
	}

	_, err = store.Size(ctx, []byte("short"))
	if !errors.Is(err, blobstore.ErrInvalidCID) {
		t.Errorf("Size with short CID: got %v, want ErrInvalidCID", err)
	}
}

func TestNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	fakeCID := sha256.Sum256([]byte("this blob does not exist"))

	_, err := store.Get(ctx, fakeCID[:])
	if !errors.Is(err, blobstore.ErrNotFound) {
		t.Errorf("Get non-existent: got %v, want ErrNotFound", err)
	}

	_, _, err = store.GetReader(ctx, fakeCID[:])
	if !errors.Is(err, blobstore.ErrNotFound) {
		t.Errorf("GetReader non-existent: got %v, want ErrNotFound", err)
	}
}

func TestCloseGuard(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, err := store.Put(ctx, []byte("data"))
	if !errors.Is(err, blobstore.ErrClosed) {
		t.Errorf("Put after close: got %v, want ErrClosed", err)
	}

	_, err = store.Get(ctx, make([]byte, 32))
	if !errors.Is(err, blobstore.ErrClosed) {
		t.Errorf("Get after close: got %v, want ErrClosed", err)
	}
}

func TestIdempotentClose(t *testing.T) {
	store := newTestStore(t)

	if err := store.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func TestInMemoryMode(t *testing.T) {
	store, err := blobstore.New(context.Background(), blobstore.BackendBadger, map[string]string{
		"in_memory": "true",
	})
	if err != nil {
		t.Fatalf("New(badger, in_memory) failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	data := []byte("in-memory test")
	cid, err := store.Put(ctx, data)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := store.Get(ctx, cid[:])
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch")
	}
}

func TestFactoryMissingPath(t *testing.T) {
	_, err := blobstore.New(context.Background(), blobstore.BackendBadger, map[string]string{})
	if err == nil {
		t.Error("expected error for missing path without in_memory")
	}
}
