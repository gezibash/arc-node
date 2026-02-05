package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/gezibash/arc/v2/internal/blobstore"
)

// mockS3 provides a minimal in-memory S3 mock for testing.
type mockS3 struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newMockS3() *mockS3 {
	return &mockS3{objects: make(map[string][]byte)}
}

func (m *mockS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// HeadBucket: HEAD /test-bucket or HEAD /test-bucket/
	if path == "/test-bucket" || path == "/test-bucket/" {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	key := strings.TrimPrefix(path, "/test-bucket/")

	switch r.Method {
	case http.MethodPut:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		m.mu.Lock()
		m.objects[key] = data
		m.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		m.mu.RLock()
		data, ok := m.objects[key]
		m.mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)

	case http.MethodHead:
		m.mu.RLock()
		data, ok := m.objects[key]
		m.mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		m.mu.Lock()
		delete(m.objects, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()

	mock := newMockS3()
	server := httptest.NewServer(mock)
	t.Cleanup(server.Close)

	store, err := blobstore.New(context.Background(), blobstore.BackendS3, map[string]string{
		"bucket":            "test-bucket",
		"region":            "us-east-1",
		"endpoint":          server.URL,
		"access_key_id":     "test",
		"secret_access_key": "test",
		"force_path_style":  "true",
	})
	if err != nil {
		t.Fatalf("New(s3) failed: %v", err)
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

func TestCloseGuard(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_ = store.Close()

	_, err := store.Put(ctx, []byte("data"))
	if !errors.Is(err, blobstore.ErrClosed) {
		t.Errorf("Put after close: got %v, want ErrClosed", err)
	}

	_, err = store.Get(ctx, make([]byte, 32))
	if !errors.Is(err, blobstore.ErrClosed) {
		t.Errorf("Get after close: got %v, want ErrClosed", err)
	}
}

func TestFactoryMissingBucket(t *testing.T) {
	_, err := blobstore.New(context.Background(), blobstore.BackendS3, map[string]string{})
	if err == nil {
		t.Error("expected error for missing bucket")
	}
}

func TestIntegration(t *testing.T) {
	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("S3_TEST_BUCKET not set, skipping integration test")
	}

	ctx := context.Background()
	cfg := map[string]string{
		"bucket": bucket,
	}

	region := os.Getenv("S3_TEST_REGION")
	if region != "" {
		cfg["region"] = region
	}
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	if endpoint != "" {
		cfg["endpoint"] = endpoint
		cfg["force_path_style"] = "true"
	}

	store, err := blobstore.New(ctx, blobstore.BackendS3, cfg)
	if err != nil {
		t.Fatalf("New(s3) failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Round-trip test
	data := []byte("integration test data")
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

	// Cleanup
	_ = store.Delete(ctx, cid[:])
}
