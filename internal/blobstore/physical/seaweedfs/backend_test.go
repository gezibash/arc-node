package seaweedfs

import (
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
)

func testRef(data []byte) reference.Reference {
	return reference.Reference(sha256.Sum256(data))
}

// mockFiler creates an httptest server that emulates a SeaweedFS filer.
func mockFiler() (*httptest.Server, *mockStore) {
	store := &mockStore{blobs: make(map[string][]byte)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		switch r.Method {
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			store.put(key, data)
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			data, ok := store.get(key)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write(data)
		case http.MethodHead:
			if !store.exists(key) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			if !store.exists(key) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			store.del(key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	return srv, store
}

type mockStore struct {
	mu    sync.Mutex
	blobs map[string][]byte
}

func (m *mockStore) put(key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[key] = data
}

func (m *mockStore) get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.blobs[key]
	return d, ok
}

func (m *mockStore) exists(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.blobs[key]
	return ok
}

func (m *mockStore) del(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, key)
}

func newTestBackend(t *testing.T) (*Backend, *httptest.Server) {
	t.Helper()
	srv, _ := mockFiler()
	t.Cleanup(srv.Close)

	b, err := NewFactory(context.Background(), map[string]string{
		KeyFilerURL: srv.URL,
		KeyPrefix:   "/blobs",
		KeyTimeout:  "5s",
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.(*Backend), srv
}

func TestPutGetRoundTrip(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	data := []byte("hello seaweedfs")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}

	got, err := b.Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestGetNotFound(t *testing.T) {
	b, _ := newTestBackend(t)
	ref := testRef([]byte("missing"))

	_, err := b.Get(context.Background(), ref)
	if err != physical.ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestExists(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	data := []byte("exists test")
	ref := testRef(data)

	ok, err := b.Exists(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected false before put")
	}

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}

	ok, err = b.Exists(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected true after put")
	}
}

func TestDeleteIdempotent(t *testing.T) {
	b, _ := newTestBackend(t)
	ctx := context.Background()
	ref := testRef([]byte("delete me"))

	// Delete non-existent should not error (404 is accepted).
	if err := b.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}

	// Put then delete.
	if err := b.Put(ctx, ref, []byte("delete me")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}

	_, err := b.Get(ctx, ref)
	if err != physical.ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound after delete", err)
	}
}

func TestStats(t *testing.T) {
	b, _ := newTestBackend(t)
	stats, err := b.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.BackendType != "seaweedfs" {
		t.Fatalf("backend type = %q, want %q", stats.BackendType, "seaweedfs")
	}
}

func TestClosedBackend(t *testing.T) {
	b, _ := newTestBackend(t)
	b.Close()

	ref := testRef([]byte("closed"))
	ctx := context.Background()

	if err := b.Put(ctx, ref, []byte("closed")); err != physical.ErrClosed {
		t.Fatalf("Put after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Get(ctx, ref); err != physical.ErrClosed {
		t.Fatalf("Get after close: got %v, want ErrClosed", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	b, _ := newTestBackend(t)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPrefixValidation(t *testing.T) {
	srv, _ := mockFiler()
	defer srv.Close()

	_, err := NewFactory(context.Background(), map[string]string{
		KeyFilerURL: srv.URL,
		KeyPrefix:   "no-leading-slash",
	})
	if err == nil {
		t.Fatal("expected error for prefix without leading /")
	}
	if !strings.Contains(err.Error(), "must start with /") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIntegration(t *testing.T) {
	filerURL := os.Getenv("SEAWEEDFS_FILER_URL")
	if filerURL == "" {
		t.Skip("SEAWEEDFS_FILER_URL not set, skipping integration test")
	}

	b, err := NewFactory(context.Background(), map[string]string{
		KeyFilerURL: filerURL,
		KeyPrefix:   "/test-blobs",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ctx := context.Background()
	data := []byte("integration test")
	ref := testRef(data)

	if err := b.(*Backend).Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}

	got, err := b.(*Backend).Get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}

	if err := b.(*Backend).Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
}
