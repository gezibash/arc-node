package s3

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
)

func testRef(data []byte) reference.Reference {
	return reference.Reference(sha256.Sum256(data))
}

// mockS3Server creates an httptest server that emulates a minimal S3 API.
func mockS3Server() *httptest.Server {
	store := &mockStore{blobs: make(map[string][]byte)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Path format: /bucket/key or /bucket (HeadBucket)
		parts := strings.SplitN(r.URL.Path, "/", 3)

		// HeadBucket: PUT/GET/HEAD on /bucket
		if len(parts) < 3 || parts[2] == "" {
			w.WriteHeader(http.StatusOK)
			return
		}

		key := parts[2]
		switch r.Method {
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			store.put(key, data)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, ok := store.get(key)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>`))
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
			store.del(key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	return srv
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

func newTestBackend(t *testing.T) *Backend {
	t.Helper()
	srv := mockS3Server()
	t.Cleanup(srv.Close)

	b, err := NewFactory(context.Background(), map[string]string{
		KeyBucket:          "test-bucket",
		KeyRegion:          "us-east-1",
		KeyEndpoint:        srv.URL,
		KeyForcePathStyle:  "true",
		KeyAccessKeyID:     "test",
		KeySecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.(*Backend)
}

func TestPutGetRoundTrip(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("hello s3")
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
	b := newTestBackend(t)
	ref := testRef([]byte("missing"))

	_, err := b.Get(context.Background(), ref)
	if !errors.Is(err, physical.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestExistsHeadObject(t *testing.T) {
	b := newTestBackend(t)
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
	b := newTestBackend(t)
	ctx := context.Background()
	ref := testRef([]byte("delete me"))

	// Delete non-existent should not error.
	if err := b.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
}

func TestStats(t *testing.T) {
	b := newTestBackend(t)
	stats, err := b.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.BackendType != "s3" {
		t.Fatalf("backend type = %q, want %q", stats.BackendType, "s3")
	}
}

func TestClosedBackend(t *testing.T) {
	b := newTestBackend(t)
	b.Close()

	ref := testRef([]byte("closed"))
	ctx := context.Background()

	if err := b.Put(ctx, ref, []byte("closed")); !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Put after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Get(ctx, ref); !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Get after close: got %v, want ErrClosed", err)
	}
}

func TestIntegration(t *testing.T) {
	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("S3_TEST_BUCKET not set, skipping integration test")
	}

	b, err := NewFactory(context.Background(), map[string]string{
		KeyBucket: bucket,
		KeyPrefix: "test/",
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

func TestDefaults(t *testing.T) {
	d := Defaults()
	if d[KeyRegion] == "" {
		t.Error("default region should not be empty")
	}
}

func TestNewFactoryMissingBucket(t *testing.T) {
	_, err := NewFactory(context.Background(), map[string]string{})
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestNewFactoryEmptyBucket(t *testing.T) {
	_, err := NewFactory(context.Background(), map[string]string{
		KeyBucket: "",
	})
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
}

func TestNewFactoryInvalidForcePathStyle(t *testing.T) {
	srv := mockS3Server()
	defer srv.Close()

	_, err := NewFactory(context.Background(), map[string]string{
		KeyBucket:          "test-bucket",
		KeyEndpoint:        srv.URL,
		KeyForcePathStyle:  "not-a-bool",
		KeyAccessKeyID:     "test",
		KeySecretAccessKey: "test",
	})
	if err == nil {
		t.Fatal("expected error for invalid force_path_style")
	}
}

func TestNewFactoryWithPrefix(t *testing.T) {
	srv := mockS3Server()
	defer srv.Close()

	b, err := NewFactory(context.Background(), map[string]string{
		KeyBucket:          "test-bucket",
		KeyEndpoint:        srv.URL,
		KeyForcePathStyle:  "true",
		KeyPrefix:          "my-prefix/",
		KeyAccessKeyID:     "test",
		KeySecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	be := b.(*Backend)
	if be.prefix != "my-prefix/" {
		t.Errorf("prefix = %q, want %q", be.prefix, "my-prefix/")
	}
	b.Close()
}

func TestExistsAfterClose(t *testing.T) {
	b := newTestBackend(t)
	b.Close()

	ref := testRef([]byte("closed"))
	_, err := b.Exists(context.Background(), ref)
	if !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Exists after close: got %v, want ErrClosed", err)
	}
}

func TestDeleteAfterClose(t *testing.T) {
	b := newTestBackend(t)
	b.Close()

	ref := testRef([]byte("closed"))
	err := b.Delete(context.Background(), ref)
	if !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Delete after close: got %v, want ErrClosed", err)
	}
}

func TestStatsAfterClose(t *testing.T) {
	b := newTestBackend(t)
	b.Close()

	_, err := b.Stats(context.Background())
	if !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Stats after close: got %v, want ErrClosed", err)
	}
}

func TestPutAndDelete(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("to be deleted")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}
	ok, _ := b.Exists(ctx, ref)
	if !ok {
		t.Fatal("expected exists=true after put")
	}

	if err := b.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	ok, _ = b.Exists(ctx, ref)
	if ok {
		t.Fatal("expected exists=false after delete")
	}
}

func TestNewFactoryBucketNotAccessible(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := NewFactory(context.Background(), map[string]string{
		KeyBucket:          "nonexistent",
		KeyEndpoint:        srv.URL,
		KeyForcePathStyle:  "true",
		KeyAccessKeyID:     "test",
		KeySecretAccessKey: "test",
	})
	if err == nil {
		t.Fatal("expected error for inaccessible bucket")
	}
}

func TestCloseIdempotent(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIsNotFoundTypedErrors(t *testing.T) {
	// Test NoSuchKey
	var noSuchKey *types.NoSuchKey
	err1 := &types.NoSuchKey{Message: aws.String("no such key")}
	if !errors.As(err1, &noSuchKey) {
		t.Fatal("expected NoSuchKey to match")
	}
	if !isNotFound(err1) {
		t.Error("isNotFound should return true for NoSuchKey")
	}

	// Test NotFound
	err2 := &types.NotFound{Message: aws.String("not found")}
	if !isNotFound(err2) {
		t.Error("isNotFound should return true for NotFound")
	}

	// Test non-matching error
	err3 := errors.New("some other error")
	if isNotFound(err3) {
		t.Error("isNotFound should return false for unrelated error")
	}
}

func TestPutServerError(t *testing.T) {
	// Server that always returns 500 for PUT
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.URL.Path, "/", 3)
		if len(parts) < 3 || parts[2] == "" {
			w.WriteHeader(http.StatusOK) // HeadBucket OK
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	b, err := NewFactory(context.Background(), map[string]string{
		KeyBucket:          "test-bucket",
		KeyEndpoint:        srv.URL,
		KeyForcePathStyle:  "true",
		KeyAccessKeyID:     "test",
		KeySecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ref := testRef([]byte("fail"))
	if err := b.(*Backend).Put(context.Background(), ref, []byte("fail")); err == nil {
		t.Fatal("expected error on server error")
	}
}

func TestGetServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.URL.Path, "/", 3)
		if len(parts) < 3 || parts[2] == "" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	b, err := NewFactory(context.Background(), map[string]string{
		KeyBucket:          "test-bucket",
		KeyEndpoint:        srv.URL,
		KeyForcePathStyle:  "true",
		KeyAccessKeyID:     "test",
		KeySecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ref := testRef([]byte("fail"))
	_, err = b.(*Backend).Get(context.Background(), ref)
	if err == nil {
		t.Fatal("expected error on server error")
	}
}

func TestExistsServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.URL.Path, "/", 3)
		if len(parts) < 3 || parts[2] == "" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	b, err := NewFactory(context.Background(), map[string]string{
		KeyBucket:          "test-bucket",
		KeyEndpoint:        srv.URL,
		KeyForcePathStyle:  "true",
		KeyAccessKeyID:     "test",
		KeySecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ref := testRef([]byte("fail"))
	_, err = b.(*Backend).Exists(context.Background(), ref)
	if err == nil {
		t.Fatal("expected error on server error")
	}
}

func TestDeleteServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.URL.Path, "/", 3)
		if len(parts) < 3 || parts[2] == "" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	b, err := NewFactory(context.Background(), map[string]string{
		KeyBucket:          "test-bucket",
		KeyEndpoint:        srv.URL,
		KeyForcePathStyle:  "true",
		KeyAccessKeyID:     "test",
		KeySecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ref := testRef([]byte("fail"))
	err = b.(*Backend).Delete(context.Background(), ref)
	if err == nil {
		t.Fatal("expected error on server error")
	}
}
