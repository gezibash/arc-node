package fs

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
)

func testRef(data []byte) reference.Reference {
	return reference.Reference(sha256.Sum256(data))
}

func newTestBackend(t *testing.T) *Backend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewFactory(context.Background(), map[string]string{
		KeyPath:            dir,
		KeyDirPermissions:  "0700",
		KeyFilePermissions: "0600",
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.(*Backend)
}

func TestPutGetRoundTrip(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("hello world")
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
	if err != physical.ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestExists(t *testing.T) {
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

	// Delete non-existent should succeed.
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

func TestShardDirectoryStructure(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("shard test")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}

	hex := reference.Hex(ref)
	shardDir := filepath.Join(b.rootPath, hex[:2])
	blobFile := filepath.Join(shardDir, hex)

	if _, err := os.Stat(shardDir); err != nil {
		t.Fatalf("shard dir not found: %v", err)
	}
	if _, err := os.Stat(blobFile); err != nil {
		t.Fatalf("blob file not found: %v", err)
	}
}

func TestFilePermissions(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("perm test")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(b.blobPath(ref))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("file permissions = %04o, want 0600", got)
	}
}

func TestNoLeftoverTempFiles(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("temp test")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}

	hex := reference.Hex(ref)
	shardDir := filepath.Join(b.rootPath, hex[:2])
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name()[0] == '.' {
			t.Fatalf("leftover temp file: %s", e.Name())
		}
	}
}

func TestStats(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	stats, err := b.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SizeBytes != 0 {
		t.Fatalf("empty stats size = %d, want 0", stats.SizeBytes)
	}
	if stats.BackendType != "fs" {
		t.Fatalf("backend type = %q, want %q", stats.BackendType, "fs")
	}

	data := []byte("stats data")
	if err := b.Put(ctx, testRef(data), data); err != nil {
		t.Fatal(err)
	}

	stats, err = b.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SizeBytes != int64(len(data)) {
		t.Fatalf("stats size = %d, want %d", stats.SizeBytes, len(data))
	}
}

func TestClosedBackend(t *testing.T) {
	b := newTestBackend(t)
	b.Close()

	ref := testRef([]byte("closed"))
	ctx := context.Background()

	if err := b.Put(ctx, ref, []byte("closed")); err != physical.ErrClosed {
		t.Fatalf("Put after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Get(ctx, ref); err != physical.ErrClosed {
		t.Fatalf("Get after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Exists(ctx, ref); err != physical.ErrClosed {
		t.Fatalf("Exists after close: got %v, want ErrClosed", err)
	}
	if err := b.Delete(ctx, ref); err != physical.ErrClosed {
		t.Fatalf("Delete after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Stats(ctx); err != physical.ErrClosed {
		t.Fatalf("Stats after close: got %v, want ErrClosed", err)
	}
}
