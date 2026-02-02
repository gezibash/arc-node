package fs

import (
	"context"
	"crypto/sha256"
	"errors"
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
	if !errors.Is(err, physical.ErrNotFound) {
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
	if !errors.Is(err, physical.ErrNotFound) {
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
	_ = b.Close()

	ref := testRef([]byte("closed"))
	ctx := context.Background()

	if err := b.Put(ctx, ref, []byte("closed")); !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Put after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Get(ctx, ref); !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Get after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Exists(ctx, ref); !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Exists after close: got %v, want ErrClosed", err)
	}
	if err := b.Delete(ctx, ref); !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Delete after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Stats(ctx); !errors.Is(err, physical.ErrClosed) {
		t.Fatalf("Stats after close: got %v, want ErrClosed", err)
	}
}

func TestDefaults(t *testing.T) {
	d := Defaults()
	if d[KeyPath] == "" {
		t.Error("default path should not be empty")
	}
	if d[KeyDirPermissions] == "" {
		t.Error("default dir permissions should not be empty")
	}
	if d[KeyFilePermissions] == "" {
		t.Error("default file permissions should not be empty")
	}
}

func TestNewFactoryEmptyPath(t *testing.T) {
	_, err := NewFactory(context.Background(), map[string]string{
		KeyPath: "",
	})
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestNewFactoryNoPath(t *testing.T) {
	_, err := NewFactory(context.Background(), map[string]string{})
	if err == nil {
		t.Fatal("expected error when path key is missing")
	}
}

func TestNewFactoryInvalidDirPermissions(t *testing.T) {
	_, err := NewFactory(context.Background(), map[string]string{
		KeyPath:           t.TempDir(),
		KeyDirPermissions: "not-octal",
	})
	if err == nil {
		t.Fatal("expected error for invalid dir permissions")
	}
}

func TestNewFactoryInvalidFilePermissions(t *testing.T) {
	_, err := NewFactory(context.Background(), map[string]string{
		KeyPath:            t.TempDir(),
		KeyFilePermissions: "not-octal",
	})
	if err == nil {
		t.Fatal("expected error for invalid file permissions")
	}
}

func TestNewFactoryDefaultPermissions(t *testing.T) {
	dir := t.TempDir()
	b, err := NewFactory(context.Background(), map[string]string{
		KeyPath: dir,
	})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	be := b.(*Backend)
	if be.dirPerms != 0o700 {
		t.Errorf("dirPerms = %04o, want 0700", be.dirPerms)
	}
	if be.filePerms != 0o600 {
		t.Errorf("filePerms = %04o, want 0600", be.filePerms)
	}
}

func TestNewFactoryCreatesNestedDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "dir")
	b, err := NewFactory(context.Background(), map[string]string{
		KeyPath: dir,
	})
	if err != nil {
		t.Fatalf("NewFactory: %v", err)
	}
	b.Close()
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("directory not created: %v", err)
	}
}

func TestPutToReadOnlyDir(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	// Make root read-only so MkdirAll for shard dir fails
	if err := os.Chmod(b.rootPath, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(b.rootPath, 0o700) })

	data := []byte("readonly test")
	ref := testRef(data)
	err := b.Put(ctx, ref, data)
	if err == nil {
		t.Fatal("expected error putting to read-only dir")
	}
}

func TestGetNonReadableFile(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("unreadable")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}
	// Make the file unreadable
	if err := os.Chmod(b.blobPath(ref), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(b.blobPath(ref), 0o600) })

	_, err := b.Get(ctx, ref)
	if err == nil {
		t.Fatal("expected error reading unreadable file")
	}
	// Should NOT be ErrNotFound
	if errors.Is(err, physical.ErrNotFound) {
		t.Fatal("should not be ErrNotFound for permission error")
	}
}

func TestExistsPermissionError(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("perm-exists")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}
	hex := reference.Hex(ref)
	shardDir := filepath.Join(b.rootPath, hex[:2])
	if err := os.Chmod(shardDir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(shardDir, 0o700) })

	_, err := b.Exists(ctx, ref)
	if err == nil {
		t.Fatal("expected error for permission-denied stat")
	}
}

func TestDeletePermissionError(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("perm-delete")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}
	hex := reference.Hex(ref)
	shardDir := filepath.Join(b.rootPath, hex[:2])
	if err := os.Chmod(shardDir, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(shardDir, 0o700) })

	err := b.Delete(ctx, ref)
	if err == nil {
		t.Fatal("expected error for permission-denied delete")
	}
}

func TestStatsWithWalkError(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("walk-error")
	ref := testRef(data)

	if err := b.Put(ctx, ref, data); err != nil {
		t.Fatal(err)
	}

	hex := reference.Hex(ref)
	shardDir := filepath.Join(b.rootPath, hex[:2])
	if err := os.Chmod(shardDir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(shardDir, 0o700) })

	_, err := b.Stats(ctx)
	if err == nil {
		t.Fatal("expected error from Stats with unreadable directory")
	}
}

func TestPutRenameFailure(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("rename fail")
	ref := testRef(data)

	// Pre-create the target path as a directory so rename fails
	targetPath := b.blobPath(ref)
	if err := os.MkdirAll(targetPath, 0o700); err != nil {
		t.Fatal(err)
	}

	err := b.Put(ctx, ref, data)
	if err == nil {
		t.Fatal("expected error when rename target is a directory")
	}
}

func TestPutChmodFailure(t *testing.T) {
	// This test creates a scenario where chmod on the temp file fails
	// by removing the temp file between creation and chmod.
	// We can't easily do this without modifying the code, so instead
	// we test that writing to a directory with no write permission fails
	// at the CreateTemp stage.
	b := newTestBackend(t)
	ctx := context.Background()
	data := []byte("chmod fail")
	ref := testRef(data)

	// Create the shard dir, then make it read-only
	hex := reference.Hex(ref)
	shardDir := filepath.Join(b.rootPath, hex[:2])
	if err := os.MkdirAll(shardDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shardDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(shardDir, 0o700) })

	err := b.Put(ctx, ref, data)
	if err == nil {
		t.Fatal("expected error when shard dir is read-only")
	}
}
