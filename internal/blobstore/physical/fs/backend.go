// Package fs provides a filesystem-backed blob storage backend.
package fs

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/gezibash/arc/v2/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
	"github.com/gezibash/arc-node/internal/storage"
)

const (
	KeyPath            = "path"
	KeyDirPermissions  = "dir_permissions"
	KeyFilePermissions = "file_permissions"
)

func init() {
	physical.Register("fs", NewFactory, Defaults)
}

// Defaults returns the default configuration for the filesystem backend.
func Defaults() map[string]string {
	return map[string]string{
		KeyPath:            "~/.arc/blobs-fs",
		KeyDirPermissions:  "0700",
		KeyFilePermissions: "0600",
	}
}

// NewFactory creates a new filesystem backend from a configuration map.
func NewFactory(_ context.Context, config map[string]string) (physical.Backend, error) {
	path := storage.GetString(config, KeyPath, "")
	if path == "" {
		return nil, storage.NewConfigError("fs", KeyPath, "cannot be empty")
	}
	path = storage.ExpandPath(path)

	dirPerms, err := parseFileMode(config[KeyDirPermissions], 0o700)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("fs", KeyDirPermissions, config[KeyDirPermissions], "must be an octal permission string (e.g. 0700)")
	}

	filePerms, err := parseFileMode(config[KeyFilePermissions], 0o600)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("fs", KeyFilePermissions, config[KeyFilePermissions], "must be an octal permission string (e.g. 0600)")
	}

	if err := os.MkdirAll(path, dirPerms); err != nil {
		return nil, storage.NewConfigErrorWithCause("fs", KeyPath, "failed to create directory", err)
	}

	slog.Info("fs blobstore initialized", "path", path, "dir_permissions", fmt.Sprintf("%04o", dirPerms), "file_permissions", fmt.Sprintf("%04o", filePerms))

	return &Backend{
		rootPath:  path,
		dirPerms:  dirPerms,
		filePerms: filePerms,
	}, nil
}

func parseFileMode(s string, defaultMode os.FileMode) (os.FileMode, error) {
	if s == "" {
		return defaultMode, nil
	}
	v, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return 0, err
	}
	return os.FileMode(v), nil
}

// Backend is a filesystem implementation of physical.Backend.
type Backend struct {
	rootPath  string
	dirPerms  os.FileMode
	filePerms os.FileMode
	closed    atomic.Bool
}

func (b *Backend) blobPath(r reference.Reference) string {
	hex := reference.Hex(r)
	return filepath.Join(b.rootPath, hex[:2], hex)
}

// Put stores data at the given reference using atomic rename.
func (b *Backend) Put(_ context.Context, r reference.Reference, data []byte) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	path := b.blobPath(r)
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, b.dirPerms); err != nil {
		return fmt.Errorf("fs put: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("fs put: %w", err)
	}
	tmpName := tmp.Name()

	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if writeErr != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("fs put: %w", writeErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("fs put: %w", closeErr)
	}

	if err := os.Chmod(tmpName, b.filePerms); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("fs put: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("fs put: %w", err)
	}

	return nil
}

// Get retrieves data by reference.
func (b *Backend) Get(_ context.Context, r reference.Reference) ([]byte, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	data, err := os.ReadFile(b.blobPath(r))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, physical.ErrNotFound
		}
		return nil, fmt.Errorf("fs get: %w", err)
	}
	return data, nil
}

// Exists checks if a blob exists.
func (b *Backend) Exists(_ context.Context, r reference.Reference) (bool, error) {
	if b.closed.Load() {
		return false, physical.ErrClosed
	}

	_, err := os.Stat(b.blobPath(r))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("fs exists: %w", err)
	}
	return true, nil
}

// Delete removes a blob by reference.
func (b *Backend) Delete(_ context.Context, r reference.Reference) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	err := os.Remove(b.blobPath(r))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs delete: %w", err)
	}
	return nil
}

// Stats returns storage statistics.
func (b *Backend) Stats(_ context.Context) (*physical.Stats, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	var totalSize int64
	err := filepath.WalkDir(b.rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if name := d.Name(); len(name) > 0 && name[0] == '.' {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		totalSize += info.Size()
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fs stats: %w", err)
	}

	return &physical.Stats{
		SizeBytes:   totalSize,
		BackendType: "fs",
	}, nil
}

// Close marks the backend as closed.
func (b *Backend) Close() error {
	b.closed.Store(true)
	return nil
}
