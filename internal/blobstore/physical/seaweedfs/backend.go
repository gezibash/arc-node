// Package seaweedfs provides a SeaweedFS filer-backed blob storage backend.
package seaweedfs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gezibash/arc/pkg/reference"

	"github.com/gezibash/arc-node/internal/blobstore/physical"
	"github.com/gezibash/arc-node/internal/storage"
)

const (
	KeyFilerURL = "filer_url"
	KeyPrefix   = "prefix"
	KeyTimeout  = "timeout"
)

func init() {
	physical.Register("seaweedfs", NewFactory, Defaults)
}

// Defaults returns the default configuration for the SeaweedFS backend.
func Defaults() map[string]string {
	return map[string]string{
		KeyPrefix:  "/blobs",
		KeyTimeout: "30s",
	}
}

// NewFactory creates a new SeaweedFS backend from a configuration map.
func NewFactory(_ context.Context, config map[string]string) (physical.Backend, error) {
	filerURL := storage.GetString(config, KeyFilerURL, "")
	if filerURL == "" {
		return nil, storage.NewConfigError("seaweedfs", KeyFilerURL, "cannot be empty")
	}
	filerURL = strings.TrimRight(filerURL, "/")

	prefix := storage.GetString(config, KeyPrefix, "/blobs")
	if !strings.HasPrefix(prefix, "/") {
		return nil, storage.NewConfigErrorWithValue("seaweedfs", KeyPrefix, prefix, "must start with /")
	}
	prefix = strings.TrimRight(prefix, "/")

	timeout, err := storage.GetDuration(config, KeyTimeout, 30*time.Second)
	if err != nil {
		return nil, storage.NewConfigErrorWithValue("seaweedfs", KeyTimeout, config[KeyTimeout], err.Error())
	}

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
		},
	}

	slog.Info("seaweedfs blobstore initialized", "filer_url", filerURL, "prefix", prefix, "timeout", timeout)

	return &Backend{
		filerURL: filerURL,
		prefix:   prefix,
		client:   client,
	}, nil
}

// Backend is a SeaweedFS filer implementation of physical.Backend.
type Backend struct {
	filerURL string
	prefix   string
	client   *http.Client
	closed   atomic.Bool
}

func (b *Backend) blobURL(r reference.Reference) string {
	return b.filerURL + b.prefix + "/" + reference.Hex(r)
}

// Put stores data at the given reference.
func (b *Backend) Put(ctx context.Context, r reference.Reference, data []byte) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, b.blobURL(r), strings.NewReader(string(data)))
	if err != nil {
		return fmt.Errorf("seaweedfs put: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("seaweedfs put: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("seaweedfs put: unexpected status %d", resp.StatusCode)
	}
	return nil
}

// Get retrieves data by reference.
func (b *Backend) Get(ctx context.Context, r reference.Reference) ([]byte, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.blobURL(r), nil)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs get: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		io.Copy(io.Discard, resp.Body)
		return nil, physical.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("seaweedfs get: unexpected status %d: %s", resp.StatusCode, body)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("seaweedfs get: %w", err)
	}
	return data, nil
}

// Exists checks if a blob exists.
func (b *Backend) Exists(ctx context.Context, r reference.Reference) (bool, error) {
	if b.closed.Load() {
		return false, physical.ErrClosed
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, b.blobURL(r), nil)
	if err != nil {
		return false, fmt.Errorf("seaweedfs exists: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("seaweedfs exists: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("seaweedfs exists: unexpected status %d", resp.StatusCode)
	}
	return true, nil
}

// Delete removes a blob by reference. Idempotent: 404 is not an error.
func (b *Backend) Delete(ctx context.Context, r reference.Reference) error {
	if b.closed.Load() {
		return physical.ErrClosed
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, b.blobURL(r), nil)
	if err != nil {
		return fmt.Errorf("seaweedfs delete: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("seaweedfs delete: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("seaweedfs delete: unexpected status %d", resp.StatusCode)
}

// Stats returns storage statistics.
func (b *Backend) Stats(_ context.Context) (*physical.Stats, error) {
	if b.closed.Load() {
		return nil, physical.ErrClosed
	}

	return &physical.Stats{
		SizeBytes:   0,
		BackendType: "seaweedfs",
	}, nil
}

// Close closes idle connections.
func (b *Backend) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	b.client.CloseIdleConnections()
	return nil
}
