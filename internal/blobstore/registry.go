package blobstore

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/gezibash/arc/v2/internal/storage"
)

// Factory creates a BlobStore from configuration.
type Factory func(ctx context.Context, config map[string]string) (BlobStore, error)

// DefaultsFunc returns the default configuration for a backend.
type DefaultsFunc func() map[string]string

type registration struct {
	factory  Factory
	defaults DefaultsFunc
}

var (
	registryMu sync.RWMutex
	registry   = make(map[string]registration)
)

// Register adds a backend factory to the registry.
// Panics if a backend with the same name is already registered.
func Register(name string, factory Factory, defaults DefaultsFunc) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("blobstore: backend %q already registered", name))
	}
	registry[name] = registration{factory: factory, defaults: defaults}
}

// New creates a BlobStore using the named backend.
// Config values are merged with the backend's defaults (explicit config wins).
func New(ctx context.Context, name string, config map[string]string) (BlobStore, error) {
	registryMu.RLock()
	reg, ok := registry[name]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("blobstore: unknown backend %q (registered: %v)", name, ListBackends())
	}

	merged := config
	if reg.defaults != nil {
		merged = storage.MergeConfig(reg.defaults(), config)
	}

	return reg.factory(ctx, merged)
}

// ListBackends returns the names of all registered backends, sorted.
func ListBackends() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// IsRegistered returns true if a backend with the given name is registered.
func IsRegistered(name string) bool {
	registryMu.RLock()
	defer registryMu.RUnlock()
	_, ok := registry[name]
	return ok
}

// GetDefaults returns the default configuration for a backend, or nil if not registered.
func GetDefaults(name string) map[string]string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	reg, ok := registry[name]
	if !ok || reg.defaults == nil {
		return nil
	}
	return reg.defaults()
}
