package physical

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/internal/storage"
)

// Factory creates a backend from a configuration map.
type Factory func(ctx context.Context, config map[string]string) (Backend, error)

// DefaultsFunc returns the default configuration for a backend.
type DefaultsFunc func() map[string]string

type backendEntry struct {
	Factory  Factory
	Defaults DefaultsFunc
}

var (
	backends   = make(map[string]backendEntry)
	backendsMu sync.RWMutex
)

// Register registers a backend factory with the given name.
// Panics if a backend with the same name is already registered.
func Register(name string, factory Factory, defaults DefaultsFunc) {
	backendsMu.Lock()
	defer backendsMu.Unlock()

	if _, exists := backends[name]; exists {
		panic(fmt.Sprintf("indexstore backend %q already registered", name))
	}
	backends[name] = backendEntry{Factory: factory, Defaults: defaults}
}

// GetDefaults returns the default configuration for a backend.
func GetDefaults(name string) map[string]string {
	backendsMu.RLock()
	defer backendsMu.RUnlock()

	entry, ok := backends[name]
	if !ok || entry.Defaults == nil {
		return nil
	}
	return entry.Defaults()
}

// ListBackends returns the names of all registered backends.
func ListBackends() []string {
	backendsMu.RLock()
	defer backendsMu.RUnlock()

	names := make([]string, 0, len(backends))
	for name := range backends {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// New creates a backend by name with the given configuration.
func New(ctx context.Context, name string, config map[string]string, metrics *observability.Metrics) (Backend, error) {
	op, ctx := observability.StartOperation(ctx, metrics, "indexstore.physical.new")
	var err error
	defer op.End(err)

	slog.InfoContext(ctx, "creating indexstore backend", "backend", name)

	backendsMu.RLock()
	entry, ok := backends[name]
	backendsMu.RUnlock()

	if !ok {
		err = storage.NewConfigError(name, "", fmt.Sprintf("unknown indexstore backend %q (available: %v)", name, ListBackends()))
		return nil, err
	}

	var defaults map[string]string
	if entry.Defaults != nil {
		defaults = entry.Defaults()
	}
	mergedConfig := storage.MergeConfig(defaults, config)

	backend, err := entry.Factory(ctx, mergedConfig)
	if err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "indexstore backend created", "backend", name)
	return backend, nil
}

// IsRegistered returns true if a backend with the given name is registered.
func IsRegistered(name string) bool {
	backendsMu.RLock()
	defer backendsMu.RUnlock()
	_, ok := backends[name]
	return ok
}
