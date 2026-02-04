// Package cli provides helpers for building CLI commands with the runtime pattern.
package cli

import (
	"os"
	"path/filepath"

	"github.com/gezibash/arc/v2/internal/config"
	"github.com/gezibash/arc/v2/pkg/capability"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/viper"
)

// DefaultRelayAddr is the default relay address.
const DefaultRelayAddr = "localhost:50051"

// NewBuilder creates a runtime builder configured from viper settings.
// Common CLI flags are automatically applied:
//   - data_dir: data directory path
//   - log_level: logging level (debug, info, warn, error)
//   - log_format: logging format (text, json)
//
// Client-side logs are written to {data_dir}/log/cli.log instead of stdout.
func NewBuilder(name string, v *viper.Viper) *runtime.Builder {
	builder := runtime.New(name)

	dataDir := v.GetString("data_dir")
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}
	builder = builder.DataDir(dataDir)

	// Client commands log to file, not stdout.
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0o700); err == nil {
		f, err := os.OpenFile(filepath.Join(logDir, "cli.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // path is constructed from known data dir
		if err == nil {
			builder = builder.LogWriter(f)
		}
	}

	// Check both "observability.log_level" (from BindCommonFlags) and
	// "log_level" (from direct viper set) for backwards compatibility.
	level := v.GetString("observability.log_level")
	if level == "" {
		level = v.GetString("log_level")
	}
	format := v.GetString("observability.log_format")
	if format == "" {
		format = v.GetString("log_format")
	}
	if level != "" {
		if format == "" {
			format = "text"
		}
		builder = builder.Logging(level, format)
	}

	return builder
}

// WithRelay configures a relay connection and applies the relay capability.
// The relay address is resolved from: explicit addr > ARC_RELAY env > default.
func WithRelay(addr string) runtime.Extension {
	return func(rt *runtime.Runtime) error {
		// Resolve address
		relayAddr := addr
		if relayAddr == "" {
			relayAddr = os.Getenv("ARC_RELAY")
		}
		if relayAddr == "" {
			relayAddr = DefaultRelayAddr
		}

		// Apply relay capability
		return capability.Relay(capability.RelayConfig{Addr: relayAddr})(rt)
	}
}
