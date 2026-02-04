package config

import "os"

// BaseConfig contains configuration fields shared across all arc capabilities.
// Capability configs embed this struct with mapstructure:",squash" to get
// standard fields (data_dir, relay_addr, key_name, key_path, observability)
// without redefinition.
type BaseConfig struct {
	DataDir       string              `mapstructure:"data_dir"`
	RelayAddr     string              `mapstructure:"relay_addr"`
	KeyName       string              `mapstructure:"key_name"`
	KeyPath       string              `mapstructure:"key_path"`
	Observability ObservabilityConfig `mapstructure:"observability"`
}

// ObservabilityConfig holds logging and metrics settings.
type ObservabilityConfig struct {
	LogLevel  string `mapstructure:"log_level"`
	LogFormat string `mapstructure:"log_format"`
}

// ResolvedRelayAddr returns the relay address, checking config > ARC_RELAY env > default.
func (c BaseConfig) ResolvedRelayAddr() string {
	if c.RelayAddr != "" {
		return c.RelayAddr
	}
	if addr := os.Getenv("ARC_RELAY"); addr != "" {
		return addr
	}
	return Common.RelayAddr
}

// ResolvedDataDir returns the data directory from config, or the default (~/.arc).
func (c BaseConfig) ResolvedDataDir() string {
	if c.DataDir != "" {
		return c.DataDir
	}
	return DefaultDataDir()
}
