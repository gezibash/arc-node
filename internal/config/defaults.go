// Package config provides shared configuration patterns and defaults for arc components.
package config

import (
	"os"
	"path/filepath"
)

// Common contains default values shared across arc components.
var Common = struct {
	RelayAddr string
	LogLevel  string
	LogFormat string
	DataDir   string
}{
	RelayAddr: "localhost:50051",
	LogLevel:  "info",
	LogFormat: "text",
	DataDir:   DefaultDataDir(),
}

// DefaultDataDir returns the default data directory (~/.arc).
func DefaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".arc"
	}
	return filepath.Join(home, ".arc")
}

// RelayDefaults contains default values for the relay server.
var RelayDefaults = struct {
	ListenAddr       string
	BufferSize       int
	MaxRecvMsgSize   int
	MaxSendMsgSize   int
	EnableReflection bool
	MetricsAddr      string
	KeyName          string
}{
	ListenAddr:       ":50051",
	BufferSize:       100,
	MaxRecvMsgSize:   4 * 1024 * 1024, // 4MB
	MaxSendMsgSize:   4 * 1024 * 1024, // 4MB
	EnableReflection: false,
	MetricsAddr:      ":9090",
	KeyName:          "relay",
}

// BlobDefaults contains default values for the blob capability.
var BlobDefaults = struct {
	Backend     string
	KeyName     string
	ListenAddr  string
	Capacity    int64
	MaxBlobSize int64
}{
	Backend:     "file",
	KeyName:     "blob",
	ListenAddr:  "",               // relay-only by default
	Capacity:    1 << 30,          // 1GB
	MaxBlobSize: 64 * 1024 * 1024, // 64MB
}
