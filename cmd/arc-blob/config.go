package main

import (
	"path/filepath"

	"github.com/gezibash/arc/v2/internal/config"
	"github.com/spf13/viper"
)

// Config represents the arc-blob configuration.
type Config struct {
	config.BaseConfig `mapstructure:",squash"`
	ListenAddr        string        `mapstructure:"listen_addr"`
	Storage           StorageConfig `mapstructure:"storage"`
}

// StorageConfig configures the blob storage backend.
type StorageConfig struct {
	Backend string            `mapstructure:"backend"`
	Config  map[string]string `mapstructure:"config"`
}

func loadConfig(v *viper.Viper, configFile string) (Config, error) {
	v.SetDefault("key_name", config.BlobDefaults.KeyName)
	v.SetDefault("listen_addr", config.BlobDefaults.ListenAddr)
	v.SetDefault("storage.backend", config.BlobDefaults.Backend)

	var cfg Config
	err := config.LoadInto(v, "ARC_BLOB", configFile, &cfg,
		filepath.Join(config.DefaultDataDir(), "blob"),
		"/etc/arc/blob",
	)
	return cfg, err
}
