package config

import (
	"errors"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// SetCommonDefaults configures standard defaults on a Viper instance.
func SetCommonDefaults(v *viper.Viper) {
	v.SetDefault("data_dir", Common.DataDir)
	v.SetDefault("relay_addr", Common.RelayAddr)
	v.SetDefault("observability.log_level", Common.LogLevel)
	v.SetDefault("observability.log_format", Common.LogFormat)
}

// BindCommonFlags binds standard CLI flags to Viper.
func BindCommonFlags(cmd *cobra.Command, v *viper.Viper) {
	f := cmd.Flags()

	f.String("data-dir", "", "data directory (default ~/.arc)")
	f.String("relay", "", "relay address (default localhost:50051)")
	f.String("key", "", "key name to use")
	f.String("key-path", "", "path to key file (overrides --key)")
	f.String("log-level", "", "log level (debug, info, warn, error)")
	f.String("log-format", "", "log format (json, text)")

	_ = v.BindPFlag("data_dir", f.Lookup("data-dir"))
	_ = v.BindPFlag("relay_addr", f.Lookup("relay"))
	_ = v.BindPFlag("key_name", f.Lookup("key"))
	_ = v.BindPFlag("key_path", f.Lookup("key-path"))
	_ = v.BindPFlag("observability.log_level", f.Lookup("log-level"))
	_ = v.BindPFlag("observability.log_format", f.Lookup("log-format"))
}

// BindServerFlags binds server-specific flags (addr, buffer-size, reflection).
func BindServerFlags(cmd *cobra.Command, v *viper.Viper) {
	f := cmd.Flags()

	f.String("addr", "", "gRPC listen address")
	f.String("config", "", "config file path")
	f.String("metrics-addr", "", "metrics HTTP listen address")
	f.Bool("reflection", false, "enable gRPC reflection")

	_ = v.BindPFlag("grpc.addr", f.Lookup("addr"))
	_ = v.BindPFlag("observability.metrics_addr", f.Lookup("metrics-addr"))
	_ = v.BindPFlag("grpc.enable_reflection", f.Lookup("reflection"))
}

// Load reads config from flags, env, and file.
// The envPrefix is used for environment variable lookups (e.g., "ARC_RELAY").
// The configPaths are directories to search for config files.
func Load(v *viper.Viper, envPrefix string, configFile string, configPaths ...string) error {
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	if configFile != "" {
		v.SetConfigFile(configFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("hcl")
		v.AddConfigPath(".")
		for _, p := range configPaths {
			v.AddConfigPath(p)
		}
	}

	if err := v.ReadInConfig(); err != nil {
		var cfgErr viper.ConfigFileNotFoundError
		if !errors.As(err, &cfgErr) && configFile != "" {
			return err
		}
		// Config file not found is OK if not explicitly specified
	}

	return nil
}

// LoadInto applies common defaults, loads config from flags/env/file, and
// unmarshals into the provided struct. Use with capability configs that embed BaseConfig.
func LoadInto(v *viper.Viper, envPrefix, configFile string, cfg any, paths ...string) error {
	SetCommonDefaults(v)
	if err := Load(v, envPrefix, configFile, paths...); err != nil {
		return err
	}
	return v.Unmarshal(cfg)
}

// RelayAddr returns the relay address, checking multiple sources.
// Priority: viper value > ARC_RELAY env > default.
func RelayAddr(v *viper.Viper) string {
	if addr := v.GetString("relay_addr"); addr != "" {
		return addr
	}
	if addr := v.GetString("relay"); addr != "" {
		return addr
	}
	// Check ARC_RELAY directly since viper may not have it bound
	if addr := strings.TrimSpace(strings.ToLower(viper.GetString("ARC_RELAY"))); addr != "" {
		return addr
	}
	return Common.RelayAddr
}
