package config

import (
	"os"
	"testing"

	"github.com/spf13/viper"
)

func TestBaseConfig_ResolvedRelayAddr(t *testing.T) {
	t.Run("returns config value when set", func(t *testing.T) {
		cfg := BaseConfig{RelayAddr: "relay.example.com:50051"}
		if got := cfg.ResolvedRelayAddr(); got != "relay.example.com:50051" {
			t.Errorf("ResolvedRelayAddr() = %q, want %q", got, "relay.example.com:50051")
		}
	})

	t.Run("falls back to ARC_RELAY env", func(t *testing.T) {
		t.Setenv("ARC_RELAY", "env-relay:50051")
		cfg := BaseConfig{}
		if got := cfg.ResolvedRelayAddr(); got != "env-relay:50051" {
			t.Errorf("ResolvedRelayAddr() = %q, want %q", got, "env-relay:50051")
		}
	})

	t.Run("falls back to default", func(t *testing.T) {
		os.Unsetenv("ARC_RELAY")
		cfg := BaseConfig{}
		if got := cfg.ResolvedRelayAddr(); got != Common.RelayAddr {
			t.Errorf("ResolvedRelayAddr() = %q, want %q", got, Common.RelayAddr)
		}
	})

	t.Run("config takes precedence over env", func(t *testing.T) {
		t.Setenv("ARC_RELAY", "env-relay:50051")
		cfg := BaseConfig{RelayAddr: "config-relay:50051"}
		if got := cfg.ResolvedRelayAddr(); got != "config-relay:50051" {
			t.Errorf("ResolvedRelayAddr() = %q, want %q", got, "config-relay:50051")
		}
	})
}

func TestBaseConfig_ResolvedDataDir(t *testing.T) {
	t.Run("returns config value when set", func(t *testing.T) {
		cfg := BaseConfig{DataDir: "/custom/data"}
		if got := cfg.ResolvedDataDir(); got != "/custom/data" {
			t.Errorf("ResolvedDataDir() = %q, want %q", got, "/custom/data")
		}
	})

	t.Run("falls back to default", func(t *testing.T) {
		cfg := BaseConfig{}
		got := cfg.ResolvedDataDir()
		want := DefaultDataDir()
		if got != want {
			t.Errorf("ResolvedDataDir() = %q, want %q", got, want)
		}
	})
}

func TestLoadInto(t *testing.T) {
	t.Run("sets common defaults and unmarshals", func(t *testing.T) {
		v := viper.New()

		type testConfig struct {
			BaseConfig `mapstructure:",squash"`
			Extra      string `mapstructure:"extra"`
		}

		v.Set("extra", "hello")

		var cfg testConfig
		err := LoadInto(v, "TEST", "", &cfg)
		if err != nil {
			t.Fatalf("LoadInto() error = %v", err)
		}

		if cfg.DataDir != Common.DataDir {
			t.Errorf("DataDir = %q, want %q", cfg.DataDir, Common.DataDir)
		}
		if cfg.RelayAddr != Common.RelayAddr {
			t.Errorf("RelayAddr = %q, want %q", cfg.RelayAddr, Common.RelayAddr)
		}
		if cfg.Observability.LogLevel != Common.LogLevel {
			t.Errorf("LogLevel = %q, want %q", cfg.Observability.LogLevel, Common.LogLevel)
		}
		if cfg.Extra != "hello" {
			t.Errorf("Extra = %q, want %q", cfg.Extra, "hello")
		}
	})

	t.Run("overrides from viper take precedence", func(t *testing.T) {
		v := viper.New()

		type testConfig struct {
			BaseConfig `mapstructure:",squash"`
		}

		v.Set("relay_addr", "custom:9999")

		var cfg testConfig
		err := LoadInto(v, "TEST", "", &cfg)
		if err != nil {
			t.Fatalf("LoadInto() error = %v", err)
		}

		if cfg.RelayAddr != "custom:9999" {
			t.Errorf("RelayAddr = %q, want %q", cfg.RelayAddr, "custom:9999")
		}
	})
}
