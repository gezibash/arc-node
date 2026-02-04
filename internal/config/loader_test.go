package config

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func TestBindCommonFlags(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	v := viper.New()

	BindCommonFlags(cmd, v)

	// Parse flags with values
	err := cmd.Flags().Parse([]string{
		"--data-dir", "/custom/dir",
		"--relay", "relay.example.com:50051",
		"--key", "mykey",
		"--key-path", "/path/to/key",
		"--log-level", "debug",
		"--log-format", "json",
	})
	if err != nil {
		t.Fatalf("Parse flags: %v", err)
	}

	tests := []struct {
		key  string
		want string
	}{
		{"data_dir", "/custom/dir"},
		{"relay_addr", "relay.example.com:50051"},
		{"key_name", "mykey"},
		{"key_path", "/path/to/key"},
		{"observability.log_level", "debug"},
		{"observability.log_format", "json"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := v.GetString(tt.key); got != tt.want {
				t.Errorf("v.GetString(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestBindCommonFlags_defaults(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	v := viper.New()

	BindCommonFlags(cmd, v)
	SetCommonDefaults(v)

	// Parse with no flags set
	if err := cmd.Flags().Parse([]string{}); err != nil {
		t.Fatalf("Parse flags: %v", err)
	}

	// Defaults should be applied
	if got := v.GetString("data_dir"); got != Common.DataDir {
		t.Errorf("data_dir = %q, want %q", got, Common.DataDir)
	}
	if got := v.GetString("relay_addr"); got != Common.RelayAddr {
		t.Errorf("relay_addr = %q, want %q", got, Common.RelayAddr)
	}
	if got := v.GetString("observability.log_level"); got != Common.LogLevel {
		t.Errorf("observability.log_level = %q, want %q", got, Common.LogLevel)
	}
}

func TestLoadInto_configFile(t *testing.T) {
	v := viper.New()

	type cfg struct {
		BaseConfig `mapstructure:",squash"`
		Custom     string `mapstructure:"custom"`
	}

	// Set a custom value
	v.Set("custom", "test-value")
	v.Set("relay_addr", "myrelay:1234")

	var c cfg
	err := LoadInto(v, "TEST_PREFIX", "", &c)
	if err != nil {
		t.Fatalf("LoadInto() error = %v", err)
	}

	if c.RelayAddr != "myrelay:1234" {
		t.Errorf("RelayAddr = %q, want %q", c.RelayAddr, "myrelay:1234")
	}
	if c.Custom != "test-value" {
		t.Errorf("Custom = %q, want %q", c.Custom, "test-value")
	}
	// Common defaults should still be applied for fields not overridden
	if c.Observability.LogLevel != Common.LogLevel {
		t.Errorf("LogLevel = %q, want %q", c.Observability.LogLevel, Common.LogLevel)
	}
}

func TestBindServerFlags(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	v := viper.New()

	BindServerFlags(cmd, v)

	err := cmd.Flags().Parse([]string{
		"--addr", ":8080",
		"--config", "/etc/arc/config.hcl",
		"--metrics-addr", ":9090",
		"--reflection",
	})
	if err != nil {
		t.Fatalf("Parse flags: %v", err)
	}

	t.Run("grpc.addr", func(t *testing.T) {
		if got := v.GetString("grpc.addr"); got != ":8080" {
			t.Errorf("grpc.addr = %q, want %q", got, ":8080")
		}
	})

	t.Run("observability.metrics_addr", func(t *testing.T) {
		if got := v.GetString("observability.metrics_addr"); got != ":9090" {
			t.Errorf("observability.metrics_addr = %q, want %q", got, ":9090")
		}
	})

	t.Run("grpc.enable_reflection", func(t *testing.T) {
		if got := v.GetBool("grpc.enable_reflection"); !got {
			t.Errorf("grpc.enable_reflection = %v, want true", got)
		}
	})

	t.Run("config flag not bound to viper", func(t *testing.T) {
		// --config is a flag but not bound to viper
		if got := v.GetString("config"); got != "" {
			t.Errorf("config should not be in viper, got %q", got)
		}
		// But it should be available as a cobra flag
		configVal, err := cmd.Flags().GetString("config")
		if err != nil {
			t.Fatalf("get config flag: %v", err)
		}
		if configVal != "/etc/arc/config.hcl" {
			t.Errorf("config flag = %q, want %q", configVal, "/etc/arc/config.hcl")
		}
	})

	t.Run("defaults when no flags set", func(t *testing.T) {
		cmd2 := &cobra.Command{Use: "test2"}
		v2 := viper.New()
		BindServerFlags(cmd2, v2)

		if err := cmd2.Flags().Parse([]string{}); err != nil {
			t.Fatalf("Parse flags: %v", err)
		}

		if got := v2.GetString("grpc.addr"); got != "" {
			t.Errorf("grpc.addr = %q, want empty", got)
		}
		if got := v2.GetString("observability.metrics_addr"); got != "" {
			t.Errorf("observability.metrics_addr = %q, want empty", got)
		}
		if got := v2.GetBool("grpc.enable_reflection"); got {
			t.Errorf("grpc.enable_reflection = %v, want false", got)
		}
	})
}

func TestRelayAddr(t *testing.T) {
	t.Run("from relay_addr", func(t *testing.T) {
		v := viper.New()
		v.Set("relay_addr", "custom:1234")

		if got := RelayAddr(v); got != "custom:1234" {
			t.Errorf("RelayAddr() = %q, want %q", got, "custom:1234")
		}
	})

	t.Run("from relay key", func(t *testing.T) {
		v := viper.New()
		v.Set("relay", "relay-host:5678")

		if got := RelayAddr(v); got != "relay-host:5678" {
			t.Errorf("RelayAddr() = %q, want %q", got, "relay-host:5678")
		}
	})

	t.Run("relay_addr takes precedence over relay", func(t *testing.T) {
		v := viper.New()
		v.Set("relay_addr", "primary:1111")
		v.Set("relay", "secondary:2222")

		if got := RelayAddr(v); got != "primary:1111" {
			t.Errorf("RelayAddr() = %q, want %q", got, "primary:1111")
		}
	})

	t.Run("from ARC_RELAY env", func(t *testing.T) {
		t.Setenv("ARC_RELAY", "envrelay:9999")
		// The RelayAddr function checks the global viper for ARC_RELAY,
		// so we need AutomaticEnv on the global instance.
		viper.Reset()
		viper.AutomaticEnv()
		t.Cleanup(func() { viper.Reset() })

		v := viper.New()

		if got := RelayAddr(v); got != "envrelay:9999" {
			t.Errorf("RelayAddr() = %q, want %q", got, "envrelay:9999")
		}
	})

	t.Run("default fallback", func(t *testing.T) {
		v := viper.New()

		if got := RelayAddr(v); got != Common.RelayAddr {
			t.Errorf("RelayAddr() = %q, want %q", got, Common.RelayAddr)
		}
	})

	t.Run("default is localhost:50051", func(t *testing.T) {
		v := viper.New()

		if got := RelayAddr(v); got != "localhost:50051" {
			t.Errorf("RelayAddr() = %q, want %q", got, "localhost:50051")
		}
	})
}

func TestLoadInto_envPrefix(t *testing.T) {
	t.Setenv("MYAPP_RELAY_ADDR", "envrelay:5555")

	v := viper.New()

	type cfg struct {
		BaseConfig `mapstructure:",squash"`
	}

	var c cfg
	err := LoadInto(v, "MYAPP", "", &c)
	if err != nil {
		t.Fatalf("LoadInto() error = %v", err)
	}

	if c.RelayAddr != "envrelay:5555" {
		t.Errorf("RelayAddr = %q, want %q", c.RelayAddr, "envrelay:5555")
	}
}
