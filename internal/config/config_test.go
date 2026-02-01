package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// TestDefaultDataDir verifies that DefaultDataDir returns a path ending in .arc
func TestDefaultDataDir(t *testing.T) {
	dataDir := DefaultDataDir()
	if !strings.HasSuffix(dataDir, ".arc") {
		t.Errorf("DefaultDataDir() should end with .arc, got: %s", dataDir)
	}

	// Should be an absolute path
	if !filepath.IsAbs(dataDir) {
		t.Errorf("DefaultDataDir() should return absolute path, got: %s", dataDir)
	}
}

// TestDefaultDataDirFallback tests fallback behavior when UserHomeDir fails
func TestDefaultDataDirFallback(t *testing.T) {
	// This test documents that when UserHomeDir succeeds, we use it.
	// The fallback ".arc" case is hard to test without mocking os.UserHomeDir.
	dataDir := DefaultDataDir()
	if dataDir == "" {
		t.Error("DefaultDataDir() should never return empty string")
	}
}

// TestLoadDefaults verifies that Load applies all defaults when no config file
// or env vars are set.
func TestLoadDefaults(t *testing.T) {
	v := viper.New()
	cfg, err := Load(v, "")
	if err != nil {
		t.Fatalf("Load with no config file should not error, got: %v", err)
	}

	// Check data_dir default
	if !strings.HasSuffix(cfg.DataDir, ".arc") {
		t.Errorf("DataDir should end with .arc, got: %s", cfg.DataDir)
	}

	// Check gRPC defaults
	if cfg.GRPC.Addr != ":50051" {
		t.Errorf("GRPC.Addr default should be :50051, got: %s", cfg.GRPC.Addr)
	}
	if cfg.GRPC.MaxRecvMsgSize != 4*1024*1024 {
		t.Errorf("GRPC.MaxRecvMsgSize default should be 4MB, got: %d", cfg.GRPC.MaxRecvMsgSize)
	}
	if cfg.GRPC.MaxSendMsgSize != 4*1024*1024 {
		t.Errorf("GRPC.MaxSendMsgSize default should be 4MB, got: %d", cfg.GRPC.MaxSendMsgSize)
	}
	if cfg.GRPC.EnableReflection != false {
		t.Errorf("GRPC.EnableReflection default should be false, got: %v", cfg.GRPC.EnableReflection)
	}

	// Check observability defaults
	if cfg.Observability.LogLevel != "info" {
		t.Errorf("Observability.LogLevel default should be 'info', got: %s", cfg.Observability.LogLevel)
	}
	if cfg.Observability.LogFormat != "text" {
		t.Errorf("Observability.LogFormat default should be 'text', got: %s", cfg.Observability.LogFormat)
	}
	if cfg.Observability.MetricsAddr != ":9090" {
		t.Errorf("Observability.MetricsAddr default should be :9090, got: %s", cfg.Observability.MetricsAddr)
	}
	if cfg.Observability.OTLPProtocol != "http" {
		t.Errorf("Observability.OTLPProtocol default should be 'http', got: %s", cfg.Observability.OTLPProtocol)
	}
	if cfg.Observability.ServiceName != "arc" {
		t.Errorf("Observability.ServiceName default should be 'arc', got: %s", cfg.Observability.ServiceName)
	}
	if cfg.Observability.ServiceVersion != "dev" {
		t.Errorf("Observability.ServiceVersion default should be 'dev', got: %s", cfg.Observability.ServiceVersion)
	}

	// Check storage defaults
	if cfg.Storage.Blob.Backend != "badger" {
		t.Errorf("Storage.Blob.Backend default should be 'badger', got: %s", cfg.Storage.Blob.Backend)
	}
	if cfg.Storage.Index.Backend != "badger" {
		t.Errorf("Storage.Index.Backend default should be 'badger', got: %s", cfg.Storage.Index.Backend)
	}
}

// TestLoadWithEnvOverride verifies that environment variables override defaults
func TestLoadWithEnvOverride(t *testing.T) {
	t.Setenv("ARC_GRPC_ADDR", ":55555")
	t.Setenv("ARC_OBSERVABILITY_LOG_LEVEL", "debug")
	t.Setenv("ARC_DATA_DIR", "/custom/data/dir")

	v := viper.New()
	cfg, err := Load(v, "")
	if err != nil {
		t.Fatalf("Load with env overrides should not error, got: %v", err)
	}

	if cfg.GRPC.Addr != ":55555" {
		t.Errorf("GRPC.Addr should be :55555 (from env), got: %s", cfg.GRPC.Addr)
	}

	if cfg.Observability.LogLevel != "debug" {
		t.Errorf("Observability.LogLevel should be 'debug' (from env), got: %s", cfg.Observability.LogLevel)
	}

	if cfg.DataDir != "/custom/data/dir" {
		t.Errorf("DataDir should be /custom/data/dir (from env), got: %s", cfg.DataDir)
	}
}

// TestLoadWithConfigFile verifies that a config file is properly loaded and
// its values override defaults
func TestLoadWithConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "arc.yaml")

	// Write a test YAML config file (more reliable than HCL for testing)
	configContent := `
data_dir: /tmp/arc-test
grpc:
  addr: :6000
  enable_reflection: true
  max_recv_msg_size: 8388608
observability:
  log_level: warn
  log_format: json
  metrics_addr: :9091
storage:
  blob:
    backend: memory
    config:
      max_size: 1GB
  index:
    backend: sqlite
    config:
      path: /tmp/index.db
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	v := viper.New()
	v.SetConfigType("yaml")
	cfg, err := Load(v, configPath)
	if err != nil {
		t.Fatalf("Load with config file should not error, got: %v", err)
	}

	if cfg.DataDir != "/tmp/arc-test" {
		t.Errorf("DataDir should be /tmp/arc-test from config file, got: %s", cfg.DataDir)
	}

	if cfg.GRPC.Addr != ":6000" {
		t.Errorf("GRPC.Addr should be :6000 from config file, got: %s", cfg.GRPC.Addr)
	}

	if !cfg.GRPC.EnableReflection {
		t.Errorf("GRPC.EnableReflection should be true from config file, got: %v", cfg.GRPC.EnableReflection)
	}

	if cfg.GRPC.MaxRecvMsgSize != 8388608 {
		t.Errorf("GRPC.MaxRecvMsgSize should be 8388608 from config file, got: %d", cfg.GRPC.MaxRecvMsgSize)
	}

	if cfg.Observability.LogLevel != "warn" {
		t.Errorf("Observability.LogLevel should be 'warn' from config file, got: %s", cfg.Observability.LogLevel)
	}

	if cfg.Observability.LogFormat != "json" {
		t.Errorf("Observability.LogFormat should be 'json' from config file, got: %s", cfg.Observability.LogFormat)
	}

	if cfg.Observability.MetricsAddr != ":9091" {
		t.Errorf("Observability.MetricsAddr should be :9091 from config file, got: %s", cfg.Observability.MetricsAddr)
	}

	if cfg.Storage.Blob.Backend != "memory" {
		t.Errorf("Storage.Blob.Backend should be 'memory' from config file, got: %s", cfg.Storage.Blob.Backend)
	}

	if cfg.Storage.Index.Backend != "sqlite" {
		t.Errorf("Storage.Index.Backend should be 'sqlite' from config file, got: %s", cfg.Storage.Index.Backend)
	}

	if cfg.Storage.Blob.Config["max_size"] != "1GB" {
		t.Errorf("Storage.Blob.Config should contain max_size=1GB, got: %v", cfg.Storage.Blob.Config)
	}

	if cfg.Storage.Index.Config["path"] != "/tmp/index.db" {
		t.Errorf("Storage.Index.Config should contain path=/tmp/index.db, got: %v", cfg.Storage.Index.Config)
	}
}

// TestLoadMissingExplicitConfigFile verifies that an explicit config file path
// that doesn't exist returns an error
func TestLoadMissingExplicitConfigFile(t *testing.T) {
	nonExistentPath := "/nonexistent/path/to/config.hcl"

	v := viper.New()
	_, err := Load(v, nonExistentPath)
	if err == nil {
		t.Error("Load with explicit missing config file should error")
	}
}

// TestLoadNoConfigFileSilent verifies that when no explicit config file is
// specified and none can be found, Load still succeeds with defaults
func TestLoadNoConfigFileSilent(t *testing.T) {
	// Change to a temp directory with no arc config files
	tmpDir := t.TempDir()
	oldCwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current directory: %v", err)
	}
	defer os.Chdir(oldCwd)

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("Failed to change directory: %v", err)
	}

	// Ensure no config files are searched for in home or /etc
	// by using a fresh viper instance
	v := viper.New()
	cfg, err := Load(v, "")
	if err != nil {
		t.Fatalf("Load with no config file found should not error, got: %v", err)
	}

	// Should still have defaults
	if cfg.GRPC.Addr != ":50051" {
		t.Errorf("Should still have default GRPC.Addr, got: %s", cfg.GRPC.Addr)
	}
}

// TestConfigStructFields verifies that Config struct has expected fields
func TestConfigStructFields(t *testing.T) {
	cfg := Config{}

	// Create a dummy value to ensure fields exist and are accessible
	cfg.DataDir = "/test"
	cfg.GRPC.Addr = ":50051"
	cfg.Storage.Blob.Backend = "badger"
	cfg.Storage.Index.Backend = "badger"
	cfg.Observability.LogLevel = "info"

	if cfg.DataDir != "/test" {
		t.Error("Config.DataDir field not working correctly")
	}

	if cfg.GRPC.Addr != ":50051" {
		t.Error("Config.GRPC.Addr field not working correctly")
	}

	if cfg.Storage.Blob.Backend != "badger" {
		t.Error("Config.Storage.Blob.Backend field not working correctly")
	}

	if cfg.Storage.Index.Backend != "badger" {
		t.Error("Config.Storage.Index.Backend field not working correctly")
	}

	if cfg.Observability.LogLevel != "info" {
		t.Error("Config.Observability.LogLevel field not working correctly")
	}
}

// TestBindServeFlags verifies that BindServeFlags properly binds flags to viper
func TestBindServeFlags(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	v := viper.New()

	BindServeFlags(cmd, v)

	// Parse flags with values
	err := cmd.Flags().Parse([]string{
		"--data-dir", "/custom/dir",
		"--addr", ":7000",
		"--log-level", "debug",
		"--log-format", "json",
		"--metrics-addr", ":9092",
		"--reflection",
	})
	if err != nil {
		t.Fatalf("Failed to parse flags: %v", err)
	}

	// Load defaults first, then flags should override
	v.SetDefault("data_dir", DefaultDataDir())
	v.SetDefault("grpc.addr", ":50051")
	v.SetDefault("observability.log_level", "info")
	v.SetDefault("observability.log_format", "text")
	v.SetDefault("observability.metrics_addr", ":9090")
	v.SetDefault("grpc.enable_reflection", false)

	// Check that flag values are bound
	if v.GetString("data_dir") != "/custom/dir" {
		t.Errorf("data_dir flag not bound correctly, got: %s", v.GetString("data_dir"))
	}

	if v.GetString("grpc.addr") != ":7000" {
		t.Errorf("grpc.addr flag not bound correctly, got: %s", v.GetString("grpc.addr"))
	}

	if v.GetString("observability.log_level") != "debug" {
		t.Errorf("observability.log_level flag not bound correctly, got: %s", v.GetString("observability.log_level"))
	}

	if v.GetString("observability.log_format") != "json" {
		t.Errorf("observability.log_format flag not bound correctly, got: %s", v.GetString("observability.log_format"))
	}

	if v.GetString("observability.metrics_addr") != ":9092" {
		t.Errorf("observability.metrics_addr flag not bound correctly, got: %s", v.GetString("observability.metrics_addr"))
	}

	if !v.GetBool("grpc.enable_reflection") {
		t.Errorf("grpc.enable_reflection flag not bound correctly, got: %v", v.GetBool("grpc.enable_reflection"))
	}
}

// TestEnvVarPriority verifies that flag values take priority over env vars
func TestEnvVarPriority(t *testing.T) {
	t.Setenv("ARC_GRPC_ADDR", ":55555")

	cmd := &cobra.Command{Use: "test"}
	v := viper.New()

	BindServeFlags(cmd, v)

	// Parse flags with value
	err := cmd.Flags().Parse([]string{
		"--addr", ":6000",
	})
	if err != nil {
		t.Fatalf("Failed to parse flags: %v", err)
	}

	setDefaults(v)
	v.SetEnvPrefix("ARC")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Flag should take priority over env var
	if v.GetString("grpc.addr") != ":6000" {
		t.Errorf("Flag should take priority over env var, got: %s", v.GetString("grpc.addr"))
	}
}

// TestLoadYAMLConfig tests loading a YAML config file
func TestLoadYAMLConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "arc.yaml")

	// Write a test YAML config file
	yamlContent := `
data_dir: /tmp/yaml-test
grpc:
  addr: :7001
  enable_reflection: true
observability:
  log_level: warn
  log_format: json
storage:
  blob:
    backend: fs
  index:
    backend: memory
`
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write YAML config file: %v", err)
	}

	v := viper.New()
	v.SetConfigType("yaml")
	cfg, err := Load(v, configPath)
	if err != nil {
		t.Fatalf("Load with YAML config file should not error, got: %v", err)
	}

	if cfg.DataDir != "/tmp/yaml-test" {
		t.Errorf("DataDir from YAML should be /tmp/yaml-test, got: %s", cfg.DataDir)
	}

	if cfg.GRPC.Addr != ":7001" {
		t.Errorf("GRPC.Addr from YAML should be :7001, got: %s", cfg.GRPC.Addr)
	}

	if cfg.Storage.Blob.Backend != "fs" {
		t.Errorf("Storage.Blob.Backend from YAML should be 'fs', got: %s", cfg.Storage.Blob.Backend)
	}

	if cfg.Storage.Index.Backend != "memory" {
		t.Errorf("Storage.Index.Backend from YAML should be 'memory', got: %s", cfg.Storage.Index.Backend)
	}
}

// TestLoadWithBackendConfig tests loading backend-specific configurations
func TestLoadWithBackendConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "arc.yaml")

	configContent := `
storage:
  blob:
    backend: s3
    config:
      bucket: my-bucket
      region: us-west-2
      endpoint: https://s3.example.com
  index:
    backend: redis
    config:
      addr: localhost:6379
      db: "0"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	v := viper.New()
	v.SetConfigType("yaml")
	cfg, err := Load(v, configPath)
	if err != nil {
		t.Fatalf("Load with backend config should not error, got: %v", err)
	}

	if cfg.Storage.Blob.Backend != "s3" {
		t.Errorf("Storage.Blob.Backend should be 's3', got: %s", cfg.Storage.Blob.Backend)
	}

	if cfg.Storage.Blob.Config["bucket"] != "my-bucket" {
		t.Errorf("S3 bucket config should be 'my-bucket', got: %v", cfg.Storage.Blob.Config["bucket"])
	}

	if cfg.Storage.Blob.Config["region"] != "us-west-2" {
		t.Errorf("S3 region config should be 'us-west-2', got: %v", cfg.Storage.Blob.Config["region"])
	}

	if cfg.Storage.Index.Backend != "redis" {
		t.Errorf("Storage.Index.Backend should be 'redis', got: %s", cfg.Storage.Index.Backend)
	}

	if cfg.Storage.Index.Config["addr"] != "localhost:6379" {
		t.Errorf("Redis addr config should be 'localhost:6379', got: %v", cfg.Storage.Index.Config["addr"])
	}
}
