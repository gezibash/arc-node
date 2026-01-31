package config

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	DataDir       string              `mapstructure:"data_dir"`
	GRPC          GRPCConfig          `mapstructure:"grpc"`
	Observability ObservabilityConfig `mapstructure:"observability"`
	Storage       StorageConfig       `mapstructure:"storage"`
}

type StorageConfig struct {
	Blob  BackendConfig `mapstructure:"blob"`
	Index BackendConfig `mapstructure:"index"`
}

type BackendConfig struct {
	Backend string            `mapstructure:"backend"`
	Config  map[string]string `mapstructure:"config"`
}

type GRPCConfig struct {
	Addr             string `mapstructure:"addr"`
	MaxRecvMsgSize   int    `mapstructure:"max_recv_msg_size"`
	MaxSendMsgSize   int    `mapstructure:"max_send_msg_size"`
	EnableReflection bool   `mapstructure:"enable_reflection"`
}

type ObservabilityConfig struct {
	LogLevel       string `mapstructure:"log_level"`
	LogFormat      string `mapstructure:"log_format"`
	MetricsAddr    string `mapstructure:"metrics_addr"`
	OTLPEndpoint   string `mapstructure:"otlp_endpoint"`
	OTLPProtocol   string `mapstructure:"otlp_protocol"`
	ServiceName    string `mapstructure:"service_name"`
	ServiceVersion string `mapstructure:"service_version"`
}

func DefaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".arc"
	}
	return filepath.Join(home, ".arc")
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("data_dir", DefaultDataDir())

	v.SetDefault("grpc.addr", ":50051")
	v.SetDefault("grpc.max_recv_msg_size", 4*1024*1024)
	v.SetDefault("grpc.max_send_msg_size", 4*1024*1024)
	v.SetDefault("grpc.enable_reflection", false)

	v.SetDefault("observability.log_level", "info")
	v.SetDefault("observability.log_format", "text")
	v.SetDefault("observability.metrics_addr", ":9090")
	v.SetDefault("observability.otlp_endpoint", "")
	v.SetDefault("observability.otlp_protocol", "http")
	v.SetDefault("observability.service_name", "arc")
	v.SetDefault("observability.service_version", "dev")

	v.SetDefault("storage.blob.backend", "badger")
	v.SetDefault("storage.index.backend", "badger")
}

// BindServeFlags binds cobra flags to viper for the serve command.
func BindServeFlags(cmd *cobra.Command, v *viper.Viper) {
	f := cmd.Flags()
	f.String("data-dir", "", "data directory (default ~/.arc)")
	f.String("addr", "", "gRPC listen address")
	f.String("config", "", "config file path")
	f.String("log-level", "", "log level (debug, info, warn, error)")
	f.String("log-format", "", "log format (json, text)")
	f.String("metrics-addr", "", "metrics HTTP listen address")
	f.Bool("reflection", false, "enable gRPC reflection")

	_ = v.BindPFlag("data_dir", f.Lookup("data-dir"))
	_ = v.BindPFlag("grpc.addr", f.Lookup("addr"))
	_ = v.BindPFlag("observability.log_level", f.Lookup("log-level"))
	_ = v.BindPFlag("observability.log_format", f.Lookup("log-format"))
	_ = v.BindPFlag("observability.metrics_addr", f.Lookup("metrics-addr"))
	_ = v.BindPFlag("grpc.enable_reflection", f.Lookup("reflection"))
}

// Load reads config from flags, env, and file, returning the merged Config.
func Load(v *viper.Viper, configFile string) (Config, error) {
	setDefaults(v)

	v.SetEnvPrefix("ARC")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if configFile != "" {
		v.SetConfigFile(configFile)
	} else {
		v.SetConfigName("arc")
		v.SetConfigType("hcl")
		v.AddConfigPath(".")
		v.AddConfigPath("$HOME/.arc")
		v.AddConfigPath("/etc/arc")
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok && configFile != "" {
			return Config{}, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
