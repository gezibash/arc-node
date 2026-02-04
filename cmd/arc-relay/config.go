package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/gezibash/arc/v2/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Config holds relay server configuration.
type Config struct {
	DataDir       string              `mapstructure:"data_dir"`
	KeyName       string              `mapstructure:"key_name"`
	KeyPath       string              `mapstructure:"key_path"`
	GRPC          GRPCConfig          `mapstructure:"grpc"`
	Gossip        GossipConfig        `mapstructure:"gossip"`
	Observability ObservabilityConfig `mapstructure:"observability"`
}

// GossipConfig holds gossip cluster settings.
type GossipConfig struct {
	NodeName      string   `mapstructure:"node_name"`
	BindAddr      string   `mapstructure:"bind_addr"`
	BindPort      int      `mapstructure:"bind_port"`
	AdvertiseAddr string   `mapstructure:"advertise_addr"`
	AdvertisePort int      `mapstructure:"advertise_port"`
	Seeds         []string `mapstructure:"seeds"`
}

// Enabled returns true if gossip should be started (seeds or bind_port configured).
func (g GossipConfig) Enabled() bool {
	return len(g.Seeds) > 0 || g.BindPort > 0
}

// GRPCConfig holds gRPC server settings.
type GRPCConfig struct {
	Addr             string `mapstructure:"addr"`
	MaxRecvMsgSize   int    `mapstructure:"max_recv_msg_size"`
	MaxSendMsgSize   int    `mapstructure:"max_send_msg_size"`
	EnableReflection bool   `mapstructure:"enable_reflection"`
}

// ObservabilityConfig holds logging and metrics settings.
type ObservabilityConfig struct {
	LogLevel    string `mapstructure:"log_level"`
	LogFormat   string `mapstructure:"log_format"`
	MetricsAddr string `mapstructure:"metrics_addr"`
}

// Defaults contains default values for the relay server.
var Defaults = struct {
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

func setDefaults(v *viper.Viper) {
	v.SetDefault("data_dir", config.Common.DataDir)
	v.SetDefault("key_name", Defaults.KeyName)
	v.SetDefault("key_path", "")

	v.SetDefault("grpc.addr", Defaults.ListenAddr)
	v.SetDefault("grpc.max_recv_msg_size", Defaults.MaxRecvMsgSize)
	v.SetDefault("grpc.max_send_msg_size", Defaults.MaxSendMsgSize)
	v.SetDefault("grpc.enable_reflection", Defaults.EnableReflection)

	v.SetDefault("observability.log_level", config.Common.LogLevel)
	v.SetDefault("observability.log_format", config.Common.LogFormat)
	v.SetDefault("observability.metrics_addr", Defaults.MetricsAddr)

	v.SetDefault("gossip.bind_addr", "0.0.0.0")
	v.SetDefault("gossip.bind_port", 0) // 0 = gossip disabled unless --gossip-bind or seeds set
}

func bindServeFlags(cmd *cobra.Command, v *viper.Viper) {
	f := cmd.Flags()
	f.String("data-dir", "", "data directory (default ~/.arc)")
	f.String("addr", "", "gRPC listen address")
	f.String("config", "", "config file path")
	f.String("key", "", "key name to use (default: relay)")
	f.String("key-path", "", "path to key file (overrides --key)")
	f.String("log-level", "", "log level (debug, info, warn, error)")
	f.String("log-format", "", "log format (json, text)")
	f.String("metrics-addr", "", "metrics HTTP listen address")
	f.Bool("reflection", false, "enable gRPC reflection")

	// Gossip flags
	f.String("gossip-name", "", "gossip node name (default: BIP-39 petname from relay pubkey)")
	f.String("gossip-bind", "", "gossip bind address (host:port, default 0.0.0.0:7946)")
	f.StringSlice("gossip-join", nil, "peers to join on startup (host:port)")

	_ = v.BindPFlag("data_dir", f.Lookup("data-dir"))
	_ = v.BindPFlag("key_name", f.Lookup("key"))
	_ = v.BindPFlag("key_path", f.Lookup("key-path"))
	_ = v.BindPFlag("grpc.addr", f.Lookup("addr"))
	_ = v.BindPFlag("observability.log_level", f.Lookup("log-level"))
	_ = v.BindPFlag("observability.log_format", f.Lookup("log-format"))
	_ = v.BindPFlag("observability.metrics_addr", f.Lookup("metrics-addr"))
	_ = v.BindPFlag("grpc.enable_reflection", f.Lookup("reflection"))
	_ = v.BindPFlag("gossip.node_name", f.Lookup("gossip-name"))
	_ = v.BindPFlag("gossip.seeds", f.Lookup("gossip-join"))
}

func loadConfig(v *viper.Viper, configFile string) (Config, error) {
	setDefaults(v)

	v.SetEnvPrefix("ARC_RELAY")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Also check ARC_ prefix for common settings
	if v.GetString("data_dir") == "" {
		if d := os.Getenv("ARC_DATA_DIR"); d != "" {
			v.Set("data_dir", d)
		}
	}

	if configFile != "" {
		v.SetConfigFile(configFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("hcl")
		v.AddConfigPath(".")
		v.AddConfigPath(filepath.Join(config.Common.DataDir, "relay"))
		v.AddConfigPath("/etc/arc/relay")
	}

	if err := v.ReadInConfig(); err != nil {
		var cfgErr viper.ConfigFileNotFoundError
		if !errors.As(err, &cfgErr) && configFile != "" {
			return Config{}, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
