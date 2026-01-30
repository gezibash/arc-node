package node

import (
	"fmt"
	"os"

	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type nodeCmd struct {
	v      *viper.Viper
	client *client.Client
}

func Entrypoint(v *viper.Viper) *cobra.Command {
	n := &nodeCmd{v: v}

	cmd := &cobra.Command{
		Use:   "node",
		Short: "Manage the arc node",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			logFile, _ := cmd.Flags().GetString("log-file")
			if logFile != "" {
				f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					return fmt.Errorf("open log file: %w", err)
				}
				observability.SetupLogger("debug", "json", f)
			}
			if cmd.Name() != "start" {
				c, err := dialNode(cmd, n.v)
				if err != nil {
					return err
				}
				n.client = c
			}
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if n.client != nil {
				return n.client.Close()
			}
			return nil
		},
	}
	cmd.PersistentFlags().String("addr", "localhost:50051", "node gRPC address")
	cmd.PersistentFlags().String("log-file", "", "write debug logs to file (JSON format)")
	cmd.AddCommand(
		newStartCmd(v),
		newStatusCmd(n),
		newPutCmd(n),
		newGetCmd(n),
		newPublishCmd(n),
		newQueryCmd(n),
		newWatchCmd(n),
	)
	return cmd
}

func dialNode(cmd *cobra.Command, v *viper.Viper) (*client.Client, error) {
	addr, _ := cmd.Flags().GetString("addr")
	kr := openKeyring(v)
	key, err := loadKey(cmd, v, kr)
	if err != nil {
		return nil, fmt.Errorf("load key: %w", err)
	}
	nodeKey, err := kr.Load(cmd.Context(), "node")
	if err != nil {
		return nil, fmt.Errorf("load node key: %w", err)
	}
	return client.Dial(addr,
		client.WithIdentity(key.Keypair),
		client.WithNodeKey(nodeKey.Keypair.PublicKey()),
	)
}

func loadKey(cmd *cobra.Command, v *viper.Viper, kr *keyring.Keyring) (*keyring.Key, error) {
	name := v.GetString("key")
	if name != "" {
		return kr.Load(cmd.Context(), name)
	}
	return kr.LoadDefault(cmd.Context())
}

func openKeyring(v *viper.Viper) *keyring.Keyring {
	dataDir := v.GetString("data_dir")
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}
	return keyring.New(dataDir)
}
