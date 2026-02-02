package node

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type nodeCmd struct {
	v         *viper.Viper
	client    NodeClient
	keyLoader KeyLoader
	readInput func([]string) ([]byte, error)
}

func Entrypoint(v *viper.Viper) *cobra.Command {
	n := &nodeCmd{v: v}

	cmd := &cobra.Command{
		Use:   "node",
		Short: "Manage the arc node",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			logFile, _ := cmd.Flags().GetString("log-file")
			if logFile != "" {
				f, err := os.OpenFile(filepath.Clean(logFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
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
		newFederateCmd(n),
		newPeersCmd(n),
	)
	return cmd
}

func dialNode(cmd *cobra.Command, v *viper.Viper) (NodeClient, error) {
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

func (n *nodeCmd) loadKeypair(cmd *cobra.Command) (*identity.Keypair, error) {
	if n.keyLoader != nil {
		name := n.v.GetString("key")
		if name != "" {
			return n.keyLoader.Load(cmd.Context(), name)
		}
		return n.keyLoader.LoadDefault(cmd.Context())
	}
	kr := openKeyring(n.v)
	key, err := loadKey(cmd, n.v, kr)
	if err != nil {
		return nil, err
	}
	return key.Keypair, nil
}

func (n *nodeCmd) loadNodeKey(cmd *cobra.Command) (identity.PublicKey, error) {
	if n.keyLoader != nil {
		kp, err := n.keyLoader.Load(cmd.Context(), "node")
		if err != nil {
			return identity.PublicKey{}, err
		}
		return kp.PublicKey(), nil
	}
	kr := openKeyring(n.v)
	nodeKey, err := kr.Load(cmd.Context(), "node")
	if err != nil {
		return identity.PublicKey{}, err
	}
	return nodeKey.Keypair.PublicKey(), nil
}

func (n *nodeCmd) readInputFn(args []string) ([]byte, error) {
	if n.readInput != nil {
		return n.readInput(args)
	}
	return readInput(args)
}

func openKeyring(v *viper.Viper) *keyring.Keyring {
	dataDir := v.GetString("data_dir")
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}
	return keyring.New(dataDir)
}
