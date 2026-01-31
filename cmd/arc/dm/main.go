package dm

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/dm"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type dmCmd struct {
	v       *viper.Viper
	client  *client.Client
	threads *dm.Threads
	kp      *identity.Keypair
}

func Entrypoint(v *viper.Viper) *cobra.Command {
	d := &dmCmd{v: v}

	cmd := &cobra.Command{
		Use:   "dm [command]",
		Short: "Encrypted direct messages",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return d.init(cmd)
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if d.client != nil {
				return d.client.Close()
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if isatty.IsTerminal(os.Stdin.Fd()) || isatty.IsCygwinTerminal(os.Stdin.Fd()) {
				return runTUI(cmd.Context(), d.client, d.threads, d.kp)
			}
			return runThreadsMarkdown(cmd.Context(), d.threads, d.kp, os.Stdout)
		},
	}
	cmd.PersistentFlags().String("addr", "localhost:50051", "node gRPC address")
	cmd.AddCommand(
		newSendCmd(d),
		newListCmd(d),
		newReadCmd(d),
	)
	return cmd
}

func (d *dmCmd) init(cmd *cobra.Command) error {
	c, kp, err := dialNode(cmd, d.v)
	if err != nil {
		return err
	}
	d.client = c
	d.kp = kp

	d.threads = dm.NewThreads(c, kp)
	return nil
}

// openConversation parses a peer pubkey hex string and creates a DM instance.
func (d *dmCmd) openConversation(peerHex string) (*dm.DM, identity.PublicKey, error) {
	peerBytes, err := hex.DecodeString(peerHex)
	if err != nil {
		return nil, identity.PublicKey{}, fmt.Errorf("invalid peer public key: %w", err)
	}
	if len(peerBytes) != 32 {
		return nil, identity.PublicKey{}, fmt.Errorf("peer public key must be 32 bytes (64 hex chars)")
	}
	var peerPub identity.PublicKey
	copy(peerPub[:], peerBytes)

	sdk, err := d.threads.OpenConversation(peerPub)
	if err != nil {
		return nil, identity.PublicKey{}, fmt.Errorf("init dm: %w", err)
	}
	return sdk, peerPub, nil
}

func dialNode(cmd *cobra.Command, v *viper.Viper) (*client.Client, *identity.Keypair, error) {
	addr, _ := cmd.Flags().GetString("addr")
	kr := openKeyring(v)
	key, err := loadKey(cmd, v, kr)
	if err != nil {
		return nil, nil, fmt.Errorf("load key: %w", err)
	}
	nodeKey, err := kr.Load(cmd.Context(), "node")
	if err != nil {
		return nil, nil, fmt.Errorf("load node key: %w", err)
	}
	nodePub := nodeKey.Keypair.PublicKey()
	c, err := client.Dial(addr,
		client.WithIdentity(key.Keypair),
		client.WithNodeKey(nodePub),
	)
	if err != nil {
		return nil, nil, err
	}
	return c, key.Keypair, nil
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
