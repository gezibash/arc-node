package journal

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc-node/pkg/journal"
	"github.com/gezibash/arc/pkg/identity"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type journalCmd struct {
	v          *viper.Viper
	client     *client.Client
	sdk        *journal.Journal
	search     *journal.SearchIndex
	searchPath string
	searchDir  string
}

func Entrypoint(v *viper.Viper) *cobra.Command {
	j := &journalCmd{v: v}

	cmd := &cobra.Command{
		Use:   "journal",
		Short: "Encrypted journal entries",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return j.init(cmd)
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if j.search != nil {
				j.search.Close()
			}
			if j.client != nil {
				return j.client.Close()
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if isatty.IsTerminal(os.Stdin.Fd()) || isatty.IsCygwinTerminal(os.Stdin.Fd()) {
				return runTUI(cmd.Context(), j.client, j.sdk, j.searchDir)
			}
			return runListMarkdown(cmd.Context(), j.sdk, os.Stdout)
		},
	}
	cmd.PersistentFlags().String("addr", "localhost:50051", "node gRPC address")
	cmd.AddCommand(
		newWriteCmd(j),
		newListCmd(j),
		newReadCmd(j),
		newEditCmd(j),
		newSearchCmd(j),
		newFetchCmd(j),
		newPullCmd(j),
		newPushCmd(j),
		newReindexCmd(j),
	)
	return cmd
}

func (j *journalCmd) init(cmd *cobra.Command) error {
	c, kp, nodeKey, err := dialNode(cmd, j.v)
	if err != nil {
		return err
	}
	j.client = c

	dataDir := j.v.GetString("data_dir")
	if dataDir == "" {
		dataDir = config.DefaultDataDir()
	}
	pub := kp.PublicKey()
	keyHex := hex.EncodeToString(pub[:])
	j.searchDir = filepath.Join(dataDir, "journal", keyHex)
	j.searchPath = filepath.Join(j.searchDir, "search.db")
	idx, err := journal.OpenSearchIndex(j.searchPath)
	if err != nil {
		return fmt.Errorf("open search index: %w", err)
	}
	j.search = idx

	var opts []journal.Option
	if nodeKey != nil {
		opts = append(opts, journal.WithNodeKey(*nodeKey))
	}
	opts = append(opts, journal.WithSearchIndex(idx))
	j.sdk = journal.New(c, kp, opts...)
	return nil
}

func dialNode(cmd *cobra.Command, v *viper.Viper) (*client.Client, *identity.Keypair, *identity.PublicKey, error) {
	addr, _ := cmd.Flags().GetString("addr")
	kr := openKeyring(v)
	key, err := loadKey(cmd, v, kr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load key: %w", err)
	}
	nodeKey, err := kr.Load(cmd.Context(), "node")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load node key: %w", err)
	}
	nodePub := nodeKey.Keypair.PublicKey()
	c, err := client.Dial(addr,
		client.WithIdentity(key.Keypair),
		client.WithNodeKey(nodePub),
	)
	if err != nil {
		return nil, nil, nil, err
	}
	return c, key.Keypair, &nodePub, nil
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
