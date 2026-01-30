package keys

import (
	"fmt"
	"path/filepath"

	"github.com/gezibash/arc-node/internal/keyring"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newGenerateCmd(v *viper.Viper) *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "generate [alias]",
		Short: "Generate a new key",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			alias := keyring.DefaultAlias
			if len(args) > 0 {
				alias = args[0]
			}

			ctx := cmd.Context()
			kr := openKeyring(v)

			if !force {
				if _, err := kr.Load(ctx, alias); err == nil {
					return fmt.Errorf("key with alias %q already exists (use --force to overwrite)", alias)
				}
			}

			key, err := kr.Generate(ctx, alias)
			if err != nil {
				return fmt.Errorf("generate key: %w", err)
			}

			if alias == keyring.DefaultAlias {
				_ = kr.SetDefault(alias)
			}

			fmt.Printf("Key created: %s\n", alias)
			fmt.Printf("  Public Key: %s\n", key.PublicKey)
			fmt.Printf("  Stored at:  %s\n", filepath.Join(dataDir(v), "keys", key.PublicKey+".key"))
			return nil
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "overwrite existing key")
	return cmd
}
