package keys

import (
	"encoding/hex"
	"fmt"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newImportCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "import <hex-seed> [alias]",
		Short: "Import a key from a hex-encoded seed",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			seed, err := hex.DecodeString(args[0])
			if err != nil {
				return fmt.Errorf("invalid hex seed: %w", err)
			}

			alias := ""
			if len(args) > 1 {
				alias = args[1]
			}

			ctx := cmd.Context()
			kr := openKeyring(v)

			key, err := kr.Import(ctx, seed, alias)
			if err != nil {
				return fmt.Errorf("import key: %w", err)
			}

			out := cli.NewOutputFromViper(v)
			result := out.Result("key-imported", "Key imported").
				With("Public Key", key.PublicKey)

			if alias != "" {
				result.With("Alias", alias)
			}

			return result.Render()
		},
	}
}
