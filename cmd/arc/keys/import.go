package keys

import (
	"encoding/hex"
	"fmt"

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

			fmt.Printf("Key imported: %s\n", key.PublicKey)
			if alias != "" {
				fmt.Printf("  Alias: %s\n", alias)
			}
			return nil
		},
	}
}
