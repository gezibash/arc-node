package keys

import (
	"fmt"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newExportCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "export <alias|public-key>",
		Short: "Export public key hex",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			kr := openKeyring(v)
			key, err := kr.Load(ctx, args[0])
			if err != nil {
				return fmt.Errorf("key %q not found: %w", args[0], err)
			}

			out := cli.NewOutputFromViper(v)
			return out.KV("key-export").
				Set("Public Key", key.PublicKey).
				Render()
		},
	}
}
