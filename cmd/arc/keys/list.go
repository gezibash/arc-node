package keys

import (
	"fmt"
	"strings"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newListCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all keys",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			kr := openKeyring(v)

			infos, err := kr.List(ctx)
			if err != nil {
				return fmt.Errorf("list keys: %w", err)
			}

			out := cli.NewOutputFromViper(v)

			if len(infos) == 0 {
				return out.Result("keys-list", "No keys found. Create one with: arc keys generate").Render()
			}

			table := out.Table("keys-list", "Public Key", "Aliases", "Default")
			for _, info := range infos {
				aliases := strings.Join(info.Aliases, ", ")
				if aliases == "" {
					aliases = "-"
				}
				def := ""
				if info.IsDefault {
					def = "*"
				}
				table.AddRow(info.PublicKey, aliases, def)
			}

			return table.Render()
		},
	}
}
