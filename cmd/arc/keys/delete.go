package keys

import (
	"fmt"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newDeleteCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <alias|public-key>",
		Short: "Delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			kr := openKeyring(v)
			if err := kr.Delete(ctx, args[0]); err != nil {
				return fmt.Errorf("delete key: %w", err)
			}

			out := cli.NewOutputFromViper(v)
			return out.Result("key-deleted", fmt.Sprintf("Key %q deleted", args[0])).
				With("Key", args[0]).
				Render()
		},
	}
}
