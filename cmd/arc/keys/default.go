package keys

import (
	"fmt"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newDefaultCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "default <alias>",
		Short: "Set the default key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kr := openKeyring(v)
			if err := kr.SetDefault(args[0]); err != nil {
				return fmt.Errorf("set default: %w", err)
			}

			out := cli.NewOutputFromViper(v)
			return out.Result("default-set", fmt.Sprintf("Default key set to %q", args[0])).
				With("Alias", args[0]).
				Render()
		},
	}
}
