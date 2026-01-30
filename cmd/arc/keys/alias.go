package keys

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newAliasCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "alias <name> <public-key>",
		Short: "Set an alias for a key",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			kr := openKeyring(v)
			if err := kr.SetAlias(args[0], args[1]); err != nil {
				return fmt.Errorf("set alias: %w", err)
			}
			fmt.Printf("Alias %q set for key %s\n", args[0], args[1])
			return nil
		},
	}
}
