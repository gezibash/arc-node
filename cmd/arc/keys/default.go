package keys

import (
	"fmt"

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
			fmt.Printf("Default key set to %q\n", args[0])
			return nil
		},
	}
}
