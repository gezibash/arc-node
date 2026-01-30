package keys

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newListCmd(v *viper.Viper) *cobra.Command {
	var outputFmt string

	cmd := &cobra.Command{
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

			if len(infos) == 0 {
				fmt.Println("No keys found. Create one with: arc keys generate")
				return nil
			}

			if outputFmt == "json" {
				return writeJSON(os.Stdout, infos)
			}

			fmt.Printf("%-66s %-14s %s\n", "PUBLIC KEY", "ALIASES", "DEFAULT")
			for _, info := range infos {
				aliases := strings.Join(info.Aliases, ", ")
				if aliases == "" {
					aliases = "-"
				}
				def := ""
				if info.IsDefault {
					def = "*"
				}
				fmt.Printf("%-66s %-14s %s\n", info.PublicKey, truncate(aliases, 14), def)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&outputFmt, "output", "o", "text", "output format (text, json)")
	return cmd
}
