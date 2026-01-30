package keys

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newShowCmd(v *viper.Viper) *cobra.Command {
	var outputFmt string

	cmd := &cobra.Command{
		Use:   "show <alias|public-key>",
		Short: "Show key details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			kr := openKeyring(v)

			key, err := kr.Load(ctx, args[0])
			if err != nil {
				return fmt.Errorf("key %q not found: %w", args[0], err)
			}

			info := map[string]any{
				"public_key": key.PublicKey,
				"created_at": key.Metadata.CreatedAt.Format(time.RFC3339),
				"key_file":   filepath.Join(dataDir(v), "keys", key.PublicKey+".key"),
			}

			if outputFmt == "json" {
				return writeJSON(os.Stdout, info)
			}

			fmt.Printf("Public Key: %s\n", info["public_key"])
			fmt.Printf("Created At: %s\n", info["created_at"])
			fmt.Printf("Key File:   %s\n", info["key_file"])
			return nil
		},
	}

	cmd.Flags().StringVarP(&outputFmt, "output", "o", "text", "output format (text, json)")
	return cmd
}
