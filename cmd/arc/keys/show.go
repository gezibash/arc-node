package keys

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newShowCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
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

			out := cli.NewOutputFromViper(v)
			return out.KV("key-details").
				Set("Public Key", key.PublicKey).
				Set("Created At", key.Metadata.CreatedAt.Format(time.RFC3339)).
				Set("Key File", filepath.Join(dataDir(v), "keys", key.PublicKey+".key")).
				Render()
		},
	}
}
