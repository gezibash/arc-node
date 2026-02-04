package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/pkg/blob"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newWantCmd(v *viper.Viper) *cobra.Command {
	var relayServer string
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "want <cid>",
		Short: "Discover who has a blob",
		Long: `Query the network to find which blob stores have a given CID.

Sends a WANT request and collects HAVE responses from stores.

Examples:
  arc-blob want abc123...
  arc-blob want --timeout 5s abc123...`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse CID first (before building runtime)
			cidHex := args[0]
			cidBytes, err := hex.DecodeString(cidHex)
			if err != nil {
				return fmt.Errorf("invalid CID: %w", err)
			}
			if len(cidBytes) != 32 {
				return fmt.Errorf("invalid CID length: expected 32 bytes, got %d", len(cidBytes))
			}
			var cid [32]byte
			copy(cid[:], cidBytes)

			return cli.RunCommand(cli.CommandConfig{
				Name:    "blob-want",
				KeyName: "blob",
				Viper:   v,
				Timeout: timeout,
				Extensions: []runtime.Extension{
					cli.WithRelay(relayServer),
					blob.Capability(blob.Config{}),
				},
				Run: func(ctx context.Context, rt *runtime.Runtime, out *cli.Output) error {
					c := blob.From(rt)
					if c == nil {
						return fmt.Errorf("blob capability not available")
					}

					result, err := c.Want(ctx, cid)
					if err != nil {
						return out.Error("blob-want", err).Render()
					}

					targets := result.Targets.All()
					if len(targets) == 0 {
						return out.Result("blob-want", "No sources found").
							With("cid", cidHex).
							With("found", 0).
							Render()
					}

					// Output as table
					table := out.Table("blob-sources", "From", "Size", "Address")
					for _, t := range targets {
						table.AddRow(
							t.DisplayName(),
							fmt.Sprintf("%d", result.Size),
							t.Address,
						)
					}

					return table.Render()
				},
			})
		},
	}

	cmd.Flags().StringVar(&relayServer, "relay", "", "relay address (default localhost:50051 or ARC_RELAY)")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "how long to wait for responses")

	return cmd
}
