package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/pkg/blob"
	"github.com/gezibash/arc/v2/pkg/capability"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newGetCmd(v *viper.Viper) *cobra.Command {
	var relayServer string
	var outputFile string
	var timeout time.Duration
	var fromTarget string
	var direct bool

	cmd := &cobra.Command{
		Use:   "get <cid>",
		Short: "Retrieve a blob",
		Long: `Retrieve a blob by its content ID (CID).

The client discovers available blob targets and selects the best one.
Use --from to explicitly retrieve from a specific target.

Small blobs are returned inline through the relay.
Large blobs are downloaded directly after receiving a redirect.

Use --direct to force direct gRPC download, bypassing relay.

Examples:
  arc-blob get abc123...                           # print to stdout
  arc-blob get abc123... -f myfile.txt             # save to file
  arc-blob get abc123... --from clever-penguin     # get from specific target
  arc-blob get abc123... --direct                  # force direct download
  arc-blob get abc123... > myfile.txt              # redirect to file`,
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
				Name:    "blob-get",
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

					opts := blob.GetOptions{Direct: direct}

					if fromTarget != "" {
						target, err := capability.ResolveTarget(ctx, fromTarget, func(ctx context.Context) (*capability.TargetSet, error) {
							return c.Discover(ctx, blob.DiscoverFilter{Direct: direct})
						})
						if err != nil {
							return out.Error("blob-get", err).Render()
						}
						opts.Target = target
					}

					result, err := c.GetWithOptions(ctx, cid, opts)
					if err != nil {
						return out.Error("blob-get", err).Render()
					}

					// Write output
					if outputFile != "" {
						if err := os.WriteFile(outputFile, result.Data, 0600); err != nil {
							return fmt.Errorf("write file: %w", err)
						}
						return out.KV("blob-retrieved").
							Set("status", "OK").
							Set("cid", cidHex).
							Set("size", len(result.Data)).
							Set("output", outputFile).
							Set("target", result.Target).
							Set("direct", result.Direct).
							Render()
					}

					// Raw data to stdout (no formatting for binary data)
					_, err = os.Stdout.Write(result.Data)
					return err
				},
			})
		},
	}

	cmd.Flags().StringVar(&relayServer, "relay", "", "relay address (default localhost:50051 or ARC_RELAY)")
	cmd.Flags().StringVarP(&outputFile, "file", "f", "", "output file (default: stdout)")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "timeout")
	cmd.Flags().StringVar(&fromTarget, "from", "", "target to get from (name, petname, or pubkey hex)")
	cmd.Flags().BoolVar(&direct, "direct", false, "force direct gRPC download (bypass relay)")

	return cmd
}
