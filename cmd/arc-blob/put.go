package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/pkg/blob"
	"github.com/gezibash/arc/v2/pkg/capability"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newPutCmd(v *viper.Viper) *cobra.Command {
	var relayServer string
	var timeout time.Duration
	var direct bool
	var toTarget string

	cmd := &cobra.Command{
		Use:   "put <file>",
		Short: "Store a blob",
		Long: `Store a blob in the blob capability.

The client discovers available blob targets and selects the best one.
Use --to to explicitly target a specific store.

Small blobs (<1MB) are sent inline through the relay.
Large blobs are uploaded directly after receiving a redirect.

Use --direct to force direct gRPC upload for any size blob,
bypassing relay for data transfer (better performance).

Examples:
  arc-blob put myfile.txt
  arc-blob put --direct myfile.txt              # force direct upload
  arc-blob put --to clever-penguin myfile.txt   # send to specific target
  arc-blob put --relay localhost:50051 large.zip
  cat data | arc-blob put -`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Read file first (before building runtime)
			filename := args[0]
			var data []byte
			var err error
			if filename == "-" {
				data, err = io.ReadAll(os.Stdin)
			} else {
				data, err = os.ReadFile(filename) //nolint:gosec // G304: intentional CLI file read
			}
			if err != nil {
				return fmt.Errorf("read file: %w", err)
			}

			return cli.RunCommand(cli.CommandConfig{
				Name:    "blob-put",
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

					opts := blob.PutOptions{Direct: direct}

					if toTarget != "" {
						target, err := capability.ResolveTarget(ctx, toTarget, func(ctx context.Context) (*capability.TargetSet, error) {
							return c.Discover(ctx, blob.DiscoverFilter{Direct: direct})
						})
						if err != nil {
							return out.Error("blob-put", err).Render()
						}
						opts.Target = target
					}

					result, err := c.PutWithOptions(ctx, data, opts)
					if err != nil {
						return out.Error("blob-put", err).Render()
					}

					return out.KV("blob-stored").
						Set("status", "STORED").
						Set("cid", hex.EncodeToString(result.CID[:])).
						Set("size", result.Size).
						Set("target", result.Target).
						Set("direct", result.Direct).
						Render()
				},
			})
		},
	}

	cmd.Flags().StringVar(&relayServer, "relay", "", "relay address (default localhost:50051 or ARC_RELAY)")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "timeout")
	cmd.Flags().BoolVar(&direct, "direct", false, "force direct gRPC upload (bypass relay for data)")
	cmd.Flags().StringVar(&toTarget, "to", "", "target (name, petname, or pubkey hex)")

	return cmd
}
