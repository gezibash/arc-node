package main

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/pkg/identity"
	arclabels "github.com/gezibash/arc/v2/pkg/labels"
	"github.com/gezibash/arc/v2/pkg/relay"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newListenCmd(v *viper.Viper) *cobra.Command {
	var (
		labels      []string
		name        string
		relayServer string
		raw         bool
	)

	cmd := &cobra.Command{
		Use:   "listen",
		Short: "Subscribe and receive envelopes from the relay",
		Long: `Connect to the relay and receive envelopes matching your subscription.

Outputs received envelopes as JSON (one per line).

Examples:
  arc-relay listen                              # listen for all (match-all subscription)
  arc-relay listen --labels topic=news          # filter by labels
  arc-relay listen --labels capability=blob     # subscribe as capability provider
  arc-relay listen --name alice                 # register name, receive addressed messages
  arc-relay listen --relay localhost:50051      # specify relay`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Build runtime with relay capability
			builder := cli.NewBuilder("listen", v).
				Use(cli.WithRelay(relayServer))
			signer, err := loadSigner(v, "listen")
			if err != nil {
				return fmt.Errorf("load signer: %w", err)
			}
			builder = builder.Signer(signer)
			rt, err := builder.Build()
			if err != nil {
				return fmt.Errorf("init: %w", err)
			}
			defer func() { _ = rt.Close() }()

			// Get relay client
			c := relay.From(rt)
			if c == nil {
				return fmt.Errorf("relay not connected")
			}

			// Build labels (typed — supports int64, float64, bool inference)
			subLabels, err := arclabels.ParseTyped(labels)
			if err != nil {
				return err
			}

			// Register name if specified
			if name != "" {
				if err := c.RegisterName(name); err != nil {
					return fmt.Errorf("register name: %w", err)
				}
			}

			// Subscribe
			subID := uuid.New().String()
			if err := c.Subscribe(subID, subLabels); err != nil {
				return fmt.Errorf("subscribe: %w", err)
			}

			out := cli.NewOutputFromViper(v)
			ctx := rt.Context()

			// Receive loop - context cancelled on SIGINT/SIGTERM via runtime
			for {
				delivery, err := c.Receive(ctx)
				if err != nil {
					if ctx.Err() != nil {
						// Graceful shutdown
						return nil
					}
					return fmt.Errorf("receive: %w", err)
				}

				// Build payload string
				var payload string
				if raw {
					payload = base64.StdEncoding.EncodeToString(delivery.Payload)
				} else if isText(delivery.Payload) {
					payload = string(delivery.Payload)
				} else {
					payload = base64.StdEncoding.EncodeToString(delivery.Payload)
				}

				// Output envelope
				kv := out.KV("envelope").
					Set("from", identity.EncodePublicKey(delivery.Sender)).
					Set("payload", payload)

				// Add labels
				for k, val := range delivery.Labels {
					kv.Set("label:"+k, val)
				}

				if err := kv.Render(); err != nil {
					return fmt.Errorf("render: %w", err)
				}

				// Print separator for text output
				if out.Format() == cli.FormatText {
					_, _ = fmt.Fprintln(os.Stdout, "---")
				}
			}
		},
	}

	cmd.Flags().StringSliceVarP(&labels, "labels", "l", nil, "filter labels (key=value, can repeat)")
	cmd.Flags().StringVar(&name, "name", "", "register addressed name (without @ prefix)")
	cmd.Flags().StringVar(&relayServer, "relay", "", "relay address (default localhost:50051, or ARC_RELAY env)")
	cmd.Flags().BoolVar(&raw, "raw", false, "always output payload as base64")

	return cmd
}

// isText returns true if the data looks like UTF-8 text.
func isText(data []byte) bool {
	for _, b := range data {
		// Allow printable ASCII and common whitespace
		if b < 0x20 && b != '\t' && b != '\n' && b != '\r' {
			return false
		}
		if b == 0x7f {
			return false
		}
	}
	return true
}
