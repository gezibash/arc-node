package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/internal/names"
	arclabels "github.com/gezibash/arc/v2/pkg/labels"
	"github.com/gezibash/arc/v2/pkg/relay"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newSendCmd(v *viper.Viper) *cobra.Command {
	var (
		to          string
		labels      []string
		capability  string
		relayServer string
		timeout     time.Duration
	)

	cmd := &cobra.Command{
		Use:   "send [data]",
		Short: "Send an envelope through the relay",
		Long: `Send an envelope to the relay. Data can be provided as an argument or via stdin.

Examples:
  arc-relay send "hello world"                    # send to default relay
  arc-relay send --to @alice "hello"              # addressed send
  arc-relay send --labels topic=news "breaking"   # label-match send
  arc-relay send --capability blob <file          # capability send
  arc-relay send --relay localhost:50051 "test"   # specify relay`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Build runtime with relay capability
			builder := cli.NewBuilder("send", v).
				Use(cli.WithRelay(relayServer))
			signer, err := loadSigner(v, "send")
			if err != nil {
				return fmt.Errorf("load signer: %w", err)
			}
			builder = builder.Signer(signer)
			rt, err := builder.Build()
			if err != nil {
				return fmt.Errorf("init: %w", err)
			}
			defer func() { _ = rt.Close() }()

			// Create timeout context for the operation
			ctx := rt.Context()
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			// Read data
			var data []byte
			if len(args) > 0 {
				data = []byte(args[0])
			} else {
				data, err = io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("read stdin: %w", err)
				}
			}

			// Build labels
			envLabels, err := arclabels.Parse(labels)
			if err != nil {
				return err
			}

			// Add special routing labels
			if to != "" {
				// Try local names first
				ns := names.New(rt.DataDir())
				if err := ns.Load(); err == nil {
					resolved, fromNames := ns.ResolveRecipient(to)
					if fromNames {
						fmt.Fprintf(os.Stderr, "Resolved @%s from names\n", strings.TrimPrefix(to, "@"))
					}
					envLabels["to"] = resolved
				} else {
					// Names load failed, use raw value
					envLabels["to"] = strings.TrimPrefix(to, "@")
				}
			}
			if capability != "" {
				envLabels["capability"] = capability
			}

			// Get relay client and send
			c := relay.From(rt)
			if c == nil {
				return fmt.Errorf("relay not connected")
			}

			env := &relay.Envelope{
				Labels:      envLabels,
				Payload:     data,
				Correlation: uuid.New().String(),
			}

			receipt, err := c.Send(ctx, env)
			if err != nil {
				return fmt.Errorf("send: %w", err)
			}

			// Output receipt
			out := cli.NewOutputFromViper(v)
			kv := out.KV("send-receipt").
				Set("ref", hex.EncodeToString(receipt.Ref)).
				Set("status", receipt.Status)

			if receipt.Delivered > 0 {
				kv.Set("delivered", receipt.Delivered)
			}
			if receipt.Reason != "" {
				kv.Set("reason", receipt.Reason)
			}

			return kv.Render()
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "recipient name (@name) or public key (hex)")
	cmd.Flags().StringSliceVarP(&labels, "labels", "l", nil, "labels (key=value, can repeat)")
	cmd.Flags().StringVar(&capability, "capability", "", "target capability")
	cmd.Flags().StringVar(&relayServer, "relay", "", "relay address (default localhost:50051, or ARC_RELAY env)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "send timeout")

	return cmd
}
