package main

import (
	"fmt"
	"os"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/pkg/identity"
	arclabels "github.com/gezibash/arc/v2/pkg/labels"
	"github.com/gezibash/arc/v2/pkg/relay"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newDiscoverCmd(v *viper.Viper) *cobra.Command {
	var (
		capability  string
		labels      []string
		relayServer string
		limit       int
	)

	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Discover capability providers",
		Long: `Discover capability providers currently connected to the relay.

Examples:
  arc-relay discover --capability blob       # find blob providers
  arc-relay discover --labels topic=news     # find by labels
  arc-relay discover                          # list all subscriptions`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Build runtime with relay capability
			builder := cli.NewBuilder("discover", v).
				Use(cli.WithRelay(relayServer))
			signer, err := loadSigner(v, "discover")
			if err != nil {
				return fmt.Errorf("load signer: %w", err)
			}
			builder = builder.Signer(signer)
			rt, err := builder.Build()
			if err != nil {
				return fmt.Errorf("init: %w", err)
			}
			defer func() { _ = rt.Close() }()

			// Build filter
			filter, err := arclabels.Parse(labels)
			if err != nil {
				return err
			}
			if capability != "" {
				filter["capability"] = capability
			}

			// Get relay client and discover
			c := relay.From(rt)
			if c == nil {
				return fmt.Errorf("relay not connected")
			}

			result, err := c.DiscoverWithLimit(rt.Context(), filter, limit)
			if err != nil {
				return fmt.Errorf("discover: %w", err)
			}

			// Output results
			out := cli.NewOutputFromViper(v)
			table := out.Table("discover", "Pubkey", "Name", "Labels", "Subscription")

			for _, p := range result.Providers {
				pubkeyStr := identity.EncodePublicKey(p.Pubkey)
				if len(pubkeyStr) > 24 {
					pubkeyStr = pubkeyStr[:24] + "..."
				}

				name := p.Name
				if name == "" {
					name = p.Petname
				}
				if name == "" {
					name = "-"
				}

				labelsStr := arclabels.Format(p.Labels)

				table.AddRow(pubkeyStr, name, labelsStr, p.SubscriptionID)
			}

			table.WithPagination("", result.HasMore)

			if err := table.Render(); err != nil {
				return err
			}

			// Show total if there are more results
			if result.HasMore {
				fmt.Fprintf(os.Stderr, "\nShowing %d of %d results (use --limit to see more)\n",
					len(result.Providers), result.Total)
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&capability, "capability", "", "capability to discover (e.g., blob, index)")
	cmd.Flags().StringSliceVarP(&labels, "labels", "l", nil, "filter labels (key=value, can repeat)")
	cmd.Flags().StringVar(&relayServer, "relay", "", "relay address (default localhost:50051, or ARC_RELAY env)")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum number of results")

	return cmd
}
