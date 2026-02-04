package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/internal/names"
	"github.com/gezibash/arc/v2/pkg/identity"
	arclabels "github.com/gezibash/arc/v2/pkg/labels"
	"github.com/gezibash/arc/v2/pkg/relay"
	"github.com/gezibash/arc/v2/pkg/transport"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newDiscoverCmd(v *viper.Viper) *cobra.Command {
	var (
		capability  string
		labels      []string
		relayServer string
		limit       int
		verbose     bool
	)

	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Discover capability providers",
		Long: `Discover capability providers currently connected to the relay.

Examples:
  arc-relay discover --capability blob       # find blob providers
  arc-relay discover --labels topic=news     # find by labels
  arc-relay discover                          # list all subscriptions
  arc-relay discover --verbose               # show per-hop latency breakdown`,
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

			// Client → relay latency (hop 1)
			clientLatency := c.Latency()

			// Output results
			out := cli.NewOutputFromViper(v)
			table := out.Table("discover", "Pubkey", "Name", "Labels", "Relay", "Latency")

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
				relayName := names.Petname(p.RelayPubkey.Bytes)
				if p.InterRelayLatency == 0 {
					relayName += " (local)"
				}
				latencyStr := formatLatency(clientLatency, p, verbose)

				table.AddRow(pubkeyStr, name, labelsStr, relayName, latencyStr)
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
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "show per-hop latency breakdown")

	return cmd
}

// formatLatency formats the total end-to-end latency.
// In verbose mode, shows per-hop breakdown: "1.2ms (0.3+0.5+0.4)".
func formatLatency(clientLatency time.Duration, p transport.Provider, verbose bool) string {
	hop1 := clientLatency       // client → local relay
	hop2 := p.InterRelayLatency // local relay → remote relay (0 for local)
	hop3 := p.Latency           // relay → capability

	total := hop1 + hop2 + hop3
	if total == 0 {
		return "-"
	}

	if verbose {
		var hops []string
		for _, h := range []time.Duration{hop1, hop2, hop3} {
			if h > 0 {
				hops = append(hops, formatDuration(h))
			}
		}
		return fmt.Sprintf("%s (%s)", formatDuration(total), strings.Join(hops, "+"))
	}
	return formatDuration(total)
}

// formatDuration formats a duration for human display.
func formatDuration(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		us := float64(d.Nanoseconds()) / 1000
		return fmt.Sprintf("%.0fus", us)
	case d < time.Second:
		ms := float64(d.Nanoseconds()) / 1e6
		if ms < 10 {
			return fmt.Sprintf("%.1fms", ms)
		}
		return fmt.Sprintf("%.0fms", ms)
	default:
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
}
