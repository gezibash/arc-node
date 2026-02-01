package node

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/spf13/cobra"
)

func newPeersCmd(n *nodeCmd) *cobra.Command {
	var output string

	const pageSize = 20

	cmd := &cobra.Command{
		Use:       "peers [inbound|outbound]",
		Short:     "List active federation peers",
		Args:      cobra.MaximumNArgs(1),
		ValidArgs: []string{"inbound", "outbound"},
		RunE: func(cmd *cobra.Command, args []string) error {
			var filter string
			if len(args) == 1 {
				filter = args[0]
				if filter != "inbound" && filter != "outbound" {
					return fmt.Errorf("invalid direction %q: must be inbound or outbound", filter)
				}
			}

			peers, err := n.client.ListPeers(cmd.Context())
			if err != nil {
				return fmt.Errorf("list peers: %w", err)
			}

			if output == "json" {
				if filter != "" {
					var filtered []client.PeerInfo
					for _, p := range peers {
						if filter == "inbound" && p.Direction == client.PeerDirectionInbound {
							filtered = append(filtered, p)
						} else if filter == "outbound" && p.Direction != client.PeerDirectionInbound {
							filtered = append(filtered, p)
						}
					}
					return writeJSON(os.Stdout, filtered)
				}
				return writeJSON(os.Stdout, peers)
			}

			var filtered []client.PeerInfo
			for _, p := range peers {
				if filter == "inbound" && p.Direction != client.PeerDirectionInbound {
					continue
				}
				if filter == "outbound" && p.Direction == client.PeerDirectionInbound {
					continue
				}
				filtered = append(filtered, p)
			}

			if len(filtered) == 0 {
				fmt.Fprintln(os.Stdout, "no active peers")
				return nil
			}

			fmt.Fprintf(os.Stdout, "%-30s %-10s %-20s %10s %8s %8s %s\n",
				"PEER", "OUTBOUND", "LABELS", "RECEIVED", "SENT", "ENTRIES", "UPTIME")

			display := filtered
			truncated := 0
			if len(display) > pageSize {
				truncated = len(display) - pageSize
				display = display[:pageSize]
			}

			for _, p := range display {
				peer := p.Address
				outbound := "true"
				if p.Direction == client.PeerDirectionInbound {
					outbound = "false"
					peer = fmt.Sprintf("%x", p.PublicKey)
					if len(peer) > 16 {
						peer = peer[:16]
					}
				}
				fmt.Fprintf(os.Stdout, "%-30s %-10s %-20s %10s %8d %8d %s\n",
					peer,
					outbound,
					formatLabels(p.Labels),
					formatBytes(p.BytesReceived),
					p.EntriesSent,
					p.EntriesReplicated,
					formatDuration(time.Since(time.UnixMilli(p.StartedAt)).Truncate(time.Second)),
				)
			}

			if truncated > 0 {
				fmt.Fprintf(os.Stdout, "... and %d more (use -o json for full list)\n", truncated)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	return cmd
}

func formatLabels(labels map[string]string) string {
	var parts []string
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	slices.Sort(parts)
	return strings.Join(parts, " ")
}

func formatBytes(b int64) string {
	const (
		kB = 1000
		mB = 1000 * kB
		gB = 1000 * mB
	)
	switch {
	case b >= gB:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gB))
	case b >= mB:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mB))
	case b >= kB:
		return fmt.Sprintf("%.1f kB", float64(b)/float64(kB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
