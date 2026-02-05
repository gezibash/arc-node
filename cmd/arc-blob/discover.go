package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/internal/names"
	"github.com/gezibash/arc/v2/pkg/blob"
	"github.com/gezibash/arc/v2/pkg/capability"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newDiscoverCmd(v *viper.Viper) *cobra.Command {
	var (
		relayServer string
		backend     string
		region      string
		expr        string
		verbose     bool
		timeout     time.Duration
	)

	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Discover blob storage providers",
		Long: `Discover blob storage providers on the network.

Shows an aggregated view of blob storage capacity, usage, and latency
across all connected relays.

Examples:
  arc-blob discover                                    # list all blob providers
  arc-blob discover --backend file                     # filter by backend
  arc-blob discover --relay localhost:50051             # specific relay
  arc-blob discover --expr 'capacity > 1000000000'     # CEL expression filter
  arc-blob discover --verbose                          # show per-hop latency`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cli.RunCommand(cli.CommandConfig{
				Name:    "blob-discover",
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

					var ts *capability.TargetSet
					var err error

					if expr != "" {
						// CEL expression-based discovery — prepend capability filter
						fullExpr := fmt.Sprintf(`capability == "blob" && %s`, expr)
						ts, err = c.CapabilityClient().Discover(ctx, capability.DiscoverConfig{
							Capability: blob.CapabilityName,
							Expression: fullExpr,
						})
					} else {
						ts, err = c.Discover(ctx, blob.DiscoverFilter{
							Backend: blob.Backend(backend),
							Region:  region,
						})
					}
					if err != nil {
						return out.Error("blob-discover", err).Render()
					}

					table := out.Table("blob-discover", "Relay", "Backend", "Capacity", "Free", "Blobs", "Latency")

					for _, t := range ts.All() {
						relayName := names.Petname(t.RelayPubkey.Bytes)
						if t.InterRelayLatency == 0 {
							relayName += " (local)"
						}

						backendStr := stringFromAny(t.Labels, blob.LabelBackend)
						capacity := int64FromAny(t.Labels, blob.LabelCapacity)
						usedBytes := int64FromAny(t.State, blob.StateUsedBytes)
						blobCount := int64FromAny(t.State, blob.StateBlobCount)

						capacityStr := "-"
						freeStr := "-"
						if capacity > 0 {
							capacityStr = humanize.IBytes(uint64(capacity))
							free := capacity - usedBytes
							if free < 0 {
								free = 0
							}
							freeStr = humanize.IBytes(uint64(free))
						}

						blobsStr := fmt.Sprintf("%d", blobCount)
						latencyStr := formatLatency(t, verbose)

						table.AddRow(relayName, backendStr, capacityStr, freeStr, blobsStr, latencyStr)
					}

					if ts.IsEmpty() {
						_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "No blob providers found.")
						return nil
					}

					return table.Render()
				},
			})
		},
	}

	cmd.Flags().StringVar(&relayServer, "relay", "", "relay address (default localhost:50051 or ARC_RELAY)")
	cmd.Flags().StringVar(&backend, "backend", "", "filter by backend (file, badger, s3, memory)")
	cmd.Flags().StringVar(&region, "region", "", "filter by region")
	cmd.Flags().StringVar(&expr, "expr", "", "CEL expression filter")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "show per-hop latency breakdown")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "timeout")

	return cmd
}

func int64FromAny(m map[string]any, key string) int64 {
	if m == nil {
		return 0
	}
	if v, ok := m[key].(int64); ok {
		return v
	}
	return 0
}

func stringFromAny(m map[string]any, key string) string {
	if m == nil {
		return "-"
	}
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return "-"
}

// formatLatency formats the end-to-end latency for a target.
// In verbose mode, shows per-hop breakdown: "1.5ms (0.5+1.0)".
func formatLatency(t capability.Target, verbose bool) string {
	hop1 := t.InterRelayLatency // local relay → remote relay (0 for local)
	hop2 := t.Latency           // relay → capability

	total := hop1 + hop2
	if total == 0 {
		return "-"
	}

	if verbose && hop1 > 0 {
		return fmt.Sprintf("%s (%s+%s)", fmtDuration(total), fmtDuration(hop1), fmtDuration(hop2))
	}
	return fmtDuration(total)
}

func fmtDuration(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.0fus", float64(d.Nanoseconds())/1000)
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
