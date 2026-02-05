package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
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
	var batch bool
	var replicas int
	var minAcks int

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

Use --batch to read lines from stdin, storing each line as a separate
blob over a single connection (much faster for bulk loads).

Use --replicas N to replicate to N targets. Use --min-acks M to succeed
after M acknowledgments (defaults to N). Targets are selected by most
free capacity.

Examples:
  arc-blob put myfile.txt
  arc-blob put --direct myfile.txt              # force direct upload
  arc-blob put --to clever-penguin myfile.txt   # send to specific target
  arc-blob put --relay localhost:50051 large.zip
  arc-blob put --replicas 3 --min-acks 2 -      # replicate, 2-of-3
  cat data | arc-blob put -
  printf '%s\n' f-{1..2000} | arc-blob put --batch`,
		Args: func(cmd *cobra.Command, args []string) error {
			if batch {
				return cobra.MaximumNArgs(0)(cmd, args)
			}
			return cobra.ExactArgs(1)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if batch {
				return runBatchPut(cmd, v, relayServer, timeout, direct, toTarget, replicas, minAcks)
			}

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

					// Replicated put
					if replicas > 0 {
						return runReplicatedPut(ctx, c, out, data, direct, toTarget, replicas, minAcks)
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
	cmd.Flags().BoolVar(&batch, "batch", false, "read lines from stdin, each line is a separate blob")
	cmd.Flags().IntVar(&replicas, "replicas", 0, "replicate to N targets (0 = single target)")
	cmd.Flags().IntVar(&minAcks, "min-acks", 0, "minimum acks for success (default = replicas)")

	return cmd
}

// runReplicatedPut stores a blob to N targets and reports results.
func runReplicatedPut(ctx context.Context, c *blob.Client, out *cli.Output, data []byte, direct bool, toTarget string, replicas, minAcks int) error {
	if minAcks <= 0 {
		minAcks = replicas
	}

	rOpts := blob.ReplicatedPutOptions{
		Replicas: replicas,
		MinAcks:  minAcks,
		Direct:   direct,
	}

	// If --to specified, discover and filter to the resolved target's set
	if toTarget != "" {
		ts, err := c.Discover(ctx, blob.DiscoverFilter{Direct: direct})
		if err != nil {
			return out.Error("blob-put", err).Render()
		}
		rOpts.Targets = ts
	}

	rr, err := c.PutReplicated(ctx, data, rOpts)
	if err != nil && rr == nil {
		return out.Error("blob-put", err).Render()
	}

	// Build per-target summary
	var targetSummaries []string
	for _, tr := range rr.Results {
		if tr.Err != nil {
			targetSummaries = append(targetSummaries, fmt.Sprintf("%s (error: %v)", tr.Target, tr.Err))
		} else {
			targetSummaries = append(targetSummaries, fmt.Sprintf("%s (ok)", tr.Target))
		}
	}

	status := "REPLICATED"
	if err != nil {
		status = "PARTIAL"
	}

	return out.KV("blob-replicated").
		Set("status", status).
		Set("cid", hex.EncodeToString(rr.CID[:])).
		Set("size", rr.Size).
		Set("acked", fmt.Sprintf("%d/%d", rr.Acked, len(rr.Results))).
		Set("targets", strings.Join(targetSummaries, ", ")).
		Render()
}

// runBatchPut reads lines from stdin and stores each as a blob over a single connection.
func runBatchPut(cmd *cobra.Command, v *viper.Viper, relayServer string, timeout time.Duration, direct bool, toTarget string, replicas, minAcks int) error {
	return cli.RunCommand(cli.CommandConfig{
		Name:    "blob-put-batch",
		KeyName: "blob",
		Viper:   v,
		Extensions: []runtime.Extension{
			cli.WithRelay(relayServer),
			blob.Capability(blob.Config{}),
		},
		Run: func(ctx context.Context, rt *runtime.Runtime, out *cli.Output) error {
			c := blob.From(rt)
			if c == nil {
				return fmt.Errorf("blob capability not available")
			}

			// Replicated batch mode
			if replicas > 0 {
				return runBatchReplicatedPut(cmd, ctx, c, timeout, direct, replicas, minAcks)
			}

			// Resolve target once
			opts := blob.PutOptions{Direct: direct}
			if toTarget != "" {
				target, err := capability.ResolveTarget(ctx, toTarget, func(ctx context.Context) (*capability.TargetSet, error) {
					return c.Discover(ctx, blob.DiscoverFilter{Direct: direct})
				})
				if err != nil {
					return fmt.Errorf("resolve target: %w", err)
				}
				opts.Target = target
			} else {
				// Discover once, pin target for entire batch
				ts, err := c.Discover(ctx, blob.DiscoverFilter{Direct: direct})
				if err != nil {
					return fmt.Errorf("discover: %w", err)
				}
				if ts.IsEmpty() {
					return fmt.Errorf("no blob targets available")
				}
				opts.Target = ts.Sort(blob.ByFreeCapacity).First()
			}

			start := time.Now()
			var stored, errors int

			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				data := scanner.Bytes()
				if len(data) == 0 {
					continue
				}

				putCtx := ctx
				if timeout > 0 {
					var cancel context.CancelFunc
					putCtx, cancel = context.WithTimeout(ctx, timeout)
					defer cancel() //nolint:gocritic // defers in loop are fine here, bounded by stdin
				}

				result, err := c.PutWithOptions(putCtx, data, opts)
				if err != nil {
					errors++
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", err)
					continue
				}
				stored++
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s %d\n",
					hex.EncodeToString(result.CID[:]), result.Size)
			}
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read stdin: %w", err)
			}

			elapsed := time.Since(start)
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\n%d stored, %d errors in %s\n", stored, errors, elapsed.Round(time.Millisecond))
			return nil
		},
	})
}

// runBatchReplicatedPut reads lines from stdin and replicates each to N targets.
func runBatchReplicatedPut(cmd *cobra.Command, ctx context.Context, c *blob.Client, timeout time.Duration, direct bool, replicas, minAcks int) error {
	if minAcks <= 0 {
		minAcks = replicas
	}

	// Discover once, pass target set to each put
	ts, err := c.Discover(ctx, blob.DiscoverFilter{Direct: direct})
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}

	start := time.Now()
	var stored, errors int

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		data := scanner.Bytes()
		if len(data) == 0 {
			continue
		}

		putCtx := ctx
		if timeout > 0 {
			var cancel context.CancelFunc
			putCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel() //nolint:gocritic // defers in loop are fine here, bounded by stdin
		}

		rr, err := c.PutReplicated(putCtx, data, blob.ReplicatedPutOptions{
			Replicas: replicas,
			MinAcks:  minAcks,
			Direct:   direct,
			Targets:  ts,
		})
		if err != nil && rr == nil {
			errors++
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", err)
			continue
		}
		if err != nil {
			// Partial success
			errors++
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s %d acked=%d/%d (partial)\n",
				hex.EncodeToString(rr.CID[:]), rr.Size, rr.Acked, replicas)
			continue
		}
		stored++
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s %d acked=%d/%d\n",
			hex.EncodeToString(rr.CID[:]), rr.Size, rr.Acked, replicas)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}

	elapsed := time.Since(start)
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "\n%d stored, %d errors in %s\n", stored, errors, elapsed.Round(time.Millisecond))
	return nil
}
