package node

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/gezibash/arc/pkg/reference"
	"github.com/spf13/cobra"
)

func newGetCmd(n *nodeCmd) *cobra.Command {
	var file string

	cmd := &cobra.Command{
		Use:   "get <reference>",
		Short: "Retrieve content or message by reference",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			input := args[0]

			// Fast path: full 64-char hex → direct blob fetch.
			if len(input) == 64 {
				ref, err := reference.FromHex(input)
				if err != nil {
					return fmt.Errorf("invalid reference: %w", err)
				}
				slog.DebugContext(cmd.Context(), "getting content", "ref", input)
				data, err := n.client.GetContent(cmd.Context(), ref)
				if err != nil {
					return fmt.Errorf("get content: %w", err)
				}
				if file != "" {
					return os.WriteFile(file, data, 0644)
				}
				_, err = os.Stdout.Write(data)
				return err
			}

			// Prefix path: resolve against blobs and messages.
			result, err := n.client.ResolveGet(cmd.Context(), input)
			if err != nil {
				return fmt.Errorf("resolve get: %w", err)
			}

			switch result.Kind {
			case client.GetKindBlob:
				slog.DebugContext(cmd.Context(), "resolved to blob", "ref", reference.Hex(result.Ref))
				if file != "" {
					return os.WriteFile(file, result.Data, 0644)
				}
				_, err = os.Stdout.Write(result.Data)
				return err

			case client.GetKindMessage:
				slog.DebugContext(cmd.Context(), "resolved to message", "ref", reference.Hex(result.Ref))
				fmt.Fprintf(os.Stdout, "ref:       %s\n", reference.Hex(result.Ref))
				fmt.Fprintf(os.Stdout, "timestamp: %d (%s)\n", result.Timestamp, time.Unix(0, result.Timestamp).UTC().Format(time.RFC3339))

				if len(result.Labels) > 0 {
					fmt.Fprintln(os.Stdout, "labels:")
					keys := make([]string, 0, len(result.Labels))
					for k := range result.Labels {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					for _, k := range keys {
						fmt.Fprintf(os.Stdout, "  %s: %s\n", k, result.Labels[k])
					}
				}
				return nil

			default:
				return fmt.Errorf("unknown result kind: %d", result.Kind)
			}
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "write to file instead of stdout")
	return cmd
}
