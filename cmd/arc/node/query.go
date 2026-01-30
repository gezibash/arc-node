package node

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/spf13/cobra"
)

func newQueryCmd(n *nodeCmd) *cobra.Command {
	var (
		limit      int
		cursor     string
		descending bool
		labels     []string
		output     string
		preview    bool
	)

	cmd := &cobra.Command{
		Use:   "query [expression]",
		Short: "Query indexed messages",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			expr := "true"
			if len(args) == 1 {
				expr = args[0]
			}

			labelMap, err := parseLabels(labels)
			if err != nil {
				return err
			}

			slog.DebugContext(cmd.Context(), "querying messages", "expression", expr, "labels", labelMap, "limit", limit)
			result, err := n.client.QueryMessages(cmd.Context(), &client.QueryOptions{
				Expression: expr,
				Labels:     labelMap,
				Limit:      limit,
				Cursor:     cursor,
				Descending: descending,
			})
			if err != nil {
				return fmt.Errorf("query: %w", err)
			}

			var loader ContentLoader
			if preview {
				loader = n.client.GetContent
			}
			if output == "text" && len(result.Entries) > 0 {
				formatTextHeader(os.Stdout)
			}
			for _, e := range result.Entries {
				if err := formatEntry(os.Stdout, e, output, preview, cmd.Context(), loader); err != nil {
					return err
				}
			}

			if result.HasMore {
				fmt.Fprintf(os.Stderr, "next cursor: %s\n", result.NextCursor)
			}
			return nil
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 10, "max entries to return")
	cmd.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	cmd.Flags().BoolVar(&descending, "descending", true, "reverse chronological order")
	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	cmd.Flags().BoolVar(&preview, "preview", false, "show truncated content preview")
	return cmd
}
