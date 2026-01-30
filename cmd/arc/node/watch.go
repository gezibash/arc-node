package node

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

func newWatchCmd(n *nodeCmd) *cobra.Command {
	var (
		labels  []string
		output  string
		preview bool
	)

	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Stream indexed messages in real time",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			expr := "true"

			labelMap, err := parseLabels(labels)
			if err != nil {
				return err
			}

			slog.DebugContext(cmd.Context(), "subscribing to messages", "expression", expr, "labels", labelMap)
			entries, errs, err := n.client.SubscribeMessages(cmd.Context(), expr, labelMap)
			if err != nil {
				return fmt.Errorf("subscribe: %w", err)
			}

			var loader ContentLoader
			if preview {
				loader = n.client.GetContent
			}
			if output == "text" {
				formatTextHeader(os.Stdout)
			}

			for {
				select {
				case e, ok := <-entries:
					if !ok {
						return nil
					}
					if err := formatEntry(os.Stdout, e, output, preview, cmd.Context(), loader); err != nil {
						return err
					}
				case err := <-errs:
					if err != nil {
						return err
					}
					return nil
				}
			}
		},
	}

	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	cmd.Flags().BoolVar(&preview, "preview", false, "show truncated content preview")
	return cmd
}
