package node

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newFederateCmd(n *nodeCmd) *cobra.Command {
	var (
		labels      []string
		labelsAlias []string
		output      string
	)

	cmd := &cobra.Command{
		Use:   "federate <peer>",
		Short: "Start federation with a peer node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			peer := normalizePeerAddr(args[0])

			combinedLabels := append([]string{}, labels...)
			combinedLabels = append(combinedLabels, labelsAlias...)
			labelMap, err := parseLabels(combinedLabels)
			if err != nil {
				return err
			}

			result, err := n.client.Federate(cmd.Context(), peer, labelMap)
			if err != nil {
				return fmt.Errorf("federate: %w", err)
			}

			if output == "json" {
				return writeJSON(os.Stdout, result)
			}
			if result != nil {
				if result.Message != "" {
					_, _ = fmt.Fprintln(os.Stdout, result.Message)
					return nil
				}
				if result.Status != "" {
					_, _ = fmt.Fprintln(os.Stdout, result.Status)
					return nil
				}
			}
			_, _ = fmt.Fprintln(os.Stdout, "ok")
			return nil
		},
	}

	cmd.Flags().StringSliceVarP(&labels, "label", "l", nil, "label key=value (repeatable)")
	cmd.Flags().StringSliceVar(&labelsAlias, "labels", nil, "alias for --label")
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	return cmd
}

func normalizePeerAddr(raw string) string {
	if strings.Contains(raw, "://") {
		if parsed, err := url.Parse(raw); err == nil && parsed.Host != "" {
			return parsed.Host
		}
	}
	return raw
}
