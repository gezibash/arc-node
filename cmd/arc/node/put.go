package node

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/gezibash/arc/v2/pkg/reference"
	"github.com/spf13/cobra"
)

func newPutCmd(n *nodeCmd) *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "put [file]",
		Short: "Store content and print its reference",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := readInput(args)
			if err != nil {
				return err
			}

			slog.DebugContext(cmd.Context(), "putting content", "data_len", len(data))
			ref, err := n.client.PutContent(cmd.Context(), data)
			if err != nil {
				return fmt.Errorf("put content: %w", err)
			}
			slog.DebugContext(cmd.Context(), "put content complete", "ref", reference.Hex(ref))
			if output == "json" {
				return writeJSON(os.Stdout, map[string]string{"reference": reference.Hex(ref)})
			}
			fmt.Println(reference.Hex(ref))
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "text", "output format: text or json")
	return cmd
}
