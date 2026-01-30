package node

import (
	"fmt"
	"time"

	"github.com/gezibash/arc-node/pkg/client"
	"github.com/spf13/cobra"
)

func newStatusCmd(n *nodeCmd) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Check if the node is reachable",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			_, err := n.client.QueryMessages(cmd.Context(), &client.QueryOptions{
				Expression: "true",
				Limit:      1,
			})
			elapsed := time.Since(start)
			if err != nil {
				fmt.Printf("unreachable (%s)\n", err)
				return err
			}
			fmt.Printf("online (%s)\n", elapsed.Round(time.Millisecond))
			return nil
		},
	}
}
