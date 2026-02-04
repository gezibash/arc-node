package main

import (
	"context"
	"fmt"
	"os"
	"time"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newMembersCmd() *cobra.Command {
	var relayAddr string

	cmd := &cobra.Command{
		Use:   "members",
		Short: "List gossip cluster members",
		Long: `Show all relays in the gossip cluster.

Examples:
  arc-relay members
  arc-relay members --relay :50052`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if relayAddr == "" {
				relayAddr = "localhost:50051"
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			conn, err := grpc.NewClient(relayAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				return fmt.Errorf("connect: %w", err)
			}
			defer func() { _ = conn.Close() }()

			client := relayv1.NewRelayServiceClient(conn)
			resp, err := client.GossipMembers(ctx, &relayv1.GossipMembersRequest{})
			if err != nil {
				return fmt.Errorf("members: %w", err)
			}

			t := table.NewWriter()
			t.SetOutputMirror(os.Stdout)
			t.AppendHeader(table.Row{"Name", "Addr", "Status", "gRPC Addr", "Connections", "Uptime"})

			for _, m := range resp.GetMembers() {
				uptime := time.Duration(m.GetUptimeNs()).Truncate(time.Second)
				t.AppendRow(table.Row{
					m.GetName(),
					m.GetAddr(),
					m.GetStatus(),
					m.GetGrpcAddr(),
					m.GetConnections(),
					uptime,
				})
			}

			t.Render()
			return nil
		},
	}

	cmd.Flags().StringVar(&relayAddr, "relay", "", "relay gRPC address (default localhost:50051)")
	return cmd
}
