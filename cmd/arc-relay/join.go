package main

import (
	"context"
	"fmt"
	"time"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newJoinCmd() *cobra.Command {
	var relayAddr string

	cmd := &cobra.Command{
		Use:   "join [peers...]",
		Short: "Join gossip peers at runtime",
		Long: `Join one or more gossip peers to the running relay's cluster.

Examples:
  arc-relay join localhost:7946
  arc-relay join relay-b:7946 relay-c:7946
  arc-relay join --relay :50052 localhost:7946`,
		Args: cobra.MinimumNArgs(1),
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
			resp, err := client.GossipJoin(ctx, &relayv1.GossipJoinRequest{
				Peers: args,
			})
			if err != nil {
				return fmt.Errorf("join: %w", err)
			}

			fmt.Printf("joined %d peer(s)\n", resp.GetJoined())
			return nil
		},
	}

	cmd.Flags().StringVar(&relayAddr, "relay", "", "relay gRPC address (default localhost:50051)")
	return cmd
}
