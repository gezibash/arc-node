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

func newLeaveCmd() *cobra.Command {
	var relayAddr string

	cmd := &cobra.Command{
		Use:   "leave",
		Short: "Gracefully leave the gossip cluster",
		Long: `Gracefully leave the gossip cluster. The relay keeps running for local clients.

Examples:
  arc-relay leave
  arc-relay leave --relay :50052`,
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
			_, err = client.GossipLeave(ctx, &relayv1.GossipLeaveRequest{})
			if err != nil {
				return fmt.Errorf("leave: %w", err)
			}

			fmt.Println("left gossip cluster")
			return nil
		},
	}

	cmd.Flags().StringVar(&relayAddr, "relay", "", "relay gRPC address (default localhost:50051)")
	return cmd
}
