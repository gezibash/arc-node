package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"

	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
	"github.com/gezibash/arc/v2/internal/config"
	internalgossip "github.com/gezibash/arc/v2/internal/gossip"
	"github.com/gezibash/arc/v2/internal/keyring"
	internalrelay "github.com/gezibash/arc/v2/internal/relay"
	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func newStartCmd() *cobra.Command {
	v := viper.New()
	var bufferSize int

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the relay server",
		Long: `Start the relay server for routing messages.

The relay is a stateless message router that connects clients and capabilities.
It routes envelopes by exact-match on labels.

Examples:
  arc-relay start                           # default settings
  arc-relay start --addr :50052             # custom port
  arc-relay start --log-level debug         # debug logging
  arc-relay start --gossip-bind :7946       # enable gossip
  arc-relay start --gossip-join host:7946   # join cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load config
			configFile, _ := cmd.Flags().GetString("config")
			cfg, err := loadConfig(v, configFile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			// Parse --gossip-bind flag (host:port) into config fields
			if gossipBind, _ := cmd.Flags().GetString("gossip-bind"); gossipBind != "" {
				host, portStr, splitErr := net.SplitHostPort(gossipBind)
				if splitErr != nil {
					// Assume it's just a port like ":7946"
					host = ""
					portStr = strings.TrimPrefix(gossipBind, ":")
				}
				if host != "" {
					cfg.Gossip.BindAddr = host
				}
				if port, e := strconv.Atoi(portStr); e == nil {
					cfg.Gossip.BindPort = port
				}
			}

			// Determine key name
			keyName := cfg.KeyName
			if keyName == "" {
				keyName = Defaults.KeyName
			}

			dataDir := cfg.DataDir
			if dataDir == "" {
				dataDir = config.DefaultDataDir()
			}

			kr := keyring.New(dataDir)
			signer, err := kr.LoadSigner(context.Background(), keyName, cfg.KeyPath)
			if err != nil {
				return fmt.Errorf("load signer: %w", err)
			}

			// Build runtime
			builder := runtime.New("relay").
				Logging(cfg.Observability.LogLevel, cfg.Observability.LogFormat).
				DataDir(dataDir).
				Signer(signer)

			rt, err := builder.Build()
			if err != nil {
				return fmt.Errorf("create runtime: %w", err)
			}
			defer func() { _ = rt.Close() }()

			// Create relay
			r, err := internalrelay.New(internalrelay.Config{
				BufferSize: bufferSize,
				Signer:     rt.Signer(),
			})
			if err != nil {
				return fmt.Errorf("create relay: %w", err)
			}
			r.Start(rt.Context())
			defer func() { _ = r.Close() }()

			// Create service
			svc := internalrelay.NewService(r, bufferSize)

			// Start gossip if enabled
			if cfg.Gossip.Enabled() {
				gossipPort := cfg.Gossip.BindPort
				if gossipPort == 0 {
					gossipPort = 7946 // default gossip port
				}
				g, gossipErr := internalgossip.New(internalgossip.Config{
					NodeName:      cfg.Gossip.NodeName,
					BindAddr:      cfg.Gossip.BindAddr,
					BindPort:      gossipPort,
					AdvertiseAddr: cfg.Gossip.AdvertiseAddr,
					AdvertisePort: cfg.Gossip.AdvertisePort,
					Seeds:         cfg.Gossip.Seeds,
					GRPCAddr:      cfg.GRPC.Addr,
					RelayPubkey:   identity.EncodePublicKey(rt.Signer().PublicKey()),
				})
				if gossipErr != nil {
					return fmt.Errorf("create gossip: %w", gossipErr)
				}
				defer func() { _ = g.Close() }()

				// Wire observer, discovery, and forwarding
				svc.SetObserver(g)
				svc.SetRemoteDiscovery(g)
				svc.SetGossipAdmin(internalgossip.NewGossipAdmin(g))

				// Create forwarder for cross-relay envelope routing
				fwd := internalgossip.NewForwarder(g, rt.Signer())
				defer func() { _ = fwd.Close() }()
				svc.SetRemoteForwarder(fwd)

				if startErr := g.Start(rt.Context()); startErr != nil {
					return fmt.Errorf("start gossip: %w", startErr)
				}
			}

			// Create gRPC server
			serverOpts := []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					internalrelay.UnaryServerInterceptor(),
				),
				grpc.ChainStreamInterceptor(
					internalrelay.StreamServerInterceptor(),
				),
			}
			grpcServer := grpc.NewServer(serverOpts...)

			// Register services
			relayv1.RegisterRelayServiceServer(grpcServer, svc)

			// Health check
			hs := health.NewServer()
			grpc_health_v1.RegisterHealthServer(grpcServer, hs)

			// Reflection
			if cfg.GRPC.EnableReflection {
				reflection.Register(grpcServer)
			}

			// Listen
			lc := net.ListenConfig{}
			lis, err := lc.Listen(rt.Context(), "tcp", cfg.GRPC.Addr)
			if err != nil {
				return fmt.Errorf("listen: %w", err)
			}

			slog.Info("relay listening", "addr", lis.Addr().String())
			hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

			// Serve in background
			errCh := make(chan error, 1)
			go func() {
				errCh <- grpcServer.Serve(lis)
			}()

			// Wait for shutdown or error
			select {
			case <-rt.Context().Done():
				slog.Info("shutting down")
				hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
				grpcServer.GracefulStop()
				return nil
			case err := <-errCh:
				return err
			}
		},
	}

	bindServeFlags(cmd, v)
	cmd.Flags().IntVar(&bufferSize, "buffer-size", Defaults.BufferSize, "subscriber buffer size")

	return cmd
}
