package relay

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	relayv1 "github.com/gezibash/arc-node/api/arc/relay/v1"
	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/logging"
	"github.com/gezibash/arc-node/internal/relay"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Entrypoint returns the relay subcommand tree.
func Entrypoint(_ *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "relay",
		Short: "Relay server commands",
	}

	cmd.AddCommand(
		newStartCmd(),
	)

	return cmd
}

func newStartCmd() *cobra.Command {
	v := viper.New()
	var bufferSize int

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the relay server",
		RunE: func(cmd *cobra.Command, args []string) error {
			configFile, _ := cmd.Flags().GetString("config")
			cfg, err := config.Load(v, configFile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			logging.Setup(cfg.Observability.LogLevel, cfg.Observability.LogFormat)

			return runRelay(cmd.Context(), cfg.GRPC.Addr, bufferSize, cfg.GRPC.EnableReflection)
		},
	}

	config.BindServeFlags(cmd, v)
	cmd.Flags().IntVar(&bufferSize, "buffer-size", relay.DefaultBufferSize, "subscriber buffer size")

	return cmd
}

func runRelay(ctx context.Context, addr string, bufferSize int, enableReflect bool) error {
	// Create relay
	cfg := relay.Config{
		BufferSize: bufferSize,
	}
	r := relay.New(cfg)
	r.Start(ctx)

	// Create gRPC server
	serverOpts := []grpc.ServerOption{
		grpc.ChainStreamInterceptor(
			relay.StreamServerInterceptor(),
		),
	}
	grpcServer := grpc.NewServer(serverOpts...)

	// Register services
	svc := relay.NewService(r, bufferSize)
	relayv1.RegisterRelayServiceServer(grpcServer, svc)

	// Health check
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, hs)

	// Reflection
	if enableReflect {
		reflection.Register(grpcServer)
	}

	// Listen
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	slog.Info("relay starting", "addr", lis.Addr().String())
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(lis)
	}()

	select {
	case sig := <-sigCh:
		slog.Info("shutting down", "signal", sig)
		hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		grpcServer.GracefulStop()
		_ = r.Close()
		return nil
	case err := <-errCh:
		_ = r.Close()
		return err
	}
}
