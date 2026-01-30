package node

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gezibash/arc-node/internal/config"
	"github.com/gezibash/arc-node/internal/keyring"
	arcnode "github.com/gezibash/arc-node/internal/node"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc-node/internal/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newStartCmd(v *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the arc node",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStart(cmd, v)
		},
	}
	config.BindServeFlags(cmd, v)
	return cmd
}

func runStart(cmd *cobra.Command, v *viper.Viper) error {
	configFile, _ := cmd.Flags().GetString("config")
	cfg, err := config.Load(v, configFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obs, err := observability.New(ctx, observability.ObsConfig{
		LogLevel:       cfg.Observability.LogLevel,
		LogFormat:      cfg.Observability.LogFormat,
		OTLPEndpoint:   cfg.Observability.OTLPEndpoint,
		OTLPProtocol:   cfg.Observability.OTLPProtocol,
		ServiceName:    cfg.Observability.ServiceName,
		ServiceVersion: cfg.Observability.ServiceVersion,
	}, os.Stderr)
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}

	obs.ServeMetrics(ctx, cfg.Observability.MetricsAddr)

	// Initialize storage
	blobStore, err := arcnode.NewBlobStore(ctx, &cfg.Storage.Blob, obs.Metrics)
	if err != nil {
		return fmt.Errorf("init blob store: %w", err)
	}
	obs.Shutdown.Register("blobstore", func(ctx context.Context) error {
		return blobStore.Close()
	})

	indexStore, err := arcnode.NewIndexStore(ctx, &cfg.Storage.Index, obs.Metrics)
	if err != nil {
		return fmt.Errorf("init index store: %w", err)
	}
	obs.Shutdown.Register("indexstore", func(ctx context.Context) error {
		return indexStore.Close()
	})

	indexStore.StartCleanup(ctx, 5*time.Minute)

	slog.Info("storage initialized",
		"blob_backend", cfg.Storage.Blob.Backend,
		"index_backend", cfg.Storage.Index.Backend,
	)

	kr := keyring.New(cfg.DataDir)
	key, err := kr.LoadOrGenerate(ctx, "node")
	if err != nil {
		return fmt.Errorf("load node identity: %w", err)
	}
	// Only set the default if none is configured yet.
	if _, err := kr.LoadDefault(ctx); err != nil {
		_ = kr.SetDefault("node")
	}

	slog.Info("node identity", "public_key", key.PublicKey)

	srv, err := server.New(cfg.GRPC.Addr, obs, cfg.GRPC.EnableReflection, key.Keypair, blobStore, indexStore)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}

	obs.Shutdown.Register("grpc-server", func(ctx context.Context) error {
		srv.Stop()
		return nil
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()

		if err := obs.Close(shutdownCtx); err != nil {
			slog.Error("shutdown error", "error", err)
		}
	}()

	slog.Info("serving", "addr", srv.Addr(), "metrics", cfg.Observability.MetricsAddr)
	return srv.Serve()
}
