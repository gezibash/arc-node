package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	blobv1 "github.com/gezibash/arc/v2/api/arc/blob/v1"
	"github.com/gezibash/arc/v2/internal/blobstore"
	"github.com/gezibash/arc/v2/internal/config"
	"github.com/gezibash/arc/v2/internal/keyring"
	"github.com/gezibash/arc/v2/pkg/blob"
	"github.com/gezibash/arc/v2/pkg/capability"
	"github.com/gezibash/arc/v2/pkg/logging"
	"github.com/gezibash/arc/v2/pkg/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	// Register storage backends.
	_ "github.com/gezibash/arc/v2/internal/blobstore/badger"
	_ "github.com/gezibash/arc/v2/internal/blobstore/fs"
	_ "github.com/gezibash/arc/v2/internal/blobstore/s3"
)

func newStartCmd() *cobra.Command {
	v := viper.New()
	var configFile string

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the blob storage server",
		Long: `Start the blob storage server.

The blob server connects to the relay and handles blob storage requests.
Small blobs are transferred inline through the relay.
Large blobs use direct gRPC connections (if --listen is specified).

Examples:
  arc-blob start --relay localhost:50051 --listen :50052
  arc-blob start --relay localhost:50051                   # relay-only (no direct)
  arc-blob start --data-dir /data/blobs --key blobstore
  arc-blob start --config /etc/arc/arc-blob.hcl`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load config
			cfg, err := loadConfig(v, configFile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			dataDir := cfg.ResolvedDataDir()
			relayAddr := cfg.ResolvedRelayAddr()

			keyName := cfg.KeyName
			if keyName == "" {
				keyName = config.BlobDefaults.KeyName
			}

			kr := keyring.New(dataDir)
			signer, err := kr.LoadSigner(context.Background(), keyName, cfg.KeyPath)
			if err != nil {
				return fmt.Errorf("load signer: %w", err)
			}

			// Build runtime with relay capability
			builder := runtime.New("blob").
				Logging(cfg.Observability.LogLevel, cfg.Observability.LogFormat).
				Use(capability.Relay(capability.RelayConfig{
					Addr: relayAddr,
				}))

			builder = builder.DataDir(dataDir).Signer(signer)

			rt, err := builder.Build()
			if err != nil {
				return fmt.Errorf("create runtime: %w", err)
			}
			defer func() { _ = rt.Close() }()

			// Determine backend
			backend := cfg.Storage.Backend
			if backend == "" {
				backend = blobstore.BackendFile
			}

			// Create store config
			storeCfg := cfg.Storage.Config
			if storeCfg == nil {
				storeCfg = make(map[string]string)
			}
			if storeCfg["path"] == "" && (backend == blobstore.BackendFile || backend == blobstore.BackendBadger) {
				storeCfg["path"] = filepath.Join(rt.DataDir(), "blobs")
			}

			store, err := blobstore.New(rt.Context(), backend, storeCfg)
			if err != nil {
				return fmt.Errorf("create store: %w", err)
			}
			defer func() { _ = store.Close() }()

			// Start direct gRPC server for large blob transfers (optional)
			var grpcServer *grpc.Server
			var externalAddr string

			if cfg.ListenAddr != "" {
				lc := &net.ListenConfig{}
				lis, err := lc.Listen(rt.Context(), "tcp", cfg.ListenAddr)
				if err != nil {
					return fmt.Errorf("listen: %w", err)
				}

				grpcServer = grpc.NewServer()
				blobv1.RegisterBlobServiceServer(grpcServer, blobstore.NewService(store))

				go func() {
					rt.Log().Info("direct server listening", "addr", lis.Addr().String())
					if err := grpcServer.Serve(lis); err != nil {
						rt.Log().WithError(err).Error("grpc serve failed")
					}
				}()

				// Determine external address for REDIRECT responses
				externalAddr = cfg.ListenAddr
				if externalAddr[0] == ':' {
					if hostname, _ := os.Hostname(); hostname != "" {
						externalAddr = hostname + cfg.ListenAddr
					}
				}
			} else {
				rt.Log().Info("running in relay-only mode (no direct gRPC server)")
			}

			// Create handler using capability framework
			handler := blobstore.NewBlobHandler(store, externalAddr, rt.Log())

			// Build typed labels for subscription — advertise limits early
			labels := blob.Labels{
				Backend:     blob.Backend(backend),
				Direct:      externalAddr,
				Capacity:    cfg.Capacity,
				MaxBlobSize: cfg.MaxBlobSize,
			}

			// Create capability server
			capServer, err := capability.NewServer(capability.ServerConfig{
				Name:    blob.CapabilityName,
				Labels:  labels.ToMap(),
				Handler: handler,
				Runtime: rt,
			})
			if err != nil {
				return fmt.Errorf("create capability server: %w", err)
			}

			// Stop gRPC server on shutdown (if started)
			if grpcServer != nil {
				go func() {
					<-rt.Context().Done()
					grpcServer.GracefulStop()
				}()
			}

			// Report store stats as dynamic state
			if stater, ok := store.(blobstore.Stater); ok {
				go reportState(rt.Context(), capServer, stater, rt.Log())
			}

			// Run capability server (blocks until context cancelled)
			return capServer.Run(rt.Context())
		},
	}

	// Bind common flags (data-dir, relay, key, key-path, log-level, log-format)
	config.BindCommonFlags(cmd, v)

	// Blob-specific flags
	f := cmd.Flags()
	f.String("listen", "", "direct gRPC address (empty = relay-only)")
	f.String("storage-backend", "", "storage backend: file, s3, badger, memory")
	f.Int64("capacity", 0, "total storage capacity in bytes (default 1GB)")
	f.Int64("max-blob-size", 0, "max single blob size in bytes (default 64MB)")
	f.StringVar(&configFile, "config", "", "config file path")

	_ = v.BindPFlag("listen_addr", f.Lookup("listen"))
	_ = v.BindPFlag("storage.backend", f.Lookup("storage-backend"))
	_ = v.BindPFlag("capacity", f.Lookup("capacity"))
	_ = v.BindPFlag("max_blob_size", f.Lookup("max-blob-size"))

	return cmd
}

func reportState(ctx context.Context, server *capability.Server, stater blobstore.Stater, log *logging.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := stater.Stats(ctx)
			if err != nil {
				log.WithError(err).Warn("stats collection failed")
				continue
			}
			if err := server.ReportState(map[string]any{
				blob.StateUsedBytes: stats.BytesUsed,
				blob.StateBlobCount: stats.BlobCount,
			}); err != nil {
				log.WithError(err).Warn("state report failed")
			}
		}
	}
}
