package server

import (
	"context"
	"log/slog"
	"net"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/blobstore"
	"github.com/gezibash/arc-node/internal/envelope"
	"github.com/gezibash/arc-node/internal/indexstore"
	"github.com/gezibash/arc-node/internal/middleware"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc/v2/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	keypair    *identity.Keypair
	health     *health.Server
	federator  *federationManager
}

func New(addr string, obs *observability.Observability, enableReflection bool, kp *identity.Keypair, blobs *blobstore.BlobStore, index *indexstore.IndexStore, opts ...grpc.ServerOption) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	mw := &middleware.Chain{}

	meta := map[string]string{}
	if obs != nil {
		if obs.ServiceName != "" {
			meta["service_name"] = obs.ServiceName
		}
		if obs.ServiceVersion != "" {
			meta["service_version"] = obs.ServiceVersion
		}
	}

	serverOpts := []grpc.ServerOption{
		grpc.ChainStreamInterceptor(
			observability.StreamServerInterceptor(obs.Metrics),
			envelope.StreamServerInterceptor(kp, mw),
		),
	}

	serverOpts = append(serverOpts, opts...)

	grpcServer := grpc.NewServer(serverOpts...)

	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, hs)
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// Wire group cache and visibility filter.
	gc := newGroupCache()
	vc := newVisibilityChecker(gc)
	index.SetVisibilityFilter(vc.Check)

	federator := newFederationManager(blobs, index, kp, gc)
	if obs != nil && obs.Shutdown != nil {
		obs.Shutdown.Register("federation", func(ctx context.Context) error {
			federator.StopAll()
			return nil
		})
	}

	nodev1.RegisterNodeServiceServer(grpcServer, &nodeService{
		blobs:       blobs,
		index:       index,
		metrics:     obs.Metrics,
		federator:   federator,
		subscribers: newSubscriberTracker(),
		visCheck:    vc.Check,
		groupCache:  gc,
		adminKey:    kp.PublicKey(),
	})

	if enableReflection {
		reflection.Register(grpcServer)
	}

	return &Server{
		grpcServer: grpcServer,
		listener:   lis,
		keypair:    kp,
		health:     hs,
		federator:  federator,
	}, nil
}

func (s *Server) SetServingStatus(status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	if s.health != nil {
		s.health.SetServingStatus("", status)
	}
}

func (s *Server) Serve() error {
	return s.grpcServer.Serve(s.listener)
}

func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

func (s *Server) Stop(ctx context.Context) {
	if s.health != nil {
		s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}
	if s.federator != nil {
		s.federator.StopAll()
	}

	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("graceful stop timed out, forcing")
		s.grpcServer.Stop()
		<-done
	}
}

func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

func (s *Server) Keypair() *identity.Keypair {
	return s.keypair
}
