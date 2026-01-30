package server

import (
	"net"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/blobstore"
	"github.com/gezibash/arc-node/internal/envelope"
	"github.com/gezibash/arc-node/internal/indexstore"
	"github.com/gezibash/arc-node/internal/middleware"
	"github.com/gezibash/arc-node/internal/observability"
	"github.com/gezibash/arc/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	keypair    *identity.Keypair
}

func New(addr string, obs *observability.Observability, enableReflection bool, kp *identity.Keypair, blobs *blobstore.BlobStore, index *indexstore.IndexStore, opts ...grpc.ServerOption) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	mw := &middleware.Chain{}

	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			observability.UnaryServerInterceptor(obs.Metrics),
			envelope.UnaryServerInterceptor(kp, mw),
		),
		grpc.ChainStreamInterceptor(
			observability.StreamServerInterceptor(obs.Metrics),
			envelope.StreamServerInterceptor(kp, mw),
		),
	}

	serverOpts = append(serverOpts, opts...)

	grpcServer := grpc.NewServer(serverOpts...)

	nodev1.RegisterNodeServiceServer(grpcServer, &nodeService{
		blobs:   blobs,
		index:   index,
		metrics: obs.Metrics,
	})

	if enableReflection {
		reflection.Register(grpcServer)
	}

	return &Server{
		grpcServer: grpcServer,
		listener:   lis,
		keypair:    kp,
	}, nil
}

func (s *Server) Serve() error {
	return s.grpcServer.Serve(s.listener)
}

func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}

func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

func (s *Server) Keypair() *identity.Keypair {
	return s.keypair
}
