package server

import (
	"context"

	"github.com/gezibash/arc-node/internal/middleware"
	"github.com/gezibash/arc-node/pkg/envelope"
	"github.com/gezibash/arc/v2/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmd "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// StreamServerInterceptor returns a gRPC stream interceptor that verifies
// the caller's envelope at stream open and sets the Caller in context.
func StreamServerInterceptor(kp *identity.Keypair, chain *middleware.Chain) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		md, ok := grpcmd.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "missing envelope metadata")
		}

		from, to, origin, ts, sig, ct, hopCount, meta, dims, err := envelope.Extract(md)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "extract envelope: %v", err)
		}

		// Use the "to" from the client's envelope for verification.
		// This allows clients that don't know the server's key (e.g. federation)
		// to open streams with a zero "to" field.
		_, err = envelope.Open(from, to, []byte{}, ct, ts, sig, origin, hopCount, meta, dims)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "verify envelope: %v", err)
		}

		caller := &envelope.Caller{
			PublicKey:  from,
			Origin:     origin,
			HopCount:   hopCount,
			Metadata:   meta,
			Dimensions: dims,
		}
		ctx = envelope.WithCaller(ctx, caller)

		callInfo := &middleware.CallInfo{
			FullMethod: info.FullMethod,
			IsStream:   true,
		}
		ctx, err = chain.RunPre(ctx, callInfo)
		if err != nil {
			return err
		}

		wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
		return handler(srv, wrapped)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
