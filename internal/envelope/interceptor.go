package envelope

import (
	"context"

	"github.com/gezibash/arc-node/internal/middleware"
	"github.com/gezibash/arc/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmd "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var marshalOpts = proto.MarshalOptions{Deterministic: true}

// UnaryServerInterceptor returns a gRPC unary interceptor that verifies
// incoming request envelopes, runs middleware hooks, signs response envelopes,
// and injects trailing metadata.
func UnaryServerInterceptor(kp *identity.Keypair, chain *middleware.Chain, respMeta map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := grpcmd.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing envelope metadata")
		}

		from, to, origin, ts, sig, ct, hopCount, reqMeta, err := Extract(md)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "extract envelope: %v", err)
		}

		protoMsg, ok := req.(proto.Message)
		if !ok {
			return nil, status.Error(codes.Internal, "request is not a proto message")
		}
		payload, err := marshalOpts.Marshal(protoMsg)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal request: %v", err)
		}

		_, err = Open(from, to, payload, ct, ts, sig, origin, hopCount, reqMeta)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "verify envelope: %v", err)
		}

		caller := &Caller{
			PublicKey: from,
			Origin:    origin,
			HopCount:  hopCount,
			Metadata:  reqMeta,
		}
		ctx = WithCaller(ctx, caller)

		callInfo := &middleware.CallInfo{
			FullMethod: info.FullMethod,
		}
		ctx, err = chain.RunPre(ctx, callInfo)
		if err != nil {
			return nil, err
		}

		resp, handlerErr := handler(ctx, req)

		ctx, err = chain.RunPost(ctx, callInfo)
		if err != nil {
			return nil, err
		}

		if resp != nil {
			if respMsg, ok := resp.(proto.Message); ok {
				if respPayload, marshalErr := marshalOpts.Marshal(respMsg); marshalErr == nil {
					if respEnv, sealErr := Seal(kp, from, respPayload, info.FullMethod, kp.PublicKey(), 0, respMeta); sealErr == nil {
						Inject(ctx, respEnv, *respEnv.Message.Signature)
					}
				}
			}
		}

		return resp, handlerErr
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor that verifies
// the caller's envelope at stream open and sets the Caller in context.
func StreamServerInterceptor(kp *identity.Keypair, chain *middleware.Chain) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		md, ok := grpcmd.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "missing envelope metadata")
		}

		from, _, origin, ts, sig, ct, hopCount, meta, err := Extract(md)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "extract envelope: %v", err)
		}

		_, err = Open(from, kp.PublicKey(), []byte{}, ct, ts, sig, origin, hopCount, meta)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "verify envelope: %v", err)
		}

		caller := &Caller{
			PublicKey: from,
			Origin:    origin,
			HopCount:  hopCount,
			Metadata:  meta,
		}
		ctx = WithCaller(ctx, caller)

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
