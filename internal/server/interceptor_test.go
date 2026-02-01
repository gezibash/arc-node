package server

import (
	"context"
	"testing"

	"github.com/gezibash/arc-node/internal/middleware"
	"github.com/gezibash/arc-node/pkg/envelope"
	"github.com/gezibash/arc/v2/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func generateKeypair(t *testing.T) *identity.Keypair {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	return kp
}

// mockServerStream implements grpc.ServerStream for testing.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context { return m.ctx }

func TestStreamServerInterceptor_Success(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{}

	interceptor := StreamServerInterceptor(serverKP, chain)

	// Stream interceptor verifies with empty payload
	env, err := envelope.Seal(clientKP, serverKP.PublicKey(), []byte{}, "test/stream", clientKP.PublicKey(), 0, map[string]string{"s": "t"}, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sig := *env.Message.Signature
	ctx := envelope.InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewIncomingContext(context.Background(), outMD)

	ss := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/arc.node.v1.Node/SubscribeMessages", IsServerStream: true}

	handlerCalled := false
	err = interceptor(nil, ss, info, func(srv any, stream grpc.ServerStream) error {
		handlerCalled = true
		caller, ok := envelope.GetCaller(stream.Context())
		if !ok {
			t.Error("expected Caller in stream context")
		}
		if caller.PublicKey != clientKP.PublicKey() {
			t.Error("caller PublicKey mismatch")
		}
		if caller.Metadata["s"] != "t" {
			t.Error("caller Metadata mismatch")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("interceptor error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler was not called")
	}
}

func TestStreamServerInterceptor_MissingMetadata(t *testing.T) {
	serverKP := generateKeypair(t)
	chain := &middleware.Chain{}
	interceptor := StreamServerInterceptor(serverKP, chain)

	ss := &mockServerStream{ctx: context.Background()}
	info := &grpc.StreamServerInfo{FullMethod: "/test"}

	err := interceptor(nil, ss, info, func(srv any, stream grpc.ServerStream) error {
		t.Fatal("handler should not be called")
		return nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("got code %v, want Unauthenticated", st.Code())
	}
}

func TestStreamServerInterceptor_BadSignature(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{}
	interceptor := StreamServerInterceptor(serverKP, chain)

	env, _ := envelope.Seal(clientKP, serverKP.PublicKey(), []byte{}, "test", clientKP.PublicKey(), 0, nil, nil)
	sig := *env.Message.Signature
	sig[0] ^= 0xFF
	ctx := envelope.InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewIncomingContext(context.Background(), outMD)

	ss := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/test"}

	err := interceptor(nil, ss, info, func(srv any, stream grpc.ServerStream) error {
		t.Fatal("handler should not be called")
		return nil
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("got code %v, want Unauthenticated", st.Code())
	}
}

func TestStreamServerInterceptor_PreHookRejects(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{
		Pre: []middleware.Hook{
			func(ctx context.Context, info *middleware.CallInfo) (context.Context, error) {
				if !info.IsStream {
					t.Error("expected IsStream to be true")
				}
				return ctx, status.Error(codes.PermissionDenied, "blocked")
			},
		},
	}
	interceptor := StreamServerInterceptor(serverKP, chain)

	env, _ := envelope.Seal(clientKP, serverKP.PublicKey(), []byte{}, "test", clientKP.PublicKey(), 0, nil, nil)
	sig := *env.Message.Signature
	ctx := envelope.InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewIncomingContext(context.Background(), outMD)

	ss := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/test"}

	err := interceptor(nil, ss, info, func(srv any, stream grpc.ServerStream) error {
		t.Fatal("handler should not be called")
		return nil
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Errorf("got code %v, want PermissionDenied", st.Code())
	}
}

func TestStreamServerInterceptor_InvalidExtract(t *testing.T) {
	serverKP := generateKeypair(t)
	chain := &middleware.Chain{}
	interceptor := StreamServerInterceptor(serverKP, chain)

	md := metadata.Pairs("arc-from-bin", "short")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	ss := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/test"}

	err := interceptor(nil, ss, info, func(srv any, stream grpc.ServerStream) error {
		t.Fatal("handler should not be called")
		return nil
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("got code %v, want Unauthenticated", st.Code())
	}
}

func TestWrappedStreamContext(t *testing.T) {
	caller := &envelope.Caller{HopCount: 42}
	ctx := envelope.WithCaller(context.Background(), caller)
	ss := &mockServerStream{ctx: context.Background()}
	ws := &wrappedStream{ServerStream: ss, ctx: ctx}

	got, ok := envelope.GetCaller(ws.Context())
	if !ok {
		t.Fatal("expected Caller from wrappedStream context")
	}
	if got.HopCount != 42 {
		t.Errorf("HopCount = %d, want 42", got.HopCount)
	}
}
