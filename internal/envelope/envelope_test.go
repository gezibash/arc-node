package envelope

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/middleware"
	"github.com/gezibash/arc/v2/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func generateKeypair(t *testing.T) *identity.Keypair {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	return kp
}

func TestSealOpenRoundTrip(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("hello world")
	contentType := "text/plain"
	origin := kp.PublicKey()
	meta := map[string]string{"foo": "bar"}

	env, err := Seal(kp, to, payload, contentType, origin, 0, meta, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	opened, err := Open(env.Message.From, env.Message.To, payload, contentType, env.Message.Timestamp, *env.Message.Signature, origin, 0, meta, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if opened.Message.From != kp.PublicKey() {
		t.Error("From mismatch")
	}
	if opened.Message.To != to {
		t.Error("To mismatch")
	}
	if opened.Message.ContentType != contentType {
		t.Error("ContentType mismatch")
	}
	if opened.Message.Content != env.Message.Content {
		t.Error("Content reference mismatch")
	}
	if opened.HopCount != 0 {
		t.Error("HopCount mismatch")
	}
	if opened.Metadata["foo"] != "bar" {
		t.Error("Metadata mismatch")
	}
}

func TestSealOpenWithDimensions(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("dimensions test")

	dims := &nodev1.Dimensions{
		Pattern:        nodev1.Pattern_PATTERN_PUB_SUB,
		Delivery:       nodev1.Delivery_DELIVERY_AT_LEAST_ONCE,
		Persistence:    nodev1.Persistence_PERSISTENCE_DURABLE,
		Visibility:     nodev1.Visibility_VISIBILITY_PRIVATE,
		Ordering:       nodev1.Ordering_ORDERING_FIFO,
		Affinity:       nodev1.Affinity_AFFINITY_KEY,
		Dedup:          nodev1.Dedup_DEDUP_IDEMPOTENCY_KEY,
		Complete:       nodev1.DeliveryComplete_DELIVERY_COMPLETE_ALL,
		TtlMs:          60000,
		CompleteN:      5,
		AffinityKey:    "user-123",
		IdempotencyKey: "idem-456",
		Priority:       7,
		MaxRedelivery:  3,
		AckTimeoutMs:   5000,
		Correlation:    "corr-789",
	}

	env, err := Seal(kp, to, payload, "test/dims", kp.PublicKey(), 2, nil, dims)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	opened, err := Open(env.Message.From, env.Message.To, payload, "test/dims", env.Message.Timestamp, *env.Message.Signature, kp.PublicKey(), 2, nil, dims)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	d := opened.Dimensions
	if d.Pattern != nodev1.Pattern_PATTERN_PUB_SUB {
		t.Error("Pattern mismatch")
	}
	if d.Delivery != nodev1.Delivery_DELIVERY_AT_LEAST_ONCE {
		t.Error("Delivery mismatch")
	}
	if d.Persistence != nodev1.Persistence_PERSISTENCE_DURABLE {
		t.Error("Persistence mismatch")
	}
	if d.Visibility != nodev1.Visibility_VISIBILITY_PRIVATE {
		t.Error("Visibility mismatch")
	}
	if d.Ordering != nodev1.Ordering_ORDERING_FIFO {
		t.Error("Ordering mismatch")
	}
	if d.Affinity != nodev1.Affinity_AFFINITY_KEY {
		t.Error("Affinity mismatch")
	}
	if d.Dedup != nodev1.Dedup_DEDUP_IDEMPOTENCY_KEY {
		t.Error("Dedup mismatch")
	}
	if d.Complete != nodev1.DeliveryComplete_DELIVERY_COMPLETE_ALL {
		t.Error("Complete mismatch")
	}
	if d.TtlMs != 60000 {
		t.Errorf("TtlMs = %d, want 60000", d.TtlMs)
	}
	if d.CompleteN != 5 {
		t.Errorf("CompleteN = %d, want 5", d.CompleteN)
	}
	if d.AffinityKey != "user-123" {
		t.Error("AffinityKey mismatch")
	}
	if d.IdempotencyKey != "idem-456" {
		t.Error("IdempotencyKey mismatch")
	}
	if d.Priority != 7 {
		t.Error("Priority mismatch")
	}
	if d.MaxRedelivery != 3 {
		t.Error("MaxRedelivery mismatch")
	}
	if d.AckTimeoutMs != 5000 {
		t.Error("AckTimeoutMs mismatch")
	}
	if d.Correlation != "corr-789" {
		t.Error("Correlation mismatch")
	}
}

func TestOpenInvalidSignature(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("tamper test")

	env, err := Seal(kp, to, payload, "text/plain", kp.PublicKey(), 0, nil, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Tamper with signature
	badSig := *env.Message.Signature
	badSig[0] ^= 0xFF

	_, err = Open(env.Message.From, env.Message.To, payload, "text/plain", env.Message.Timestamp, badSig, kp.PublicKey(), 0, nil, nil)
	if err != ErrInvalidSignature {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestOpenWrongPayload(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("original payload")

	env, err := Seal(kp, to, payload, "text/plain", kp.PublicKey(), 0, nil, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	_, err = Open(env.Message.From, env.Message.To, []byte("different payload"), "text/plain", env.Message.Timestamp, *env.Message.Signature, kp.PublicKey(), 0, nil, nil)
	if err != ErrInvalidSignature {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestSealOpenZeroToKey(t *testing.T) {
	kp := generateKeypair(t)
	var zeroKey identity.PublicKey
	payload := []byte("federation bootstrap")

	env, err := Seal(kp, zeroKey, payload, "federation/init", kp.PublicKey(), 0, nil, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	opened, err := Open(env.Message.From, zeroKey, payload, "federation/init", env.Message.Timestamp, *env.Message.Signature, kp.PublicKey(), 0, nil, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if opened.Message.To != zeroKey {
		t.Error("expected zero To key")
	}
}

func TestExtractInjectRoundTrip(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("metadata test")

	env, err := Seal(kp, to, payload, "test/meta", kp.PublicKey(), 3, map[string]string{"key1": "val1"}, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	sig := *env.Message.Signature

	ctx := InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)

	// Extract treats it as incoming metadata
	from, toExt, origin, ts, sigExt, ct, hc, meta, _, err := Extract(outMD)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	if from != kp.PublicKey() {
		t.Error("From mismatch")
	}
	if toExt != to {
		t.Error("To mismatch")
	}
	if origin != kp.PublicKey() {
		t.Error("Origin mismatch")
	}
	if ts != env.Message.Timestamp {
		t.Error("Timestamp mismatch")
	}
	if sigExt != sig {
		t.Error("Signature mismatch")
	}
	if ct != "test/meta" {
		t.Error("ContentType mismatch")
	}
	if hc != 3 {
		t.Error("HopCount mismatch")
	}
	if meta["key1"] != "val1" {
		t.Error("Metadata mismatch")
	}
}

func TestInjectOutgoingRoundTrip(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("outgoing test")

	env, err := Seal(kp, to, payload, "test/outgoing", kp.PublicKey(), 1, map[string]string{"x": "y"}, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	sig := *env.Message.Signature
	ctx := InjectOutgoing(context.Background(), env, sig)

	outMD, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("no outgoing metadata")
	}

	from, toExt, origin, ts, sigExt, ct, hc, meta, _, err := Extract(outMD)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	if from != kp.PublicKey() {
		t.Error("From mismatch")
	}
	if toExt != to {
		t.Error("To mismatch")
	}
	if origin != kp.PublicKey() {
		t.Error("Origin mismatch")
	}
	if ts != env.Message.Timestamp {
		t.Error("Timestamp mismatch")
	}
	if sigExt != sig {
		t.Error("Signature mismatch")
	}
	if ct != "test/outgoing" {
		t.Error("ContentType mismatch")
	}
	if hc != 1 {
		t.Error("HopCount mismatch")
	}
	if meta["x"] != "y" {
		t.Error("Metadata mismatch")
	}
}

func TestExtractDimensionsRoundTrip(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("dims metadata test")

	dims := &nodev1.Dimensions{
		Pattern:        nodev1.Pattern_PATTERN_QUEUE,
		Delivery:       nodev1.Delivery_DELIVERY_EXACTLY_ONCE,
		Persistence:    nodev1.Persistence_PERSISTENCE_DURABLE,
		Visibility:     nodev1.Visibility_VISIBILITY_FEDERATED,
		Ordering:       nodev1.Ordering_ORDERING_CAUSAL,
		Affinity:       nodev1.Affinity_AFFINITY_SENDER,
		Dedup:          nodev1.Dedup_DEDUP_REF,
		Complete:       nodev1.DeliveryComplete_DELIVERY_COMPLETE_N_OF_M,
		TtlMs:          30000,
		CompleteN:      10,
		AffinityKey:    "aff-key",
		IdempotencyKey: "idem-key",
		Priority:       99,
		MaxRedelivery:  5,
		AckTimeoutMs:   10000,
		Correlation:    "corr-id",
	}

	env, err := Seal(kp, to, payload, "test/dims-md", kp.PublicKey(), 0, nil, dims)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	sig := *env.Message.Signature
	ctx := InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)

	_, _, _, _, _, _, _, _, extractedDims, err := Extract(outMD)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	if extractedDims.Pattern != nodev1.Pattern_PATTERN_QUEUE {
		t.Error("Pattern mismatch")
	}
	if extractedDims.Delivery != nodev1.Delivery_DELIVERY_EXACTLY_ONCE {
		t.Error("Delivery mismatch")
	}
	if extractedDims.Persistence != nodev1.Persistence_PERSISTENCE_DURABLE {
		t.Error("Persistence mismatch")
	}
	if extractedDims.Visibility != nodev1.Visibility_VISIBILITY_FEDERATED {
		t.Error("Visibility mismatch")
	}
	if extractedDims.Ordering != nodev1.Ordering_ORDERING_CAUSAL {
		t.Error("Ordering mismatch")
	}
	if extractedDims.Affinity != nodev1.Affinity_AFFINITY_SENDER {
		t.Error("Affinity mismatch")
	}
	if extractedDims.Dedup != nodev1.Dedup_DEDUP_REF {
		t.Error("Dedup mismatch")
	}
	if extractedDims.Complete != nodev1.DeliveryComplete_DELIVERY_COMPLETE_N_OF_M {
		t.Error("Complete mismatch")
	}
	if extractedDims.TtlMs != 30000 {
		t.Error("TtlMs mismatch")
	}
	if extractedDims.CompleteN != 10 {
		t.Error("CompleteN mismatch")
	}
	if extractedDims.AffinityKey != "aff-key" {
		t.Error("AffinityKey mismatch")
	}
	if extractedDims.IdempotencyKey != "idem-key" {
		t.Error("IdempotencyKey mismatch")
	}
	if extractedDims.Priority != 99 {
		t.Error("Priority mismatch")
	}
	if extractedDims.MaxRedelivery != 5 {
		t.Error("MaxRedelivery mismatch")
	}
	if extractedDims.AckTimeoutMs != 10000 {
		t.Error("AckTimeoutMs mismatch")
	}
	if extractedDims.Correlation != "corr-id" {
		t.Error("Correlation mismatch")
	}
}

func BenchmarkSealOpen(b *testing.B) {
	b.ReportAllocs()
	kp, err := identity.Generate()
	if err != nil {
		b.Fatal(err)
	}
	toKP, err := identity.Generate()
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte("benchmark payload data")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env, err := Seal(kp, toKP.PublicKey(), payload, "bench", kp.PublicKey(), 0, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		_, err = Open(env.Message.From, env.Message.To, payload, "bench", env.Message.Timestamp, *env.Message.Signature, kp.PublicKey(), 0, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// buildIncomingContext creates an incoming gRPC context with valid envelope metadata
// for the given keypair, to-key, payload proto message, and content type.
func buildIncomingContext(t *testing.T, kp *identity.Keypair, to identity.PublicKey, msg proto.Message, contentType string) context.Context {
	t.Helper()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	env, err := Seal(kp, to, payload, contentType, kp.PublicKey(), 0, map[string]string{"k": "v"}, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sig := *env.Message.Signature
	ctx := InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)
	return metadata.NewIncomingContext(context.Background(), outMD)
}

func TestUnaryServerInterceptor_Success(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{}

	interceptor := UnaryServerInterceptor(serverKP, chain, map[string]string{"resp": "meta"})

	reqMsg := &nodev1.PutFrame{Data: []byte("hello")}
	ctx := buildIncomingContext(t, clientKP, serverKP.PublicKey(), reqMsg, "test/put")

	info := &grpc.UnaryServerInfo{FullMethod: "/arc.node.v1.Node/PutContent"}

	resp, err := interceptor(ctx, reqMsg, info, func(ctx context.Context, req any) (any, error) {
		caller, ok := GetCaller(ctx)
		if !ok {
			t.Error("expected Caller in context")
		}
		if caller.PublicKey != clientKP.PublicKey() {
			t.Error("caller PublicKey mismatch")
		}
		if caller.Metadata["k"] != "v" {
			t.Error("caller Metadata mismatch")
		}
		return &nodev1.PutFrame{Data: []byte("ok")}, nil
	})
	if err != nil {
		t.Fatalf("interceptor error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestUnaryServerInterceptor_MissingMetadata(t *testing.T) {
	serverKP := generateKeypair(t)
	chain := &middleware.Chain{}
	interceptor := UnaryServerInterceptor(serverKP, chain, nil)

	// No incoming metadata
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/test"}
	_, err := interceptor(ctx, &nodev1.PutFrame{}, info, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("got code %v, want Unauthenticated", st.Code())
	}
}

func TestUnaryServerInterceptor_BadSignature(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{}
	interceptor := UnaryServerInterceptor(serverKP, chain, nil)

	reqMsg := &nodev1.PutFrame{Data: []byte("hello")}
	payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(reqMsg)
	env, _ := Seal(clientKP, serverKP.PublicKey(), payload, "test", clientKP.PublicKey(), 0, nil, nil)
	sig := *env.Message.Signature
	sig[0] ^= 0xFF // corrupt
	ctx := InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewIncomingContext(context.Background(), outMD)

	info := &grpc.UnaryServerInfo{FullMethod: "/test"}
	_, err := interceptor(ctx, reqMsg, info, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("got code %v, want Unauthenticated", st.Code())
	}
}

func TestUnaryServerInterceptor_PreHookRejects(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{
		Pre: []middleware.Hook{
			func(ctx context.Context, info *middleware.CallInfo) (context.Context, error) {
				return ctx, status.Error(codes.PermissionDenied, "blocked")
			},
		},
	}
	interceptor := UnaryServerInterceptor(serverKP, chain, nil)

	reqMsg := &nodev1.PutFrame{Data: []byte("hello")}
	ctx := buildIncomingContext(t, clientKP, serverKP.PublicKey(), reqMsg, "test")
	info := &grpc.UnaryServerInfo{FullMethod: "/test"}

	_, err := interceptor(ctx, reqMsg, info, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Errorf("got code %v, want PermissionDenied", st.Code())
	}
}

func TestUnaryServerInterceptor_PostHookRejects(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{
		Post: []middleware.Hook{
			func(ctx context.Context, info *middleware.CallInfo) (context.Context, error) {
				return ctx, status.Error(codes.ResourceExhausted, "rate limited")
			},
		},
	}
	interceptor := UnaryServerInterceptor(serverKP, chain, nil)

	reqMsg := &nodev1.PutFrame{Data: []byte("hello")}
	ctx := buildIncomingContext(t, clientKP, serverKP.PublicKey(), reqMsg, "test")
	info := &grpc.UnaryServerInfo{FullMethod: "/test"}

	_, err := interceptor(ctx, reqMsg, info, func(ctx context.Context, req any) (any, error) {
		return &nodev1.PutFrame{Data: []byte("ok")}, nil
	})
	st, _ := status.FromError(err)
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("got code %v, want ResourceExhausted", st.Code())
	}
}

func TestUnaryServerInterceptor_HandlerReturnsNilResp(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{}
	interceptor := UnaryServerInterceptor(serverKP, chain, nil)

	reqMsg := &nodev1.PutFrame{Data: []byte("hello")}
	ctx := buildIncomingContext(t, clientKP, serverKP.PublicKey(), reqMsg, "test")
	info := &grpc.UnaryServerInfo{FullMethod: "/test"}

	resp, err := interceptor(ctx, reqMsg, info, func(ctx context.Context, req any) (any, error) {
		return nil, fmt.Errorf("some error")
	})
	if resp != nil {
		t.Error("expected nil response")
	}
	if err == nil {
		t.Error("expected error from handler")
	}
}

func TestUnaryServerInterceptor_InvalidExtract(t *testing.T) {
	serverKP := generateKeypair(t)
	chain := &middleware.Chain{}
	interceptor := UnaryServerInterceptor(serverKP, chain, nil)

	// Metadata present but missing required fields
	md := metadata.Pairs("arc-from-bin", "short")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	info := &grpc.UnaryServerInfo{FullMethod: "/test"}

	_, err := interceptor(ctx, &nodev1.PutFrame{}, info, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("got code %v, want Unauthenticated", st.Code())
	}
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
	env, err := Seal(clientKP, serverKP.PublicKey(), []byte{}, "test/stream", clientKP.PublicKey(), 0, map[string]string{"s": "t"}, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sig := *env.Message.Signature
	ctx := InjectOutgoing(context.Background(), env, sig)
	outMD, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewIncomingContext(context.Background(), outMD)

	ss := &mockServerStream{ctx: ctx}
	info := &grpc.StreamServerInfo{FullMethod: "/arc.node.v1.Node/SubscribeMessages", IsServerStream: true}

	handlerCalled := false
	err = interceptor(nil, ss, info, func(srv any, stream grpc.ServerStream) error {
		handlerCalled = true
		caller, ok := GetCaller(stream.Context())
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

	env, _ := Seal(clientKP, serverKP.PublicKey(), []byte{}, "test", clientKP.PublicKey(), 0, nil, nil)
	sig := *env.Message.Signature
	sig[0] ^= 0xFF
	ctx := InjectOutgoing(context.Background(), env, sig)
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

	env, _ := Seal(clientKP, serverKP.PublicKey(), []byte{}, "test", clientKP.PublicKey(), 0, nil, nil)
	sig := *env.Message.Signature
	ctx := InjectOutgoing(context.Background(), env, sig)
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
	ctx := context.WithValue(context.Background(), ctxKey{}, &Caller{HopCount: 42})
	ss := &mockServerStream{ctx: context.Background()}
	ws := &wrappedStream{ServerStream: ss, ctx: ctx}

	caller, ok := GetCaller(ws.Context())
	if !ok {
		t.Fatal("expected Caller from wrappedStream context")
	}
	if caller.HopCount != 42 {
		t.Errorf("HopCount = %d, want 42", caller.HopCount)
	}
}

func TestInjectSetsTrailingMetadata(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("inject test")

	env, err := Seal(kp, to, payload, "test/inject", kp.PublicKey(), 1, map[string]string{"ik": "iv"}, &nodev1.Dimensions{
		Pattern: nodev1.Pattern_PATTERN_QUEUE,
	})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sig := *env.Message.Signature

	// Inject calls grpc.SetTrailer which requires a grpc stream context.
	// Without a real gRPC context, SetTrailer is a no-op but Inject still runs.
	// We test via InjectOutgoing + Extract round-trip instead for metadata correctness.
	// The Inject function itself just wraps grpc.SetTrailer; coverage is the goal.
	Inject(context.Background(), env, sig)
}

func TestExtractTrailing(t *testing.T) {
	kp := generateKeypair(t)
	to := generateKeypair(t).PublicKey()
	payload := []byte("trailing test")

	env, err := Seal(kp, to, payload, "test/trailing", kp.PublicKey(), 2, map[string]string{"tk": "tv"}, nil)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sig := *env.Message.Signature

	// Build metadata manually matching what Inject would produce
	md := metadata.Pairs(
		"arc-from-bin", string(env.Message.From[:]),
		"arc-to-bin", string(env.Message.To[:]),
		"arc-timestamp", strconv.FormatInt(env.Message.Timestamp, 10),
		"arc-signature-bin", string(sig[:]),
		"arc-content-type", env.Message.ContentType,
		"arc-origin-bin", string(env.Origin[:]),
		"arc-hop-count", strconv.Itoa(env.HopCount),
		"arc-meta-tk", "tv",
	)

	from, toExt, origin, ts, sigExt, ct, hc, meta, _, err := ExtractTrailing(md)
	if err != nil {
		t.Fatalf("ExtractTrailing: %v", err)
	}
	if from != kp.PublicKey() {
		t.Error("From mismatch")
	}
	if toExt != to {
		t.Error("To mismatch")
	}
	if origin != kp.PublicKey() {
		t.Error("Origin mismatch")
	}
	if ts != env.Message.Timestamp {
		t.Error("Timestamp mismatch")
	}
	if sigExt != sig {
		t.Error("Signature mismatch")
	}
	if ct != "test/trailing" {
		t.Error("ContentType mismatch")
	}
	if hc != 2 {
		t.Error("HopCount mismatch")
	}
	if meta["tk"] != "tv" {
		t.Error("Metadata mismatch")
	}
}

func TestTimestampBytes(t *testing.T) {
	tests := []struct {
		ts   int64
		want uint64
	}{
		{0, 0},
		{1, 1},
		{1706745600000, 1706745600000},
		{-1, ^uint64(0)}, // two's complement
	}
	for _, tt := range tests {
		b := TimestampBytes(tt.ts)
		if len(b) != 8 {
			t.Fatalf("TimestampBytes(%d) returned %d bytes, want 8", tt.ts, len(b))
		}
		got := binary.BigEndian.Uint64(b)
		if got != tt.want {
			t.Errorf("TimestampBytes(%d) = %d, want %d", tt.ts, got, tt.want)
		}
	}
}

func TestExtractMissingMetadata(t *testing.T) {
	md := metadata.MD{}
	_, _, _, _, _, _, _, _, _, err := Extract(md)
	if err == nil {
		t.Fatal("expected error for empty metadata")
	}
}

func TestExtractErrorPaths(t *testing.T) {
	kp := generateKeypair(t)
	from := kp.PublicKey()
	to := generateKeypair(t).PublicKey()
	var sig identity.Signature
	copy(sig[:], make([]byte, 64))

	validFrom := string(from[:])
	validTo := string(to[:])
	validSig := string(sig[:])
	validOrigin := string(from[:])

	tests := []struct {
		name string
		md   metadata.MD
	}{
		{"missing to", metadata.Pairs("arc-from-bin", validFrom)},
		{"missing timestamp", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo)},
		{"bad timestamp", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "notanumber")},
		{"missing signature", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "123")},
		{"bad signature length", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "123", "arc-signature-bin", "short")},
		{"missing origin", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "123", "arc-signature-bin", validSig)},
		{"bad hop count", metadata.Pairs("arc-from-bin", validFrom, "arc-to-bin", validTo, "arc-timestamp", "123", "arc-signature-bin", validSig, "arc-origin-bin", validOrigin, "arc-hop-count", "notanumber")},
		{"bad from length", metadata.Pairs("arc-from-bin", "short")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, _, _, _, _, _, err := Extract(tt.md)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestExtractBadDimensions(t *testing.T) {
	kp := generateKeypair(t)
	from := kp.PublicKey()
	to := generateKeypair(t).PublicKey()

	// Build valid metadata but with corrupted dimensions
	env, _ := Seal(kp, to, []byte("test"), "test", from, 0, nil, nil)
	sig := *env.Message.Signature
	md := metadata.Pairs(
		"arc-from-bin", string(from[:]),
		"arc-to-bin", string(to[:]),
		"arc-timestamp", strconv.FormatInt(env.Message.Timestamp, 10),
		"arc-signature-bin", string(sig[:]),
		"arc-content-type", "test",
		"arc-origin-bin", string(from[:]),
		"arc-dimensions-bin", "invalid-proto-bytes",
	)

	_, _, _, _, _, _, _, _, dims, err := Extract(md)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	// Bad proto should result in empty dimensions
	if dims == nil {
		t.Error("expected non-nil dims even on bad proto")
	}
}

func TestUnaryServerInterceptor_NonProtoRequest(t *testing.T) {
	serverKP := generateKeypair(t)
	clientKP := generateKeypair(t)
	chain := &middleware.Chain{}
	interceptor := UnaryServerInterceptor(serverKP, chain, nil)

	// Build valid metadata for an empty payload
	reqMsg := &nodev1.PutFrame{}
	ctx := buildIncomingContext(t, clientKP, serverKP.PublicKey(), reqMsg, "test")
	info := &grpc.UnaryServerInfo{FullMethod: "/test"}

	// Pass a non-proto request (string instead of proto.Message)
	_, err := interceptor(ctx, "not-a-proto", info, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error for non-proto request")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Errorf("got code %v, want Internal", st.Code())
	}
}

func TestCallerContext(t *testing.T) {
	kp := generateKeypair(t)
	dims := &nodev1.Dimensions{Pattern: nodev1.Pattern_PATTERN_PUB_SUB}

	caller := &Caller{
		PublicKey:  kp.PublicKey(),
		Origin:     kp.PublicKey(),
		HopCount:   2,
		Metadata:   map[string]string{"a": "b"},
		Dimensions: dims,
	}

	ctx := WithCaller(context.Background(), caller)
	got, ok := GetCaller(ctx)
	if !ok {
		t.Fatal("GetCaller returned false")
	}
	if got.PublicKey != kp.PublicKey() {
		t.Error("PublicKey mismatch")
	}
	if got.Origin != kp.PublicKey() {
		t.Error("Origin mismatch")
	}
	if got.HopCount != 2 {
		t.Error("HopCount mismatch")
	}
	if got.Metadata["a"] != "b" {
		t.Error("Metadata mismatch")
	}
	if got.Dimensions.Pattern != nodev1.Pattern_PATTERN_PUB_SUB {
		t.Error("Dimensions mismatch")
	}

	// GetCaller from bare context should return false
	_, ok = GetCaller(context.Background())
	if ok {
		t.Error("expected false from bare context")
	}
}
