package relay

import (
	"context"
	"log/slog"

	"github.com/gezibash/arc/v2/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmd "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Metadata keys for envelope authentication
const (
	MetaFrom      = "arc-from"      // hex-encoded public key
	MetaSignature = "arc-signature" // hex-encoded signature
	MetaTimestamp = "arc-timestamp" // unix millis
)

// UnaryServerInterceptor returns a gRPC unary interceptor that verifies
// the caller's keypair signature and sets the sender in context.
// If auth metadata is present, it's verified. If absent, the request
// passes through unauthenticated (handler decides whether to require it).
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		sender, err := extractAndVerifySender(ctx)
		if err != nil {
			// No valid auth — pass through without sender in context.
			// Handlers that require auth (e.g. ForwardEnvelope) check
			// SenderFromContext themselves and return Unauthenticated.
			return handler(ctx, req)
		}
		ctx = WithSender(ctx, sender)
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor that verifies
// the caller's envelope signature and sets the sender in context.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()

		sender, err := extractAndVerifySender(ctx)
		if err != nil {
			slog.Warn("auth failed",
				"component", "interceptor",
				"reason", err.Error(),
			)
			return err
		}

		ctx = WithSender(ctx, sender)
		wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
		return handler(srv, wrapped)
	}
}

// extractAndVerifySender extracts and verifies the sender from metadata.
func extractAndVerifySender(ctx context.Context) (identity.PublicKey, error) {
	md, ok := grpcmd.FromIncomingContext(ctx)
	if !ok {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "missing metadata")
	}

	// Extract public key
	fromVals := md.Get(MetaFrom)
	if len(fromVals) == 0 {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "missing arc-from")
	}
	from, err := identity.DecodePublicKey(fromVals[0])
	if err != nil {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "invalid arc-from")
	}

	// Extract signature
	sigVals := md.Get(MetaSignature)
	if len(sigVals) == 0 {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "missing arc-signature")
	}

	sig, err := identity.DecodeSignature(sigVals[0])
	if err != nil {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "invalid arc-signature")
	}

	// Extract timestamp
	tsVals := md.Get(MetaTimestamp)
	if len(tsVals) == 0 {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "missing arc-timestamp")
	}

	var ts int64
	if !parseTimestamp(tsVals[0], &ts) {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "invalid arc-timestamp")
	}

	// Verify signature over (from || timestamp)
	// This is a minimal auth envelope - just proves identity ownership
	payload := authPayload(from, ts)
	if !identity.Verify(from, payload, sig) {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "invalid signature")
	}

	return from, nil
}

func parseTimestamp(s string, ts *int64) bool {
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
		n = n*10 + int64(c-'0')
	}
	*ts = n
	return true
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
