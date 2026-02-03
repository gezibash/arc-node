package relay

import (
	"context"
	"encoding/hex"

	"github.com/gezibash/arc/v2/pkg/identity"
	"github.com/gezibash/arc/v2/pkg/message"
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

// StreamServerInterceptor returns a gRPC stream interceptor that verifies
// the caller's envelope signature and sets the sender in context.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()

		sender, err := extractAndVerifySender(ctx)
		if err != nil {
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

	fromBytes, err := hex.DecodeString(fromVals[0])
	if err != nil || len(fromBytes) != identity.PublicKeySize {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "invalid arc-from")
	}
	var from identity.PublicKey
	copy(from[:], fromBytes)

	// Extract signature
	sigVals := md.Get(MetaSignature)
	if len(sigVals) == 0 {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "missing arc-signature")
	}

	sigBytes, err := hex.DecodeString(sigVals[0])
	if err != nil || len(sigBytes) != identity.SignatureSize {
		return identity.PublicKey{}, status.Error(codes.Unauthenticated, "invalid arc-signature")
	}
	var sig identity.Signature
	copy(sig[:], sigBytes)

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
	msg := message.Message{
		From:      from,
		Timestamp: ts,
		Signature: &sig,
	}

	ok, err = message.Verify(msg)
	if err != nil {
		return identity.PublicKey{}, status.Errorf(codes.Unauthenticated, "verify failed: %v", err)
	}
	if !ok {
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
