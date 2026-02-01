package server

import (
	"context"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc-node/internal/envelope"
	"github.com/gezibash/arc-node/internal/middleware"
	"github.com/gezibash/arc/pkg/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func adminOnlyHook(admin identity.PublicKey, methods map[string]struct{}) middleware.Hook {
	return func(ctx context.Context, info *middleware.CallInfo) (context.Context, error) {
		if _, ok := methods[info.FullMethod]; !ok {
			return ctx, nil
		}
		caller, ok := envelope.GetCaller(ctx)
		if !ok {
			return ctx, status.Error(codes.Unauthenticated, "missing caller")
		}
		if caller.PublicKey != admin {
			return ctx, status.Error(codes.PermissionDenied, "admin key required")
		}
		return ctx, nil
	}
}

var adminMethods = map[string]struct{}{
	nodev1.NodeService_Federate_FullMethodName:  {},
	nodev1.NodeService_ListPeers_FullMethodName: {},
}
