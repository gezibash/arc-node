package envelope

import (
	"context"

	nodev1 "github.com/gezibash/arc-node/api/arc/node/v1"
	"github.com/gezibash/arc/v2/pkg/identity"
)

type ctxKey struct{}

// Caller holds the authenticated identity extracted from an envelope.
type Caller struct {
	PublicKey  identity.PublicKey
	Origin     identity.PublicKey
	HopCount   int
	Metadata   map[string]string
	Dimensions *nodev1.Dimensions
}

// WithCaller stores a Caller in the context.
func WithCaller(ctx context.Context, c *Caller) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// GetCaller retrieves the Caller from the context.
func GetCaller(ctx context.Context) (*Caller, bool) {
	c, ok := ctx.Value(ctxKey{}).(*Caller)
	return c, ok
}
