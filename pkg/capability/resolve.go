package capability

import (
	"context"
	"fmt"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// DiscoverFunc discovers capability targets. Used by ResolveTarget to find
// targets when the input string is not a public key.
type DiscoverFunc func(ctx context.Context) (*TargetSet, error)

// ResolveTarget resolves a target string to a Target.
// It first tries to decode the string as a public key (algo:hex or raw hex >= 64 chars).
// If that fails, it calls discover and matches by Name, Petname, or DisplayName.
func ResolveTarget(ctx context.Context, target string, discover DiscoverFunc) (*Target, error) {
	// Try as public key first
	if pk, ok := identity.TryDecodePublicKey(target); ok {
		return &Target{PublicKey: pk}, nil
	}

	// Discover and match by name
	ts, err := discover(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover targets: %w", err)
	}

	found := ts.Pick(func(t Target) bool {
		return t.Name == target || t.Petname == target || t.DisplayName() == target
	})

	if found == nil {
		return nil, fmt.Errorf("target not found: %s", target)
	}

	return found, nil
}
