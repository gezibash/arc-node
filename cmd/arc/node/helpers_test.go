package node

import (
	"context"
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// testKeypair generates a keypair for testing.
func testKeypair(t *testing.T) *identity.Keypair {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	return kp
}

// staticKeyLoader returns a mockKeyLoader that always returns the given keypair.
func staticKeyLoader(kp *identity.Keypair) *mockKeyLoader {
	return &mockKeyLoader{
		loadFn:        func(_ context.Context, _ string) (*identity.Keypair, error) { return kp, nil },
		loadDefaultFn: func(_ context.Context) (*identity.Keypair, error) { return kp, nil },
	}
}
