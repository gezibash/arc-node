package capability

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
)

func TestResolveTarget(t *testing.T) {
	ctx := context.Background()

	// Create a test public key (64 hex chars = 32 bytes)
	testKeyBytes := make([]byte, 32)
	for i := range testKeyBytes {
		testKeyBytes[i] = byte(i)
	}
	testKeyHex := hex.EncodeToString(testKeyBytes)

	t.Run("resolves pubkey with algo prefix", func(t *testing.T) {
		input := "ed25519:" + testKeyHex
		target, err := ResolveTarget(ctx, input, func(ctx context.Context) (*TargetSet, error) {
			t.Fatal("discover should not be called for pubkey input")
			return nil, nil
		})
		if err != nil {
			t.Fatalf("ResolveTarget() error = %v", err)
		}
		if target.PublicKey.Algo != identity.AlgEd25519 {
			t.Errorf("Algo = %q, want %q", target.PublicKey.Algo, identity.AlgEd25519)
		}
		if hex.EncodeToString(target.PublicKey.Bytes) != testKeyHex {
			t.Errorf("PublicKey bytes mismatch")
		}
	})

	t.Run("resolves raw hex pubkey", func(t *testing.T) {
		target, err := ResolveTarget(ctx, testKeyHex, func(ctx context.Context) (*TargetSet, error) {
			t.Fatal("discover should not be called for pubkey input")
			return nil, nil
		})
		if err != nil {
			t.Fatalf("ResolveTarget() error = %v", err)
		}
		if hex.EncodeToString(target.PublicKey.Bytes) != testKeyHex {
			t.Errorf("PublicKey bytes mismatch")
		}
	})

	t.Run("discovers by petname", func(t *testing.T) {
		target, err := ResolveTarget(ctx, "clever-penguin", func(ctx context.Context) (*TargetSet, error) {
			return NewTargetSet([]Target{
				{Petname: "happy-dolphin", PublicKey: identity.PublicKey{Algo: identity.AlgEd25519, Bytes: []byte("a")}},
				{Petname: "clever-penguin", PublicKey: identity.PublicKey{Algo: identity.AlgEd25519, Bytes: []byte("b")}},
			}), nil
		})
		if err != nil {
			t.Fatalf("ResolveTarget() error = %v", err)
		}
		if target.Petname != "clever-penguin" {
			t.Errorf("Petname = %q, want %q", target.Petname, "clever-penguin")
		}
	})

	t.Run("discovers by @name", func(t *testing.T) {
		target, err := ResolveTarget(ctx, "@alice", func(ctx context.Context) (*TargetSet, error) {
			return NewTargetSet([]Target{
				{Name: "alice", Petname: "happy-dolphin"},
			}), nil
		})
		if err != nil {
			t.Fatalf("ResolveTarget() error = %v", err)
		}
		if target.Name != "alice" {
			t.Errorf("Name = %q, want %q", target.Name, "alice")
		}
	})

	t.Run("returns error when not found", func(t *testing.T) {
		_, err := ResolveTarget(ctx, "nonexistent", func(ctx context.Context) (*TargetSet, error) {
			return NewTargetSet([]Target{
				{Petname: "happy-dolphin"},
			}), nil
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "target not found: nonexistent" {
			t.Errorf("error = %q, want %q", err.Error(), "target not found: nonexistent")
		}
	})

	t.Run("returns discover error", func(t *testing.T) {
		_, err := ResolveTarget(ctx, "some-name", func(ctx context.Context) (*TargetSet, error) {
			return nil, context.DeadlineExceeded
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
