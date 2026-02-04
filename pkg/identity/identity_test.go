package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestTryDecodePublicKey(t *testing.T) {
	// Create a valid 32-byte key for testing
	keyBytes := make([]byte, 32)
	for i := range keyBytes {
		keyBytes[i] = byte(i)
	}
	keyHex := hex.EncodeToString(keyBytes)

	tests := []struct {
		name    string
		input   string
		wantOK  bool
		wantHex string
	}{
		{
			name:    "algo:hex format",
			input:   "ed25519:" + keyHex,
			wantOK:  true,
			wantHex: keyHex,
		},
		{
			name:    "raw hex 64 chars",
			input:   keyHex,
			wantOK:  true,
			wantHex: keyHex,
		},
		{
			name:   "short string (name)",
			input:  "alice",
			wantOK: false,
		},
		{
			name:   "petname",
			input:  "clever-penguin",
			wantOK: false,
		},
		{
			name:   "empty string",
			input:  "",
			wantOK: false,
		},
		{
			name:   "short hex (not a pubkey)",
			input:  "abcdef",
			wantOK: false,
		},
		{
			name:   "colon but invalid hex",
			input:  "ed25519:not-hex-data",
			wantOK: false,
		},
		{
			name:    "secp256k1 prefix",
			input:   "secp256k1:" + keyHex,
			wantOK:  true,
			wantHex: keyHex,
		},
		{
			name:   "at-name",
			input:  "@bob",
			wantOK: false,
		},
		{
			name:    "64 char valid hex without prefix",
			input:   strings.Repeat("ab", 32),
			wantOK:  true,
			wantHex: strings.Repeat("ab", 32),
		},
		{
			name:   "63 char hex (too short, no colon)",
			input:  strings.Repeat("a", 63),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pk, ok := TryDecodePublicKey(tt.input)
			if ok != tt.wantOK {
				t.Errorf("TryDecodePublicKey(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
				return
			}
			if ok && tt.wantHex != "" {
				gotHex := hex.EncodeToString(pk.Bytes)
				if gotHex != tt.wantHex {
					t.Errorf("TryDecodePublicKey(%q) hex = %q, want %q", tt.input, gotHex, tt.wantHex)
				}
			}
		})
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	keyBytes := make([]byte, 32)
	for i := range keyBytes {
		keyBytes[i] = byte(i + 10)
	}
	pk := PublicKey{Algo: AlgEd25519, Bytes: keyBytes}

	encoded := EncodePublicKey(pk)
	decoded, err := DecodePublicKey(encoded)
	if err != nil {
		t.Fatalf("DecodePublicKey(%q) error = %v", encoded, err)
	}

	if decoded.Algo != pk.Algo {
		t.Errorf("Algo = %q, want %q", decoded.Algo, pk.Algo)
	}
	if hex.EncodeToString(decoded.Bytes) != hex.EncodeToString(pk.Bytes) {
		t.Error("Bytes mismatch after round-trip")
	}

	// TryDecode should also work on the encoded form
	tryPK, ok := TryDecodePublicKey(encoded)
	if !ok {
		t.Fatal("TryDecodePublicKey should succeed on encoded public key")
	}
	if tryPK.Algo != pk.Algo {
		t.Errorf("TryDecode Algo = %q, want %q", tryPK.Algo, pk.Algo)
	}
}

func TestDecodePublicKeyBytes(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		keyBytes := make([]byte, 32)
		for i := range keyBytes {
			keyBytes[i] = byte(i)
		}
		encoded := "ed25519:" + hex.EncodeToString(keyBytes)

		pk, err := DecodePublicKeyBytes([]byte(encoded))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pk.Algo != AlgEd25519 {
			t.Errorf("Algo = %q, want %q", pk.Algo, AlgEd25519)
		}
		if hex.EncodeToString(pk.Bytes) != hex.EncodeToString(keyBytes) {
			t.Error("Bytes mismatch")
		}
	})

	t.Run("invalid empty", func(t *testing.T) {
		_, err := DecodePublicKeyBytes([]byte(""))
		if err == nil {
			t.Error("expected error for empty input")
		}
	})

	t.Run("invalid hex", func(t *testing.T) {
		_, err := DecodePublicKeyBytes([]byte("ed25519:not-valid-hex"))
		if err == nil {
			t.Error("expected error for invalid hex")
		}
	})
}

func TestSignatureEncodeDecodeRoundTrip(t *testing.T) {
	sigBytes := make([]byte, 64)
	for i := range sigBytes {
		sigBytes[i] = byte(i * 3)
	}

	t.Run("ed25519", func(t *testing.T) {
		sig := Signature{Algo: AlgEd25519, Bytes: sigBytes}
		encoded := EncodeSignature(sig)
		decoded, err := DecodeSignature(encoded)
		if err != nil {
			t.Fatalf("DecodeSignature(%q) error = %v", encoded, err)
		}
		if decoded.Algo != AlgEd25519 {
			t.Errorf("Algo = %q, want %q", decoded.Algo, AlgEd25519)
		}
		if hex.EncodeToString(decoded.Bytes) != hex.EncodeToString(sigBytes) {
			t.Error("Bytes mismatch after round-trip")
		}
	})

	t.Run("secp256k1", func(t *testing.T) {
		sig := Signature{Algo: AlgSecp256k1, Bytes: sigBytes}
		encoded := EncodeSignature(sig)
		decoded, err := DecodeSignature(encoded)
		if err != nil {
			t.Fatalf("DecodeSignature(%q) error = %v", encoded, err)
		}
		if decoded.Algo != AlgSecp256k1 {
			t.Errorf("Algo = %q, want %q", decoded.Algo, AlgSecp256k1)
		}
		if hex.EncodeToString(decoded.Bytes) != hex.EncodeToString(sigBytes) {
			t.Error("Bytes mismatch after round-trip")
		}
	})

	t.Run("empty algo defaults to ed25519", func(t *testing.T) {
		sig := Signature{Algo: "", Bytes: sigBytes}
		encoded := EncodeSignature(sig)
		if !strings.HasPrefix(encoded, "ed25519:") {
			t.Errorf("expected ed25519 prefix for empty algo, got %q", encoded)
		}
		decoded, err := DecodeSignature(encoded)
		if err != nil {
			t.Fatalf("DecodeSignature(%q) error = %v", encoded, err)
		}
		if decoded.Algo != AlgEd25519 {
			t.Errorf("Algo = %q, want %q", decoded.Algo, AlgEd25519)
		}
	})
}

func TestDecodeSignature(t *testing.T) {
	sigBytes := make([]byte, 64)
	for i := range sigBytes {
		sigBytes[i] = byte(i)
	}
	sigHex := hex.EncodeToString(sigBytes)

	t.Run("valid with ed25519 prefix", func(t *testing.T) {
		sig, err := DecodeSignature("ed25519:" + sigHex)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sig.Algo != AlgEd25519 {
			t.Errorf("Algo = %q, want %q", sig.Algo, AlgEd25519)
		}
		if hex.EncodeToString(sig.Bytes) != sigHex {
			t.Error("Bytes mismatch")
		}
	})

	t.Run("valid with secp256k1 prefix", func(t *testing.T) {
		sig, err := DecodeSignature("secp256k1:" + sigHex)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sig.Algo != AlgSecp256k1 {
			t.Errorf("Algo = %q, want %q", sig.Algo, AlgSecp256k1)
		}
	})

	t.Run("without prefix defaults to ed25519", func(t *testing.T) {
		sig, err := DecodeSignature(sigHex)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sig.Algo != AlgEd25519 {
			t.Errorf("Algo = %q, want %q", sig.Algo, AlgEd25519)
		}
		if hex.EncodeToString(sig.Bytes) != sigHex {
			t.Error("Bytes mismatch")
		}
	})

	t.Run("empty string", func(t *testing.T) {
		_, err := DecodeSignature("")
		if err == nil {
			t.Error("expected error for empty string")
		}
		if !errors.Is(err, ErrInvalidEncoding) {
			t.Errorf("error = %v, want ErrInvalidEncoding", err)
		}
	})

	t.Run("invalid hex", func(t *testing.T) {
		_, err := DecodeSignature("ed25519:zzzz-not-hex")
		if err == nil {
			t.Error("expected error for invalid hex")
		}
		if !errors.Is(err, ErrInvalidEncoding) {
			t.Errorf("error = %v, want ErrInvalidEncoding", err)
		}
	})
}

func TestDecodeSignatureBytes(t *testing.T) {
	sigBytes := make([]byte, 64)
	for i := range sigBytes {
		sigBytes[i] = byte(i + 5)
	}
	sigHex := hex.EncodeToString(sigBytes)

	t.Run("valid", func(t *testing.T) {
		sig, err := DecodeSignatureBytes([]byte("ed25519:" + sigHex))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sig.Algo != AlgEd25519 {
			t.Errorf("Algo = %q, want %q", sig.Algo, AlgEd25519)
		}
		if hex.EncodeToString(sig.Bytes) != sigHex {
			t.Error("Bytes mismatch")
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := DecodeSignatureBytes([]byte(""))
		if err == nil {
			t.Error("expected error for empty input")
		}
	})

	t.Run("matches DecodeSignature", func(t *testing.T) {
		input := "secp256k1:" + sigHex
		fromString, errS := DecodeSignature(input)
		fromBytes, errB := DecodeSignatureBytes([]byte(input))
		if !errors.Is(errS, errB) {
			t.Fatalf("error mismatch: string=%v bytes=%v", errS, errB)
		}
		if fromString.Algo != fromBytes.Algo {
			t.Errorf("Algo mismatch: string=%q bytes=%q", fromString.Algo, fromBytes.Algo)
		}
		if hex.EncodeToString(fromString.Bytes) != hex.EncodeToString(fromBytes.Bytes) {
			t.Error("Bytes mismatch between string and bytes variants")
		}
	})
}

func TestVerify(t *testing.T) {
	// Generate a real Ed25519 keypair using crypto/ed25519.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	payload := []byte("test message for signing")
	sigRaw := ed25519.Sign(priv, payload)

	pk := PublicKey{Algo: AlgEd25519, Bytes: pub}
	sig := Signature{Algo: AlgEd25519, Bytes: sigRaw}

	t.Run("valid signature", func(t *testing.T) {
		if !Verify(pk, payload, sig) {
			t.Error("expected valid signature to verify")
		}
	})

	t.Run("wrong payload", func(t *testing.T) {
		if Verify(pk, []byte("different payload"), sig) {
			t.Error("expected verification to fail with wrong payload")
		}
	})

	t.Run("wrong key", func(t *testing.T) {
		otherPub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("GenerateKey: %v", err)
		}
		otherPK := PublicKey{Algo: AlgEd25519, Bytes: otherPub}
		if Verify(otherPK, payload, sig) {
			t.Error("expected verification to fail with wrong key")
		}
	})

	t.Run("mismatched algo", func(t *testing.T) {
		// Public key says secp256k1 but signature says ed25519.
		mismatchPK := PublicKey{Algo: AlgSecp256k1, Bytes: pub}
		if Verify(mismatchPK, payload, sig) {
			t.Error("expected verification to fail with mismatched algorithms")
		}
	})

	t.Run("unknown algo", func(t *testing.T) {
		unknownPK := PublicKey{Algo: Algorithm("unknown"), Bytes: pub}
		unknownSig := Signature{Algo: Algorithm("unknown"), Bytes: sigRaw}
		if Verify(unknownPK, payload, unknownSig) {
			t.Error("expected verification to fail with unknown algorithm")
		}
	})
}

// mockSigner is a minimal Signer for testing ProviderFunc.
type mockSigner struct {
	pk PublicKey
}

func (m *mockSigner) PublicKey() PublicKey           { return m.pk }
func (m *mockSigner) Sign([]byte) (Signature, error) { return Signature{}, nil }
func (m *mockSigner) Algorithm() Algorithm           { return m.pk.Algo }

func TestProviderFunc(t *testing.T) {
	t.Run("returns signer", func(t *testing.T) {
		expected := &mockSigner{
			pk: PublicKey{Algo: AlgEd25519, Bytes: []byte("test-key-bytes")},
		}
		pf := ProviderFunc(func(_ context.Context) (Signer, error) {
			return expected, nil
		})

		signer, err := pf.Load(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if signer != expected {
			t.Error("expected returned signer to match")
		}
		if signer.Algorithm() != AlgEd25519 {
			t.Errorf("Algorithm = %q, want %q", signer.Algorithm(), AlgEd25519)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		expectedErr := errors.New("key not found")
		pf := ProviderFunc(func(_ context.Context) (Signer, error) {
			return nil, expectedErr
		})

		signer, err := pf.Load(context.Background())
		if signer != nil {
			t.Error("expected nil signer on error")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("error = %v, want %v", err, expectedErr)
		}
	})
}
