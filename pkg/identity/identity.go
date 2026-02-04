// Package identity provides pluggable public-key identity primitives.
package identity

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
)

// Algorithm identifies a signing algorithm.
type Algorithm string

const (
	AlgEd25519   Algorithm = "ed25519"
	AlgSecp256k1 Algorithm = "secp256k1"
)

// PublicKey is an algorithm-tagged public key.
type PublicKey struct {
	Algo  Algorithm
	Bytes []byte
}

// Signature is an algorithm-tagged signature.
type Signature struct {
	Algo  Algorithm
	Bytes []byte
}

// Signer represents a private key capable of signing.
type Signer interface {
	PublicKey() PublicKey
	Sign(payload []byte) (Signature, error)
	Algorithm() Algorithm
}

// Provider loads or generates a signer for a runtime.
type Provider interface {
	Load(ctx context.Context) (Signer, error)
}

// ProviderFunc adapts a function to a Provider.
type ProviderFunc func(ctx context.Context) (Signer, error)

// Load implements Provider.
func (f ProviderFunc) Load(ctx context.Context) (Signer, error) {
	return f(ctx)
}

var (
	// ErrUnknownAlgorithm indicates an unknown algorithm.
	ErrUnknownAlgorithm = errors.New("unknown algorithm")
	// ErrInvalidEncoding indicates an invalid encoded key/signature.
	ErrInvalidEncoding = errors.New("invalid encoding")
)

// EncodePublicKey encodes a public key as "algo:hex".
func EncodePublicKey(pk PublicKey) string {
	algo := strings.ToLower(string(pk.Algo))
	if algo == "" {
		algo = string(AlgEd25519)
	}
	return algo + ":" + hex.EncodeToString(pk.Bytes)
}

// DecodePublicKey decodes a public key from "algo:hex".
// If no algorithm prefix is present, defaults to ed25519 for compatibility.
func DecodePublicKey(s string) (PublicKey, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return PublicKey{}, ErrInvalidEncoding
	}
	algo, hexPart, ok := strings.Cut(s, ":")
	if !ok {
		algo = string(AlgEd25519)
		hexPart = s
	}
	algo = strings.ToLower(strings.TrimSpace(algo))
	raw, err := hex.DecodeString(hexPart)
	if err != nil {
		return PublicKey{}, ErrInvalidEncoding
	}
	return PublicKey{Algo: Algorithm(algo), Bytes: raw}, nil
}

// TryDecodePublicKey attempts to decode a public key from a string.
// Returns (zero, false) if the string doesn't look like a public key or fails to decode.
// Use this when a string might be a pubkey or might be a name/petname.
func TryDecodePublicKey(s string) (PublicKey, bool) {
	if !strings.Contains(s, ":") && len(s) < 64 {
		return PublicKey{}, false
	}
	pk, err := DecodePublicKey(s)
	if err != nil {
		return PublicKey{}, false
	}
	return pk, true
}

// DecodePublicKeyBytes decodes a public key from encoded bytes.
func DecodePublicKeyBytes(b []byte) (PublicKey, error) {
	return DecodePublicKey(string(b))
}

// EncodeSignature encodes a signature as "algo:hex".
func EncodeSignature(sig Signature) string {
	algo := strings.ToLower(string(sig.Algo))
	if algo == "" {
		algo = string(AlgEd25519)
	}
	return algo + ":" + hex.EncodeToString(sig.Bytes)
}

// DecodeSignature decodes a signature from "algo:hex".
// If no algorithm prefix is present, defaults to ed25519 for compatibility.
func DecodeSignature(s string) (Signature, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Signature{}, ErrInvalidEncoding
	}
	algo, hexPart, ok := strings.Cut(s, ":")
	if !ok {
		algo = string(AlgEd25519)
		hexPart = s
	}
	algo = strings.ToLower(strings.TrimSpace(algo))
	raw, err := hex.DecodeString(hexPart)
	if err != nil {
		return Signature{}, ErrInvalidEncoding
	}
	return Signature{Algo: Algorithm(algo), Bytes: raw}, nil
}

// DecodeSignatureBytes decodes a signature from encoded bytes.
func DecodeSignatureBytes(b []byte) (Signature, error) {
	return DecodeSignature(string(b))
}

// Verify checks a signature over the given payload.
func Verify(pub PublicKey, payload []byte, sig Signature) bool {
	algo := pub.Algo
	if algo == "" {
		algo = sig.Algo
	}
	if algo == "" {
		algo = AlgEd25519
	}
	if sig.Algo != "" && algo != sig.Algo {
		return false
	}

	switch algo {
	case AlgEd25519:
		return ed25519.Verify(pub.Bytes, payload, sig.Bytes)
	case AlgSecp256k1:
		curve := secp256k1Curve()
		x, y := elliptic.UnmarshalCompressed(curve, pub.Bytes)
		if x == nil || y == nil {
			return false
		}
		var parsed ecdsaSignature
		if _, err := asn1.Unmarshal(sig.Bytes, &parsed); err != nil {
			return false
		}
		if parsed.R == nil || parsed.S == nil {
			return false
		}
		key := ecdsa.PublicKey{Curve: curve, X: x, Y: y}
		hash := sha256.Sum256(payload)
		return ecdsa.Verify(&key, hash[:], parsed.R, parsed.S)
	default:
		return false
	}
}

type ecdsaSignature struct {
	R, S *big.Int
}

var secp256k1CurveCached elliptic.Curve

func secp256k1Curve() elliptic.Curve {
	if secp256k1CurveCached != nil {
		return secp256k1CurveCached
	}
	params := &elliptic.CurveParams{Name: "secp256k1"}
	params.P, _ = new(big.Int).SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F", 16)
	params.N, _ = new(big.Int).SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16)
	params.B = big.NewInt(7)
	params.Gx, _ = new(big.Int).SetString("79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798", 16)
	params.Gy, _ = new(big.Int).SetString("483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8", 16)
	params.BitSize = 256
	secp256k1CurveCached = params
	return params
}
