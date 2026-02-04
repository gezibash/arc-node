// Package secp256k1 provides an identity.Signer implementation.
package secp256k1

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"errors"
	"math/big"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// Keypair implements identity.Signer for secp256k1.
type Keypair struct {
	private *ecdsa.PrivateKey
}

// Generate creates a new random keypair.
func Generate() (*Keypair, error) {
	priv, err := ecdsa.GenerateKey(secp256k1Curve(), rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Keypair{private: priv}, nil
}

// FromSeed creates a keypair from a 32-byte seed.
func FromSeed(seed []byte) (*Keypair, error) {
	if len(seed) != 32 {
		return nil, errors.New("invalid seed length")
	}
	curve := secp256k1Curve()
	n := curve.Params().N
	d := new(big.Int).SetBytes(seed)
	d.Mod(d, n)
	if d.Sign() == 0 {
		return nil, errors.New("invalid seed")
	}
	priv := &ecdsa.PrivateKey{
		PublicKey: ecdsa.PublicKey{Curve: curve},
		D:         d,
	}
	priv.X, priv.Y = curve.ScalarBaseMult(d.Bytes())
	return &Keypair{private: priv}, nil
}

// PublicKey returns the public key (compressed).
func (k *Keypair) PublicKey() identity.PublicKey {
	pub := k.private.PublicKey
	encoded := elliptic.MarshalCompressed(pub.Curve, pub.X, pub.Y)
	return identity.PublicKey{Algo: identity.AlgSecp256k1, Bytes: encoded}
}

// Sign signs a payload with sha256 hashing.
func (k *Keypair) Sign(payload []byte) (identity.Signature, error) {
	hash := sha256.Sum256(payload)
	r, s, err := ecdsa.Sign(rand.Reader, k.private, hash[:])
	if err != nil {
		return identity.Signature{}, err
	}
	der, err := asn1.Marshal(ecdsaSignature{R: r, S: s})
	if err != nil {
		return identity.Signature{}, err
	}
	return identity.Signature{Algo: identity.AlgSecp256k1, Bytes: der}, nil
}

// Algorithm returns the algorithm identifier.
func (k *Keypair) Algorithm() identity.Algorithm {
	return identity.AlgSecp256k1
}

// Provider returns an identity.Provider that loads from a seed.
type Provider struct {
	Seed []byte
}

// Load implements identity.Provider.
func (p Provider) Load(_ context.Context) (identity.Signer, error) {
	if len(p.Seed) == 0 {
		return Generate()
	}
	return FromSeed(p.Seed)
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
