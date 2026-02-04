// Package ed25519 provides an identity.Signer implementation.
package ed25519

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// Keypair implements identity.Signer for Ed25519.
type Keypair struct {
	private ed25519.PrivateKey
	public  ed25519.PublicKey
}

// Generate creates a new random keypair.
func Generate() (*Keypair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Keypair{
		private: priv,
		public:  pub,
	}, nil
}

// FromSeed creates a keypair from a 32-byte seed.
func FromSeed(seed []byte) (*Keypair, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, errors.New("invalid seed length")
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub, _ := priv.Public().(ed25519.PublicKey)
	return &Keypair{
		private: priv,
		public:  pub,
	}, nil
}

// Seed returns the 32-byte seed for this keypair.
func (k *Keypair) Seed() []byte {
	return k.private.Seed()
}

// PublicKey returns the public key.
func (k *Keypair) PublicKey() identity.PublicKey {
	out := make([]byte, len(k.public))
	copy(out, k.public)
	return identity.PublicKey{Algo: identity.AlgEd25519, Bytes: out}
}

// Sign signs a payload.
func (k *Keypair) Sign(payload []byte) (identity.Signature, error) {
	sig := ed25519.Sign(k.private, payload)
	return identity.Signature{Algo: identity.AlgEd25519, Bytes: sig}, nil
}

// Algorithm returns the algorithm identifier.
func (k *Keypair) Algorithm() identity.Algorithm {
	return identity.AlgEd25519
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
