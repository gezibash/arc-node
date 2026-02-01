package group

import (
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"fmt"

	"filippo.io/edwards25519"
	"github.com/gezibash/arc/v2/pkg/identity"
	"golang.org/x/crypto/nacl/box"
)

// EdToX25519Private converts an Ed25519 seed to an X25519 private key.
// This mirrors the Ed25519 key expansion: SHA-512(seed)[:32] with clamping.
func EdToX25519Private(seed []byte) [32]byte {
	h := sha512.Sum512(seed)
	// Clamp per RFC 7748.
	h[0] &= 248
	h[31] &= 127
	h[31] |= 64
	var priv [32]byte
	copy(priv[:], h[:32])
	return priv
}

// EdToX25519Public converts an Ed25519 public key to an X25519 public key
// using the birational map from Edwards to Montgomery form.
func EdToX25519Public(pub identity.PublicKey) ([32]byte, error) {
	p, err := new(edwards25519.Point).SetBytes(pub[:])
	if err != nil {
		return [32]byte{}, fmt.Errorf("invalid Ed25519 public key: %w", err)
	}
	return *(*[32]byte)(p.BytesMontgomery()), nil
}

// SealSeed encrypts a group seed for a recipient identified by their
// Ed25519 public key. Output format: ephemeralPub(32) || nonce(24) || ciphertext.
// Uses NaCl box (X25519 + XSalsa20-Poly1305).
func SealSeed(seed []byte, recipientPub identity.PublicKey) ([]byte, error) {
	recipientX, err := EdToX25519Public(recipientPub)
	if err != nil {
		return nil, fmt.Errorf("convert recipient key: %w", err)
	}

	ephPub, ephPriv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral key: %w", err)
	}

	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	out := make([]byte, 32+24)
	copy(out[:32], ephPub[:])
	copy(out[32:56], nonce[:])
	out = box.Seal(out, seed, &nonce, &recipientX, ephPriv)
	return out, nil
}

// OpenSeed decrypts a sealed group seed using the recipient's Ed25519 keypair.
func OpenSeed(sealed []byte, recipientKP *identity.Keypair) ([]byte, error) {
	if len(sealed) < 32+24+box.Overhead {
		return nil, errors.New("sealed data too short")
	}

	var ephPub [32]byte
	copy(ephPub[:], sealed[:32])
	var nonce [24]byte
	copy(nonce[:], sealed[32:56])
	ciphertext := sealed[56:]

	recipientX := EdToX25519Private(recipientKP.Seed())
	plaintext, ok := box.Open(nil, ciphertext, &nonce, &ephPub, &recipientX)
	if !ok {
		return nil, ErrDecryptFailed
	}
	return plaintext, nil
}
