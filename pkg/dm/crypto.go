package dm

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"io"
	"sort"

	"filippo.io/edwards25519"
	"github.com/gezibash/arc/pkg/identity"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/secretbox"
)

const nonceSize = 24

// deriveSharedKey converts both Ed25519 keys to Curve25519 and performs X25519 DH,
// then hashes the shared secret with SHA-256.
func deriveSharedKey(kp *identity.Keypair, peerPub identity.PublicKey) ([32]byte, error) {
	privCurve := ed25519SeedToCurve25519Private(kp.Seed())
	pubCurve, err := ed25519PublicToCurve25519(peerPub)
	if err != nil {
		return [32]byte{}, err
	}

	shared, err := curve25519.X25519(privCurve, pubCurve)
	if err != nil {
		return [32]byte{}, err
	}

	return sha256.Sum256(shared), nil
}

// ConversationID produces a deterministic hex string from two public keys,
// independent of argument order.
func ConversationID(a, b identity.PublicKey) string {
	keys := [][]byte{a[:], b[:]}
	sort.Slice(keys, func(i, j int) bool {
		for k := 0; k < len(keys[i]); k++ {
			if keys[i][k] != keys[j][k] {
				return keys[i][k] < keys[j][k]
			}
		}
		return false
	})
	h := sha256.New()
	h.Write(keys[0])
	h.Write(keys[1])
	return hex.EncodeToString(h.Sum(nil))
}

func encrypt(plaintext []byte, key *[32]byte) ([]byte, error) {
	var nonce [nonceSize]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, err
	}
	return secretbox.Seal(nonce[:], plaintext, &nonce, key), nil
}

func decrypt(ciphertext []byte, key *[32]byte) ([]byte, error) {
	if len(ciphertext) < nonceSize+secretbox.Overhead {
		return nil, errors.New("ciphertext too short")
	}
	var nonce [nonceSize]byte
	copy(nonce[:], ciphertext[:nonceSize])
	plaintext, ok := secretbox.Open(nil, ciphertext[nonceSize:], &nonce, key)
	if !ok {
		return nil, errors.New("decryption failed")
	}
	return plaintext, nil
}

// ed25519SeedToCurve25519Private derives a Curve25519 private key from an Ed25519 seed.
func ed25519SeedToCurve25519Private(seed []byte) []byte {
	h := sha512.Sum512(seed)
	h[0] &= 248
	h[31] &= 127
	h[31] |= 64
	return h[:32]
}

// ed25519PublicToCurve25519 converts an Ed25519 public key to a Curve25519 public key.
func ed25519PublicToCurve25519(pub identity.PublicKey) ([]byte, error) {
	p, err := new(edwards25519.Point).SetBytes(pub[:])
	if err != nil {
		return nil, err
	}
	return p.BytesMontgomery(), nil
}
