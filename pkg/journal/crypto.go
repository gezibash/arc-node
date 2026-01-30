package journal

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"

	"github.com/gezibash/arc/pkg/identity"
	"golang.org/x/crypto/nacl/secretbox"
)

const nonceSize = 24

func deriveKey(kp *identity.Keypair) [32]byte {
	return sha256.Sum256(kp.Seed())
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
