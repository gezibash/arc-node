package journal

import (
	"bytes"
	"testing"

	"github.com/gezibash/arc/v2/pkg/identity"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	key := deriveKey(kp)

	plaintext := []byte("hello, encrypted journal")
	ciphertext, err := encrypt(plaintext, &key)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(plaintext, ciphertext) {
		t.Fatal("ciphertext should differ from plaintext")
	}

	got, err := decrypt(ciphertext, &key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plaintext, got) {
		t.Fatalf("got %q, want %q", got, plaintext)
	}
}

func TestDecryptTooShort(t *testing.T) {
	var key [32]byte
	_, err := decrypt([]byte("short"), &key)
	if err == nil {
		t.Fatal("expected error for short ciphertext")
	}
}

func TestDecryptWrongKey(t *testing.T) {
	kp, _ := identity.Generate()
	key := deriveKey(kp)

	ciphertext, err := encrypt([]byte("secret"), &key)
	if err != nil {
		t.Fatal(err)
	}

	var wrongKey [32]byte
	_, err = decrypt(ciphertext, &wrongKey)
	if err == nil {
		t.Fatal("expected decryption failure with wrong key")
	}
}

func TestDeriveKeyDeterministic(t *testing.T) {
	kp, _ := identity.Generate()
	k1 := deriveKey(kp)
	k2 := deriveKey(kp)
	if k1 != k2 {
		t.Fatal("deriveKey should be deterministic")
	}
}
