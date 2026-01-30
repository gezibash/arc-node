package dm

import (
	"bytes"
	"testing"

	"github.com/gezibash/arc/pkg/identity"
)

func TestDeriveSharedKeySymmetric(t *testing.T) {
	a, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	b, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	keyAB, err := deriveSharedKey(a, b.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	keyBA, err := deriveSharedKey(b, a.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	if keyAB != keyBA {
		t.Fatal("shared keys should be symmetric")
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	a, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	b, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	key, err := deriveSharedKey(a, b.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("hello, encrypted dm")
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
	a, _ := identity.Generate()
	b, _ := identity.Generate()
	key, _ := deriveSharedKey(a, b.PublicKey())

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

func TestConversationIDSymmetric(t *testing.T) {
	a, _ := identity.Generate()
	b, _ := identity.Generate()

	id1 := ConversationID(a.PublicKey(), b.PublicKey())
	id2 := ConversationID(b.PublicKey(), a.PublicKey())

	if id1 != id2 {
		t.Fatalf("conversation IDs should be order-independent: %s != %s", id1, id2)
	}
}

func TestConversationIDDeterministic(t *testing.T) {
	a, _ := identity.Generate()
	b, _ := identity.Generate()

	id1 := ConversationID(a.PublicKey(), b.PublicKey())
	id2 := ConversationID(a.PublicKey(), b.PublicKey())

	if id1 != id2 {
		t.Fatal("conversation ID should be deterministic")
	}
}
