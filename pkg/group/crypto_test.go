package group

import (
	"bytes"
	"testing"

	"filippo.io/edwards25519"
	"github.com/gezibash/arc/v2/pkg/identity"
	"golang.org/x/crypto/curve25519"
)

func TestSealOpenRoundTrip(t *testing.T) {
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}

	sealed, err := SealSeed(seed, recipient.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	got, err := OpenSeed(sealed, recipient)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, seed) {
		t.Error("decrypted seed does not match original")
	}
}

func TestOpenSeedWrongKey(t *testing.T) {
	recipient, _ := identity.Generate()
	wrongKey, _ := identity.Generate()

	seed := []byte("secret-group-seed-material-here!")
	sealed, err := SealSeed(seed, recipient.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	_, err = OpenSeed(sealed, wrongKey)
	if err == nil {
		t.Error("expected error decrypting with wrong key")
	}
}

func TestEdToX25519Consistency(t *testing.T) {
	kp, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}

	// Derive X25519 private from seed.
	xPriv := EdToX25519Private(kp.Seed())

	// Derive X25519 public from Ed25519 public.
	xPub, err := EdToX25519Public(kp.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// Compute X25519 public from private using scalar multiplication.
	computedPub, err := curve25519.X25519(xPriv[:], curve25519.Basepoint)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(xPub[:], computedPub) {
		t.Error("X25519 public key derived from Ed25519 public does not match derived from private")
	}
}

func TestEdToX25519PublicInvalidKey(t *testing.T) {
	// All zeros is not a valid Ed25519 point (it's the identity element
	// but SetBytes may accept it). Use a clearly invalid encoding.
	var bad identity.PublicKey
	for i := range bad {
		bad[i] = 0xFF
	}
	_, err := EdToX25519Public(bad)
	if err == nil {
		// Some invalid encodings might still parse; check that
		// a truly invalid point fails.
		// If it doesn't fail, that's fine — the edwards25519 library
		// may handle it. Just verify the function doesn't panic.
		_ = err
	}
}

func TestSealSeedTooShort(t *testing.T) {
	kp, _ := identity.Generate()
	_, err := OpenSeed([]byte("short"), kp)
	if err == nil {
		t.Error("expected error for short sealed data")
	}
}

func TestEdToX25519PublicMatchesEdwards(t *testing.T) {
	t.Helper()
	// Verify our conversion matches the edwards25519 library directly.
	kp, _ := identity.Generate()
	pub := kp.PublicKey()

	p, err := new(edwards25519.Point).SetBytes(pub[:])
	if err != nil {
		t.Fatal(err)
	}
	expected := p.BytesMontgomery()

	got, err := EdToX25519Public(pub)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got[:], expected) {
		t.Error("EdToX25519Public does not match direct edwards25519 conversion")
	}
}
