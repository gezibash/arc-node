package names

import (
	"strings"
	"testing"
)

func TestPetname_Deterministic(t *testing.T) {
	t.Helper()
	pubkey := make([]byte, 32)
	for i := range pubkey {
		pubkey[i] = byte(i)
	}
	name1 := Petname(pubkey)
	name2 := Petname(pubkey)
	if name1 != name2 {
		t.Errorf("same pubkey produced different names: %q vs %q", name1, name2)
	}
}

func TestPetname_ThreeWords(t *testing.T) {
	pubkey := make([]byte, 32)
	for i := range pubkey {
		pubkey[i] = byte(i * 7)
	}
	name := Petname(pubkey)
	parts := strings.Split(name, "-")
	if len(parts) != 3 {
		t.Errorf("expected 3 words, got %d: %q", len(parts), name)
	}
}

func TestPetname_DifferentKeys(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	key2[0] = 1

	name1 := Petname(key1)
	name2 := Petname(key2)
	if name1 == name2 {
		t.Errorf("different pubkeys produced same name: %q", name1)
	}
}

func TestPetname_ShortKey(t *testing.T) {
	short := make([]byte, 8)
	if name := Petname(short); name != "unknown" {
		t.Errorf("expected unknown for short key, got %q", name)
	}
}

func TestPetnameFromHex(t *testing.T) {
	// 32 bytes of zeros in hex
	hex := "0000000000000000000000000000000000000000000000000000000000000000"
	name := PetnameFromHex(hex)
	if name == "unknown" {
		t.Error("expected valid name from hex, got unknown")
	}
	parts := strings.Split(name, "-")
	if len(parts) != 3 {
		t.Errorf("expected 3 words, got %d: %q", len(parts), name)
	}
}

func TestPetnameFromEncoded(t *testing.T) {
	hex := "0102030405060708091011121314151617181920212223242526272829303132"
	direct := PetnameFromHex(hex)
	encoded := PetnameFromEncoded("ed25519:" + hex)
	if direct != encoded {
		t.Errorf("encoded wrapper gave different result: %q vs %q", direct, encoded)
	}
}

func TestPetnameFromHex_Invalid(t *testing.T) {
	if name := PetnameFromHex("xyz"); name != "unknown" {
		t.Errorf("expected unknown for invalid hex, got %q", name)
	}
}
