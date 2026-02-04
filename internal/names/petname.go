package names

import (
	"fmt"
	"strings"

	"github.com/tyler-smith/go-bip39"
)

// Petname generates a deterministic 3-word name from a public key using BIP-39.
// Same pubkey always produces the same name (e.g., "leader-monkey-parrot").
// Uses 3 words from a 2048-word vocabulary, giving ~8.5 billion combinations.
func Petname(pubkey []byte) string {
	if len(pubkey) < 16 {
		return "unknown"
	}
	// BIP-39 requires 16, 20, 24, 28, or 32 bytes of entropy.
	// Ed25519 keys are 32 bytes. Pad/truncate for safety.
	entropy := make([]byte, 32)
	copy(entropy, pubkey)
	mnemonic, err := bip39.NewMnemonic(entropy)
	if err != nil {
		return "unknown"
	}
	words := strings.Fields(mnemonic)
	if len(words) < 3 {
		return "unknown"
	}
	return words[0] + "-" + words[1] + "-" + words[2]
}

// PetnameFromHex generates a petname from a hex-encoded public key.
func PetnameFromHex(pubkeyHex string) string {
	pubkey, err := hexDecode(pubkeyHex)
	if err != nil {
		return "unknown"
	}
	return Petname(pubkey)
}

// PetnameFromEncoded generates a petname from an encoded public key.
// Accepts both "algo:hex" format (e.g., "ed25519:41d9...") and raw hex.
func PetnameFromEncoded(encoded string) string {
	hex := encoded
	if i := strings.Index(encoded, ":"); i >= 0 {
		hex = encoded[i+1:]
	}
	return PetnameFromHex(hex)
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd length hex string")
	}
	b := make([]byte, len(s)/2)
	for i := 0; i < len(b); i++ {
		hi := hexVal(s[i*2])
		lo := hexVal(s[i*2+1])
		if hi < 0 || lo < 0 {
			return nil, fmt.Errorf("invalid hex char")
		}
		b[i] = byte(hi<<4 | lo)
	}
	return b, nil
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c - 'a' + 10)
	case c >= 'A' && c <= 'F':
		return int(c - 'A' + 10)
	}
	return -1
}
