package addressbook

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAddressbook(t *testing.T) {
	// Create temp directory
	dir := t.TempDir()

	ab := New(dir)

	// Load empty (should succeed)
	if err := ab.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Add entry
	pubkey := "7f3a8b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a"
	if err := ab.Add("alice", pubkey); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Lookup
	got, err := ab.Lookup("alice")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if got != pubkey {
		t.Errorf("Lookup returned %q, want %q", got, pubkey)
	}

	// Lookup with @ prefix
	got, err = ab.Lookup("@alice")
	if err != nil {
		t.Fatalf("Lookup with @ failed: %v", err)
	}
	if got != pubkey {
		t.Errorf("Lookup with @ returned %q, want %q", got, pubkey)
	}

	// List
	entries := ab.List()
	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want 1", len(entries))
	}
	if entries[0].Name != "alice" || entries[0].Pubkey != pubkey {
		t.Errorf("List returned unexpected entry: %+v", entries[0])
	}

	// Verify file was created
	if _, err := os.Stat(filepath.Join(dir, Filename)); os.IsNotExist(err) {
		t.Error("Addressbook file not created")
	}

	// Remove
	if err := ab.Remove("alice"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Lookup should fail
	if _, err := ab.Lookup("alice"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Lookup after remove: got %v, want ErrNotFound", err)
	}
}

func TestResolveRecipient(t *testing.T) {
	dir := t.TempDir()
	ab := New(dir)
	_ = ab.Load()

	pubkey := "7f3a8b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a"
	_ = ab.Add("alice", pubkey)

	tests := []struct {
		input      string
		wantResult string
		wantFromAB bool
	}{
		{"@alice", pubkey, true},
		{"alice", pubkey, true},
		{"@bob", "bob", false},  // not in addressbook
		{"bob", "bob", false},   // not in addressbook
		{pubkey, pubkey, false}, // already a pubkey
	}

	for _, tt := range tests {
		result, fromAB := ab.ResolveRecipient(tt.input)
		if result != tt.wantResult || fromAB != tt.wantFromAB {
			t.Errorf("ResolveRecipient(%q) = %q, %v; want %q, %v",
				tt.input, result, fromAB, tt.wantResult, tt.wantFromAB)
		}
	}
}

func TestIsValidPubkey(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"7f3a8b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a", true},
		{"7F3A8B9C2D1E4F5A6B7C8D9E0F1A2B3C4D5E6F7A8B9C0D1E2F3A4B5C6D7E8F9A", true}, // uppercase ok
		{"alice", false}, // too short
		{"7f3a8b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9", false},   // 63 chars
		{"7f3a8b9c2d1e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9az", false}, // invalid hex
	}

	for _, tt := range tests {
		got := IsValidPubkey(tt.input)
		if got != tt.want {
			t.Errorf("IsValidPubkey(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
