package addressbook

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// PubkeyHexLen is the length of a hex-encoded Ed25519 public key.
	PubkeyHexLen = 64
	// Filename is the addressbook file name within the data directory.
	Filename = "addressbook.json"
)

var (
	ErrNotFound    = errors.New("name not found in addressbook")
	ErrInvalidName = errors.New("invalid name")
	ErrInvalidKey  = errors.New("invalid public key")
	ErrNameExists  = errors.New("name already exists")
)

// Entry represents an addressbook entry.
type Entry struct {
	Name   string `json:"name"`
	Pubkey string `json:"pubkey"` // hex-encoded
}

// Addressbook manages name-to-pubkey mappings stored locally.
type Addressbook struct {
	path    string
	entries map[string]string // name → pubkey (hex)
	mu      sync.RWMutex
}

// New creates an addressbook using the given data directory.
func New(dataDir string) *Addressbook {
	return &Addressbook{
		path:    filepath.Join(dataDir, Filename),
		entries: make(map[string]string),
	}
}

// Load reads the addressbook from disk. Creates empty if not exists.
func (a *Addressbook) Load() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	data, err := os.ReadFile(a.path)
	if err != nil {
		if os.IsNotExist(err) {
			a.entries = make(map[string]string)
			return nil
		}
		return fmt.Errorf("read addressbook: %w", err)
	}

	if err := json.Unmarshal(data, &a.entries); err != nil {
		return fmt.Errorf("parse addressbook: %w", err)
	}

	return nil
}

// Save writes the addressbook to disk.
func (a *Addressbook) Save() error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(a.path), 0700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	data, err := json.MarshalIndent(a.entries, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal addressbook: %w", err)
	}

	if err := os.WriteFile(a.path, data, 0600); err != nil {
		return fmt.Errorf("write addressbook: %w", err)
	}

	return nil
}

// Add adds or updates an entry. Name should not include @ prefix.
func (a *Addressbook) Add(name, pubkey string) error {
	name = normalizeName(name)
	if name == "" {
		return ErrInvalidName
	}

	pubkey = strings.ToLower(pubkey)
	if !IsValidPubkey(pubkey) {
		return ErrInvalidKey
	}

	a.mu.Lock()
	a.entries[name] = pubkey
	a.mu.Unlock()

	return a.Save()
}

// Remove deletes an entry by name.
func (a *Addressbook) Remove(name string) error {
	name = normalizeName(name)

	a.mu.Lock()
	if _, ok := a.entries[name]; !ok {
		a.mu.Unlock()
		return ErrNotFound
	}
	delete(a.entries, name)
	a.mu.Unlock()

	return a.Save()
}

// Lookup returns the pubkey for a name, or ErrNotFound.
func (a *Addressbook) Lookup(name string) (string, error) {
	name = normalizeName(name)

	a.mu.RLock()
	defer a.mu.RUnlock()

	pubkey, ok := a.entries[name]
	if !ok {
		return "", ErrNotFound
	}
	return pubkey, nil
}

// List returns all entries sorted by name.
func (a *Addressbook) List() []Entry {
	a.mu.RLock()
	defer a.mu.RUnlock()

	entries := make([]Entry, 0, len(a.entries))
	for name, pubkey := range a.entries {
		entries = append(entries, Entry{Name: name, Pubkey: pubkey})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	return entries
}

// ResolveRecipient resolves a recipient string.
// If it starts with @, strips it and looks up in addressbook.
// Returns the resolved value (pubkey if found, name otherwise) and whether it was resolved.
func (a *Addressbook) ResolveRecipient(recipient string) (resolved string, fromAddressbook bool) {
	// Strip @ prefix if present
	name := normalizeName(recipient)
	if name == "" {
		return recipient, false
	}

	// Try addressbook lookup
	if pubkey, err := a.Lookup(name); err == nil {
		return pubkey, true
	}

	// Not in addressbook, return name for relay resolution
	return name, false
}

// IsValidPubkey checks if a string is a valid hex-encoded Ed25519 public key.
func IsValidPubkey(s string) bool {
	if len(s) != PubkeyHexLen {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// normalizeName strips @ prefix and lowercases.
func normalizeName(name string) string {
	name = strings.TrimPrefix(name, "@")
	name = strings.TrimSpace(name)
	return strings.ToLower(name)
}
