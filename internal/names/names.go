// Package names manages local name-to-pubkey mappings.
//
// The names file (addressbook.json) maps @names to Ed25519 public keys.
// When sending to @alice, the client checks local names first before
// asking the relay for resolution.
package names

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/gezibash/arc/v2/pkg/identity"
)

const (
	// Filename is the names file name within the data directory.
	Filename = "addressbook.json"
)

var (
	ErrNotFound    = errors.New("name not found")
	ErrInvalidName = errors.New("invalid name")
	ErrInvalidKey  = errors.New("invalid public key")
	ErrNameExists  = errors.New("name already exists")
)

// Entry represents a name entry.
type Entry struct {
	Name   string `json:"name"`
	Pubkey string `json:"pubkey"` // hex-encoded
}

// Store manages name-to-pubkey mappings stored locally.
type Store struct {
	path    string
	entries map[string]string // name → pubkey (hex)
	mu      sync.RWMutex
}

// New creates a name store using the given data directory.
func New(dataDir string) *Store {
	return &Store{
		path:    filepath.Join(dataDir, Filename),
		entries: make(map[string]string),
	}
}

// Load reads the names from disk. Creates empty if not exists.
func (s *Store) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.entries = make(map[string]string)
			return nil
		}
		return fmt.Errorf("read names: %w", err)
	}

	if err := json.Unmarshal(data, &s.entries); err != nil {
		return fmt.Errorf("parse names: %w", err)
	}

	return nil
}

// Save writes the names to disk.
func (s *Store) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(s.path), 0700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	data, err := json.MarshalIndent(s.entries, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal names: %w", err)
	}

	if err := os.WriteFile(s.path, data, 0600); err != nil {
		return fmt.Errorf("write names: %w", err)
	}

	return nil
}

// Add adds or updates an entry. Name should not include @ prefix.
func (s *Store) Add(name, pubkey string) error {
	name = normalizeName(name)
	if name == "" {
		return ErrInvalidName
	}

	pubkey = strings.ToLower(pubkey)
	if !IsValidPubkey(pubkey) {
		return ErrInvalidKey
	}

	s.mu.Lock()
	s.entries[name] = pubkey
	s.mu.Unlock()

	return s.Save()
}

// Remove deletes an entry by name.
func (s *Store) Remove(name string) error {
	name = normalizeName(name)

	s.mu.Lock()
	if _, ok := s.entries[name]; !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	delete(s.entries, name)
	s.mu.Unlock()

	return s.Save()
}

// Lookup returns the pubkey for a name, or ErrNotFound.
func (s *Store) Lookup(name string) (string, error) {
	name = normalizeName(name)

	s.mu.RLock()
	defer s.mu.RUnlock()

	pubkey, ok := s.entries[name]
	if !ok {
		return "", ErrNotFound
	}
	return pubkey, nil
}

// List returns all entries sorted by name.
func (s *Store) List() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]Entry, 0, len(s.entries))
	for name, pubkey := range s.entries {
		entries = append(entries, Entry{Name: name, Pubkey: pubkey})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	return entries
}

// ResolveRecipient resolves a recipient string.
// If it starts with @, strips it and looks up in names.
// Returns the resolved value (pubkey if found, name otherwise) and whether it was resolved.
func (s *Store) ResolveRecipient(recipient string) (resolved string, fromNames bool) {
	// Strip @ prefix if present
	name := normalizeName(recipient)
	if name == "" {
		return recipient, false
	}

	// Try names lookup
	if pubkey, err := s.Lookup(name); err == nil {
		return pubkey, true
	}

	// Not in names, return name for relay resolution
	return name, false
}

// IsValidPubkey checks if a string is a valid encoded public key.
func IsValidPubkey(s string) bool {
	if strings.Contains(s, ":") || len(s) >= 64 {
		_, err := identity.DecodePublicKey(s)
		return err == nil
	}
	return false
}

// normalizeName strips @ prefix and lowercases.
func normalizeName(name string) string {
	name = strings.TrimPrefix(name, "@")
	name = strings.TrimSpace(name)
	return strings.ToLower(name)
}
