package relay

import (
	"encoding/hex"
	"errors"
	"sync"
)

var (
	ErrNameTaken = errors.New("name already registered")
)

// Table manages subscriber registrations for routing.
type Table struct {
	mu          sync.RWMutex
	subscribers map[string]*Subscriber // id → subscriber
	names       map[string]*Subscriber // @name → subscriber (for "to" routing)
	pubkeys     map[string]*Subscriber // hex pubkey → subscriber (for pubkey routing)
}

// NewTable creates an empty subscription table.
func NewTable() *Table {
	return &Table{
		subscribers: make(map[string]*Subscriber),
		names:       make(map[string]*Subscriber),
		pubkeys:     make(map[string]*Subscriber),
	}
}

// Add registers a subscriber.
func (t *Table) Add(s *Subscriber) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subscribers[s.id] = s

	// Index by pubkey for direct addressing
	pubkeyHex := hex.EncodeToString(s.sender[:])
	t.pubkeys[pubkeyHex] = s
}

// Remove unregisters a subscriber and cleans up all associations.
func (t *Table) Remove(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s, ok := t.subscribers[id]
	if !ok {
		return
	}

	// Remove from names
	if s.name != "" {
		delete(t.names, s.name)
	}

	// Remove from pubkeys
	pubkeyHex := hex.EncodeToString(s.sender[:])
	delete(t.pubkeys, pubkeyHex)

	delete(t.subscribers, id)
}

// Get returns a subscriber by ID.
func (t *Table) Get(id string) (*Subscriber, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.subscribers[id]
	return s, ok
}

// RegisterName claims an addressed name for a subscriber.
func (t *Table) RegisterName(id, name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	s, ok := t.subscribers[id]
	if !ok {
		return nil
	}

	// Check if name is already taken
	if existing, taken := t.names[name]; taken && existing.id != id {
		return ErrNameTaken
	}

	// Remove old name if subscriber had one
	if s.name != "" && s.name != name {
		delete(t.names, s.name)
	}

	s.name = name
	t.names[name] = s
	return nil
}

// LookupName finds a subscriber by addressed name.
func (t *Table) LookupName(name string) (*Subscriber, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.names[name]
	return s, ok
}

// LookupPubkey finds a subscriber by hex-encoded public key.
func (t *Table) LookupPubkey(pubkeyHex string) (*Subscriber, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.pubkeys[pubkeyHex]
	return s, ok
}

// LookupLabels returns subscribers with subscriptions matching the given labels.
// Uses exact-match semantics: all subscription labels must be present in the envelope.
func (t *Table) LookupLabels(labels map[string]string) []*Subscriber {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var matches []*Subscriber
	seen := make(map[string]bool)

	for _, s := range t.subscribers {
		subs := s.Subscriptions()
		for _, subLabels := range subs {
			if matchesLabels(subLabels, labels) && !seen[s.id] {
				matches = append(matches, s)
				seen[s.id] = true
			}
		}
	}
	return matches
}

// matchesLabels returns true if all subscription labels match the envelope labels.
func matchesLabels(subLabels, envLabels map[string]string) bool {
	if len(subLabels) == 0 {
		return true // empty subscription matches all
	}
	for k, v := range subLabels {
		if envLabels[k] != v {
			return false
		}
	}
	return true
}

// All returns all registered subscribers.
func (t *Table) All() []*Subscriber {
	t.mu.RLock()
	defer t.mu.RUnlock()

	subs := make([]*Subscriber, 0, len(t.subscribers))
	for _, s := range t.subscribers {
		subs = append(subs, s)
	}
	return subs
}

// Count returns the number of registered subscribers.
func (t *Table) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.subscribers)
}
