package relay

import (
	"errors"
	"sync"

	arccel "github.com/gezibash/arc/v2/internal/cel"
	"github.com/gezibash/arc/v2/pkg/identity"
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
	pubkeyHex := identity.EncodePublicKey(s.sender)
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
	pubkeyHex := identity.EncodePublicKey(s.sender)
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
// Uses exact-match semantics: all string-valued subscription labels must be present in the envelope.
// Non-string subscription labels are skipped during routing (they participate in discovery only).
func (t *Table) LookupLabels(labels map[string]string) []*Subscriber {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var matches []*Subscriber
	seen := make(map[string]bool)

	for _, s := range t.subscribers {
		subs := s.Subscriptions()
		for _, data := range subs {
			if matchesLabels(data.Labels, labels) && !seen[s.id] {
				matches = append(matches, s)
				seen[s.id] = true
			}
		}
	}
	return matches
}

// matchesLabels returns true if all string-valued subscription labels match the envelope labels.
// Non-string subscription labels are skipped (they participate in discovery, not routing).
func matchesLabels(subLabels map[string]any, envLabels map[string]string) bool {
	if len(subLabels) == 0 {
		return true
	}
	for k, v := range subLabels {
		sv, ok := v.(string)
		if !ok {
			continue // non-string labels don't participate in routing
		}
		if envLabels[k] != sv {
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

// DiscoveryResult represents a single subscription match for discovery.
type DiscoveryResult struct {
	Subscriber     *Subscriber
	SubscriptionID string
	Labels         map[string]any
	State          map[string]any
}

// Discover finds subscriptions where all filter labels are present in the subscription labels.
// This is inverted from routing: for discovery, filter ⊆ subLabels (filter is subset of subscription).
// Returns results up to limit (0 = 100 default) and total count of matches.
func (t *Table) Discover(filter map[string]string, limit int) ([]DiscoveryResult, int) {
	if limit <= 0 {
		limit = 100
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	var results []DiscoveryResult
	total := 0

	for _, s := range t.subscribers {
		subs := s.Subscriptions()
		for subID, data := range subs {
			if matchesFilter(filter, data.Labels) {
				total++
				if len(results) < limit {
					results = append(results, DiscoveryResult{
						Subscriber:     s,
						SubscriptionID: subID,
						Labels:         data.Labels,
						State:          data.State,
					})
				}
			}
		}
	}
	return results, total
}

// DiscoverCEL finds subscriptions matching a CEL expression against merged labels ∪ state.
// Returns results up to limit (0 = 100 default) and total count of matches.
func (t *Table) DiscoverCEL(expr string, limit int) ([]DiscoveryResult, int, error) {
	if limit <= 0 {
		limit = 100
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	// Collect all unique keys across subscriptions for CEL environment.
	keys := make(map[string]bool)
	type subEntry struct {
		sub    *Subscriber
		subID  string
		labels map[string]any
		state  map[string]any
		merged map[string]any
	}
	var entries []subEntry

	for _, s := range t.subscribers {
		subs := s.Subscriptions()
		for subID, data := range subs {
			merged := make(map[string]any, len(data.Labels)+len(data.State))
			for k, v := range data.State {
				merged[k] = v
			}
			for k, v := range data.Labels {
				merged[k] = v // labels win on conflict
				keys[k] = true
			}
			for k := range data.State {
				keys[k] = true
			}
			entries = append(entries, subEntry{s, subID, data.Labels, data.State, merged})
		}
	}

	filter, err := arccel.Compile(expr, keys)
	if err != nil {
		return nil, 0, err
	}

	var results []DiscoveryResult
	total := 0
	for _, e := range entries {
		if filter.Match(e.merged) {
			total++
			if len(results) < limit {
				results = append(results, DiscoveryResult{
					Subscriber:     e.sub,
					SubscriptionID: e.subID,
					Labels:         e.labels,
					State:          e.state,
				})
			}
		}
	}
	return results, total, nil
}

// matchesFilter returns true if all filter labels are present in subLabels (filter ⊆ subLabels).
// Compares string filter values against string-valued entries in the typed sub labels.
func matchesFilter(filter map[string]string, subLabels map[string]any) bool {
	for k, v := range filter {
		sv, ok := subLabels[k]
		if !ok {
			return false
		}
		s, ok := sv.(string)
		if !ok || s != v {
			return false
		}
	}
	return true
}
