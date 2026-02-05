package capability

import (
	"time"

	"github.com/gezibash/arc/v2/pkg/identity"
)

// Target represents a discovered capability target.
// A target can be an app, person, service, device - anything addressable.
type Target struct {
	// Identity
	PublicKey identity.PublicKey
	Name      string // registered @name, if any
	Petname   string // docker-style name (e.g., "clever-penguin")

	// Capability metadata
	Labels map[string]any // advertised labels (typed: string, int64, float64, bool)
	State  map[string]any // dynamic metrics (typed)

	// Infrastructure metadata (measured by relay)
	Address           string             // direct connection address, from labels["direct"]
	RelayPubkey       identity.PublicKey // relay this target is connected to
	Latency           time.Duration      // relay → target RTT
	InterRelayLatency time.Duration      // local relay → remote relay RTT (0 for local)
	LastSeen          time.Time          // last activity
	Connected         time.Duration      // how long connected to relay
}

// DisplayName returns the best human-readable name for the target.
// Returns @name if registered, otherwise petname.
func (t *Target) DisplayName() string {
	if t.Name != "" {
		return "@" + t.Name
	}
	return t.Petname
}

// HasDirect returns true if the target has a direct connection address.
func (t *Target) HasDirect() bool {
	return t.Address != ""
}

// TargetSet is a collection of discovered targets.
// Supports filtering, picking, and iteration.
type TargetSet struct {
	targets []Target
}

// NewTargetSet creates a TargetSet from a slice of targets.
func NewTargetSet(targets []Target) *TargetSet {
	return &TargetSet{targets: targets}
}

// EmptyTargetSet returns an empty TargetSet.
func EmptyTargetSet() *TargetSet {
	return &TargetSet{targets: nil}
}

// TargetFromPubkey creates a TargetSet with a single explicit target.
// Use when you know the exact target (--to flag).
func TargetFromPubkey(pubkey identity.PublicKey) *TargetSet {
	return &TargetSet{
		targets: []Target{{PublicKey: pubkey}},
	}
}

// TargetFromAddress creates a TargetSet with a single target by direct address.
func TargetFromAddress(addr string) *TargetSet {
	return &TargetSet{
		targets: []Target{{Address: addr}},
	}
}

// Len returns the number of targets.
func (ts *TargetSet) Len() int {
	if ts == nil {
		return 0
	}
	return len(ts.targets)
}

// IsEmpty returns true if the TargetSet has no targets.
func (ts *TargetSet) IsEmpty() bool {
	return ts.Len() == 0
}

// All returns all targets.
func (ts *TargetSet) All() []Target {
	if ts == nil {
		return nil
	}
	return ts.targets
}

// First returns the first target, or nil if empty.
func (ts *TargetSet) First() *Target {
	if ts == nil || len(ts.targets) == 0 {
		return nil
	}
	return &ts.targets[0]
}

// Pick returns the first target matching the predicate, or nil if none match.
func (ts *TargetSet) Pick(fn func(Target) bool) *Target {
	if ts == nil {
		return nil
	}
	for i := range ts.targets {
		if fn(ts.targets[i]) {
			return &ts.targets[i]
		}
	}
	return nil
}

// Filter returns a new TargetSet with only targets matching the predicate.
func (ts *TargetSet) Filter(fn func(Target) bool) *TargetSet {
	if ts == nil {
		return EmptyTargetSet()
	}
	var filtered []Target
	for _, t := range ts.targets {
		if fn(t) {
			filtered = append(filtered, t)
		}
	}
	return &TargetSet{targets: filtered}
}

// WithDirect returns a new TargetSet with only targets that have direct addresses.
func (ts *TargetSet) WithDirect() *TargetSet {
	return ts.Filter(func(t Target) bool {
		return t.HasDirect()
	})
}

// Sort returns a new TargetSet sorted by the given less function.
// The sort is stable (insertion sort — usually small lists).
func (ts *TargetSet) Sort(less func(a, b Target) bool) *TargetSet {
	if ts == nil || len(ts.targets) <= 1 {
		return ts
	}
	sorted := make([]Target, len(ts.targets))
	copy(sorted, ts.targets)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && less(sorted[j], sorted[j-1]); j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	return &TargetSet{targets: sorted}
}

// Take returns a new TargetSet with at most n targets.
func (ts *TargetSet) Take(n int) *TargetSet {
	if ts == nil || len(ts.targets) <= n {
		return ts
	}
	taken := make([]Target, n)
	copy(taken, ts.targets[:n])
	return &TargetSet{targets: taken}
}

// SortByLatency returns a new TargetSet sorted by latency (lowest first).
func (ts *TargetSet) SortByLatency() *TargetSet {
	return ts.Sort(func(a, b Target) bool {
		return a.Latency < b.Latency
	})
}

// Int64Label returns a label value as int64, or 0 if missing/wrong type.
func (t *Target) Int64Label(key string) int64 {
	if v, ok := t.Labels[key].(int64); ok {
		return v
	}
	return 0
}

// Int64State returns a state value as int64, or 0 if missing/wrong type.
func (t *Target) Int64State(key string) int64 {
	if v, ok := t.State[key].(int64); ok {
		return v
	}
	return 0
}
