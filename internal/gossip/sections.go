package gossip

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"

	arccel "github.com/gezibash/arc/v2/internal/cel"
)

// =============================================================================
// RelaysSection — tracks relay membership
// =============================================================================

// RelayEntry describes a relay node in the cluster.
type RelayEntry struct {
	Name     string
	GRPCAddr string
	Pubkey   string
	JoinedAt int64 // unix nanos
}

// RelaysSection tracks relay membership via gossip.
type RelaysSection struct {
	mu      sync.RWMutex
	entries map[string]*RelayEntry // name → entry
}

// NewRelaysSection creates an empty relays section.
func NewRelaysSection() *RelaysSection {
	return &RelaysSection{
		entries: make(map[string]*RelayEntry),
	}
}

func (s *RelaysSection) ID() SectionID { return SectionRelays }

// Add registers or updates a relay entry.
func (s *RelaysSection) Add(entry *RelayEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[entry.Name] = entry
}

// Remove deletes a relay entry.
func (s *RelaysSection) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, name)
}

// Get returns a relay entry by name.
func (s *RelaysSection) Get(name string) (*RelayEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[name]
	return e, ok
}

// All returns a snapshot of all relay entries.
func (s *RelaysSection) All() []*RelayEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*RelayEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}
	return out
}

// Encode serializes all relay entries.
// Format: [count:4][name:lenprefix][grpcAddr:lenprefix][pubkey:lenprefix][joinedAt:8]...
func (s *RelaysSection) Encode() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buf []byte
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s.entries)))

	for _, e := range s.entries {
		buf = appendString(buf, e.Name)
		buf = appendString(buf, e.GRPCAddr)
		buf = appendString(buf, e.Pubkey)
		buf = binary.BigEndian.AppendUint64(buf, uint64(e.JoinedAt))
	}
	return buf
}

// Merge decodes and merges remote relay entries.
// Uses "latest wins" — entry with later JoinedAt replaces existing.
func (s *RelaysSection) Merge(data []byte) error {
	entries, err := decodeRelayEntries(data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, remote := range entries {
		if existing, ok := s.entries[remote.Name]; ok {
			if remote.JoinedAt > existing.JoinedAt {
				s.entries[remote.Name] = remote
			}
		} else {
			s.entries[remote.Name] = remote
		}
	}
	return nil
}

func decodeRelayEntries(data []byte) ([]*RelayEntry, error) {
	if len(data) < 4 {
		return nil, nil
	}
	count := binary.BigEndian.Uint32(data[:4])
	pos := 4

	entries := make([]*RelayEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		name, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		grpcAddr, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		pubkey, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		if pos+8 > len(data) {
			return nil, errTruncated
		}
		joinedAt := int64(binary.BigEndian.Uint64(data[pos:]))
		pos += 8

		entries = append(entries, &RelayEntry{
			Name:     name,
			GRPCAddr: grpcAddr,
			Pubkey:   pubkey,
			JoinedAt: joinedAt,
		})
	}
	return entries, nil
}

// =============================================================================
// CapabilitiesSection — tracks capability subscriptions across the cluster
// =============================================================================

// CapabilityEntry describes a capability subscription on a relay.
type CapabilityEntry struct {
	RelayName      string
	ProviderPubkey string
	SubID          string
	Labels         map[string]any // structural labels (gossip on change)
	State          map[string]any // dynamic metrics (gossip on timer)
	ProviderName   string         // @name if registered
	LatencyNs      int64          // relay → capability RTT in nanoseconds
}

// capKey uniquely identifies a capability entry: relay + provider + subID.
func capKey(relayName, providerPubkey, subID string) string {
	return relayName + "\x00" + providerPubkey + "\x00" + subID
}

// CapabilitiesSection tracks capability subscriptions across the cluster.
type CapabilitiesSection struct {
	mu      sync.RWMutex
	entries map[string]*CapabilityEntry // capKey → entry
}

// NewCapabilitiesSection creates an empty capabilities section.
func NewCapabilitiesSection() *CapabilitiesSection {
	return &CapabilitiesSection{
		entries: make(map[string]*CapabilityEntry),
	}
}

func (s *CapabilitiesSection) ID() SectionID { return SectionCapabilities }

// Add registers a capability entry.
func (s *CapabilitiesSection) Add(entry *CapabilityEntry) {
	key := capKey(entry.RelayName, entry.ProviderPubkey, entry.SubID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = entry
}

// Remove deletes a capability entry.
func (s *CapabilitiesSection) Remove(relayName, providerPubkey, subID string) {
	key := capKey(relayName, providerPubkey, subID)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
}

// PurgeRelay removes all entries for a relay.
func (s *CapabilitiesSection) PurgeRelay(relayName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, entry := range s.entries {
		if entry.RelayName == relayName {
			delete(s.entries, key)
		}
	}
}

// PurgeProvider removes all entries for a specific provider on a relay.
func (s *CapabilitiesSection) PurgeProvider(relayName, providerPubkey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, entry := range s.entries {
		if entry.RelayName == relayName && entry.ProviderPubkey == providerPubkey {
			delete(s.entries, key)
		}
	}
}

// UpdateProviderName sets the ProviderName on all entries matching the relay and pubkey.
func (s *CapabilitiesSection) UpdateProviderName(relayName, providerPubkey, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.entries {
		if entry.RelayName == relayName && entry.ProviderPubkey == providerPubkey {
			entry.ProviderName = name
		}
	}
}

// UpdateLatency sets LatencyNs on all entries for a provider on a given relay.
// Returns true if any entries were updated.
func (s *CapabilitiesSection) UpdateLatency(relayName, providerPubkey string, latencyNs int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	updated := false
	for _, entry := range s.entries {
		if entry.RelayName == relayName && entry.ProviderPubkey == providerPubkey {
			entry.LatencyNs = latencyNs
			updated = true
		}
	}
	return updated
}

// FindByPubkey returns the first entry matching the given provider pubkey.
// Used for pubkey-addressed forwarding.
func (s *CapabilitiesSection) FindByPubkey(pubkey string) (*CapabilityEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, entry := range s.entries {
		if entry.ProviderPubkey == pubkey {
			return entry, true
		}
	}
	return nil, false
}

// UpdateState merges state entries for a given subscription.
// Returns true if the subscription was found and updated.
func (s *CapabilitiesSection) UpdateState(relayName, providerPubkey, subID string, state map[string]any) bool {
	key := capKey(relayName, providerPubkey, subID)
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	if !ok {
		return false
	}
	if entry.State == nil {
		entry.State = make(map[string]any, len(state))
	}
	for k, v := range state {
		entry.State[k] = v
	}
	return true
}

// Match finds entries where all filter labels are present in the entry labels.
// Compares string filter values against string-valued entries in the typed labels.
func (s *CapabilitiesSection) Match(filter map[string]string, limit int) []*CapabilityEntry {
	if limit <= 0 {
		limit = 100
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*CapabilityEntry
	for _, entry := range s.entries {
		if matchLabels(filter, entry.Labels) {
			results = append(results, entry)
			if len(results) >= limit {
				break
			}
		}
	}
	return results
}

// MatchCEL finds entries matching a CEL expression against merged labels ∪ state.
func (s *CapabilitiesSection) MatchCEL(expr string, limit int) ([]*CapabilityEntry, error) {
	if limit <= 0 {
		limit = 100
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all unique keys across entries for CEL environment.
	keys := make(map[string]bool)
	for _, entry := range s.entries {
		for k := range entry.Labels {
			keys[k] = true
		}
		for k := range entry.State {
			keys[k] = true
		}
	}

	filter, err := arccel.Compile(expr, keys)
	if err != nil {
		return nil, err
	}

	var results []*CapabilityEntry
	for _, entry := range s.entries {
		merged := make(map[string]any, len(entry.Labels)+len(entry.State))
		for k, v := range entry.State {
			merged[k] = v
		}
		for k, v := range entry.Labels {
			merged[k] = v // labels win on conflict
		}
		if filter.Match(merged) {
			results = append(results, entry)
			if len(results) >= limit {
				break
			}
		}
	}
	return results, nil
}

// Encode serializes all capability entries.
func (s *CapabilitiesSection) Encode() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buf []byte
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s.entries)))

	for _, e := range s.entries {
		buf = appendString(buf, e.RelayName)
		buf = appendString(buf, e.ProviderPubkey)
		buf = appendString(buf, e.SubID)
		buf = appendString(buf, e.ProviderName)
		buf = encodeAnyMap(buf, e.Labels)
		buf = encodeAnyMap(buf, e.State)
		buf = binary.BigEndian.AppendUint64(buf, uint64(e.LatencyNs))
	}
	return buf
}

// Merge decodes and merges remote capability entries.
// Uses purge-and-replace per relay: all existing entries for relays present in
// the incoming data are removed first, then the new entries are added.
// This ensures that removals on the source relay propagate correctly.
func (s *CapabilitiesSection) Merge(data []byte) error {
	entries, err := decodeCapabilityEntries(data)
	if err != nil {
		return err
	}

	// Find all relay names in incoming data
	incomingRelays := make(map[string]bool)
	for _, e := range entries {
		incomingRelays[e.RelayName] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Purge existing entries for those relays
	for key, entry := range s.entries {
		if incomingRelays[entry.RelayName] {
			delete(s.entries, key)
		}
	}

	// Add incoming entries
	for _, remote := range entries {
		key := capKey(remote.RelayName, remote.ProviderPubkey, remote.SubID)
		s.entries[key] = remote
	}
	return nil
}

// MergeForRelay purges all entries for the source relay, then adds the incoming entries.
// This handles both additions and removals (empty data = relay has no capabilities).
func (s *CapabilitiesSection) MergeForRelay(sourceRelay string, data []byte) error {
	entries, err := decodeCapabilityEntries(data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Purge all existing entries for this relay
	for key, entry := range s.entries {
		if entry.RelayName == sourceRelay {
			delete(s.entries, key)
		}
	}

	// Add incoming entries
	for _, remote := range entries {
		key := capKey(remote.RelayName, remote.ProviderPubkey, remote.SubID)
		s.entries[key] = remote
	}
	return nil
}

func decodeCapabilityEntries(data []byte) ([]*CapabilityEntry, error) {
	if len(data) < 4 {
		return nil, nil
	}
	count := binary.BigEndian.Uint32(data[:4])
	pos := 4

	entries := make([]*CapabilityEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		relayName, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		providerPubkey, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		subID, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		providerName, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		labels, n, err := decodeAnyMap(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		state, n, err := decodeAnyMap(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		var latencyNs int64
		if pos+8 <= len(data) {
			latencyNs = int64(binary.BigEndian.Uint64(data[pos:]))
			pos += 8
		}

		entries = append(entries, &CapabilityEntry{
			RelayName:      relayName,
			ProviderPubkey: providerPubkey,
			SubID:          subID,
			Labels:         labels,
			State:          state,
			ProviderName:   providerName,
			LatencyNs:      latencyNs,
		})
	}
	return entries, nil
}

// =============================================================================
// NamesSection — tracks name registrations across the cluster
// =============================================================================

// NameEntry describes a name registration on a relay.
type NameEntry struct {
	Name      string
	RelayName string
	Pubkey    string
	UpdatedAt int64 // unix nanos, for conflict resolution
}

// NamesSection tracks name registrations across the cluster.
type NamesSection struct {
	mu      sync.RWMutex
	entries map[string]*NameEntry // name → entry
}

// NewNamesSection creates an empty names section.
func NewNamesSection() *NamesSection {
	return &NamesSection{
		entries: make(map[string]*NameEntry),
	}
}

func (s *NamesSection) ID() SectionID { return SectionNames }

// Add registers a name entry.
func (s *NamesSection) Add(entry *NameEntry) {
	if entry.UpdatedAt == 0 {
		entry.UpdatedAt = time.Now().UnixNano()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[entry.Name] = entry
}

// Remove deletes a name entry.
func (s *NamesSection) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, name)
}

// PurgeRelay removes all entries for a relay.
func (s *NamesSection) PurgeRelay(relayName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, entry := range s.entries {
		if entry.RelayName == relayName {
			delete(s.entries, name)
		}
	}
}

// Get returns a name entry.
func (s *NamesSection) Get(name string) (*NameEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[name]
	return e, ok
}

// Encode serializes all name entries.
func (s *NamesSection) Encode() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var buf []byte
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s.entries)))

	for _, e := range s.entries {
		buf = appendString(buf, e.Name)
		buf = appendString(buf, e.RelayName)
		buf = appendString(buf, e.Pubkey)
		buf = binary.BigEndian.AppendUint64(buf, uint64(e.UpdatedAt))
	}
	return buf
}

// Merge decodes and merges remote name entries.
// Uses purge-and-replace per relay for names from relays present in the incoming data,
// then applies "latest wins" for cross-relay name conflicts.
func (s *NamesSection) Merge(data []byte) error {
	entries, err := decodeNameEntries(data)
	if err != nil {
		return err
	}

	// Find all relay names in incoming data
	incomingRelays := make(map[string]bool)
	for _, e := range entries {
		incomingRelays[e.RelayName] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Purge existing entries for those relays
	for name, entry := range s.entries {
		if incomingRelays[entry.RelayName] {
			delete(s.entries, name)
		}
	}

	// Add incoming entries (latest wins for cross-relay conflicts)
	for _, remote := range entries {
		if existing, ok := s.entries[remote.Name]; ok {
			if remote.UpdatedAt > existing.UpdatedAt {
				s.entries[remote.Name] = remote
			}
		} else {
			s.entries[remote.Name] = remote
		}
	}
	return nil
}

// MergeForRelay purges all entries for the source relay, then adds the incoming entries.
func (s *NamesSection) MergeForRelay(sourceRelay string, data []byte) error {
	entries, err := decodeNameEntries(data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Purge all existing entries for this relay
	for name, entry := range s.entries {
		if entry.RelayName == sourceRelay {
			delete(s.entries, name)
		}
	}

	// Add incoming entries (latest wins for cross-relay conflicts)
	for _, remote := range entries {
		if existing, ok := s.entries[remote.Name]; ok {
			if remote.UpdatedAt > existing.UpdatedAt {
				s.entries[remote.Name] = remote
			}
		} else {
			s.entries[remote.Name] = remote
		}
	}
	return nil
}

func decodeNameEntries(data []byte) ([]*NameEntry, error) {
	if len(data) < 4 {
		return nil, nil
	}
	count := binary.BigEndian.Uint32(data[:4])
	pos := 4

	entries := make([]*NameEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		name, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		relayName, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		pubkey, n, err := readString(data, pos)
		if err != nil {
			return nil, err
		}
		pos += n

		if pos+8 > len(data) {
			return nil, errTruncated
		}
		updatedAt := int64(binary.BigEndian.Uint64(data[pos:]))
		pos += 8

		entries = append(entries, &NameEntry{
			Name:      name,
			RelayName: relayName,
			Pubkey:    pubkey,
			UpdatedAt: updatedAt,
		})
	}
	return entries, nil
}

// =============================================================================
// Helpers
// =============================================================================

var errTruncated = errors.New("truncated data")

// matchLabels returns true if all string filter labels are present in the entry labels.
// Compares filter strings against string-valued entries in the typed labels.
func matchLabels(filter map[string]string, labels map[string]any) bool {
	for k, v := range filter {
		lv, ok := labels[k]
		if !ok {
			return false
		}
		s, ok := lv.(string)
		if !ok || s != v {
			return false
		}
	}
	return true
}

// appendString writes a length-prefixed string.
func appendString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

// readString reads a length-prefixed string from data at offset.
// Returns the string, bytes consumed, and error.
func readString(data []byte, pos int) (string, int, error) {
	if pos+2 > len(data) {
		return "", 0, errTruncated
	}
	length := int(binary.BigEndian.Uint16(data[pos:]))
	pos2 := pos + 2
	if pos2+length > len(data) {
		return "", 0, errTruncated
	}
	return string(data[pos2 : pos2+length]), 2 + length, nil
}

// Type tags for encodeAnyMap wire format.
const (
	typeTagString  byte = 0
	typeTagInt64   byte = 1
	typeTagFloat64 byte = 2
	typeTagBool    byte = 3
)

// encodeAnyMap writes a typed map as [count:2][key:lenprefix][type:1][value:varies]...
// Type tags: 0=string(lenprefix), 1=int64(8B BE), 2=float64(8B IEEE754), 3=bool(1B).
func encodeAnyMap(buf []byte, m map[string]any) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(m)))
	for k, v := range m {
		buf = appendString(buf, k)
		switch val := v.(type) {
		case string:
			buf = append(buf, typeTagString)
			buf = appendString(buf, val)
		case int64:
			buf = append(buf, typeTagInt64)
			buf = binary.BigEndian.AppendUint64(buf, uint64(val))
		case float64:
			buf = append(buf, typeTagFloat64)
			buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(val))
		case bool:
			buf = append(buf, typeTagBool)
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		default:
			// Fallback: encode as string
			buf = append(buf, typeTagString)
			buf = appendString(buf, "")
		}
	}
	return buf
}

// decodeAnyMap reads a typed map from data at offset.
// Returns the map, bytes consumed, and error.
func decodeAnyMap(data []byte, pos int) (map[string]any, int, error) {
	if pos+2 > len(data) {
		return nil, 0, errTruncated
	}
	count := int(binary.BigEndian.Uint16(data[pos:]))
	consumed := 2

	m := make(map[string]any, count)
	for i := 0; i < count; i++ {
		key, n, err := readString(data, pos+consumed)
		if err != nil {
			return nil, 0, err
		}
		consumed += n

		if pos+consumed >= len(data) {
			return nil, 0, errTruncated
		}
		tag := data[pos+consumed]
		consumed++

		switch tag {
		case typeTagString:
			val, n, err := readString(data, pos+consumed)
			if err != nil {
				return nil, 0, err
			}
			consumed += n
			m[key] = val
		case typeTagInt64:
			if pos+consumed+8 > len(data) {
				return nil, 0, errTruncated
			}
			m[key] = int64(binary.BigEndian.Uint64(data[pos+consumed:]))
			consumed += 8
		case typeTagFloat64:
			if pos+consumed+8 > len(data) {
				return nil, 0, errTruncated
			}
			m[key] = math.Float64frombits(binary.BigEndian.Uint64(data[pos+consumed:]))
			consumed += 8
		case typeTagBool:
			if pos+consumed+1 > len(data) {
				return nil, 0, errTruncated
			}
			m[key] = data[pos+consumed] != 0
			consumed++
		default:
			return nil, 0, errors.New("unknown type tag in any map")
		}
	}
	return m, consumed, nil
}
