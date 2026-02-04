package gossip

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"
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
	Labels         map[string]string
	ProviderName   string // @name if registered
	LatencyNs      int64  // relay → capability RTT in nanoseconds
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

// Match finds entries where all filter labels are present in the entry labels.
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
		buf = encodeLabels(buf, e.Labels)
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

		labels, n, err := decodeLabels(data, pos)
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

// matchLabels returns true if all filter labels are present in the entry labels.
func matchLabels(filter, labels map[string]string) bool {
	for k, v := range filter {
		if labels[k] != v {
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

// encodeLabels writes labels as [count:2][key:lenprefix][value:lenprefix]...
func encodeLabels(buf []byte, labels map[string]string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(labels)))
	for k, v := range labels {
		buf = appendString(buf, k)
		buf = appendString(buf, v)
	}
	return buf
}

// decodeLabels reads labels from data at offset.
// Returns the labels, bytes consumed, and error.
func decodeLabels(data []byte, pos int) (map[string]string, int, error) {
	if pos+2 > len(data) {
		return nil, 0, errTruncated
	}
	count := int(binary.BigEndian.Uint16(data[pos:]))
	consumed := 2

	labels := make(map[string]string, count)
	for i := 0; i < count; i++ {
		key, n, err := readString(data, pos+consumed)
		if err != nil {
			return nil, 0, err
		}
		consumed += n

		val, n, err := readString(data, pos+consumed)
		if err != nil {
			return nil, 0, err
		}
		consumed += n

		labels[key] = val
	}
	return labels, consumed, nil
}
