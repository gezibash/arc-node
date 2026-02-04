package gossip

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// SectionID identifies a gossip state section.
type SectionID uint8

const (
	SectionRelays       SectionID = 1
	SectionCapabilities SectionID = 2
	SectionNames        SectionID = 3
)

// Message type prefixes for wire format.
const (
	MsgTypeFullState uint8 = 1 // push/pull sync
	MsgTypeIncrement uint8 = 2 // broadcast delta
)

// Section is a pluggable state partition within gossip.
type Section interface {
	ID() SectionID
	Encode() []byte
	Merge(data []byte) error
}

// GossipState manages the extensible state registry.
type GossipState struct {
	mu       sync.RWMutex
	sections map[SectionID]Section
}

// NewGossipState creates an empty gossip state.
func NewGossipState() *GossipState {
	return &GossipState{
		sections: make(map[SectionID]Section),
	}
}

// Register adds a section to the state registry.
func (gs *GossipState) Register(s Section) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.sections[s.ID()] = s
}

// GetSection returns a section by ID.
func (gs *GossipState) GetSection(id SectionID) (Section, bool) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	s, ok := gs.sections[id]
	return s, ok
}

// EncodeAll encodes all sections into a full-state message.
// Wire format: [msgType:1][sectionID:1][dataLen:4][data]...
func (gs *GossipState) EncodeAll() []byte {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	var buf []byte
	buf = append(buf, MsgTypeFullState)

	for _, s := range gs.sections {
		data := s.Encode()
		buf = append(buf, byte(s.ID()))
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(data)))
		buf = append(buf, data...)
	}
	return buf
}

// MergeAll decodes a full-state message and merges each section.
func (gs *GossipState) MergeAll(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("empty state data")
	}
	if data[0] != MsgTypeFullState {
		return fmt.Errorf("unexpected message type: %d", data[0])
	}

	pos := 1
	for pos < len(data) {
		if pos+5 > len(data) {
			return fmt.Errorf("truncated section header at offset %d", pos)
		}
		sectionID := SectionID(data[pos])
		dataLen := binary.BigEndian.Uint32(data[pos+1 : pos+5])
		pos += 5

		if pos+int(dataLen) > len(data) {
			return fmt.Errorf("truncated section data for section %d", sectionID)
		}
		sectionData := data[pos : pos+int(dataLen)]
		pos += int(dataLen)

		gs.mu.RLock()
		s, ok := gs.sections[sectionID]
		gs.mu.RUnlock()
		if !ok {
			continue // unknown section, skip
		}

		if err := s.Merge(sectionData); err != nil {
			return fmt.Errorf("merge section %d: %w", sectionID, err)
		}
	}
	return nil
}

// ApplyIncrement decodes an increment message and merges the single section.
// Wire format: [msgType:1][sectionID:1][dataLen:4][data]
func (gs *GossipState) ApplyIncrement(data []byte) error {
	if len(data) < 6 {
		return fmt.Errorf("increment too short")
	}
	if data[0] != MsgTypeIncrement {
		return fmt.Errorf("unexpected message type: %d", data[0])
	}

	sectionID := SectionID(data[1])
	dataLen := binary.BigEndian.Uint32(data[2:6])
	if len(data) < 6+int(dataLen) {
		return fmt.Errorf("truncated increment data")
	}
	sectionData := data[6 : 6+int(dataLen)]

	gs.mu.RLock()
	s, ok := gs.sections[sectionID]
	gs.mu.RUnlock()
	if !ok {
		return nil // unknown section, skip
	}

	return s.Merge(sectionData)
}

// EncodeIncrement encodes a single section as an increment message for broadcast.
func EncodeIncrement(s Section) []byte {
	data := s.Encode()
	buf := make([]byte, 0, 6+len(data))
	buf = append(buf, MsgTypeIncrement)
	buf = append(buf, byte(s.ID()))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(data)))
	buf = append(buf, data...)
	return buf
}

// RelayScoped sections support purge-and-replace merge keyed by source relay.
// MsgTypeRelayIncrement wire format:
//
//	[msgType:1][sectionID:1][sourceLen:2][source][dataLen:4][data]
const MsgTypeRelayIncrement uint8 = 3

// EncodeRelayIncrement encodes a relay-scoped section increment.
// The source relay name enables purge-and-replace on the receiving end.
func EncodeRelayIncrement(s Section, sourceRelay string) []byte {
	data := s.Encode()
	src := []byte(sourceRelay)
	buf := make([]byte, 0, 8+len(src)+len(data))
	buf = append(buf, MsgTypeRelayIncrement)
	buf = append(buf, byte(s.ID()))
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(src)))
	buf = append(buf, src...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(data)))
	buf = append(buf, data...)
	return buf
}

// RelayScopedSection extends Section with relay-scoped merge.
type RelayScopedSection interface {
	Section
	MergeForRelay(sourceRelay string, data []byte) error
}

// ApplyRelayIncrement decodes a relay-scoped increment and merges.
func (gs *GossipState) ApplyRelayIncrement(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("relay increment too short")
	}

	sectionID := SectionID(data[1])
	srcLen := int(binary.BigEndian.Uint16(data[2:4]))
	if len(data) < 4+srcLen+4 {
		return fmt.Errorf("truncated relay increment")
	}
	sourceRelay := string(data[4 : 4+srcLen])
	pos := 4 + srcLen

	dataLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	if len(data) < pos+int(dataLen) {
		return fmt.Errorf("truncated relay increment data")
	}
	sectionData := data[pos : pos+int(dataLen)]

	gs.mu.RLock()
	s, ok := gs.sections[sectionID]
	gs.mu.RUnlock()
	if !ok {
		return nil
	}

	if rs, ok := s.(RelayScopedSection); ok {
		return rs.MergeForRelay(sourceRelay, sectionData)
	}
	return s.Merge(sectionData)
}
